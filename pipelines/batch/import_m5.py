"""Import source-shaped M5 rows into append-only Delta Bronze."""

import argparse
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import re
import tempfile
from uuid import uuid4

from pipelines.adapters import m5
from pipelines.storage import bronze

STORAGE = Path("/home/jovyan/work")
SOURCE = Path("/inputs/m5")
TABLES = ("calendar", "prices", "sales")
ORIGINS = {"m5": m5.RULES, "m5_fixture": "project-authored synthetic fixture"}


def header_indices(table, columns, days, source_file):
    required = {
        "calendar": {"d", "date", "wm_yr_wk"},
        "prices": {"store_id", "item_id", "wm_yr_wk", "sell_price"},
        "sales": set(m5.DIMENSIONS),
    }
    if not required[table].issubset(columns):
        raise m5.InputError(f"SCHEMA_ERROR table={table} required columns missing",
                            code="SCHEMA_ERROR", source_file=source_file)
    if table == "sales" and columns != m5.DIMENSIONS + [f"d_{i}" for i in range(1, days + 1)]:
        raise m5.InputError("SCHEMA_ERROR table=sales unexpected daily columns",
                            code="SCHEMA_ERROR", source_file=source_file)
    return {name: position for position, name in enumerate(columns)}


def classify(record, table, indices):
    try:
        values = next(csv.reader([record], strict=True))
    except (csv.Error, StopIteration):
        return "csv_syntax", None, None, None
    if len(values) != len(indices):
        return "column_count", None, None, None

    def value(name):
        return values[indices[name]]

    try:
        if table == "calendar":
            day = date.fromisoformat(value("date"))
            if day.isoformat() != value("date") or not re.fullmatch(r"d_[1-9][0-9]{0,3}", value("d")):
                raise ValueError
            m5.number(value("wm_yr_wk"))
            return None, None, None, day
        item, site = m5.identifier(value("item_id")), m5.identifier(value("store_id"))
        if table == "prices":
            m5.number(value("wm_yr_wk"))
            price = value("sell_price")
            if price and (not m5.PRICE.fullmatch(price) or Decimal(price) <= 0):
                return "price", item, site, None
        elif any(v and not re.fullmatch(r"[0-9]{1,9}", v) for v in values[len(m5.DIMENSIONS):]):
            return "quantity", item, site, None
        return None, item, site, None
    except ValueError:
        return "invalid_value", None, None, None


def classified_rows(spark, file, table, indices):
    contents = spark.sparkContext.textFile(file.as_uri(), minPartitions=2).zipWithIndex()
    return (contents.filter(lambda line: line[1] > 0)
            .map(lambda line: (line[0], line[1], classify(line[0], table, indices))))


def build_records(spark, file, table, meta, classified, provenance):
    prefix = (provenance["dataset"], table, file.name, meta["sha256"], 1,
              provenance["simulated"], provenance["origin"], provenance["retrieved_on"],
              provenance["run_id"], provenance["ingested_at"])

    def row(line):
        payload, position, (reason, item, site, business_date) = line
        kind, status = ("reject", "quarantined") if reason else ("raw", "accepted")
        return prefix + (kind, status, position + 1, payload, reason, item, site,
                         business_date, None, None)

    load = prefix + ("load", "committed", None, None, None, None, None, None,
                     meta["columns"], meta["bytes"])
    return classified.map(row).union(spark.sparkContext.parallelize([load], 1))


def snapshot(source, destination):
    stamp = source.stat()
    size = 0
    has_content = False
    with source.open("rb") as src, destination.open("xb") as dst:
        while line := src.readline(m5.LINE_LIMIT + 3):
            if len(line.rstrip(b"\r\n")) > m5.LINE_LIMIT:
                raise m5.InputError(f"CSV_LINE_LIMIT file={source.name}")
            size += len(line)
            if size > stamp.st_size:
                raise m5.InputError(f"SOURCE_CHANGED file={source.name}")
            try:
                content = line.decode("utf-8")
            except UnicodeError:
                raise m5.InputError(f"INVALID_UTF8 file={source.name}") from None
            has_content = has_content or bool(content.removeprefix("\ufeff").strip())
            dst.write(line)
    if size != stamp.st_size or source.stat().st_mtime_ns != stamp.st_mtime_ns:
        raise m5.InputError(f"SOURCE_CHANGED file={source.name}")
    if not has_content:
        raise m5.InputError("Required CSV is empty or whitespace-only.",
                            code="EMPTY_INPUT", source_file=source.name)


def ingest(spark, source, root, *, synthetic, days, evaluation=False, retrieved_on=None):
    if synthetic:
        dataset = "m5_fixture"
    else:
        m5.require_real_m5_clearance()
        dataset = "m5"
        if retrieved_on is None:
            raise m5.InputError("Provide --retrieved-on for authorized local M5 files.")
    sales_name = "sales_train_evaluation.csv" if evaluation else "sales_train_validation.csv"
    files = m5.files_for(source, sales_name)
    root.mkdir(parents=True, exist_ok=True)
    with bronze.writer_lock(root):
        bronze.check_space(root, sum(file.stat().st_size for file in files))
        with tempfile.TemporaryDirectory(prefix=".m5-input-", dir=root) as temporary:
            copies = [Path(temporary) / file.name for file in files]
            for src, dst in zip(files, copies, strict=True):
                snapshot(src, dst)
            metadata = m5.inspect(copies)
            inputs = [
                (file, table, meta, header_indices(table, meta["columns"], days, file.name))
                for table, file, meta in zip(TABLES, copies, metadata, strict=True)
            ]
            sales_file, sales_table, _, sales_indices = inputs[-1]
            sales = classified_rows(spark, sales_file, sales_table, sales_indices)
            if not sales.filter(lambda row: row[2][0] is None).take(1):
                raise m5.InputError("Required sales CSV contains no accepted data rows.",
                                    code="NO_ACCEPTED_SALES", source_file=sales_file.name)
            known = bronze.already_loaded(spark, root, dataset)
            selected = [item for item in inputs if (item[1], item[2]["sha256"]) not in known]
            if not selected:
                return {"status": "already_loaded", "dataset": dataset, "files_committed": 0}
            run_id = uuid4().hex
            provenance = dict(dataset=dataset, origin=ORIGINS[dataset], simulated=synthetic,
                              retrieved_on=retrieved_on, run_id=run_id, ingested_at=datetime.now(timezone.utc))
            streams = [
                build_records(spark, file, table, meta,
                              sales if table == sales_table else classified_rows(spark, file, table, indices),
                              provenance)
                for file, table, meta, indices in selected
            ]
            bronze.append(spark, root, spark.sparkContext.union(streams), run_id)
            return {"status": "committed", "dataset": dataset, "files_committed": len(selected),
                    "ingestion_run_id": run_id}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--fixture", action="store_true")
    modes.add_argument("--licensed", action="store_true", help="use authorized CSV files mounted at /inputs/m5")
    parser.add_argument("--evaluation", action="store_true")
    parser.add_argument("--retrieved-on", type=date.fromisoformat)
    parser.add_argument("--target")
    args = parser.parse_args()
    target = args.target or ("m5-fixture" if args.fixture else "m5-licensed")
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", target):
        parser.error("target must be a local name under the Bronze volume")
    if args.fixture and (args.evaluation or args.retrieved_on):
        parser.error("evaluation and acquisition date apply only to licensed M5")
    if args.licensed and args.retrieved_on is None:
        parser.error("licensed M5 requires --retrieved-on")
    source = m5.FIXTURE if args.fixture else SOURCE
    try:
        if args.licensed:
            m5.require_real_m5_clearance()
        m5.files_for(source, "sales_train_evaluation.csv" if args.evaluation else "sales_train_validation.csv")
        root = (STORAGE / target).resolve()
        if root.parent != STORAGE.resolve():
            raise m5.InputError("target escapes the Bronze volume")
        spark = bronze.spark_session()
        try:
            result = ingest(spark, source, root, synthetic=args.fixture,
                            days=3 if args.fixture else (1941 if args.evaluation else 1913),
                            evaluation=args.evaluation, retrieved_on=args.retrieved_on)
        finally:
            spark.stop()
    except m5.InputError as error:
        parser.exit(2, json.dumps({
            "status": "rejected", "code": error.code, "source_file": error.source_file,
            "message": str(error),
        }, sort_keys=True) + "\n")
    except (OSError, ValueError) as error:
        parser.exit(2, f"IMPORT_FAILED {error if isinstance(error, (m5.InputError, ValueError)) else type(error).__name__}\n")
    except Exception as error:
        parser.exit(2, f"IMPORT_FAILED engine_error={type(error).__name__}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
