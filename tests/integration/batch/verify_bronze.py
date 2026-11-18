"""Execute Delta integration checks and recorded synthetic volume benchmarks."""

import argparse
import csv
from datetime import date, timedelta
import json
from pathlib import Path
import shutil
import time
from uuid import uuid4

from pipelines.adapters import m5
from pipelines.batch.import_m5 import STORAGE, ingest
from pipelines.storage import bronze


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def table(spark, root, version=None):
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(bronze.table_path(root))


def expect_error(action, prefix):
    try:
        action()
    except ValueError as error:
        require(str(error).startswith(prefix), f"Expected {prefix}, got {error}")
    else:
        raise AssertionError(f"No error for {prefix}")


def sql_counts(spark, dataset):
    dataset.createOrReplaceTempView("bronze_m5")
    return {(row.source_table, row.record_type): row.n for row in spark.sql("""
        SELECT source_table, record_type, count(*) AS n
        FROM bronze_m5 GROUP BY source_table, record_type
    """).collect()}


def verify_required_inputs(spark, source, root):
    sales = source / "sales_train_validation.csv"
    original = sales.read_bytes()
    header = original.splitlines(keepends=True)[0]
    path = bronze.table_path(root)

    def committed_state():
        rows = table(spark, root).orderBy("source_table", "source_hash", "record_type", "source_line").collect()
        history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "userMetadata").collect()
        return rows, history

    expected_state = committed_state()
    cases = (
        ("R01", "header-only sales", sales, header, "NO_ACCEPTED_SALES"),
        ("R02", "zero-byte sales", sales, b"", "EMPTY_INPUT"),
        ("R03", "whitespace-only sales", sales, b" \t\r\n\t\n", "EMPTY_INPUT"),
        ("R04", "all-invalid sales", sales, header + b"broken,row\n", "NO_ACCEPTED_SALES"),
        ("R05", "whitespace-only calendar", source / "calendar.csv", b" \r\n", "EMPTY_INPUT"),
        ("R09", "invalid sales schema", sales, original.replace(b"d_3", b"bad_header", 1), "SCHEMA_ERROR"),
    )
    for identifier, label, file, content, code in cases:
        original_bytes = file.read_bytes()
        try:
            file.write_bytes(content)
            for _ in range(2):
                try:
                    ingest(spark, source, root, synthetic=True, days=3)
                except m5.InputError as error:
                    require((error.code, error.source_file) == (code, file.name),
                            f"{label} returned unexpected error {error.code}")
                else:
                    raise AssertionError(f"{label} was accepted")
            require(committed_state() == expected_state, f"{label} changed rows, load metadata or history")
            print(f"PASS {identifier} {label} rejected twice code={code} file={file.name}", flush=True)
        finally:
            file.write_bytes(original_bytes)

    # Simulate a pre-fix success marker so a historical attempt cannot bypass validation.
    legacy = root.parent / "legacy"
    sales.write_bytes(header)
    try:
        bad_hash = m5.file_hash(sales)
        marker = (table(spark, root).where("source_table='sales' AND record_type='load'")
                  .limit(1).first().asDict())
        marker.update(source_hash=bad_hash, source_bytes=len(header))
        bronze.append(spark, legacy, spark.sparkContext.parallelize([marker]), "pre-fix-marker")
        try:
            ingest(spark, source, legacy, synthetic=True, days=3)
        except m5.InputError as error:
            require(error.code == "NO_ACCEPTED_SALES", "legacy marker changed rejection")
        else:
            raise AssertionError("legacy marker bypassed validation")
        legacy_history = spark.sql(f"DESCRIBE HISTORY delta.`{bronze.table_path(legacy)}`").count()
        require(legacy_history == 1 and table(spark, legacy).count() == 1, "legacy state changed")
        print("PASS R06 legacy successful hash cannot bypass rejection; legacy version and marker unchanged")
    finally:
        sales.write_bytes(original)

    rows, history = expected_state
    versions = [entry.version for entry in history]
    load_count = sum(row.record_type == "load" for row in rows)
    print(f"PASS R07 rows and complete provenance preserved rows={len(rows)} loads={load_count} versions={versions}")
    require(ingest(spark, source, root, synthetic=True, days=3)["status"] == "already_loaded"
            and committed_state() == expected_state, "valid replay after rejections changed committed state")
    print("PASS R08 valid replay after rejections retained history and registry")


def run_checks(spark, workspace):
    source = workspace / "source"
    shutil.copytree(m5.FIXTURE, source)
    root = workspace / "table"

    first = ingest(spark, source, root, synthetic=True, days=3)
    require(first["files_committed"] == 3, "expected all fixture files")
    data = table(spark, root)
    data.printSchema()
    counts = sql_counts(spark, data)
    require(counts == {(name, kind): total for name in ("calendar", "prices", "sales")
                       for kind, total in (("load", 1), ("raw", {"calendar": 3, "prices": 3, "sales": 2}[name]))},
            f"unexpected counts {counts}")
    detail = spark.sql(f"DESCRIBE DETAIL delta.\x60{bronze.table_path(root)}\x60").first()
    require(detail.format == "delta" and not detail.partitionColumns and detail.numFiles <= 2
            and detail.properties.get("delta.appendOnly") == "true", "unexpected Delta physical layout")
    for row in data.where("record_type = 'load'").collect():
        file = source / row.source_file
        with m5.rows(file) as reader:
            require(row.source_columns == reader.fieldnames, "header lineage mismatch")
        require(row.source_hash == m5.file_hash(file) and row.source_bytes == file.stat().st_size
                and row.is_simulated and row.dataset == "m5_fixture"
                and row.ingestion_run_id == first["ingestion_run_id"], "manifest lineage mismatch")
        expected = file.read_text(encoding="utf-8-sig").splitlines()[1:]
        actual = [entry.raw for entry in data.where(data.source_file == file.name)
                  .where("record_type = 'raw'").orderBy("source_line").collect()]
        require(actual == expected, f"raw CSV mismatch for {file.name}")
    require(data.where("business_date = DATE '2024-01-06'").count() == 1, "calendar date not parsed")
    print(f"PASS T01 Delta CSV parity and provenance rows=8 register=3 files={detail.numFiles} bytes={detail.sizeInBytes}")

    history = spark.sql(f"DESCRIBE HISTORY delta.\x60{bronze.table_path(root)}\x60")
    require(history.count() == 1, "expected one commit")
    retry = ingest(spark, source, root, synthetic=True, days=3)
    require(retry["status"] == "already_loaded" and sql_counts(spark, table(spark, root)) == counts,
            "retry changed logical counts")
    require(spark.sql(f"DESCRIBE HISTORY delta.\x60{bronze.table_path(root)}\x60").count() == 1,
            "retry advanced version")
    print("PASS T02 same hashes unchanged SQL cardinality and Delta version")

    sales = source / "sales_train_validation.csv"
    with sales.open("a", encoding="utf-8") as stream:
        stream.write('broken,record\n')
    prices = source / "sell_prices.csv"
    with prices.open("a", encoding="utf-8") as stream:
        stream.write("FIX_A,FIX_P,2,-1\n")
    calendar = source / "calendar.csv"
    with calendar.open("a", encoding="utf-8") as stream:
        stream.write("2024-02-30,3,d_4\n")
    next_run = ingest(spark, source, root, synthetic=True, days=3)
    require(next_run["files_committed"] == 3, "changed sources were not versioned")
    current = table(spark, root)
    reasons = {row.reason for row in current.where("record_type = 'reject'").select("reason").collect()}
    require(reasons == {"column_count", "price", "invalid_value"}, "corrupt rows were not quarantined")
    require(current.where("record_type = 'raw'").count() == 16, "raw versions lost")
    print("PASS T03 corrupt line, invalid price, and invalid date quarantined")

    old = table(spark, root, 0)
    require(sql_counts(spark, old) == counts, "time travel did not reproduce original CSV snapshot")
    latest = spark.sql(f"DESCRIBE HISTORY delta.\x60{bronze.table_path(root)}\x60").select("version", "userMetadata").collect()
    require([row.version for row in latest] == [1, 0]
            and [row.userMetadata for row in latest] == [next_run["ingestion_run_id"], first["ingestion_run_id"]],
            "commit history missing run identifiers")
    latest_raw = current.where("record_type = 'raw' AND source_table = 'sales'").count()
    print(f"PASS T04 time travel version=0 and history=[1,0] latest_physical_sales={latest_raw}")

    changed = sales.read_bytes()
    sales.write_bytes(changed.replace(b"d_3", b"bad_header", 1))
    expect_error(lambda: ingest(spark, source, root, synthetic=True, days=3), "SCHEMA_ERROR")
    require(table(spark, root).count() == 25, "schema failure modified committed snapshot")
    print("PASS T05 invalid header aborts without publishing a new Delta version")

    disk = Path("/dev/shm") / workspace.name
    expect_error(lambda: ingest(spark, m5.FIXTURE, disk, synthetic=True, days=3), "INSUFFICIENT_DISK")
    require(not (disk / "m5_bronze").exists(), "insufficient space published Delta data")
    print(f"PASS T06 actual tmpfs available_bytes={shutil.disk_usage(disk).free} below minimum threshold")

    sales.write_bytes(changed)
    with prices.open("a", encoding="utf-8") as stream:
        stream.write("FIX_B,FIX_Q,1,2.10\n")
    result = ingest(spark, source, root, synthetic=True, days=3)
    require(result["files_committed"] == 1, "registry should only admit changed price hash")
    require(table(spark, root).where("record_type='load'").count() == 7, "registry cardinality mismatch")
    print("PASS T07 retry after header repair adds only changed price file")
    verify_required_inputs(spark, source, root)
    return root


def make_series(source, series):
    source.mkdir()
    days = 1913
    with (source / "calendar.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("date", "wm_yr_wk", "d"))
        writer.writerows(((date(2020, 1, 1) + timedelta(days=i)).isoformat(), i // 7, f"d_{i+1}")
                         for i in range(days))
    with (source / "sell_prices.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("store_id", "item_id", "wm_yr_wk", "sell_price"))
        writer.writerows((f"site_{i % 10}", f"product_{i}", week, "2.50")
                         for i in range(series) for week in range((days + 6) // 7))
    with (source / "sales_train_validation.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(m5.DIMENSIONS + [f"d_{day}" for day in range(1, days + 1)])
        for i in range(series):
            writer.writerow([f"series_{i}", f"product_{i}", "dept", "category", f"site_{i % 10}", "state"]
                            + [(i + day) % 6 for day in range(days)])


def cpu_usec():
    fields = dict(line.split() for line in Path("/sys/fs/cgroup/cpu.stat").read_text().splitlines())
    return int(fields["usage_usec"])


def run_benchmark(workspace, series):
    source = workspace / "source"
    make_series(source, series)
    input_bytes = sum(path.stat().st_size for path in source.glob("*.csv"))
    sales_hash = m5.file_hash(source / "sales_train_validation.csv")
    spark = None
    try:
        for phase in ("cold", "warm"):
            root = workspace / phase
            started, cpu = time.monotonic(), cpu_usec()
            if phase == "cold":
                spark = bronze.spark_session()
            ingest(spark, source, root, synthetic=True, days=1913)
            rows = table(spark, root).where("record_type='raw'").count()
            expected = 1913 + series * (((1913 + 6) // 7) + 1)
            require(rows == expected, f"benchmark cardinality {rows} != {expected}")
            elapsed = time.monotonic() - started
            peak = int(Path("/sys/fs/cgroup/memory.peak").read_text())
            print("BENCH", json.dumps({
                "series": series, "phase": phase, "rows": rows, "bytes": input_bytes,
                "wall_s": round(elapsed, 3), "cpu_s": round((cpu_usec() - cpu) / 1e6, 3),
                "cgroup_peak_bytes_since_container_start": peak,
                "spark": spark.version, "simulated": True, "sales_sha256": sales_hash,
            }, sort_keys=True), flush=True)
    finally:
        if spark is not None:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--series", type=int, choices=(500, 1000))
    args = parser.parse_args()
    workspace = STORAGE / f"step04-evidence-{uuid4().hex}"
    workspace.mkdir()
    if args.series:
        run_benchmark(workspace, args.series)
    else:
        spark = bronze.spark_session()
        try:
            print(f"SPARK {spark.version}")
            root = run_checks(spark, workspace)
            print(f"PASS integration volume={root}")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
