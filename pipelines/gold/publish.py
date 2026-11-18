"""Publish typed Silver and Gold views from pinned Bronze into one Delta snapshot."""

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import time

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.generators.transactions import generate
from pipelines.quality.silver import validate
from pipelines.silver.overlap_policy import APPROVALS, VERSION, fingerprint
from pipelines.silver.reconcile import batch_sales, cdc_sales, reconcile
from pipelines.storage.bronze import spark_session, writer_lock

SPEC = "step06-fixture-v3"
BRONZE = "/batch/m5-fixture/m5_bronze"
CDC = "/stream/cdc_bronze"
OUTPUT = Path("/home/jovyan/work")
SCHEMA = """kind STRING, dataset STRING, tenant STRING, site STRING, product STRING, day DATE,
    event_id STRING, op STRING, quantity LONG, currency STRING, price DECIMAL(22,6),
    source_ref STRING, overlap_ref STRING, detail STRING, source_ms LONG, bronze_version LONG,
    refreshed_source TIMESTAMP, reason STRING, simulated BOOLEAN"""
GRAIN = ["dataset", "tenant", "site", "product", "simulated"]
MARTS = {"sales_daily", "sales_weekly", "forecast_features", "product_store_metrics",
         "products", "stores", "stock_snapshot_simulated", "stock_movement"}


def read_source(spark, path, version):
    if any(char in str(path) for char in ("`", "\r", "\n")):
        raise ValueError("invalid_delta_path")
    table = spark.read.format("delta").option("versionAsOf", version).load(path)
    # shortcut: bounded fixture snapshots, use distributed normalization before licensed M5.
    limit = 10000
    rows = table.take(limit + 1)
    if len(rows) > limit:
        raise ValueError("fixture_bronze_row_limit")
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    return rows, detail.id, detail.properties


def inventory():
    fixture = generate(42)
    for event in fixture["events"]:
        if event["type"] != "stock_movement":
            continue
        payload = event["payload"]
        yield dict(kind="stock_movement", dataset="simulator:sim-v1-42", tenant=event["tenant_id"],
                   site=event["site_id"], product=payload["product_id"], simulated=True,
                   op=payload["reason"], day=date.fromisoformat(event["origin_timestamp"][:10]),
                   quantity=payload["quantity_signed"], source_ref=event["event_id"],
                   event_id=event["event_id"])


def aggregate(facts, stock):
    daily = facts.groupBy(*GRAIN, "day").agg(
        F.sum("quantity").alias("quantity"), F.max("refreshed_source").alias("refreshed_source"),
    )
    weekly = daily.withColumn("day", F.date_trunc("week", "day").cast("date")).groupBy(*GRAIN, "day").agg(
        F.sum("quantity").alias("quantity"), F.max("refreshed_source").alias("refreshed_source"),
    )
    series = Window.partitionBy(*GRAIN).orderBy("day")
    features = daily.withColumn("lag_quantity", F.when(F.datediff("day", F.lag("day").over(series)) == 1,
                                                      F.lag("quantity").over(series)))
    metrics = daily.groupBy(*GRAIN).agg(F.sum("quantity").alias("quantity"),
                                       F.max("refreshed_source").alias("refreshed_source"))
    stock_daily = stock.groupBy(*GRAIN, "day").agg(F.sum("quantity").alias("quantity"))
    stock_daily = (
        stock_daily.withColumn("coverage_days", F.col("quantity") / F.lit(5))
        .withColumn("op", F.when(F.col("quantity") <= 0, "simulated_stockout")
                    .when(F.col("coverage_days") > 20, "simulated_overstock")
                    .otherwise("within_simulated_threshold"))
        .withColumn("detail", F.lit("scenario assumption: five units/day; overstock above 20 days"))
    )
    return {
        "sales_daily": daily, "sales_weekly": weekly, "forecast_features": features,
        "product_store_metrics": metrics,
        "products": daily.select("dataset", "tenant", "product", "simulated").distinct(),
        "stores": daily.select("dataset", "tenant", "site", "simulated").distinct(),
        "stock_snapshot_simulated": stock_daily, "stock_movement": stock.drop("kind"),
    }


def rebuild(spark, root, batch_path, stream_path, batch_version, cdc_version, mapping=(), *, after_commit=None):
    output = root / "lambda_snapshot"
    with writer_lock(root):
        now = datetime.now(timezone.utc)
        batch_rows, batch_id, _ = read_source(spark, batch_path, batch_version)
        cdc_rows, stream_id, properties = read_source(spark, stream_path, cdc_version)
        batch, batch_rejects = batch_sales(batch_rows, batch_version)
        events, latest, cdc_rejects = cdc_sales(cdc_rows, cdc_version, json.loads(properties["stokora.kafkaBinding"]))
        sales = reconcile(batch, events, latest, mapping, source_table_id=batch_id, as_of=now.date())
        policy_digest = fingerprint()
        identity = json.dumps([SPEC, batch_id, batch_version, stream_id, cdc_version, mapping, policy_digest],
                              sort_keys=True, separators=(",", ":"))
        snapshot = hashlib.sha256(identity.encode()).hexdigest()
        if (output / "_delta_log").exists():
            prior = spark.read.format("delta").load(str(output))
            if not prior.where((F.col("snapshot") == snapshot) & (F.col("kind") == "quality")).isEmpty():
                return snapshot, False

        quality = validate(sales)
        frame = spark.createDataFrame([dict(r, kind="silver_sales") for r in sales], SCHEMA)
        stock = spark.createDataFrame(list(inventory()), SCHEMA)
        rows = [dict(r, kind="silver_batch") for r in batch] + [dict(r, kind="silver_cdc") for r in events]
        rows += [dict(r, kind="quarantine") for r in batch_rejects + cdc_rejects]
        rows.append(dict(kind="quality", detail=json.dumps({
            "gx_expectations_passed": quality, "batch": len(batch), "cdc_mutations": len(events),
            "quarantine": len(batch_rejects) + len(cdc_rejects), "accepted_sales": len(sales),
            "inputs": json.loads(identity),
            "policy_version": VERSION, "policy_sha256": policy_digest,
        })))
        result = spark.createDataFrame(rows, SCHEMA).unionByName(frame)
        for name, table in aggregate(frame, stock).items():
            result = result.unionByName(table.withColumn("kind", F.lit(name)), allowMissingColumns=True)
        (result.withColumn("snapshot", F.lit(snapshot)).withColumn("published_at", F.lit(now))
         .withColumn("schema_version", F.lit(1)).withColumn("unit", F.lit("each"))
         .coalesce(2).write.format("delta").mode("append").option("userMetadata", snapshot).save(str(output)))
        if after_commit:
            after_commit()
        return snapshot, True


def gold_view(spark, output, mart, snapshot, *, principal, grants, tenant):
    """The trusted host supplies authenticated identity and grants; Spark enforces the row scope."""
    if mart not in MARTS or tenant not in grants.get(principal, ()):
        raise PermissionError("gold_scope_denied")
    table = spark.read.format("delta").load(str(output)).where(F.col("snapshot") == snapshot)
    quality = table.where(F.col("kind") == "quality").select("detail").first()
    if quality is not None:
        audit = json.loads(quality.detail)
        links = audit["inputs"][5]
        if links:
            today = datetime.now(timezone.utc).date()
            active = {a.id for a in APPROVALS if not a.revoked and a.valid_from <= today < a.valid_until}
            if (audit.get("policy_sha256") != fingerprint()
                    or any(link.get("approval_id") not in active for link in links)):
                raise PermissionError("gold_mapping_policy_denied")
    return table.where((F.col("tenant") == tenant) & (F.col("kind") == mart))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-version", type=int, required=True)
    parser.add_argument("--cdc-version", type=int, required=True)
    parser.add_argument("--mapping", type=Path)
    args = parser.parse_args()
    mapping = json.loads(args.mapping.read_text()) if args.mapping else []
    started = time.monotonic()
    spark = spark_session()
    try:
        snapshot, written = rebuild(spark, OUTPUT, BRONZE, CDC, args.batch_version, args.cdc_version, mapping)
        peak = Path("/sys/fs/cgroup/memory.peak")
        print(json.dumps({"snapshot": snapshot, "written": written, "elapsed_s": time.monotonic() - started,
                          "peak_bytes": int(peak.read_text()) if peak.exists() else None}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
