"""Reject the two original defects and compare versioned Gold with a literal oracle."""

from collections import Counter
from datetime import date, timedelta
import json
from pathlib import Path
import time

from pyspark.sql import Row, functions as F

from pipelines.gold.publish import BRONZE, CDC, OUTPUT, gold_view, read_source, rebuild
from pipelines.silver.reconcile import batch_sales
from pipelines.storage.bronze import spark_session
from pipelines.streaming.merge_bronze import merge_batch
from pipelines.streaming.source import bind


def commits(path):
    return {f.name: f.read_bytes() for f in (path / "_delta_log").glob("*.json")}


def deny(call, error_message):
    try:
        call()
    except (ValueError, PermissionError) as error:
        assert str(error) == error_message, str(error)
    else:
        raise AssertionError(f"Expected {error_message}")


def calendar(spark):
    root = OUTPUT / "calendar-remediation"
    root.mkdir(exist_ok=True)
    bronze = str(root / "batch_bronze")
    table = spark.read.format("delta").option("versionAsOf", 0).load(BRONZE)
    rows = table.collect()
    january = next(r for r in rows if r.source_table == "calendar" and r.raw and "2024-01-06" in r.raw)
    conflict = Row(**dict(january.asDict(), source_line=1, raw=january.raw.replace("2024-01-06", "2024-01-05")))
    duplicate = Row(**dict(january.asDict(), source_line=99))
    original, _ = batch_sales(rows, 0)
    identical, reasons = batch_sales([duplicate, *rows], 0)
    assert len(original) == len(identical) == 5 and not any(r["reason"] == "calendar_reference_conflict" for r in reasons)
    for sequence in ([conflict, *rows], [*reversed(rows), conflict]):
        accepted, quarantine = batch_sales(sequence, 1)
        assert all(r["day"] not in {date(2024, 1, 5), date(2024, 1, 6)} for r in accepted)
        rejected = [r for r in quarantine if r["reason"] == "calendar_reference_conflict"]
        assert {r["site"] for r in rejected} == {"FIX_A", "FIX_B"} and len(rejected) == 2
        assert all(r.get("day") is None and r["source_ref"] and
                   {candidate[0] for candidate in json.loads(r["detail"])["calendar"]} ==
                   {"2024-01-05", "2024-01-06"} for r in rejected)
    without_january = [r for r in rows if r != january]
    _, missing = batch_sales(without_january, 0)
    assert sum(r["reason"] == "calendar_reference_missing" for r in missing) == 2
    for foreign in (dict(dataset="another_tenant_namespace"), dict(schema_version=2), dict(origin="foreign_tenant")):
        _, scoped = batch_sales(without_january + [Row(**dict(january.asDict(), **foreign))], 0)
        assert sum(r["reason"] == "calendar_reference_missing" for r in scoped) == 2
        assert sum(r["reason"] == "batch_reference_scope" for r in scoped) == 1
    print("PASS unique/identical/missing/foreign calendar references; both ambiguous sales quarantined in either order",
          flush=True)

    table.write.format("delta").save(bronze)
    first, _ = rebuild(spark, root, bronze, CDC, 0, 0)
    spark.createDataFrame([conflict.asDict()], table.schema).write.format("delta").mode("append").save(bronze)
    defective, written = rebuild(spark, root, bronze, CDC, 1, 0)
    assert written
    revised = [dict(r.asDict(), source_hash="c" * 64, ingested_at=r.ingested_at + timedelta(seconds=1))
               for r in rows if r.source_table == "calendar"]
    spark.createDataFrame(revised, table.schema).write.format("delta").mode("append").save(bronze)
    corrected, written = rebuild(spark, root, bronze, CDC, 2, 0)
    assert written
    path = root / "lambda_snapshot"
    log = commits(path)
    assert len(log) == 3 and rebuild(spark, root, bronze, CDC, 2, 0) == (corrected, False)
    assert commits(path) == log
    oracle = {
        ("FIX_A", "FIX_P", "2024-01-06"): 2, ("FIX_A", "FIX_P", "2024-01-07"): 0,
        ("FIX_A", "FIX_P", "2024-01-08"): 3, ("FIX_B", "FIX_Q", "2024-01-06"): 0,
        ("FIX_B", "FIX_Q", "2024-01-08"): 1,
    }
    for version, snapshot, expected, rejected in ((0, first, oracle, 1), (1, defective, {
        k: n for k, n in oracle.items() if k[2] != "2024-01-06"
    }, 3), (2, corrected, oracle, 1)):
        out = spark.read.format("delta").option("versionAsOf", version).load(str(path))
        selected = [r for r in out.collect() if r.snapshot == snapshot]
        actual = {(r.site, r.product, str(r.day)): r.quantity for r in selected if r.kind == "sales_daily"}
        quarantine = Counter(r.reason for r in selected if r.kind == "quarantine")
        audit = [json.loads(r.detail) for r in selected if r.kind == "quality"]
        assert actual == expected and sum(quarantine.values()) == rejected
        assert len(audit) == 1 and audit[0]["quarantine"] == rejected
        assert all(r.tenant == "SIM_M5" for r in selected if r.kind == "sales_daily")
        print("PASS calendar version", version, "expected and actual units", sum(expected.values()),
              "quarantine", dict(quarantine), "snapshot", snapshot, flush=True)


def tenant_rejection(spark):
    rows, _, properties = read_source(spark, CDC, 1)
    source = tuple(json.loads(properties["stokora.kafkaBinding"]))
    original = next(r for r in rows if r.record_kind == "event" and r.operation == "r")
    body = json.loads(original.raw_value)
    body["after"]["tenant_id"] = "SIM_B"
    body["after"]["business_date"] = (date(2024, 1, 6) - date(1970, 1, 1)).days
    key = dict(json.loads(original.raw_key), tenant_id="SIM_B")
    frame = spark.createDataFrame([(source[2], 0, 100, original.kafka_timestamp, json.dumps(key).encode(),
                                     json.dumps(body).encode())],
                                  "topic STRING, partition INT, offset LONG, timestamp TIMESTAMP, key BINARY, value BINARY")
    fixture = OUTPUT / "tenant-remediation"
    fixture.mkdir(exist_ok=True)
    ledger = fixture / "cdc_bronze"
    bind(spark, ledger, fixture / "checkpoint", source)
    assert merge_batch(spark, frame, ledger, "independent-synthetic", 0, source, lambda: None)
    new_snapshot, wrote = rebuild(spark, fixture, BRONZE, str(ledger), 0, 1)
    assert wrote
    snapshot_path = fixture / "lambda_snapshot"
    baseline = commits(snapshot_path)
    link = {"batch_event_id": "FIX_A:FIX_P:2024-01-06", "cdc_key": ["SIM_B", "SIM_SITE", "snapshot"]}
    for request, reason in (
        (link, "unverified_synthetic_overlap"),
        (dict(link, approval_id="fixture-jan06"), "overlap_approval_denied"),
        (dict(link, approval_id="forged"), "overlap_approval_denied"),
        (dict(link, approval_id="forged", approved=True), "unverified_synthetic_overlap"),
    ):
        deny(lambda: rebuild(spark, fixture, BRONZE, str(ledger), 0, 1, [request]), reason)
        assert commits(snapshot_path) == baseline
    scope = {"history": {"SIM_M5"}, "analyst-b": {"SIM_B"}}
    expected = {"SIM_M5": (6, 5), "SIM_B": (2, 1)}
    for principal, tenant in (("history", "SIM_M5"), ("analyst-b", "SIM_B")):
        published = gold_view(spark, snapshot_path, "sales_daily", new_snapshot, principal=principal, grants=scope,
                              tenant=tenant).collect()
        assert {r.tenant for r in published} == {tenant}
        assert (sum(r.quantity for r in published), len(published)) == expected[tenant]
    deny(lambda: gold_view(spark, snapshot_path, "sales_daily", new_snapshot, principal="analyst-b", grants=scope,
                           tenant="SIM_M5"), "gold_scope_denied")
    assert spark.read.format("delta").load(str(snapshot_path)).where(
        (F.col("tenant") == "SIM_B") & F.col("overlap_ref").isNotNull()).isEmpty()
    print("PASS SIM_M5 -> SIM_B rejected before commit; identical files, distinct 6/2 totals and no lineage leak",
          flush=True)

    published = OUTPUT / "synthetic-overlap/lambda_snapshot"
    table = spark.read.format("delta").load(str(published))
    audit_row = table.where("kind='quality'").first()
    proper_scope = {"analyst": {"SIM_A"}}
    approved = gold_view(spark, published, "sales_daily", audit_row.snapshot, principal="analyst", grants=proper_scope,
                         tenant="SIM_A").collect()
    assert len(approved) == 1 and approved[0].quantity == 2
    legacy = str(fixture / "legacy-mapped")
    audit = json.loads(audit_row.detail)
    audit.pop("policy_sha256")
    table.withColumn("detail", F.when(F.col("kind") == "quality", F.lit(json.dumps(audit)))
                     .otherwise(F.col("detail"))).write.format("delta").save(legacy)
    deny(lambda: gold_view(spark, legacy, "sales_daily", audit_row.snapshot, principal="analyst", grants=proper_scope,
                           tenant="SIM_A"), "gold_mapping_policy_denied")
    deny(lambda: read_source(spark, "/batch/`invalid", 0), "invalid_delta_path")
    print("PASS approved scoped query, legacy unverified mapping blocked, unsafe path rejected", flush=True)


spark = spark_session()
try:
    started = time.monotonic()
    calendar(spark)
    tenant_rejection(spark)
    print("PASS remediation seconds", round(time.monotonic() - started, 3),
          "peak_bytes", Path("/sys/fs/cgroup/memory.peak").read_text().strip(), flush=True)
finally:
    spark.stop()
