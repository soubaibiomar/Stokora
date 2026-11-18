"""Check published tables, logical convergence, authorization and synthetic overlap."""

from datetime import date, timedelta
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import time
from unittest.mock import patch

from pyspark.sql import functions as F

from pipelines.gold.publish import BRONZE, CDC, OUTPUT, SCHEMA, aggregate, gold_view, read_source, rebuild
from pipelines.quality.silver import validate
from pipelines.silver.overlap_policy import APPROVALS, AUTHORITY, authorize
from pipelines.silver.reconcile import batch_sales, cdc_sales, reconcile
from pipelines.storage.bronze import spark_session
from pipelines.streaming.merge_bronze import merge_batch
from pipelines.streaming.source import bind


def check(spark, root):
    path = root / "lambda_snapshot"
    table = spark.read.format("delta").load(str(path))
    snapshot = table.where("kind='quality'").first().snapshot
    rows = table.where("kind='sales_daily'").collect()
    assert {(r.tenant, r.site, r.product, str(r.day)): r.quantity for r in rows} == {
        ("SIM_M5", "FIX_A", "FIX_P", "2024-01-06"): 2,
        ("SIM_M5", "FIX_A", "FIX_P", "2024-01-07"): 0,
        ("SIM_M5", "FIX_A", "FIX_P", "2024-01-08"): 3,
        ("SIM_M5", "FIX_B", "FIX_Q", "2024-01-06"): 0,
        ("SIM_M5", "FIX_B", "FIX_Q", "2024-01-08"): 1,
        ("SIM_A", "SIM_SITE", "SIM_PRODUCT", "2024-01-01"): 6,
    }
    assert {r.tenant: r.quantity for r in table.where("kind='sales_weekly'").groupBy("tenant")
            .agg(F.sum("quantity").alias("quantity")).collect()} == {"SIM_M5": 6, "SIM_A": 6}
    assert table.where("kind='quarantine'").count() == 4
    assert {r.op for r in table.where("kind='silver_cdc'").select("op").distinct().collect()} == {"r", "c", "u", "d"}
    assert table.where("kind='silver_sales' AND op='d'").count() == 0
    assert table.where("kind='stock_snapshot_simulated' AND simulated AND coverage_days IS NOT NULL").count() == 4
    assert table.where("kind='forecast_features' AND site='FIX_B' AND day=DATE'2024-01-08'").first().lag_quantity is None
    print("PASS daily and weekly reference totals 6+6, CDC operations, quarantine, forecast and inventory", flush=True)

    grants = {"analyst-a": {"SIM_A"}, "analyst-history": {"SIM_M5"}}
    for role, tenant in (("analyst-a", "SIM_A"), ("analyst-history", "SIM_M5")):
        assert {r.tenant for r in gold_view(spark, path, "sales_daily", snapshot, principal=role,
                                          grants=grants, tenant=tenant).collect()} == {tenant}
    for principal, tenant in (("analyst-a", "SIM_M5"), ("anonymous", "SIM_A")):
        try:
            gold_view(spark, path, "sales_daily", snapshot, principal=principal, grants=grants, tenant=tenant)
        except PermissionError:
            pass
        else:
            raise AssertionError("unauthorized tenant")
    print("PASS tenant grants and row filters", flush=True)

    versions = sorted((path / "_delta_log").glob("*.json"))
    assert len(versions) == 1
    info = next(action["commitInfo"] for action in map(json.loads, versions[0].read_text().splitlines())
                if "commitInfo" in action)
    assert info["userMetadata"] == snapshot
    assert rebuild(spark, root, BRONZE, CDC, 0, 1) == (snapshot, False)
    assert sorted((path / "_delta_log").glob("*.json")) == versions
    print("PASS Delta commit identity and idempotent replay", snapshot, flush=True)
    return snapshot


def overlap_checks():
    approved = APPROVALS[0]
    fields = ("dataset", "tenant", "site", "product", "event_id")
    batch = dict(zip(fields, approved.source[1:]), quantity=2, day=approved.day, source_ref="file-hash:line", simulated=True)
    cdc = dict(zip(fields, approved.target), quantity=2, day=approved.day, op="c", simulated=True)
    mapping = [{"batch_event_id": batch["event_id"], "cdc_key": ["SIM_A", "SIM_SITE", "snapshot"],
                "approval_id": approved.id}]
    options = dict(source_table_id=approved.source[0], as_of=approved.valid_from)
    assert sum(r["quantity"] for r in reconcile([batch], [cdc], [cdc], mapping, **options)) == 2
    current = reconcile([batch], [cdc], [dict(cdc, quantity=5, op="u")], mapping, **options)
    assert [r["quantity"] for r in current] == [5] and current[0]["overlap_ref"] == batch["source_ref"]
    assert reconcile([batch], [cdc], [dict(cdc, quantity=0, op="d")], mapping, **options) == []
    for invalid in (mapping * 2, [dict(mapping[0], cdc_key=["SIM_B", "SIM_SITE", "snapshot"])]):
        try:
            reconcile([batch], [cdc], [cdc], invalid, **options)
        except ValueError as exc:
            assert str(exc) == "unverified_synthetic_overlap"
        else:
            raise AssertionError("ambiguous mapping was accepted")
    print("PASS explicit overlap insert/update/delete and ambiguous linkage denial", flush=True)
    for change, historical, initial, at, grant in (
        ({}, batch, cdc, approved.valid_until, AUTHORITY),
        ({"revoked": True}, batch, cdc, approved.valid_from, AUTHORITY),
        ({"approver": "untrusted"}, batch, cdc, approved.valid_from, AUTHORITY),
        ({"version": "wrong"}, batch, cdc, approved.valid_from, AUTHORITY),
        ({}, batch, dict(cdc, dataset="unknown"), approved.valid_from, AUTHORITY),
        ({}, batch, cdc, approved.valid_from, frozenset()),
    ):
        try:
            authorize(approved.id, approved.source[0], historical, initial, at,
                      approvals=(replace(approved, **change),), authority=grant)
        except ValueError as error:
            assert str(error) == "overlap_approval_denied"
        else:
            raise AssertionError("unexpected approval")
    same_tenant = dict(batch, tenant="SIM_A")
    policy = replace(approved, source=(approved.source[0], *(same_tenant[f] for f in fields)))
    def test_registry(approval_id, source_id, historical, initial, as_of):
        return authorize(approval_id, source_id, historical, initial, as_of, approvals=(policy,),
                         authority={(policy.approver, policy.operation, policy.source, policy.target)})
    with patch("pipelines.silver.reconcile.authorize", test_registry):
        same_scope = reconcile([same_tenant], [cdc], [cdc], mapping, **options)
    assert len(same_scope) == 1 and same_scope[0]["tenant"] == "SIM_A" and same_scope[0]["quantity"] == 2
    assert same_scope[0]["overlap_ref"] == same_tenant["source_ref"]
    try:
        reconcile([batch], [cdc], [dict(cdc, day=date(2024, 1, 7), op="u")], mapping, **options)
    except ValueError as error:
        assert str(error) == "overlap_approval_denied"
    else:
        raise AssertionError("out-of-scope correction accepted")
    print("PASS same-tenant approval, unknown source, expired, revoked, unauthorized authority and wrong version", flush=True)


def bronze_checks(spark):
    rows, _, properties = read_source(spark, CDC, 1)
    binding = json.loads(properties["stokora.kafkaBinding"])
    events, latest, rejects = cdc_sales(rows, 1, binding)
    repeated = next(row for row in rows if row.record_kind == "event")
    again, reversed_latest, reversed_rejects = cdc_sales([*reversed(rows), repeated], 1, binding)
    assert sorted(r["source_ref"] for r in events) == sorted(r["source_ref"] for r in again)
    assert sorted(r["source_ref"] for r in latest) == sorted(r["source_ref"] for r in reversed_latest)
    assert len(rejects) == len(reversed_rejects) == 3
    rows, _, _ = read_source(spark, BRONZE, 0)
    from pyspark.sql import Row
    edited = []
    for row in rows:
        if row.record_type == "raw" and row.source_table == "calendar" and "2024-01-06" in row.raw:
            row = Row(**dict(row.asDict(), raw=row.raw.replace("2024-01-06", "2024-02-31")))
        edited.append(row)
    facts, anomalies = batch_sales(edited, 0)
    assert all(r["day"] != date(2024, 1, 6) for r in facts)
    assert any(r["reason"] == "batch_reference_or_schema" for r in anomalies)
    assert sum(r["reason"] == "calendar_reference_invalid" for r in anomalies) == 2
    print("PASS reverse arrival, exact redelivery and invalid calendar date quarantine", flush=True)
    correction = []
    for row in rows:
        if row.source_table != "sales":
            continue
        fields = row.asDict()
        fields.update(source_hash="b" * 64, ingested_at=row.ingested_at + timedelta(seconds=1))
        if row.record_type == "raw" and "FIX_P" in row.raw:
            cells = row.raw.split(",")
            cells[6] = "4"
            fields["raw"] = ",".join(cells)
        correction.append(Row(**fields))
    original, _ = batch_sales(rows, 0)
    corrected, _ = batch_sales([*rows, *correction], 1)
    assert sum(r["quantity"] for r in original) == 6
    assert sum(r["quantity"] for r in corrected) == 8
    assert len(original) == len(corrected) and all(r["bronze_version"] == 1 for r in corrected)
    print("PASS corrected batch manifest replaces historical cells: six to eight units, same grain", flush=True)


def benchmark(spark):
    for size in (250, 1000):
        rows = [
            dict(dataset="benchmark", tenant="SIM_A", site="SIM_SITE", product=f"P_{i}",
                 day=date(2024, 1, 6), quantity=2, event_id=f"e_{i}", currency="MAD", simulated=True)
            for i in range(size)
        ]
        start = time.monotonic()
        rules = validate(rows)
        facts = spark.createDataFrame(rows, SCHEMA)
        daily = aggregate(facts, facts.limit(0))["sales_daily"]
        result = daily.agg(F.count("*").alias("count"), F.sum("quantity").alias("total")).first()
        assert (result["count"], result["total"]) == (size, size * 2)
        peak = Path("/sys/fs/cgroup/memory.peak")
        print("PASS bench", size, "daily", result["count"], "units", result["total"], "gx", rules,
              "seconds", round(time.monotonic() - start, 3), "peak_bytes", int(peak.read_text()), flush=True)


def publish_overlap(spark):
    rows, _, properties = read_source(spark, CDC, 1)
    source = tuple(json.loads(properties["stokora.kafkaBinding"]))
    original = next(r for r in rows if r.record_kind == "event" and r.operation == "r")
    envelope = json.loads(original.raw_value)
    envelope["after"]["business_date"] = (date(2024, 1, 6) - date(1970, 1, 1)).days
    frame = spark.createDataFrame([
        (source[2], 0, 0, original.kafka_timestamp, original.raw_key, json.dumps(envelope).encode()),
    ], "topic STRING, partition INT, offset LONG, timestamp TIMESTAMP, key BINARY, value BINARY")
    root = OUTPUT / "synthetic-overlap"
    root.mkdir(exist_ok=True)
    source_path = root / "cdc_bronze"
    bind(spark, source_path, root / "checkpoint", source)
    assert merge_batch(spark, frame, source_path, "step06-overlap", 0, source, lambda: None)
    mapping = [{"batch_event_id": "FIX_A:FIX_P:2024-01-06", "approval_id": "fixture-jan06",
                "cdc_key": [original.tenant_id, original.site_id, original.business_event_id]}]
    snapshot, written = rebuild(spark, root, BRONZE, str(source_path), 0, 1, mapping)
    assert written
    output = spark.read.format("delta").load(str(root / "lambda_snapshot"))
    assert {(r.tenant, r.site, r.product, str(r.day)): r.quantity
            for r in output.where("kind='sales_daily'").collect()} == {
        ("SIM_A", "SIM_SITE", "SIM_PRODUCT", "2024-01-06"): 2,
        ("SIM_M5", "FIX_A", "FIX_P", "2024-01-07"): 0,
        ("SIM_M5", "FIX_A", "FIX_P", "2024-01-08"): 3,
        ("SIM_M5", "FIX_B", "FIX_Q", "2024-01-06"): 0,
        ("SIM_M5", "FIX_B", "FIX_Q", "2024-01-08"): 1,
    }
    assert output.schema["day"].dataType.simpleString() == "date"
    assert output.schema["quantity"].dataType.simpleString() == "bigint"
    linked = output.where("kind='silver_sales' AND overlap_ref IS NOT NULL").first()
    assert linked.quantity == 2 and linked.tenant == "SIM_A" and linked.overlap_ref.startswith("sales_train_validation.csv:")
    assert rebuild(spark, root, BRONZE, str(source_path), 0, 1, mapping) == (snapshot, False)
    print("PASS end-to-end synthetic overlap: Gold totals 6 units; matched Silver row has both Bronze references",
          flush=True)


spark = spark_session()
try:
    stage = sys.argv[1]
    recovery = OUTPUT / "recovery"
    if stage == "crash":
        recovery.mkdir(exist_ok=True)
        rebuild(spark, recovery, BRONZE, CDC, 0, 1, after_commit=lambda: os._exit(76))
        raise AssertionError("crash hook did not execute")
    if stage == "recover":
        assert check(spark, recovery) == check(spark, OUTPUT)
        keys = ["kind", "tenant", "site", "product", "day", "quantity", "event_id", "reason"]
        original = spark.read.format("delta").load(str(OUTPUT / "lambda_snapshot")).select(*keys)
        rerun = spark.read.format("delta").load(str(recovery / "lambda_snapshot")).select(*keys)
        assert original.exceptAll(rerun).isEmpty() and rerun.exceptAll(original).isEmpty()
        print("PASS recovery from separate process and equivalent full rebuild", flush=True)
    if stage == "verify":
        check(spark, OUTPUT)
        overlap_checks()
        bronze_checks(spark)
        benchmark(spark)
    if stage == "overlap":
        try:
            validate([dict(tenant="SIM_A", site="SIM_SITE", dataset="negative-test", event_id="invalid",
                           product="SIM_PRODUCT", day=date(2024, 1, 6), quantity=-1, currency="MAD")])
        except ValueError as error:
            assert str(error) == "silver_quality_gate:ExpectColumnValuesToBeBetween"
            print("PASS Great Expectations rejects negative eligible quantity", flush=True)
        else:
            raise AssertionError("negative quantity passed GX")
        publish_overlap(spark)
    if stage == "contract":
        bronze_checks(spark)
    if stage == "policy":
        overlap_checks()
finally:
    spark.stop()
