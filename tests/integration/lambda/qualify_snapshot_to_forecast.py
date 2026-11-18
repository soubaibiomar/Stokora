"""Exercise the actual batch importer, reconciliation and forecasting on gate fixture CDC."""

from datetime import date, timedelta
import hashlib
import json
from math import isclose
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from pyspark.sql import functions as F

from pipelines.batch.import_m5 import ingest
from pipelines.features.forecast import KEY, features
from pipelines.gold.publish import gold_view, rebuild
from pipelines.ml.baselines.forecast import metrics, predictions
from pipelines.silver.overlap_policy import APPROVALS
from pipelines.silver.reconcile import reconcile
from pipelines.storage.bronze import spark_session


def check_overlap():
    approval, = APPROVALS
    batch_id, dataset, tenant, site, product, event_id = approval.source
    batch = dict(dataset=dataset, tenant=tenant, site=site, product=product, event_id=event_id,
                 day=date(2024, 1, 6), quantity=2, simulated=True, source_ref="batch-ref")
    dataset, tenant, site, product, event_id = approval.target
    initial = dict(dataset=dataset, tenant=tenant, site=site, product=product, event_id=event_id,
                   day=date(2024, 1, 6), quantity=2, simulated=True, op="r")
    mapping = [{"batch_event_id": batch["event_id"], "cdc_key": [tenant, site, event_id],
                "approval_id": approval.id}]
    when = date(2026, 6, 1)
    assert [r["quantity"] for r in reconcile([batch], [initial], [initial], mapping,
                                             source_table_id=batch_id, as_of=when)] == [2]
    correction = dict(initial, op="u", quantity=4)
    result = reconcile([batch], [initial, correction], [correction], mapping, source_table_id=batch_id, as_of=when)
    assert len(result) == 1 and result[0]["quantity"] == 4 and result[0]["overlap_ref"] == "batch-ref"
    assert not reconcile([batch], [initial], [dict(correction, op="d")], mapping,
                         source_table_id=batch_id, as_of=when)
    try:
        reconcile([batch], [initial], [initial], mapping, source_table_id=batch_id, as_of=approval.valid_until)
    except ValueError as err:
        assert str(err) == "overlap_approval_denied"
    else:
        raise AssertionError("expired fixture mapping accepted")
    print("PASS authorized overlap counts once, update/delete precedence and expiry fail closed")


def main():
    if os.environ.get("STOKORA_GATE_FIXTURE") != "isolated":
        raise SystemExit("Run only in the isolated certification fixture")
    cdc = str(Path(os.environ["STOKORA_STREAM_ROOT"]) / "cdc_bronze")
    check_overlap()
    spark = spark_session()
    try:
        batch = Path("/batch/gate")
        canonical = Path("/gold/calendar-import")
        canonical.mkdir()
        oracle = Path("/gold/gate-oracle")
        oracle.mkdir()
        with TemporaryDirectory() as tmp:
            source = Path(tmp)
            for fixture in Path("/fixtures").glob("*.csv"):
                shutil.copyfile(fixture, source / fixture.name)
            calendar = source / "calendar.csv"
            initial = calendar.read_bytes()
            header, first, second, *rest = initial.splitlines()
            for version, raw in enumerate((initial, initial.rstrip()+b"\n2024-02-31,1,d_1\n",
                                          b"\n".join([header, second, first, *rest])+b"\n")):
                calendar.write_bytes(raw)
                assert ingest(spark, source, batch, synthetic=True, days=3)["status"] == "committed"
                path = str(batch / "m5_bronze")
                snapshot, written = rebuild(spark, canonical, path, cdc, version, 2)
                assert written and rebuild(spark, canonical, path, cdc, version, 2) == (snapshot, False)
                approved = gold_view(spark, canonical / "lambda_snapshot", "sales_daily", snapshot,
                                     principal="gate", grants={"gate": ("SIM_M5",)}, tenant="SIM_M5")
                assert approved.groupBy().sum("quantity").first()[0] == (6, 4, 6)[version]
                if version == 1:
                    q = spark.read.format("delta").load(str(canonical / "lambda_snapshot")).where(
                        (F.col("snapshot") == snapshot) & (F.col("kind") == "quarantine"))
                    observed = {r.reason: r["count"] for r in q.groupBy("reason").count().collect()}
                    assert observed["calendar_reference_conflict"] == 2 and observed["invalid_value"] == 1
                else:
                    assert {r.product for r in approved.collect()} == {"FIX_P", "FIX_Q"}
            print("PASS batch Bronze and Gold calendar 6 -> 4 -> 6, two batch products, idempotent rebuild")
        path = str(batch / "m5_bronze")
        original, _ = rebuild(spark, oracle, path, cdc, 2, 1)
        target = oracle / "lambda_snapshot"
        before = (target / "_delta_log" / "00000000000000000000.json").read_bytes()
        corrected, _ = rebuild(spark, oracle, path, cdc, 2, 2)
        assert original != corrected and rebuild(spark, oracle, path, cdc, 2, 2) == (corrected, False)
        assert hashlib.sha256(before).digest() == hashlib.sha256(
            (target / "_delta_log" / "00000000000000000000.json").read_bytes()).digest()

        def mart(name, snapshot=corrected, principal="A", tenant="SIM_A"):
            return gold_view(spark, target, name, snapshot, principal=principal,
                             grants={"A": ("SIM_A",), "B": ("SIM_B",)}, tenant=tenant)

        try:
            mart("sales_daily", principal="A", tenant="SIM_B")
        except PermissionError:
            pass
        else:
            raise AssertionError("cross-tenant access")

        old = {r.day: r.quantity for r in mart("sales_daily", original).collect()}
        assert old == {date(2024, 2, i): i for i in range(1, 29) if i != 22}
        daily = mart("sales_daily")
        expected = dict(old)
        expected[date(2024, 2, 4)] += 41
        expected[date(2024, 2, 21)] = 30
        expected[date(2024, 1, 31)] = expected.pop(date(2024, 2, 8))
        expected.pop(date(2024, 2, 19))
        assert {r.day: r.quantity for r in daily.collect()} == expected
        assert [(r.day, r.quantity) for r in mart("sales_daily", principal="B", tenant="SIM_B").collect()] == [
            (date(2024, 2, 21), 999)]
        weekly = {}
        for day, quantity in expected.items():
            monday = day - timedelta(days=day.weekday())
            weekly[monday] = weekly.get(monday, 0) + quantity
        assert {r.day: r.quantity for r in mart("sales_weekly").collect()} == weekly
        assert [r.quantity for r in mart("product_store_metrics").collect()] == [sum(expected.values())]
        stock = spark.read.format("delta").load(str(target)).where(
            (F.col("snapshot") == corrected) & (F.col("kind") == "stock_snapshot_simulated")).collect()
        assert stock and all(r.simulated is True and "five units/day" in r.detail for r in stock)
        assert all(isclose(float(r.coverage_days), r.quantity / 5) for r in stock)
        for row in stock:
            expected_status = ("simulated_stockout" if row.quantity <= 0 else
                               "simulated_overstock" if row.quantity > 100 else "within_simulated_threshold")
            assert row.op == expected_status
        quality = spark.read.format("delta").load(str(target)).where(
            (F.col("snapshot") == corrected) & (F.col("kind") == "quality")).first()
        assert json.loads(quality.detail)["gx_expectations_passed"] == 11
        print("PASS pinned versions, retained Delta history, GX, tenant isolation, late arrival, date move, daily/weekly/product totals and simulated stock")

        sales = daily.select(*KEY, "day", "quantity", "simulated", F.lit(None).cast("double").alias("price"))
        windows = tuple(date(2024, 2, i) for i in (2, 9, 22, 29))
        preds = predictions(features(sales, *windows).where("origin = '2024-02-22'"))
        assert preds.count() == 21
        for row in metrics(preds, []).collect():
            error = {"naive": 27, "seasonal_naive": 34, "moving_average": 37}[row.method]
            assert row.scored_count == 6 and row.actual_units == 153
            assert isclose(row.mae, error/6) and isclose(row.wape, error/153)
        assert preds.where("method='seasonal_naive' AND horizon=5 AND fallback='last_observed'").count() == 1
        print("PASS real snapshot/CDC to forecast: 21 issued, 6 scored per method, errors 27/34/37 and denominator 153")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
