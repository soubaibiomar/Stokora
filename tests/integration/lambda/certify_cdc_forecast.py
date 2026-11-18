"""Independent oracle for the certification stack's synthetic PostgreSQL input."""

from datetime import date
import json
from math import isclose
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import functions as F

from pipelines.features.forecast import KEY, features
from pipelines.gold.publish import CDC, gold_view, read_source, rebuild
from pipelines.ml.baselines.forecast import metrics, predictions
from pipelines.storage.bronze import spark_session
from pipelines.streaming.source import PROPERTY, bind


def main():
    spark = spark_session()
    try:
        ledger = spark.read.format("delta").load(CDC)
        receipts = ledger.where("record_kind='receipt'")
        assert [r.kafka_offset for r in receipts.orderBy("kafka_offset").collect()] == list(range(72))
        assert receipts.where("disposition='tombstone'").count() == 3
        assert receipts.where("disposition='quarantine' AND reason='invalid_json'").count() == 2
        audit_events = ledger.where("record_kind='event' AND business_event_id LIKE 'oracle-%'").collect()
        assert len(audit_events) == 30
        assert all(e.kafka_partition == 0 and e.source_lsn and e.source_time_ms and e.kafka_timestamp
                   and e.is_simulated and e.kafka_cluster_id and e.kafka_topic_id for e in audit_events)
        corrected = [e for e in audit_events if e.business_event_id == "oracle-21" and e.operation == "u"]
        assert [(e.quantity_before, e.quantity_after) for e in corrected] == [(21, 30)]
        assert sum(e.operation == "d" and e.business_event_id == "oracle-19" for e in audit_events) == 1

        with TemporaryDirectory(prefix="stokora-cert-oracle-") as scratch:
            root = Path(scratch)
            batch = "/batch/m5_bronze"
            table = root / "lambda_snapshot"
            version = int(spark.sql(f"DESCRIBE HISTORY delta.`{CDC}`").first().version)
            assert version == 5, version
            binding = json.loads(spark.sql(f"DESCRIBE DETAIL delta.`{CDC}`").first().properties[PROPERTY])
            foreign = ("B" * 22, *binding[1:])
            try:
                bind(spark, Path(CDC), root / "foreign-checkpoint", foreign)
            except RuntimeError as err:
                assert str(err) == "cdc_sink_binding_mismatch_or_legacy"
            else:
                raise AssertionError("foreign Kafka source accepted")
            assert not (root / "foreign-checkpoint").exists()
            print("PASS mismatched Kafka incarnation refused before checkpoint creation", flush=True)
            snapshots = []
            for cdc_version in (4, version):
                snapshot, written = rebuild(spark, root, batch, CDC, 2, cdc_version)
                assert written and rebuild(spark, root, batch, CDC, 2, cdc_version) == (snapshot, False)
                snapshots.append(snapshot)
            assert len(set(snapshots)) == 2

            def scope(snapshot, tenant):
                return gold_view(spark, table, "sales_daily", snapshot, principal="cert-reader",
                                 grants={"cert-reader": ("SIM_A",)}, tenant=tenant)

            for snapshot, removed, day21 in ((snapshots[0], {22}, 21), (snapshots[1], {19, 22}, 30)):
                daily = scope(snapshot, "SIM_A").where("product='SIM_PRODUCT' AND month(day)=2")
                expected = {day: day for day in range(1, 29) if day not in removed}
                expected[21] = day21
                actual = {r.day.day: r.quantity for r in daily.collect()}
                assert actual == expected, (actual, expected)
                q = spark.read.format("delta").load(str(table)).where(
                    (F.col("snapshot") == snapshot) & (F.col("kind") == "quality")).first()
                assert json.loads(q.detail)["gx_expectations_passed"]
                rejected = spark.read.format("delta").load(str(table)).where(
                    (F.col("snapshot") == snapshot) & (F.col("kind") == "quarantine")
                    & (F.col("event_id") == "cert-21")).collect()
                assert rejected and all(r.reason == "invalid_cdc_lineage_or_state" for r in rejected)
            try:
                scope(snapshots[1], "SIM_B")
            except PermissionError:
                pass
            else:
                raise AssertionError("cross-tenant access")

            daily = scope(snapshots[1], "SIM_A").where("product='SIM_PRODUCT' AND month(day)=2").select(
                *KEY, "day", "quantity", "simulated", F.lit(None).cast("double").alias("price"))
            dates = [date(2024, 2, d) for d in (2, 9, 22, 29)]
            origin = features(daily, *dates).where("origin = '2024-02-22'")
            result = predictions(origin)
            assert result.count() == 21
            labels = sorted((r.horizon, r.actual) for r in result.where("method='naive'").collect())
            assert labels == [(1, None), *((h, h + 21) for h in range(2, 8))]
            baseline = {row.method: row for row in metrics(result, []).collect()}
            for method, total_error in {"naive": 27, "seasonal_naive": 34, "moving_average": 37}.items():
                score = baseline[method]
                assert score.scored_count == 6 and score.actual_units == 153
                assert isclose(score.mae, total_error / 6) and isclose(score.wape, total_error / 153)
            assert result.where("method='seasonal_naive' AND horizon=5 AND fallback='last_observed'").count() == 1
            print("PASS independent PostgreSQL-to-forecast oracle: 72 receipts, 30 oracle mutations, "
                  "three tombstones, two malformed receipts, distinct Gold versions, tenant denial, 21 predictions and "
                  "six scored labels per method with absolute errors 27/34/37", flush=True)

            boundary = root / "limit"
            spark.range(10000).write.format("delta").save(str(boundary))
            spark.range(10000, 10001).write.format("delta").mode("append").save(str(boundary))
            assert len(read_source(spark, str(boundary), 0)[0]) == 10000
            try:
                read_source(spark, str(boundary), 1)
            except ValueError as err:
                assert str(err) == "fixture_bronze_row_limit"
            else:
                raise AssertionError("10001 fixture rows accepted")
            print("PASS Gold accepts 10000 Bronze rows and rejects 10001")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
