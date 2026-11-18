"""Assertions over the real streaming Delta table and persistent Spark offsets."""

import argparse
import hashlib
import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.streaming.consume_cdc import ROOT, session
from pipelines.streaming.source import PROPERTY


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("crash", "recovered", "replay", "outage-recovered"))
    args = parser.parse_args()
    stage = args.stage
    spark = session()
    name = str(ROOT / "cdc_bronze")
    try:
        df = spark.read.format("delta").load(name)
        groups = {(r.record_kind, r.disposition): r.n for r in
                  df.groupBy("record_kind", "disposition").count().withColumnRenamed("count", "n").collect()}
        more = stage == "outage-recovered"
        assert groups == {("receipt", "accepted"): 6 + more, ("receipt", "tombstone"): 1,
                          ("receipt", "quarantine"): 3, ("event", "accepted"): 5 + more}, groups
        assert df.select("ledger_id").distinct().count() == sum(groups.values())
        binding = json.loads(spark.sql(f"DESCRIBE DETAIL delta.`{name}`").first().properties[PROPERTY])
        rows = df.collect()
        receipts = {row.ledger_id: row for row in rows if row.record_kind == "receipt"}
        for row in rows:
            assert [row.kafka_cluster_id, row.kafka_topic_id, row.kafka_topic] == binding
            assert row.source_epoch == hashlib.sha256(json.dumps(binding, separators=(",", ":")).encode()).hexdigest()
            coordinates = json.dumps([*binding, row.kafka_partition, row.kafka_offset], separators=(",", ":"))
            assert row.receipt_id == "k:" + hashlib.sha256(coordinates.encode()).hexdigest()
            for content, digest in ((row.raw_value, row.payload_sha256), (row.raw_key, row.key_sha256)):
                assert digest == (None if content is None else hashlib.sha256(content).hexdigest())
            receipt = receipts[row.receipt_id]
            if row.record_kind == "event":
                assert receipt.disposition == "accepted"
                for field in ("raw_value", "raw_key", "event_uid", "tenant_id", "site_id", "business_event_id"):
                    assert row[field] == receipt[field], field
        assert receipts[next(row.receipt_id for row in rows if row.kafka_offset == 6)].event_uid == next(
            row.event_uid for row in rows if row.record_kind == "event" and row.kafka_offset == 1)
        events = df.where("record_kind='event'")
        assert {r.operation for r in events.select("operation").distinct().collect()} == {"r", "c", "u", "d"}
        assert not events.where("""tenant_id IS NULL OR site_id IS NULL OR source_lsn IS NULL OR
            NOT is_simulated OR scenario_id IS NULL OR kafka_timestamp IS NULL OR
            ingested_at IS NULL OR raw_value IS NULL OR envelope_time_ms IS NULL""").count()
        rejects = df.where("disposition='quarantine'")
        assert {row.reason for row in rejects.select("reason").collect()} == {
            "invalid_json", "invalid_row_state", "unsupported_operation",
        }
        latest = Window.partitionBy("tenant_id", "site_id", "business_event_id").orderBy(
            F.desc("source_lsn"), F.desc("kafka_offset"),
        )
        current = events.withColumn("position", F.row_number().over(latest)).where("position=1 AND operation <> 'd'")
        assert current.agg(F.sum("quantity_after")).first()[0] == (6 if more else 4)
        change = events.where("operation='u'").select("quantity_before", "quantity_after").first()
        assert list(change) == [2, 5]
        receipt_offsets = [r.kafka_offset for r in df.where("record_kind='receipt'").orderBy("kafka_offset").collect()]
        assert receipt_offsets == list(range(10 + more))
        histories = spark.sql(f"DESCRIBE HISTORY delta.\x60{name}\x60").select(
            "version", "operation", "userMetadata",
        ).collect()
        commits = [json.loads(r.userMetadata) for r in histories if r.operation == "MERGE"]
        assert commits and all(set(item) == {"attempt", "batch"} for item in commits)
        assert sorted(row.version for row in histories) == list(range(3 if more else 2))
        for version in range(1, 3 if more else 2):
            lines = (Path(name) / "_delta_log" / f"{version:020}.json").read_text().splitlines()
            info = next(action["commitInfo"] for action in map(json.loads, lines) if "commitInfo" in action)
            assert info["txnId"] and json.loads(info["userMetadata"])["batch"] == version - 1
            print("DELTA", version, info["txnId"], info["userMetadata"], flush=True)

        checkpoint = ROOT / "checkpoint-main"
        assert (checkpoint / "offsets/0").exists()
        assert json.loads((checkpoint / "_stokora_kafka_binding").read_text()) == binding
        assert (checkpoint / "commits/0").exists() == (stage != "crash")
        if stage == "replay":
            assert (ROOT / "checkpoint-replay/commits/0").exists()
            assert json.loads((ROOT / "checkpoint-replay/_stokora_kafka_binding").read_text()) == binding
        if more:
            assert (checkpoint / "commits/1").exists()
        latency = events.selectExpr(
            "max(unix_millis(ingested_at) - envelope_time_ms) as max_ms",
            "min(unix_millis(ingested_at) - envelope_time_ms) as min_ms",
        ).first().asDict()
        print("PASS", stage, json.dumps({
            "groups": {f"{key[0]}:{key[1]}": count for key, count in groups.items()},
            "offsets": receipt_offsets, "delta_versions": [r.version for r in histories],
            "current_sales_units": 6 if more else 4, "duplicates": 1, "latency_ms_includes_fixture_wait": latency,
        }, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
