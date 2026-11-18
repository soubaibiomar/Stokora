"""Exercise the real Delta sink in an isolated table with controlled Kafka coordinates."""

import json

from pipelines.streaming.consume_cdc import ROOT, session
from pipelines.streaming.merge_bronze import SCHEMA, merge_batch
from pipelines.streaming.source import bind, lookup


def expect_error(message, function):
    try:
        function()
    except RuntimeError as error:
        assert str(error).startswith(message), str(error)
        print("PASS", str(error), flush=True)
    else:
        raise AssertionError(f"expected {message}")


def main():
    spark = session()
    try:
        source = lookup(spark)
        path = ROOT / "receipt-regression"
        checkpoint = ROOT / "checkpoint-receipt-regression"
        bind(spark, path, checkpoint, source)

        def verify():
            assert lookup(spark) == source

        original = spark.read.format("delta").load(str(ROOT / "cdc_bronze")).where(
            "record_kind = 'receipt' AND kafka_offset = 6").first()
        body = json.loads(original.raw_value)
        body["after"]["quantity"] += 1
        body["source"]["kafka_topic_id"] = "untrusted_input"
        frame_schema = "topic string, partition int, offset long, timestamp timestamp, key binary, value binary"

        def row(offset, value, key=original.raw_key):
            return source[2], 0, offset, original.kafka_timestamp, key, value

        def ingest(rows):
            frame = spark.createDataFrame(rows, frame_schema)
            return merge_batch(spark, frame, path, "receipt-regression", 0, source, verify)

        def snapshot():
            versions = {file.name: file.read_bytes() for file in (path / "_delta_log").glob("*.json")}
            rows = spark.read.format("delta").load(str(path)).orderBy("ledger_id").collect()
            return versions, rows

        first = row(6, original.raw_value)
        different = row(6, json.dumps(body).encode())
        assert ingest([first])
        baseline = snapshot()
        physical = sorted(file.name for file in path.glob("*.parquet"))
        assert len(baseline[0]) == 2 and len(baseline[1]) == 2
        assert not ingest([first, first])
        assert snapshot() == baseline
        assert sorted(file.name for file in path.glob("*.parquet")) == physical
        print("PASS identical bytes and position, including in-batch retry, keep version 1", flush=True)

        for conflict in (different, row(6, None), row(6, original.raw_value, b"wrong-key")):
            expect_error("cdc_receipt_conflict", lambda: ingest([conflict, row(8, original.raw_value)]))
            assert snapshot() == baseline
        expect_error("cdc_receipt_conflict", lambda: ingest([row(8, different[-1]), row(8, original.raw_value)]))
        assert snapshot() == baseline
        assert sorted(file.name for file in path.glob("*.parquet")) == physical
        print("PASS all conflicts preserve version 1 and both original rows", flush=True)

        assert ingest([row(7, original.raw_value), row(8, different[-1])])
        versions, rows = snapshot()
        receipts = {r.ledger_id: r for r in rows if r.record_kind == "receipt"}
        events = [r for r in rows if r.record_kind == "event"]
        assert len(versions) == 3 and len(receipts) == 3 and len(events) == 2
        assert len({r.event_uid for r in receipts.values() if r.kafka_offset in (6, 7)}) == 1
        for e in events:
            r = receipts[e.receipt_id]
            assert (e.event_uid, e.raw_key, e.raw_value, e.payload_sha256) == (
                r.event_uid, r.raw_key, r.raw_value, r.payload_sha256)
        assert all(r.kafka_topic_id == source[1] for r in rows)
        print("PASS distinct offsets preserve two identical-payload receipts and one mutation; "
              "new mutation retains exact receipt despite spoofed envelope topic ID", flush=True)

        marker = checkpoint / "_stokora_kafka_binding"
        previous = marker.read_bytes()
        fake_checkpoint = ROOT / "checkpoint-unbound-regression"
        fake_checkpoint.mkdir()
        (fake_checkpoint / "metadata").write_text('{"id":"preexisting"}')
        expect_error("cdc_checkpoint_binding_missing", lambda: bind(spark, path, fake_checkpoint, source))
        legacy = ROOT / "legacy-receipt-regression"
        spark.createDataFrame([], SCHEMA).write.format("delta").save(str(legacy))
        expect_error("cdc_sink_binding_mismatch_or_legacy", lambda: bind(spark, legacy, checkpoint, source))
        expect_error("cdc_sink_binding_mismatch_or_legacy",
                     lambda: bind(spark, path, checkpoint, (source[0], "B" * 22, source[2])))
        assert marker.read_bytes() == previous and snapshot() == (versions, rows)
        print("PASS legacy and unapproved sources did not change persisted state", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
