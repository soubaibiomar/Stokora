"""Qualify real snapshot and crash recovery in an explicitly isolated Spark container."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path

import pipelines.streaming.consume_cdc as cdc
from pipelines.streaming.source import PROPERTY, bind


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("before", "after", "consume", "replay", "snapshot", "final", "writer", "gap"))
    mode = parser.parse_args().mode
    if os.environ.get("STOKORA_GATE_FIXTURE") != "isolated":
        raise SystemExit("Run only in the isolated certification fixture")
    root = cdc.ROOT
    table = root / "cdc_bronze"
    checkpoint = root / "checkpoint-main"

    def logs():
        return {f.name: hashlib.sha256(f.read_bytes()).hexdigest()
                for f in (table / "_delta_log").glob("*.json")}

    if mode == "writer":
        with (root / ".stream-writer.lock").open("ab") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX)
            try:
                cdc.run(None, root / "competing-checkpoint", once=True)
            except BlockingIOError:
                assert not (root / "competing-checkpoint").exists()
                print("PASS competing writer denied with no checkpoint")
                return
            raise AssertionError("competing writer accepted")

    spark = cdc.session()
    try:
        if mode == "before":
            def exit_before(*_):
                os._exit(72)
            cdc.merge_batch = exit_before
            cdc.run(spark, checkpoint, once=True)
        elif mode == "after":
            assert len(logs()) == 1 and not (checkpoint / "commits/0").exists()
            print("PASS pre-commit crash left version zero and no acknowledgement", flush=True)
            cdc.run(spark, checkpoint, once=True, after_commit=lambda: os._exit(73))
        elif mode == "consume":
            cdc.run(spark, checkpoint, once=True)
        elif mode == "replay":
            before = logs()
            cdc.run(spark, root / "checkpoint-replay", once=True)
            assert logs() == before
            print("PASS fresh offset replay preserved Delta log bytes")
        elif mode == "gap":
            before = logs()
            try:
                cdc.run(spark, root / "checkpoint-gap", once=True)
            except Exception as err:
                diagnostic = str(err)
                assert "failOnDataLoss" in diagnostic or "DATA_LOSS" in diagnostic, diagnostic[:1000]
                assert logs() == before
                print("PASS retained checkpoint below Kafka watermark failed closed; Delta logs preserved")
            else:
                raise AssertionError("expired offsets were accepted")
        else:
            rows = spark.read.format("delta").load(str(table)).collect()
            receipts = sorted((r for r in rows if r.record_kind == "receipt"), key=lambda r: r.kafka_offset)
            mutations = [r for r in rows if r.record_kind == "event"]
            assert len(mutations) == len({r.event_uid for r in mutations})
            assert [r.kafka_offset for r in receipts] == list(range(28 if mode == "snapshot" else 34))
            assert len({(r.kafka_cluster_id, r.kafka_topic_id, r.kafka_partition) for r in receipts}) == 1
            assert all(r.operation == "r" and r.disposition == "accepted" and r.source_lsn > 0
                       for r in receipts[:28])
            expected = {("SIM_A", f"gate-{i}"): i for i in range(1, 29) if i != 22}
            expected[("SIM_B", "gate-21")] = 999
            actual = {(r.tenant_id, r.business_event_id): r.quantity_after for r in mutations
                      if r.operation == "r"}
            assert actual == expected
            assert all(r.kafka_timestamp and r.source_time_ms for r in receipts[:28])
            first = receipts[0]
            envelope = json.loads(first.raw_value)
            assert envelope["source"]["snapshot"] == "first"
            from types import SimpleNamespace
            from datetime import datetime, timezone
            row = SimpleNamespace(topic=first.kafka_topic, partition=0, offset=0, timestamp=first.kafka_timestamp,
                                  key=first.raw_key, value=first.raw_value)
            identity = (first.kafka_cluster_id, first.kafka_topic_id, first.kafka_topic)
            from pipelines.streaming.merge_bronze import decode
            envelope["source"]["snapshot"] = 1
            row.value = json.dumps(envelope).encode()
            assert decode(row, datetime.now(timezone.utc), "gate-test", 0, identity)[0]["reason"] == "invalid_source"
            envelope["op"], envelope["source"]["snapshot"] = "c", "first"
            row.value = json.dumps(envelope).encode()
            assert decode(row, datetime.now(timezone.utc), "gate-test", 0, identity)[0]["reason"] == "invalid_source"
            if mode == "snapshot":
                assert len(logs()) == 2 and (checkpoint / "commits/0").exists()
                print("PASS real non-empty Debezium snapshot: 28 r receipts and business mutations, exact two-tenant keys, quantities and LSNs")
            else:
                assert [r.operation for r in receipts[28:32]] == ["u", "u", "d", None]
                assert receipts[31].disposition == "tombstone" and receipts[32].operation == "c"
                assert receipts[33].disposition == "quarantine" and receipts[33].reason == "invalid_json"
                lsn = [r.source_lsn for r in receipts if r.disposition == "accepted"]
                assert lsn == sorted(lsn) and lsn[-1] > lsn[0]
                updates = {r.business_event_id: (r.quantity_before, r.quantity_after)
                           for r in mutations if r.operation == "u"}
                assert updates == {"gate-21": (21, 30), "gate-8": (8, 8)}
                assert len(mutations) == 32 and len(logs()) == 3
                detail = spark.sql(f"DESCRIBE DETAIL delta.`{table}`").first()
                _, topic_id, topic = json.loads(detail.properties[PROPERTY])
                before = logs()
                try:
                    bind(spark, table, root / "foreign-checkpoint", ("B" * 22, topic_id, topic))
                except RuntimeError as err:
                    assert str(err) == "cdc_sink_binding_mismatch_or_legacy"
                else:
                    raise AssertionError("foreign incarnation accepted")
                assert logs() == before and not (root / "foreign-checkpoint").exists()
                print("PASS 34 receipts, 32 unique events, corrected states, delete/tombstone, quarantine, LSN/broker binding and 3 Delta log files")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
