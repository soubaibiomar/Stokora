"""Validate Debezium v1 fixtures and commit receipts and unique events together."""

from datetime import date, datetime, timezone
import hashlib
import json
import re

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TOPIC = "stokora.operational.sales_events"
KEYS = ("tenant_id", "site_id", "event_id")
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,100}")
SCHEMA = """ledger_id STRING, record_kind STRING, disposition STRING, reason STRING,
    source_epoch STRING, schema_version INT, is_simulated BOOLEAN, scenario_id STRING,
    tenant_id STRING, site_id STRING, business_event_id STRING, event_uid STRING,
    operation STRING, source_name STRING, source_lsn LONG, source_tx LONG,
    source_time_ms LONG, envelope_time_ms LONG, business_date DATE, quantity_before INT,
    quantity_after INT, kafka_topic STRING, kafka_partition INT, kafka_offset LONG,
    kafka_timestamp TIMESTAMP, ingested_at TIMESTAMP, attempt_id STRING, batch_id LONG,
    raw_key BINARY, raw_value BINARY, receipt_id STRING, payload_sha256 STRING,
    key_sha256 STRING, kafka_cluster_id STRING, kafka_topic_id STRING"""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def identity(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def unique_object(pairs):
    value = dict(pairs)
    if len(value) != len(pairs):
        raise ValueError
    return value


def invalid_constant(_):
    raise ValueError


def json_object(payload):
    if len(payload) > 128 * 1024:
        raise ValueError
    obj = json.loads(bytes(payload).decode("utf-8"), object_pairs_hook=unique_object,
                     parse_constant=invalid_constant)
    if type(obj) is not dict:
        raise ValueError
    return obj


def integer(value, low=0, high=2**63-1):
    return type(value) is int and low <= value <= high


def valid_id(value):
    return type(value) is str and IDENTIFIER.fullmatch(value) is not None


def valid_state(state, key):
    return (type(state) is dict and all(state.get(k) == key[k] for k in KEYS)
            and state.get("is_simulated") is True
            and valid_id(state.get("synthetic_scenario_id"))
            and valid_id(state.get("product_id")) and valid_id(state.get("source"))
            and integer(state.get("business_date"), -719162, 2932896)
            and integer(state.get("quantity"), 1, 1000000)
            and integer(state.get("unit_price_minor"), 0, 1000000000)
            and state.get("currency") in ("USD", "MAD", "GBP", "EUR"))


def payload_hash(payload):
    return None if payload is None else hashlib.sha256(payload).hexdigest()


def decode(row, at, attempt, batch_id, source):
    """One durable receipt per position. Valid envelopes add one source mutation row."""
    cluster_id, topic_id, topic = source
    if row.topic != topic or not integer(row.partition) or not integer(row.offset):
        raise ValueError("cdc_unbound_kafka_position")
    epoch = identity(source)
    receipt_id = "k:" + identity((*source, row.partition, row.offset))
    receipt = dict.fromkeys(field.split()[0] for field in SCHEMA.replace("\n", "").split(","))
    receipt.update(ledger_id=receipt_id, receipt_id=receipt_id, record_kind="receipt",
                   disposition="quarantine", source_epoch=epoch, schema_version=1,
                   kafka_cluster_id=cluster_id, kafka_topic_id=topic_id,
                   kafka_topic=row.topic, kafka_partition=row.partition, kafka_offset=row.offset,
                   kafka_timestamp=row.timestamp, ingested_at=at, attempt_id=attempt, batch_id=batch_id,
                   raw_key=row.key, raw_value=row.value, key_sha256=payload_hash(row.key),
                   payload_sha256=payload_hash(row.value))
    try:
        key = json_object(row.key)
        if set(key) != set(KEYS) or not all(valid_id(key[k]) for k in KEYS):
            raise ValueError
    except (TypeError, RecursionError, ValueError):
        return [dict(receipt, reason="invalid_key")]
    receipt.update(tenant_id=key["tenant_id"], site_id=key["site_id"], business_event_id=key["event_id"])
    if row.value is None:
        return [dict(receipt, disposition="tombstone")]
    try:
        body = json_object(row.value)
    except (TypeError, RecursionError, ValueError):
        return [dict(receipt, reason="invalid_json")]
    op = body.get("op")
    receipt["operation"] = op if op in ("r", "c", "u", "d") else None
    if receipt["operation"] is None:
        return [dict(receipt, reason="unsupported_operation")]
    source, before, after = body.get("source"), body.get("before"), body.get("after")
    fixed = {"name": "stokora", "db": "stokora", "schema": "operational", "table": "sales_events"}
    if (type(source) is not dict or not all(source.get(k) == v for k, v in fixed.items())
            or not all(k in body for k in ("before", "after"))
            or not integer(source.get("lsn")) or not integer(source.get("ts_ms"))
            or not integer(body.get("ts_ms"))
            or not (integer(source.get("txId")) or op == "r" and source.get("txId") is None)
            or (op == "r") != (source.get("snapshot") in ("true", "last", True))):
        return [dict(receipt, reason="invalid_source")]
    needs_before, needs_after = op in ("u", "d"), op in ("r", "c", "u")
    if (not (valid_state(before, key) if needs_before else before is None)
            or not (valid_state(after, key) if needs_after else after is None)):
        return [dict(receipt, reason="invalid_row_state")]
    if before and after and any(before[f] != after[f] for f in ("synthetic_scenario_id", "currency", "source")):
        return [dict(receipt, reason="changed_provenance")]
    state = after if needs_after else before
    uid = identity((epoch, fixed, key, op, source["lsn"], source.get("txId"),
                    source.get("sequence"), source["ts_ms"], before, after))
    receipt.update(disposition="accepted", event_uid=uid, is_simulated=True, source_name=state["source"],
                   source_lsn=source["lsn"], source_tx=source.get("txId"), source_time_ms=source["ts_ms"],
                   envelope_time_ms=body["ts_ms"], scenario_id=state["synthetic_scenario_id"],
                   business_date=date.fromordinal(state["business_date"] + date(1970, 1, 1).toordinal()),
                   quantity_before=before["quantity"] if needs_before else None,
                   quantity_after=after["quantity"] if needs_after else None)
    return [receipt, dict(receipt, ledger_id="e:" + uid, record_kind="event")]


def merge_batch(spark, frame, path, attempt, batch_id, source, verify_source):
    at = datetime.now(timezone.utc)
    records = spark.createDataFrame(
        frame.rdd.flatMap(lambda row: decode(row, at, attempt, batch_id, source)), SCHEMA).cache()
    positions = ["receipt_id", "kafka_topic", "kafka_partition", "kafka_offset"]
    target = spark.read.format("delta").load(str(path))
    try:
        receipts = records.where("record_kind = 'receipt'").select(*positions, "raw_key", "raw_value")
        stored_receipts = target.where("record_kind = 'receipt'")
        conflict = receipts.distinct().groupBy(*positions).count().where("count > 1").select(*positions).first()
        if conflict is None:
            conflict = (receipts.alias("incoming").join(stored_receipts.alias("stored"), "receipt_id").where(
                "NOT (incoming.raw_key <=> stored.raw_key AND incoming.raw_value <=> stored.raw_value)")
                .select("receipt_id", *(f"incoming.{name}" for name in positions[1:])).first())
        if conflict is not None:
            raise RuntimeError("cdc_receipt_conflict " + canonical(conflict.asDict()))

        new_records = records.join(stored_receipts.select("receipt_id"), "receipt_id", "left_anti")
        has_new_rows = not new_records.isEmpty()
        verify_source()
        if not has_new_rows:
            return False
        rank = Window.partitionBy("ledger_id").orderBy("kafka_partition", "kafka_offset")
        candidates = new_records.withColumn("_rank", F.row_number().over(rank)).where("_rank = 1").drop("_rank")
        candidates.createOrReplaceTempView("cdc_candidates")
        spark.conf.set("spark.databricks.delta.commitInfo.userMetadata",
                       canonical({"attempt": attempt, "batch": batch_id}))
        spark.sql(f"""MERGE INTO delta.\x60{path}\x60 AS target USING cdc_candidates AS source
                   ON target.ledger_id = source.ledger_id
                   WHEN NOT MATCHED THEN INSERT *""").collect()
        return True
    finally:
        records.unpersist()
