"""Verify the broker identity and bind local streaming state before reading offsets."""

import os
import re
import tempfile

from pipelines.streaming.merge_bronze import SCHEMA, TOPIC, canonical

CA_PATH = os.environ.get("STOKORA_KAFKA_CA")
KAFKA = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "SASL_SSL" if CA_PATH else "SASL_PLAINTEXT",
    "sasl.mechanism": "PLAIN",
    "request.timeout.ms": "5000",
    "default.api.timeout.ms": "10000",
}
if CA_PATH:
    KAFKA.update({
        "ssl.truststore.type": "PEM",
        "ssl.truststore.location": CA_PATH,
        "ssl.endpoint.identification.algorithm": "https",
    })
PROPERTY = "stokora.kafkaBinding"


def lookup(spark, options=KAFKA):
    jvm = spark.sparkContext._jvm
    config = jvm.java.util.Properties()
    for key, value in options.items():
        config.setProperty(key, value)
    try:
        admin = jvm.org.apache.kafka.clients.admin.Admin.create(config)
        try:
            cluster_id = admin.describeCluster().clusterId().get()
            names = jvm.java.util.Collections.singletonList(TOPIC)
            topic = admin.describeTopics(names).allTopicNames().get().get(TOPIC)
            topic_id = topic.topicId().toString()
            if (not all(value and re.fullmatch(r"[A-Za-z0-9_-]{22}", value)
                        for value in (cluster_id, topic_id))
                    or topic_id == "AAAAAAAAAAAAAAAAAAAAAA" or topic.name() != TOPIC):
                raise ValueError("Missing Kafka identity")
            return cluster_id, topic_id, TOPIC
        finally:
            admin.close(jvm.java.time.Duration.ofSeconds(1))
    except Exception:
        raise RuntimeError("cdc_source_lookup_failed") from None


def bind(spark, path, checkpoint, source):
    expected = canonical(source)
    marker = checkpoint / "_stokora_kafka_binding"
    has_table = (path / "_delta_log").is_dir()
    if has_table:
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
        if detail.properties.get(PROPERTY) != expected:
            raise RuntimeError("cdc_sink_binding_mismatch_or_legacy")
    if marker.exists():
        if not has_table or marker.read_text(encoding="utf-8") != expected:
            raise RuntimeError("cdc_checkpoint_binding_mismatch")
        return
    if checkpoint.exists() and any(checkpoint.iterdir()):
        raise RuntimeError("cdc_checkpoint_binding_missing")
    if not has_table:
        spark.sql(f"""CREATE TABLE delta.`{path}` ({SCHEMA}) USING DELTA
                   TBLPROPERTIES ('{PROPERTY}' = '{expected}', 'delta.appendOnly' = 'true')""")
    checkpoint.mkdir(exist_ok=True)
    # Publish complete contents atomically and never overwrite an existing binding.
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=checkpoint.parent) as pending:
        pending.write(expected)
        pending.flush()
        os.fsync(pending.fileno())
        os.link(pending.name, marker)
    fd = os.open(checkpoint, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
