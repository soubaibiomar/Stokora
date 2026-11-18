"""Consume the private synthetic CDC broker with Structured Streaming."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
from uuid import uuid4

from pyspark.sql import SparkSession

from pipelines.storage.bronze import JARS
from pipelines.streaming.merge_bronze import TOPIC, merge_batch
from pipelines.streaming.source import KAFKA, bind, lookup

ROOT = Path(os.environ.get("STOKORA_STREAM_ROOT", "/home/jovyan/work"))
EXTRA = ("spark-sql-kafka-0-10_2.13-4.0.0.jar", "spark-token-provider-kafka-0-10_2.13-4.0.0.jar",
         "kafka-clients-3.9.2.jar", "commons-pool2-2.12.0.jar")


def session():
    jars = ",".join((JARS, *(f"/opt/kafka-jars/{name}" for name in EXTRA)))
    spark = (SparkSession.builder.master("local[2]").appName("stokora-cdc-bronze")
             .config("spark.driver.memory", "1g").config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1").config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").config("spark.databricks.delta.snapshotPartitions", "2")
             .config("spark.databricks.delta.properties.defaults.appendOnly", "true")
             .config("spark.jars", jars).config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run(spark, checkpoint, *, once, after_commit=None, stop_requested=None):
    path = ROOT / "cdc_bronze"
    attempt = uuid4().hex
    with (ROOT / ".stream-writer.lock").open("ab") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        binding = lookup(spark)
        bind(spark, path, checkpoint, binding)

        def verify_source():
            if lookup(spark) != binding:
                raise RuntimeError("cdc_source_incarnation_changed")

        def write(frame, batch_id):
            changed = merge_batch(spark, frame, path, attempt, batch_id, binding, verify_source)
            print(json.dumps({"committed": changed, "attempt": attempt, "batch": batch_id}), flush=True)
            if changed and after_commit is not None:
                after_commit()
        source = (spark.readStream.format("kafka").options(**{f"kafka.{k}": v for k, v in KAFKA.items()})
                  .option("groupIdPrefix", "stokora-bronze").option("fetchOffset.numRetries", "2")
                  .option("subscribe", TOPIC).option("startingOffsets", "earliest")
                  .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", "1000").load())
        writer = source.writeStream.foreachBatch(write).option("checkpointLocation", str(checkpoint))
        query = (writer.trigger(availableNow=True) if once else writer.trigger(processingTime="10 seconds")).start()
        try:
            while not query.awaitTermination(2 if stop_requested else 10):
                if stop_requested and stop_requested():
                    break
                if query.lastProgress:
                    print_progress(query.lastProgress)
            for progress in query.recentProgress:
                print_progress(progress)
        finally:
            if query.isActive:
                query.stop()
            cgroup = Path("/sys/fs/cgroup")
            if (cgroup / "memory.peak").exists():
                cpu = dict(line.split() for line in (cgroup / "cpu.stat").read_text().splitlines())
                print("RESOURCES", json.dumps({
                    "peak_bytes": int((cgroup / "memory.peak").read_text()),
                    "cpu_usage_usec": int(cpu["usage_usec"]),
                }), flush=True)


def print_progress(progress):
    selected = {k: progress[k] for k in ("batchId", "numInputRows", "processedRowsPerSecond", "durationMs")}
    selected["offsets"] = [{k: source.get(k) for k in ("startOffset", "endOffset", "latestOffset")}
                           for source in progress["sources"]]
    print("PROGRESS", json.dumps(selected, sort_keys=True), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--replay", action="store_true", help="use a separate persistent replay checkpoint")
    args = parser.parse_args()
    checkpoint = ROOT / ("checkpoint-replay" if args.replay else "checkpoint-main")
    stopping = False

    def request_stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    spark = session()
    try:
        run(spark, checkpoint, once=args.once, stop_requested=lambda: stopping)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
