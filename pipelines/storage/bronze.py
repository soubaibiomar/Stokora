"""Single-node Delta Bronze storage. Raw rows and load records commit together."""

from contextlib import contextmanager
import fcntl
from pathlib import Path
import shutil

from pyspark.sql import SparkSession

JARS = "/opt/bronze-jars/delta-spark_2.13-4.0.1.jar,/opt/bronze-jars/delta-storage-4.0.1.jar"
SCHEMA = """dataset STRING, source_table STRING, source_file STRING, source_hash STRING,
    schema_version INT, is_simulated BOOLEAN, origin STRING, retrieved_on DATE,
    ingestion_run_id STRING, ingested_at TIMESTAMP, record_type STRING, stage_status STRING,
    source_line BIGINT, raw STRING, reason STRING, item_key STRING, site_key STRING,
    business_date DATE, source_columns ARRAY<STRING>, source_bytes BIGINT"""


def spark_session():
    spark = (SparkSession.builder.master("local[2]").appName("stokora-bronze")
             .config("spark.driver.memory", "1g")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.databricks.delta.snapshotPartitions", "2")
             .config("spark.databricks.delta.properties.defaults.appendOnly", "true")
             .config("spark.jars", JARS)
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@contextmanager
def writer_lock(root):
    # shortcut: one Linux host and a local volume; use distributed coordination for shared storage.
    with (root / ".bronze-writer.lock").open("ab") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def check_space(root, input_bytes):
    required = 64 * 1024**2 + 3 * input_bytes
    free = shutil.disk_usage(root).free
    if free < required:
        raise ValueError(f"INSUFFICIENT_DISK free_bytes={free} required_bytes={required}")


def table_path(root):
    return str(root / "m5_bronze")


def already_loaded(spark, root, dataset):
    if not (root / "m5_bronze" / "_delta_log").exists():
        return set()
    from pyspark.sql import functions as F

    table = spark.read.format("delta").load(table_path(root))
    manifest = table.where(
        (F.col("dataset") == dataset) & (F.col("record_type") == "load")
        & (F.col("stage_status") == "committed") & (F.col("schema_version") == 1)
    )
    return {(row.source_table, row.source_hash)
            for row in manifest.select("source_table", "source_hash").distinct().toLocalIterator()}


def append(spark, root, records, run_id):
    # Registry, good rows and quarantine are rows in one atomic Delta commit.
    (spark.createDataFrame(records, SCHEMA).coalesce(2).write.format("delta").mode("append")
     .option("userMetadata", run_id).save(table_path(root)))
