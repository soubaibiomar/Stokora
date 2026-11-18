"""Publish the STEP 07 synthetic benchmark in the dedicated feature volume."""

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic

from pyspark.sql import functions as F

from pipelines.features.forecast import KEY, SPEC, features
from pipelines.gold.publish import gold_view
from pipelines.ml.baselines.coverage import coverage, coverage_summary
from pipelines.ml.baselines.forecast import metrics, predictions
from pipelines.storage.bronze import spark_session, writer_lock

GOLD = "/gold/calendar-import/lambda_snapshot"
WINDOWS = tuple(date(2024, month, day) for month, day in [(1, 6), (4, 1), (5, 1), (5, 31)])
SEED = 42
SCHEMA = "tenant string, dataset string, site string, product string, day date, quantity long, price double, simulated boolean"


def fixture_rows():
    for tenant, site, product, first, pattern in (
        ("SIM_A", "SIM_SITE", "SEASONAL", 0, (2, 2, 5, 3, 2, 1, 0)),
        ("SIM_B", "SIM_SITE", "SEASONAL", 0, (15,)),
        ("SIM_A", "SIM_SITE", "INTERMITTENT", 0, (0, 0, 0, 8, 0, 0, 0)),
        ("SIM_A", "SIM_SITE", "ZERO", 0, (0,)),
        ("SIM_A", "SIM_NEW_SITE", "NEW", 125, (1, 0, 0, 0, 0, 2, 0)),
    ):
        for offset in range(first, (WINDOWS[-1] - WINDOWS[0]).days + 1):
            if product == "INTERMITTENT" and offset in (50, 51, 120):
                continue
            yield (tenant, "simulator:step07-v1", site, product, WINDOWS[0] + timedelta(days=offset),
                   pattern[(offset + SEED) % len(pattern)], 2.0 if offset % 7 == 0 else None, True)


def approved_sales(spark, version):
    pinned = spark.read.format("delta").option("versionAsOf", version).load(GOLD)
    snapshot = pinned.where("kind = 'quality'").select("snapshot").distinct().collect()
    if len(snapshot) != version + 1:
        raise ValueError("unexpected_approved_gold_history")
    identity = spark.sql(f"DESCRIBE HISTORY delta.`{GOLD}`").where(F.col("version") == version).first()
    if not identity or identity.userMetadata not in {row.snapshot for row in snapshot}:
        raise ValueError("missing_gold_commit_identity")
    allowed = ("SIM_M5",)
    daily = gold_view(spark, GOLD, "sales_daily", identity.userMetadata,
                      principal="step07_fixture_reader", grants={"step07_fixture_reader": allowed}, tenant="SIM_M5")
    daily = daily.where("dataset = 'm5_fixture'").select(
        *KEY, "day", "quantity", F.lit(None).cast("double").alias("price"), "simulated")
    if daily.isEmpty():
        raise ValueError("missing_approved_gold_sales")
    return daily, identity.userMetadata


def main():
    started = monotonic()
    spark = spark_session()
    root = Path("/home/jovyan/work")
    try:
        daily, snapshot = approved_sales(spark, 2)
        fixture = list(fixture_rows())
        code = ["/workspace/pipelines/features/forecast.py", "/workspace/pipelines/ml/baselines/forecast.py",
                "/workspace/pipelines/ml/baselines/coverage.py",
                __file__]
        manifest = {"spec": SPEC, "label": "SYNTHETIC", "gold_snapshot": snapshot, "gold_version": 2,
                    "cutoffs": [day.isoformat() for day in WINDOWS], "seed": SEED,
                    "code_sha256": [hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in code],
                    "fixture_sha256": hashlib.sha256(json.dumps(fixture, default=str).encode()).hexdigest(),
                    "availability": "valid_as_of_snapshot"}
        run_id = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        output = root / run_id
        sales = daily.unionByName(spark.createDataFrame(fixture, SCHEMA))
        origins = features(sales, *WINDOWS).cache()
        # Touch all columns in both measurements, including labels and missingness.
        checksum = F.sum(F.xxhash64(*origins.columns).cast("decimal(38,0)"))
        summary = origins.agg(F.count("*"), checksum).first()
        count = summary[0]
        cold = monotonic() - started
        warmed = monotonic()
        origins.unpersist(blocking=True)
        origins = features(sales, *WINDOWS).cache()
        repeated = origins.agg(F.count("*"), checksum).first()
        warm = monotonic() - warmed
        if not count or repeated != summary:
            raise RuntimeError("empty_or_nonreproducible_features")
        with writer_lock(root):
            if not output.exists():
                with TemporaryDirectory(prefix=".forecast-", dir=root) as temporary:
                    staging = Path(temporary) / "run"
                    forecasts = predictions(origins).withColumn("run_id", F.lit(run_id)).cache()
                    (origins.drop("actuals", "unit_sales_next_7d").withColumn("run_id", F.lit(run_id))
                     .write.parquet(str(staging / "features")))
                    (origins.select(*KEY, "origin", "split", "actuals", "unit_sales_next_7d")
                     .withColumn("run_id", F.lit(run_id)).write.parquet(str(staging / "labels")))
                    forecasts.select(*KEY, "origin", "split", "horizon", "target_date", "target_weekday",
                                     "target_month", "actual", "method", "prediction", "simulated", "unit",
                                     "run_id").write.parquet(str(staging / "forecasts"))
                    audited = coverage(forecasts, sales)
                    (audited.select(*KEY, "origin", "target_date", "horizon", "split", "method",
                                    "scoring_status", "fallback", "run_id")
                     .write.parquet(str(staging / "coverage")))
                    counts = coverage_summary(audited).orderBy("split", "method", *KEY, "scoring_status", "fallback")
                    (staging / "coverage.json").write_text(
                        json.dumps([row.asDict() for row in counts.collect()], indent=2), encoding="utf-8")
                    aggregates = {}
                    for scope, grain in (("series", KEY), ("site", KEY[:-1]), ("aggregate", [])):
                        results = metrics(forecasts, grain).orderBy("split", "method", *grain)
                        aggregates[scope] = [row.asDict() for row in results.collect()]
                    (staging / "metrics.json").write_text(json.dumps(aggregates, indent=2), encoding="utf-8")
                    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
                    staging.rename(output)
        peak = Path("/sys/fs/cgroup/memory.peak")
        print(json.dumps({"run_id": run_id, "rows": count, "feature_checksum": str(summary[1]),
                          "cold_seconds": cold, "warm_seconds": warm,
                          "elapsed_seconds": monotonic() - started,
                          "peak_bytes": int(peak.read_text()) if peak.exists() else None,
                          "output_bytes": sum(p.stat().st_size for p in output.rglob("*") if p.is_file()),
                          "metrics": json.loads((output / "metrics.json").read_text())["aggregate"]}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
