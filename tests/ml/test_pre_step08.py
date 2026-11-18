"""Acceptance tests for independently sourced temporal and evaluation fixtures."""
from datetime import date, datetime, timedelta
import json
from pathlib import Path

from pyspark.sql import functions as F

from pipelines.features.availability import history_as_of
from pipelines.features.forecast import KEY
from pipelines.ml.baselines.coverage import category_metrics
from pipelines.storage.bronze import spark_session


def check_as_of(spark):
    monday = date(2024, 1, 1)
    keys = ("A", "synthetic:availability", "shop", "item")
    def at(day, hour=0):
        return datetime(2024, 1, day, hour)
    records = [
        (*keys, monday, "sale", at(1, 16), 1, "accepted", "5", True),
        (*keys, monday, "sale", at(4, 10), 2, "accepted", "8", True),
        (*keys, monday, "sale", at(6, 11), 3, "quarantined", "900", True),
        (*keys, monday + timedelta(days=1), "sale", at(5, 13), 1, "accepted", "12", True),
        (*keys, monday, "price", at(4, 5), 1, "accepted", "7.50", True),
        (*keys, monday, "calendar", at(1, 10), 1, "accepted", "original", True),
        (*keys, monday, "calendar", at(4, 14), 2, "accepted", "corrected", True),
        ("B", *keys[1:], monday, "sale", at(1, 2), 3, "accepted", "777", True),
    ]
    schema = ("tenant string, dataset string, site string, product string, day date, kind string, "
              "available_at timestamp_ntz, revision int, state string, value string, simulated boolean")
    revisions = spark.createDataFrame(records, schema)
    origins = spark.createDataFrame(
        [(*keys, at(day)) for day in (1, 2, 4, 5, 6, 7)],
        "tenant string, dataset string, site string, product string, origin timestamp_ntz",
    )
    def read(source):
        rows = history_as_of(source, origins).select("origin", "day", "kind", "value").collect()
        return {(r.origin.day, r.day.day, r.kind): r.value for r in rows}
    known = read(revisions)
    expected = {
        (2, 1, "sale"): "5", (2, 1, "calendar"): "original",
        (4, 1, "sale"): "5", (4, 1, "calendar"): "original",
        (5, 1, "sale"): "8", (5, 1, "calendar"): "corrected", (5, 1, "price"): "7.50",
        (6, 1, "sale"): "8", (6, 1, "calendar"): "corrected", (6, 1, "price"): "7.50",
        (6, 2, "sale"): "12",
        (7, 1, "calendar"): "corrected", (7, 1, "price"): "7.50", (7, 2, "sale"): "12",
    }
    assert known == expected and len(known) == 14, known
    assert known == read(revisions.orderBy(F.desc("revision"), F.desc("available_at")))
    print("PASS synthetic as-of oracle: correction, late arrival, price, calendar, quarantine, tenant and replay")
    try:
        history_as_of(revisions.unionByName(revisions.limit(1)), origins)
    except ValueError as exc:
        assert str(exc) == "invalid_synthetic_availability"
    else:
        raise AssertionError("ambiguous revision accepted")
    print("PASS ambiguous revision fails closed")
    for source, dates, expected_error in (
        (revisions.drop("revision"), origins, "invalid_availability_schema"),
        (revisions.withColumn("revision", F.lit(float("nan"))), origins, "invalid_availability_schema"),
        (revisions.withColumn("day", F.col("day").cast("string")), origins, "invalid_availability_schema"),
        (revisions.withColumn("revision", F.lit(-1)), origins, "invalid_synthetic_availability"),
        (revisions.withColumn("available_at", F.lit(None).cast("timestamp_ntz")), origins,
         "invalid_synthetic_availability"),
        (revisions, origins.withColumn("origin", F.lit(None).cast("timestamp_ntz")), "invalid_forecast_origin"),
        (revisions, origins.withColumn("origin", F.col("origin").cast("string")), "invalid_utc_availability_clock"),
    ):
        try:
            history_as_of(source, dates)
        except ValueError as exc:
            assert str(exc) == expected_error
        else:
            raise AssertionError(expected_error)
    same_instant = revisions.where("kind = 'price'").withColumn(
        "available_at", F.lit("2024-01-04 00:00:00").cast("timestamp_ntz"))
    assert read(same_instant)[(4, 1, "price")] == "7.50"
    print("PASS missing clock metadata, malformed origin and exact UTC boundary")


def check_category(spark):
    forecasts = spark.createDataFrame(
        [("A", "cat-fixture", "site", "p", "test", "naive", 5., 3),
         ("A", "cat-fixture", "site", "p", "test", "naive", 0., 0),
         ("A", "cat-fixture", "site", "q", "test", "naive", 6., None),
         ("B", "cat-fixture", "site", "p", "test", "naive", 5., 0),
         ("A", "gold-no-category", "site", "p", "test", "naive", 10., 110)],
        "tenant string, dataset string, site string, product string, split string, method string, prediction double, actual long")
    dimension = spark.createDataFrame([
        ("A", "cat-fixture", "site", "p", "synthetic_food"),
        ("A", "cat-fixture", "site", "q", "synthetic_food"),
        ("B", "cat-fixture", "site", "p", "synthetic_bakery"),
    ], [*KEY, "category"])
    summary = {r.tenant: r for r in category_metrics(forecasts, dimension).collect()}
    assert set(summary) == {"A", "B"}
    a, b = summary["A"], summary["B"]
    assert (a.category, a.forecast_count, a.scored_count, a.actual_units, a.mae, a.wape) == (
        "synthetic_food", 3, 2, 3, 1, 2/3)
    assert (b.category, b.mae, b.wape) == ("synthetic_bakery", 5, None)
    try:
        category_metrics(forecasts, dimension.unionByName(dimension.limit(1)))
    except ValueError as exc:
        assert str(exc) == "invalid_category_dimension"
    else:
        raise AssertionError("duplicate categories multiply scored forecasts")
    print("PASS synthetic category oracle, unknown Gold excluded, duplicate dimension rejected")


def check_coverage_artifact(spark, root):
    directory, = [path.parent for path in root.glob("*/coverage.json")]
    counts = json.loads((directory / "coverage.json").read_text())
    report = json.loads((directory / "metrics.json").read_text())
    expected = {"naive": (0.86125, 0.212326656394453),
                "seasonal_naive": (0.03, 0.007395993836671803),
                "moving_average": (0.7380565476190476, 0.18195538924352483)}
    for method, (mae, wape) in expected.items():
        population = [r for r in counts if r["method"] == method and r["split"] == "test"]
        totals = {status: sum(r["forecast_count"] for r in population if r["scoring_status"] == status)
                  for status in ("scored", "after_observed_history", "missing_date_within_history", "no_prediction")}
        assert totals == {"scored": 800, "after_observed_history": 350, "missing_date_within_history": 5,
                          "no_prediction": 0}, totals
        assert sum(r["forecast_count"] for r in population if r["fallback"] == "zero") >= 350
        result, = [r for r in report["aggregate"] if r["method"] == method and r["split"] == "test"]
        assert abs(result["mae"] - mae) < 1e-10 and abs(result["wape"] - wape) < 1e-10
        print("PASS frozen coverage and metrics", method, totals, "fallbacks", {
            k: sum(r["forecast_count"] for r in population if r["fallback"] == k)
            for k in ("zero", "last_observed", "none")})
    rows = spark.read.parquet(str(directory / "coverage"))
    common = [rows.where((F.col("method") == name) & (F.col("scoring_status") == "scored"))
              .select(*KEY, "origin", "horizon") for name in expected]
    assert all(common[0].exceptAll(other).isEmpty() and other.exceptAll(common[0]).isEmpty()
               for other in common[1:])
    print("PASS all methods use identical scored forecast keys")


if __name__ == "__main__":
    spark = spark_session()
    try:
        check_as_of(spark)
        check_category(spark)
        check_coverage_artifact(spark, Path("/home/jovyan/work"))
    finally:
        spark.stop()
