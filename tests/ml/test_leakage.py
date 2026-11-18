"""Independent synthetic oracle and negative gates for Spark forecasting."""

from datetime import date, timedelta
from math import isclose
from statistics import pstdev

from pyspark.sql import functions as F

from pipelines.features.forecast import features
from pipelines.ml.baselines.forecast import metrics, predictions
from pipelines.ml.run_baselines import SCHEMA, WINDOWS, approved_sales, fixture_rows
from pipelines.storage.bronze import spark_session


def main():
    spark = spark_session()
    try:
        rows = list(fixture_rows())
        sales = spark.createDataFrame(rows, SCHEMA)
        output = features(sales, *WINDOWS).cache()
        origin = date(2024, 5, 10)
        seasonal = (F.col("tenant") == "SIM_A") & (F.col("product") == "SEASONAL")
        at_origin = F.col("origin") == origin
        expected = {row[4]: row[5] for row in rows if row[0] == "SIM_A" and row[3] == "SEASONAL"}
        prior = [expected[origin - timedelta(days=offset)] for offset in range(28, 0, -1)]
        observed = output.where(seasonal & at_origin).first()
        assert [observed[f"lag_{lag}"] for lag in (7, 14, 28)] == [prior[-7], prior[-14], prior[-28]]
        assert isclose(observed.mean_7, sum(prior[-7:]) / 7)
        assert isclose(observed.mean_28, sum(prior) / 28)
        assert isclose(observed.stddev_28, pstdev(prior))
        assert observed.historical_price == 2.0
        print("PASS independent oracle", observed.origin, prior, observed.mean_7, observed.stddev_28)

        changed = sales.withColumn("quantity", F.when(F.col("day") >= origin, 999).otherwise(F.col("quantity")))
        changed = changed.withColumn("price", F.when(F.col("day") >= origin, 999.0).otherwise(F.col("price")))
        changed = features(changed, *WINDOWS).where(seasonal & at_origin).first()
        labels = {"actuals", "unit_sales_next_7d"}
        assert changed.actuals == [999] * 7 and changed.unit_sales_next_7d == 6993
        assert {k: v for k, v in observed.asDict().items() if k not in labels} == {
            k: v for k, v in changed.asDict().items() if k not in labels}
        print("PASS future target and prices perturbed, lags and rolling windows unchanged")

        p = predictions(output).where(seasonal & at_origin & (F.col("method") == "seasonal_naive"))
        for row in p.orderBy("horizon").collect():
            day = origin + timedelta(days=row.horizon - 1)
            assert row.target_date == day and row.actual == expected[day]
            assert row.prediction == expected[day - timedelta(days=7)]
        print("PASS seven daily horizons and seasonal offsets")

        ranges = {r.split: (r.start, r.end) for r in output.groupBy("split").agg(
            F.min("origin").alias("start"), F.max(F.date_add("origin", 6)).alias("end")).collect()}
        assert ranges["train"][1] < ranges["validation"][0] and ranges["validation"][1] < ranges["test"][0]
        assert output.where("site = 'SIM_NEW_SITE' and split <> 'test'").isEmpty()
        assert not output.where("site = 'SIM_NEW_SITE' and observed_days_28 < 7").isEmpty()
        print("PASS chronological windows and site absent from train", ranges)

        assert output.where("tenant = 'SIM_B'").where(F.col("lag_7") != 15).isEmpty()
        assert observed.mean_7 < 15
        print("PASS identical store and product scoped by tenant")

        intermittent = output.where("product = 'INTERMITTENT'")
        assert not intermittent.where("missing_days_28 > 0 AND observed_days_28 >= 26").isEmpty()
        assert not intermittent.where("unit_sales_next_7d IS NULL").isEmpty()
        zero = metrics(predictions(output.where("product = 'ZERO'"))).collect()
        assert zero and all(r.mae == 0 and r.wape is None for r in zero)
        print("PASS gaps excluded, genuine zeros retained, WAPE undefined for zero denominator")

        try:
            features(sales.unionByName(sales.limit(1)), *WINDOWS)
        except ValueError as exc:
            assert str(exc) == "invalid_synthetic_sales"
        else:
            raise AssertionError("duplicate input accepted")
        try:
            features(sales.withColumn("simulated", F.lit(False)), *WINDOWS)
        except ValueError as exc:
            assert str(exc) == "invalid_synthetic_sales"
        else:
            raise AssertionError("non-synthetic input accepted")
        print("PASS duplicate and non-synthetic input rejected")

        before, original = approved_sales(spark, 0)
        blocked, conflict = approved_sales(spark, 1)
        restored, corrected = approved_sales(spark, 2)
        days = [df.orderBy("site", "product", "day").collect() for df in (before, blocked, restored)]
        assert len({original, conflict, corrected}) == 3
        assert [sum(row.quantity for row in stage) for stage in days] == [6, 4, 6]
        assert days[0] == days[2]
        assert all(row.day != date(2024, 1, 6) for row in days[1])
        old_features = features(before, *WINDOWS)
        changed_features = features(blocked, *WINDOWS)
        assert old_features.where("origin = '2024-01-13' AND lag_7 = 2").count() == 1
        assert changed_features.where("origin = '2024-01-13' AND lag_7 = 2").isEmpty()
        reread, identity = approved_sales(spark, 0)
        assert identity == original and reread.orderBy("site", "product", "day").collect() == days[0]
        print("PASS pinned calendar correction and quarantine, original snapshot still reproducible",
              original, conflict, corrected)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
