"""Generate daily rolling-origin features for synthetic sales snapshots."""

from datetime import timedelta

from pyspark.sql import Window, functions as F

KEY = ["tenant", "dataset", "site", "product"]
HORIZON = 7
SPEC = "step07-synthetic-v1"


def features(sales, start, validation_start, test_start, end):
    """Input has one accepted row per series/day and an optional point-in-time price.

    Origin is the beginning of a day. Only demand dated before origin is visible.
    Missing daily observations remain null, including in the target.
    """
    if not (start < validation_start < test_start < end
            and min(validation_start - start, test_start - validation_start,
                    end - test_start) >= timedelta(days=7)):
        raise ValueError("invalid_forecast_windows")
    daily = sales.where(F.col("day") <= end)
    invalid = F.col("simulated").isNull() | ~F.col("simulated")
    for column in [*KEY, "day", "quantity"]:
        invalid = invalid | F.col(column).isNull()
    if (sales.where(invalid | (F.col("quantity") < 0)).limit(1).count()
            or daily.groupBy(*KEY, "day").count().where("count > 1").limit(1).count()):
        raise ValueError("invalid_synthetic_sales")
    grid = (daily.groupBy(*KEY).agg(F.min("day").alias("first_day"))
            .select(*KEY, "first_day", F.explode(F.sequence("first_day", F.lit(end))).alias("day"))
            .join(daily.select(*KEY, "day", "quantity", "price"), [*KEY, "day"], "left"))
    series = Window.partitionBy(*KEY).orderBy("day")
    past = series.rowsBetween(-28, -1)
    recent = series.rowsBetween(-7, -1)
    observed = F.last("quantity", ignorenulls=True).over(past)
    frame = grid.select(
        *KEY, "first_day", F.col("day").alias("origin"),
        *(F.lag("quantity", days).over(series).alias(f"lag_{days}") for days in (7, 14, 28)),
        F.avg("quantity").over(recent).alias("mean_7"),
        F.avg("quantity").over(past).alias("mean_28"),
        F.stddev_pop("quantity").over(past).alias("stddev_28"),
        F.count("quantity").over(past).alias("observed_days_28"),
        F.sum(F.when(F.col("quantity") == 0, 1).otherwise(0)).over(past).alias("zero_days_28"),
        F.last("price", ignorenulls=True).over(past).alias("historical_price"),
        observed.alias("last_observed"),
        F.array(*(F.lag("quantity", days).over(series) for days in range(7, 0, -1))).alias("prior_week"),
        F.array("quantity", *(F.lead("quantity", days).over(series)
                              for days in range(1, HORIZON))).alias("actuals"),
    )
    target_end = F.date_add("origin", HORIZON - 1)
    split = (F.when(target_end < validation_start, "train")
             .when((F.col("origin") >= validation_start) & (target_end < test_start), "validation")
             .when((F.col("origin") >= test_start) & (target_end <= end), "test"))
    return (frame.where((F.col("origin") >= start) & (F.col("origin") > F.col("first_day")))
            .drop("first_day").withColumn("split", split)
            .where(F.col("split").isNotNull())
            .withColumn("unit_sales_next_7d", F.when(
                ~F.exists("actuals", lambda value: value.isNull()),
                F.aggregate("actuals", F.lit(0).cast("long"), lambda total, value: total + value)))
            .withColumn("trend_7_vs_28", F.col("mean_7") - F.col("mean_28"))
            .withColumn("zero_fraction_28", F.when(F.col("observed_days_28") > 0,
                                                 F.col("zero_days_28") / F.col("observed_days_28")))
            .withColumn("missing_days_28", 28 - F.col("observed_days_28"))
            .withColumn("simulated", F.lit(True)).withColumn("unit", F.lit("each")))
