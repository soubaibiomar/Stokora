"""Deterministic baseline forecasts and error summaries in units sold."""

from pyspark.sql import functions as F

from pipelines.features.forecast import HORIZON, KEY


def predictions(origins):
    frame = origins.withColumn("horizon", F.explode(F.sequence(F.lit(1), F.lit(HORIZON))))
    frame = (frame.withColumn("target_date", F.date_add("origin", F.col("horizon") - 1))
             .withColumn("target_weekday", F.dayofweek("target_date"))
             .withColumn("target_month", F.month("target_date"))
             .withColumn("actual", F.element_at("actuals", F.col("horizon"))))
    naive = F.coalesce("last_observed", F.lit(0))
    seasonal = F.element_at("prior_week", F.col("horizon"))
    no_history = F.col("last_observed").isNull()
    fallback = F.when(no_history, "zero").otherwise("last_observed")
    return frame.withColumn("baseline", F.explode(F.array(
        F.struct(F.lit("naive").alias("method"), naive.cast("double").alias("prediction"),
                 F.when(no_history, "zero").otherwise("none").alias("fallback")),
        F.struct(F.lit("seasonal_naive").alias("method"),
                 F.coalesce(seasonal, naive).cast("double").alias("prediction"),
                 F.when(seasonal.isNull(), fallback).otherwise("none").alias("fallback")),
        F.struct(F.lit("moving_average").alias("method"),
                 F.coalesce("mean_7", naive).cast("double").alias("prediction"),
                 F.when(F.col("mean_7").isNull(), fallback).otherwise("none").alias("fallback")),
    ))).select("*", "baseline.*").drop("baseline", "actuals", "prior_week")


def metrics(forecasts, grain=KEY):
    error = F.col("prediction") - F.col("actual")
    return (forecasts.groupBy("split", "method", *grain).agg(
        F.count("*").alias("forecast_count"), F.count("actual").alias("scored_count"),
        F.avg(F.abs(error)).alias("mae"), F.avg(error).alias("signed_bias"),
        F.avg((F.col("prediction") < 0).cast("double")).alias("negative_rate"),
        F.sum(F.abs(error)).alias("absolute_error"), F.sum("actual").alias("actual_units"),
    ).withColumn("wape", F.when(F.col("actual_units") > 0,
                               F.col("absolute_error") / F.col("actual_units")))
        .withColumn("simulated", F.lit(True)).withColumn("unit", F.lit("each")))
