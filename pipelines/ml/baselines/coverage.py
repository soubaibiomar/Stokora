"""Audit common scoring support without inventing labels or categories."""

from pyspark.sql import functions as F

from pipelines.features.forecast import KEY
from pipelines.ml.baselines.forecast import metrics


def coverage(forecasts, sales):
    observed = sales.groupBy(*KEY).agg(F.max("day").alias("last_observed_day"))
    rows = forecasts.join(observed, KEY, "left")
    reason = (F.when(F.col("prediction").isNull(), "no_prediction")
              .when(F.col("actual").isNotNull(), "scored")
              .when(F.col("target_date") > F.col("last_observed_day"), "after_observed_history")
              .otherwise("missing_date_within_history"))
    return rows.withColumn("scoring_status", reason)


def coverage_summary(rows):
    return (rows.groupBy("split", "method", *KEY, "scoring_status", "fallback")
            .agg(F.count("*").alias("forecast_count"),
                 F.countDistinct("target_date").alias("distinct_target_days")))


def category_metrics(forecasts, categories):
    """Join an explicit fixture dimension by the complete series key."""
    if (categories.groupBy(*KEY).count().where("count <> 1").limit(1).count()
            or categories.where("category IS NULL OR trim(category) = ''").limit(1).count()):
        raise ValueError("invalid_category_dimension")
    return metrics(forecasts.join(categories.select(*KEY, "category"), KEY, "inner"),
                   ["tenant", "dataset", "category"])
