"""Select known synthetic daily revisions at a declared UTC forecast origin.

The source supplies business date, availability timestamp and revision sequence.
Production Gold lacks complete availability history and must not use this adapter
with timestamps derived from business date or the latest snapshot publication.
"""

from pyspark.sql import Window, functions as F
from pyspark.sql.types import (BooleanType, DateType, IntegralType, StringType,
                               TimestampNTZType, TimestampType)

from pipelines.features.forecast import KEY


def history_as_of(revisions, origins):
    """Return last known accepted revision per series, date, kind and origin.

    Origins are timestamps. Kinds distinguish sale, price and calendar revisions.
    A later quarantined or retracted revision supersedes the earlier value only
    after it becomes available. Conflicting revision identities fail closed.
    """
    identity = [*KEY, "day", "kind", "available_at", "revision"]
    for frame, fields in (
        (origins, {name: StringType for name in KEY}),
        (revisions, {**{name: StringType for name in (*KEY, "kind", "state")},
                     "day": DateType, "revision": IntegralType, "simulated": BooleanType}),
    ):
        if any(name not in frame.columns or not isinstance(frame.schema[name].dataType, kind)
               for name, kind in fields.items()):
            raise ValueError("invalid_availability_schema")
    if "origin" not in origins.columns or "available_at" not in revisions.columns:
        raise ValueError("invalid_availability_schema")
    clock = origins.schema["origin"].dataType
    if (not isinstance(clock, (TimestampType, TimestampNTZType))
            or revisions.schema["available_at"].dataType != clock
            or origins.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC"):
        raise ValueError("invalid_utc_availability_clock")
    invalid_origin = F.col("origin").isNull()
    for name in KEY:
        invalid_origin = invalid_origin | F.col(name).isNull()
    if origins.where(invalid_origin).limit(1).count():
        raise ValueError("invalid_forecast_origin")
    invalid = F.col("simulated").isNull() | ~F.col("simulated") | (F.col("revision") < 0)
    for name in identity:
        invalid = invalid | F.col(name).isNull()
    if (revisions.where(invalid | ~F.coalesce(F.col("state").isin("accepted", "quarantined", "retracted"),
                                             F.lit(False))
                        | ~F.col("kind").isin("sale", "price", "calendar")).limit(1).count()
            or revisions.groupBy(*identity).count().where("count > 1").limit(1).count()):
        raise ValueError("invalid_synthetic_availability")
    scope = origins.select(*KEY, "origin").distinct()
    known = (scope.join(revisions, KEY, "inner")
             .where((F.col("available_at") <= F.col("origin")) &
                    (F.col("day") < F.to_date("origin"))))
    order = Window.partitionBy(*KEY, "origin", "day", "kind").orderBy(
        F.desc("available_at"), F.desc("revision"))
    return (known.withColumn("position", F.row_number().over(order))
            .where("position = 1 AND state = 'accepted'").drop("position"))
