"""Exercise calendar rejection and correction through the real importer on a synthetic copy."""

from collections import Counter
import json
from pathlib import Path
import shutil

from pyspark.sql import functions as F

from pipelines.batch.import_m5 import ingest
from pipelines.gold.publish import CDC, OUTPUT, rebuild
from pipelines.storage.bronze import spark_session


def main():
    root = OUTPUT / "calendar-import"
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    for source in Path("/fixtures").glob("*.csv"):
        shutil.copyfile(source, inputs / source.name)
    calendar = inputs / "calendar.csv"
    original = calendar.read_bytes()
    header, first, second, *remaining = original.splitlines()
    # A distinct, valid file revision is required for the existing content-addressed importer.
    corrected = b"\n".join([header, second, first, *remaining]) + b"\n"
    oracle = {
        ("FIX_A", "FIX_P", "2024-01-06"): 2, ("FIX_A", "FIX_P", "2024-01-07"): 0,
        ("FIX_A", "FIX_P", "2024-01-08"): 3, ("FIX_B", "FIX_Q", "2024-01-06"): 0,
        ("FIX_B", "FIX_Q", "2024-01-08"): 1,
    }
    affected = {key for key in oracle if key[2] == "2024-01-06"}
    bad = original.rstrip(b"\r\n") + b"\n2024-02-31,1,d_1\n"
    bronze, derived = root / "m5_bronze", root / "lambda_snapshot"
    spark = spark_session()
    try:
        snapshots = []
        for version, content in enumerate((original, bad, corrected)):
            calendar.write_bytes(content)
            assert ingest(spark, inputs, root, synthetic=True, days=3)["status"] == "committed"
            batch = spark.read.format("delta").option("versionAsOf", version).load(str(bronze))
            if version == 1:
                rejected = batch.where("source_table='calendar' AND record_type='reject'").collect()
                assert len(rejected) == 1 and rejected[0].raw == "2024-02-31,1,d_1"
                assert rejected[0].reason == "invalid_value"
            snapshot, written = rebuild(spark, root, str(bronze), CDC, version, 0)
            assert written
            snapshots.append(snapshot)
            table = spark.read.format("delta").option("versionAsOf", version).load(str(derived))
            rows = table.where(F.col("snapshot") == snapshot).collect()
            expected = {k: v for k, v in oracle.items() if version != 1 or k not in affected}
            actual = {(r.site, r.product, str(r.day)): r.quantity for r in rows if r.kind == "sales_daily"}
            assert actual == expected
            quarantines = Counter(r.reason for r in rows if r.kind == "quarantine")
            assert quarantines == ({"batch_invalid_or_missing_cell": 1, "invalid_value": 1,
                                    "calendar_reference_conflict": 2} if version == 1
                                   else {"batch_invalid_or_missing_cell": 1})
            if version == 1:
                for row in (r for r in rows if r.reason == "calendar_reference_conflict"):
                    assert row.day is None and row.event_id == "d_1" and row.source_ref
                    candidates = json.loads(row.detail)["calendar"]
                    assert {candidate[0] for candidate in candidates} == {"2024-01-06", "2024-02-31"}
            quality = [json.loads(r.detail) for r in rows if r.kind == "quality"]
            assert len(quality) == 1 and quality[0]["quarantine"] == sum(quarantines.values())
            commit = [json.loads(line) for line in (derived / "_delta_log" / f"{version:020}.json")
                      .read_text().splitlines()]
            assert next(r["commitInfo"] for r in commit if "commitInfo" in r)["userMetadata"] == snapshot
            print("PASS real importer", version, "Gold", sum(actual.values()), "quarantine", dict(quarantines),
                  "snapshot", snapshot, flush=True)

        def log_bytes():
            return {f.name: f.read_bytes() for f in (derived / "_delta_log").glob("*.json")}

        baseline = log_bytes()
        assert len(baseline) == 3 and rebuild(spark, root, str(bronze), CDC, 2, 0) == (snapshots[2], False)
        assert log_bytes() == baseline
        calendar.write_bytes(corrected + b"2024-02-31,1,d_1,unexpected\n")
        assert ingest(spark, inputs, root, synthetic=True, days=3)["status"] == "committed"
        bad_width = spark.read.format("delta").option("versionAsOf", 3).load(str(bronze)).where(
            "source_table='calendar' AND record_type='reject' AND reason='column_count'",
        ).collect()
        assert len(bad_width) == 1 and bad_width[0].raw.endswith(",d_1,unexpected")
        try:
            rebuild(spark, root, str(bronze), CDC, 3, 0)
        except ValueError as error:
            assert str(error) == "calendar_rejected_key_unrecoverable"
        else:
            raise AssertionError("Unscoped calendar rejection was accepted")
        assert log_bytes() == baseline
        print("PASS correction replay and unresolved calendar key leave all three Delta commits unchanged", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
