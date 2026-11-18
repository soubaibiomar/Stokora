"""Run an ENOSPC check solely on a Docker tmpfs, with no protected mounts."""
import os
from pathlib import Path

from pyspark.sql import functions as F
from pipelines.storage.bronze import spark_session, check_space


def main():
    root = Path("/audit-tmpfs")
    mount = next(line.split() for line in Path("/proc/self/mountinfo").read_text().splitlines()
                 if line.split()[4] == str(root))
    assert mount[mount.index("-") + 1] == "tmpfs"
    assert 80 * 1024**2 <= os.statvfs(root).f_blocks * os.statvfs(root).f_frsize <= 96 * 1024**2
    table = str(root / "delta")
    guard = root / "only-audit-reservation"
    spark = spark_session()
    try:
        def payload(start, end):
            return spark.range(start, end, numPartitions=2).select(
                "id", F.sha2(F.concat(F.lit("disposable-audit"), F.col("id").cast("string")), 512).alias("payload"))
        payload(0, 3).write.format("delta").save(table)
        stat = os.statvfs(root)
        with guard.open("wb") as handle:
            os.posix_fallocate(handle.fileno(), 0, stat.f_bavail * stat.f_frsize - 1024**2)
        before = set((root / "delta").rglob("*.parquet"))
        def versions():
            return sorted(p.name for p in Path(table, "_delta_log").glob("*.json"))
        initial = versions()
        assert len(initial) == 1
        try:
            check_space(root, 0)
        except ValueError as exc:
            assert "INSUFFICIENT_DISK" in str(exc)
            print("PASS preflight rejects limited capacity", type(exc).__name__)
        else:
            raise AssertionError("capacity check accepted")
        try:
            payload(3, 400_003).write.format("delta").mode("append").save(table)
        except Exception as exc:
            assert "No space left on device" in str(exc) or "ENOSPC" in str(exc), str(exc)[:500]
            print("PASS active Delta write hits ENOSPC, no commit published", type(exc).__name__)
        else:
            raise AssertionError("expected ENOSPC")
        assert versions() == initial and spark.read.format("delta").load(table).count() == 3
        leftovers = set((root / "delta").rglob("*.parquet")) - before
        print("PASS last snapshot readable; unreferenced parquet count", len(leftovers))
        stat = os.statvfs(root)
        remaining = stat.f_bavail * stat.f_frsize
        if remaining:
            with guard.open("ab") as handle:
                os.posix_fallocate(handle.fileno(), handle.tell(), remaining)
        assert os.statvfs(root).f_bavail == 0
        try:
            spark.sql(f"ALTER TABLE delta.`{table}` SET TBLPROPERTIES ('audit.boundary' = 'enospc')")
        except Exception as exc:
            assert "No space left on device" in str(exc) or "ENOSPC" in str(exc), str(exc)[:500]
            assert versions() == initial
            print("PASS metadata-only Delta transaction cannot publish log at zero capacity")
        else:
            raise AssertionError("metadata commit succeeded at zero capacity")
        guard.unlink()
        spark.stop()
        spark = spark_session()
        for _ in range(2):
            (payload(3, 5).write.format("delta").mode("append")
             .option("txnAppId", "synthetic-disk-retry").option("txnVersion", 0).save(table))
        assert len(versions()) == 2 and [r.id for r in spark.read.format("delta").load(table).orderBy("id").collect()] == list(range(5))
        assert spark.read.format("delta").option("versionAsOf", 0).load(table).count() == 3
        print("PASS capacity-restored append and version-zero time travel")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
