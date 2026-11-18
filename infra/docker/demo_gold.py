"""Publish three replayable synthetic calendar revisions using the existing Bronze and Gold jobs."""
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from pipelines.batch.import_m5 import ingest
from pipelines.gold.publish import rebuild, CDC
from pipelines.storage.bronze import spark_session

root=Path("/gold/calendar-import")
root.mkdir(exist_ok=True)
spark=spark_session()
try:
    with TemporaryDirectory() as scratch:
        inputs=Path(scratch)
        for source in Path("/fixtures").glob("*.csv"):
            shutil.copyfile(source,inputs/source.name)
        calendar=inputs/"calendar.csv"
        initial=calendar.read_bytes()
        header,first,second,*rest=initial.splitlines()
        revisions=[initial,initial.rstrip()+b"\n2024-02-31,1,d_1\n",b"\n".join([header,second,first,*rest])+b"\n"]
        cdc_version=spark.sql(f"DESCRIBE HISTORY delta.`{CDC}`").select("version").first()[0]
        ledger=spark.read.format("delta").option("versionAsOf",cdc_version).load(CDC)
        receipts=ledger.where("record_kind='receipt'").collect()
        if sorted(r.kafka_offset for r in receipts)!=list(range(8)) or sorted(r.operation for r in receipts if r.disposition=="accepted")!=["c"]*5+["d","u"] or sum(r.disposition=="tombstone" for r in receipts)!=1:
            raise ValueError("Unexpected CDC ledger")
        print("CDC version",cdc_version,"eight receipts, seven mutations, one tombstone",flush=True)
        for version,content in enumerate(revisions):
            calendar.write_bytes(content)
            ingest(spark,inputs,Path("/batch"),synthetic=True,days=3)
            snapshot,_=rebuild(spark,root,"/batch/m5_bronze",CDC,version,cdc_version)
            data=spark.read.format("delta").load(str(root/"lambda_snapshot"))
            snapshot_data=data.where(data.snapshot==snapshot)
            if snapshot_data.where("kind='silver_cdc' AND tenant='SIM_A' AND event_id='demo-authorized'").count()!=2:
                raise ValueError("Authorized CDC mutations missing from Silver")
            if snapshot_data.where("kind='silver_cdc' AND tenant LIKE 'SIM_TENANT_%'").count():
                raise ValueError("Out-of-scope CDC tenant entered Silver")
            units=data.where((data.snapshot==snapshot)&(data.kind=="sales_daily")&(data.tenant=="SIM_M5")).groupBy().sum("quantity").first()[0]
            if units != (6,4,6)[version]:raise ValueError("Calendar correction mismatch")
            print("Synthetic Gold",version,units,snapshot,flush=True)
finally:
    spark.stop()
