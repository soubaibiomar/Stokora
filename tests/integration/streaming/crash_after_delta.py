"""Terminate the test consumer after the Delta write but before Spark acknowledges it."""
import os

from pipelines.streaming.consume_cdc import ROOT, run, session

run(session(), ROOT / "checkpoint-main", once=True, after_commit=lambda: os._exit(73))
