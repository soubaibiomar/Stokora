import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.smoke_cdc import Core, RAW


def response(text):
    return SimpleNamespace(returncode=0, stdout=text)


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.core = Core("stokora-core-smoke")
        self.connect = response(json.dumps({
            "connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}],
        }))

    def test_running_waits_for_active_streaming_slot(self):
        slots = map(response, ("f|reserved|none", "t|reserved|catchup", "t|reserved|streaming"))
        with (patch.object(self.core, "connect", return_value=self.connect),
              patch.object(self.core, "postgres", side_effect=slots) as postgres,
              patch.object(self.core, "kafka", return_value=response(f"{RAW}:0:6")) as kafka,
              patch("scripts.smoke_cdc.time.sleep")):
            self.core.require_ready()
        self.assertEqual(postgres.call_count, 3)
        kafka.assert_called_once()

    def test_failed_task_is_terminal_without_a_slot_probe(self):
        failed = response(json.dumps({
            "connector": {"state": "RUNNING"}, "tasks": [{"state": "FAILED"}],
        }))
        with (patch.object(self.core, "connect", return_value=failed),
              patch.object(self.core, "postgres") as postgres,
              self.assertRaisesRegex(RuntimeError, "Connector task failed")):
            self.core.require_ready()
        postgres.assert_not_called()

    def test_inactive_slot_hits_deadline(self):
        with (patch.object(self.core, "connect", return_value=self.connect),
              patch.object(self.core, "postgres", return_value=response("f|reserved|none")),
              self.assertRaisesRegex(RuntimeError, "not streaming within 0.01s")):
            self.core.require_ready(timeout=0.01)

    def test_running_wal_cannot_mask_unavailable_kafka(self):
        with (patch.object(self.core, "connect", return_value=self.connect),
              patch.object(self.core, "postgres", return_value=response("t|reserved|streaming")),
              patch.object(self.core, "kafka", return_value=SimpleNamespace(returncode=1, stdout="")),
              self.assertRaisesRegex(RuntimeError, "not streaming within 0.01s")):
            self.core.require_ready(timeout=0.01)
