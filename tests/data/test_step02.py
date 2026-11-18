"""STEP 02 acceptance and negative tests using only project-authored data."""

import copy
import csv
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from pipelines.adapters import m5
from pipelines.generators.events import SCHEMA, VALIDATOR, validate_batch
from pipelines.generators.transactions import SCOPES, generate


class AdapterTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "source"
        shutil.copytree(m5.FIXTURE, self.source)
        self.output = self.root / "result"

    def fixture(self):
        with patch.object(m5, "FIXTURE", self.source):
            return m5.adapt(None, self.output, fixture=True)

    def test_fixture_shape_and_provenance(self):
        profile = self.fixture()
        self.assertEqual((profile["series"], profile["facts"], profile["zero_sales"]), (2, 6, 2))
        self.assertEqual((profile["stores"], profile["products"], profile["missing_sales"]), (2, 2, 1))
        self.assertEqual(profile["zero_sales_without_price"], 1)
        self.assertEqual(profile["positive_sales_without_price"], 0)
        self.assertEqual((profile["first_date"], profile["last_date"]), ("2024-01-06", "2024-01-08"))
        manifest = json.loads((self.output / "manifest.json").read_text())
        with (self.output / "sales.csv").open(newline="") as handle:
            facts = list(csv.DictReader(handle))
        keys = {(f["source_dataset"], f["store_id"], f["product_id"], f["business_date"]) for f in facts}
        self.assertEqual(len(keys), 6)
        self.assertEqual([f["sell_price"] for f in facts], ["2.50", "2.50", "3.00", "", "", "4.25"])
        self.assertEqual([f["quantity"] for f in facts], ["2", "0", "3", "0", "", "1"])
        self.assertEqual({(f["source_dataset"], f["is_simulated"], f["currency"]) for f in facts},
                         {("m5_fixture", "true", "USD")})
        self.assertEqual(len(manifest["sample"]), 5)
        self.assertEqual(manifest["fact_sha256"], m5.file_hash(self.output / "sales.csv"))
        for info in manifest["inputs"]:
            self.assertEqual(info["sha256"], hashlib.sha256((self.source / info["file"]).read_bytes()).hexdigest())
        other = self.root / "replay"
        m5.adapt(None, other, fixture=True)
        self.assertEqual((self.output / "manifest.json").read_bytes(), (other / "manifest.json").read_bytes())
        self.assertEqual((self.output / "sales.csv").read_bytes(), (other / "sales.csv").read_bytes())

    def test_missing_kaggle_file_validation(self):
        (self.source / "sell_prices.csv").unlink()
        with self.assertRaisesRegex(m5.InputError, "Missing or non-regular sell_prices.csv"):
            m5.files_for(self.source, "sales_train_validation.csv")

    def test_real_m5_cli_refuses_before_source_validation(self):
        run = subprocess.run([
            sys.executable, "-m", "pipelines.adapters.m5", "--source", str(self.source),
            "--retrieved-on", "2026-11-17", "--output", str(self.output),
        ], capture_output=True, text=True, check=False)
        self.assertEqual(run.returncode, 2)
        self.assertIn("Real M5 rights and provenance are not cleared", run.stderr)
        self.assertFalse(self.output.exists())

    def test_duplicate_price_then_retry_and_preserve_output(self):
        path = self.source / "sell_prices.csv"
        original = path.read_bytes()
        with path.open("ab") as handle:
            handle.write(b"FIX_A,FIX_P,1,9.99\n")
        with self.assertRaisesRegex(m5.InputError, "Duplicate store/item/week"):
            self.fixture()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob(".m5-*")), [])
        path.write_bytes(original)
        self.fixture()
        digest = m5.file_hash(self.output / "sales.csv")
        with self.assertRaisesRegex(m5.InputError, "Output already exists"):
            self.fixture()
        self.assertEqual(digest, m5.file_hash(self.output / "sales.csv"))

    def test_calendar_missing_and_duplicate(self):
        path = self.source / "calendar.csv"
        contents = path.read_bytes().splitlines(keepends=True)
        path.write_bytes(b"".join(contents[:-1]))
        with self.assertRaisesRegex(m5.InputError, "Calendar coverage"):
            self.fixture()
        path.write_bytes(b"".join(contents + [contents[-1]]))
        with self.assertRaisesRegex(m5.InputError, "Duplicate calendar"):
            self.fixture()
        self.assertFalse(self.output.exists())

    def test_reports_price_calendar_and_sales_gaps(self):
        path = self.source / "sell_prices.csv"
        text = path.read_text(encoding="utf-8-sig").replace("FIX_A,FIX_P,2,3.00", "FIX_A,FIX_P,99,")
        path.write_text(text, encoding="utf-8")
        profile = self.fixture()
        self.assertEqual(profile["price_weeks_missing_calendar"], 1)
        self.assertEqual(profile["missing_price_values"], 1)
        self.assertEqual(profile["positive_sales_without_price"], 1)

    def test_invalid_quantities_series_and_csv_width(self):
        path = self.source / "sales_train_validation.csv"
        original = path.read_text(encoding="utf-8-sig")
        for invalid in (
            original.replace(",2,0,3", ",-2,0,3"),
            original.replace(",2,0,3", ",2,0"),
            original + original.splitlines()[-1] + "\n",
            original.replace("FIX_A,FIX_S", "=formula,FIX_S"),
        ):
            with self.subTest(length=len(invalid)):
                path.write_text(invalid, encoding="utf-8")
                with self.assertRaises(m5.InputError):
                    self.fixture()
                self.assertFalse(self.output.exists())

    def test_input_size_limit(self):
        with patch.object(m5, "SIZE_LIMIT", 10):
            with self.assertRaisesRegex(m5.InputError, "file must be between"):
                self.fixture()

    def test_physical_line_limit_and_licensed_output_location(self):
        path = self.source / "calendar.csv"
        path.write_text("a" * (m5.LINE_LIMIT + 1), encoding="utf-8")
        with self.assertRaisesRegex(m5.InputError, "physical CSV line exceeds limit"):
            self.fixture()
        with self.assertRaisesRegex(m5.InputError, "under data/local"):
            m5.adapt(self.source, m5.ROOT / "docs/forbidden-import", retrieved_on=date(2024, 1, 1))
        self.assertFalse(self.output.exists())

    def test_validation_and_evaluation_windows_on_synthetic_full_width(self):
        for days, evaluation in ((1913, False), (1941, True)):
            with self.subTest(days=days):
                suffix = "evaluation" if evaluation else "validation"
                origin = date(2011, 1, 29)
                codes = [f"d_{i+1}" for i in range(days)]
                with (self.source / "calendar.csv").open("w", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(("date", "wm_yr_wk", "d"))
                    writer.writerows(((origin + timedelta(days=i)).isoformat(), i // 7, day)
                                     for i, day in enumerate(codes))
                with (self.source / f"sales_train_{suffix}.csv").open("w", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(m5.DIMENSIONS + codes)
                    writer.writerow(["a", "p", "d", "c", "s", "x"] + ["0"] * days)
                    writer.writerow(["b", "q", "d", "c", "s", "x"] + ["0"] * days)
                output = self.root / suffix
                profile = m5.adapt(self.source, output, evaluation=evaluation, retrieved_on=origin)
                self.assertEqual((profile["facts"], profile["zero_sales"]), (2 * days, 2 * days))
                info = json.loads((output / "manifest.json").read_text())
                self.assertEqual(info["source_dataset"], "m5")
                self.assertNotIn("sample", info)
                self.assertEqual(info["retrieved_on"], origin.isoformat())

    def test_source_changes_abort_publication(self):
        original = m5.inspect
        with patch.object(m5, "inspect", side_effect=[original(m5.files_for(self.source, "sales_train_validation.csv")), []]):
            with self.assertRaisesRegex(m5.InputError, "Source files changed"):
                self.fixture()
        self.assertFalse(self.output.exists())


class EventTests(unittest.TestCase):
    def setUp(self):
        self.generated = generate(42)
        self.tenant = "SIM_TENANT_A"
        self.sites = SCOPES[self.tenant]
        self.events = [event for event in self.generated["events"] if event["site_id"] == self.sites[0]]
        self.create, self.update, self.cancel = [e for e in self.events if e["type"].startswith("sale_")]

    def validate(self, events):
        return validate_batch(events, tenant_id=self.tenant, allowed_sites=self.sites)

    def test_determinism_and_simulation_labels(self):
        self.assertEqual(self.generated, generate(42))
        self.assertNotEqual(self.generated, generate(43))
        self.assertEqual(len(self.generated["events"]), 28)
        for key in ("events", "initial_inventory", "suppliers"):
            for row in self.generated[key]:
                self.assertEqual((row["source"], row["is_simulated"], row["synthetic_scenario_id"]),
                                 ("stokora_simulator", True, "sim-v1-42"))
        self.assertEqual(len(self.generated["initial_inventory"]), 4)
        self.assertEqual(len(self.generated["suppliers"]), 4)
        self.assertTrue(all(row["lead_time_days"] > 0 for row in self.generated["suppliers"]))

    def test_replacement_cancellation_and_stock_integrity(self):
        result = self.validate([self.create, self.update])
        self.assertEqual(sum(result["sale_units"].values()), self.update["payload"]["quantity"])
        result = self.validate(self.events)
        self.assertEqual(result["quarantine"], [])
        self.assertEqual(list(result["sale_units"].values()), [0])
        stock = sum(e["payload"]["quantity_signed"] for e in result["accepted"] if e["type"] == "stock_movement")
        self.assertEqual(stock, self.generated["initial_inventory"][0]["stock_initial"])

    def test_replay_and_conflicting_duplicate(self):
        replay = dict(self.create, ingestion_timestamp="2024-01-02T00:00:00Z")
        conflict = copy.deepcopy(replay)
        conflict["payload"]["quantity"] += 2
        result = self.validate([self.create, replay, conflict, self.update, self.cancel] * 2)
        self.assertEqual((len(result["accepted"]), result["duplicates"]), (3, 5))
        self.assertEqual(result["quarantine"], [
            {"record": 3, "reason": "event_id_conflict"}, {"record": 8, "reason": "event_id_conflict"},
        ])
        self.assertEqual(sum(result["sale_units"].values()), 0)

    def test_unknown_schema_quantity_currency_date_and_version(self):
        wrong = [
            dict(self.create, type="unknown"), dict(self.create, unexpected="do not log"),
            dict(self.create, is_simulated=False), dict(self.create, version=2),
            dict(self.create, origin_timestamp="2024-02-30T00:00:00Z"),
            dict(self.create, ingestion_timestamp="2023-01-01T00:00:00Z"),
        ]
        for field, value in (("quantity", -2), ("currency", "XYZ"), ("unit_price_minor", 1.5)):
            event = copy.deepcopy(self.create)
            event["payload"][field] = value
            wrong.append(event)
        result = self.validate(wrong)
        self.assertEqual(len(result["quarantine"]), 9)
        self.assertEqual(result["accepted"], [])
        self.assertNotIn("do not log", str(result["quarantine"]))
        self.assertIn("unsupported_version_or_envelope", {r["reason"] for r in result["quarantine"]})

    def test_scope_and_sale_order(self):
        foreign = dict(self.create, tenant_id="SIM_TENANT_B")
        other_site = dict(self.create, site_id="SIM_SITE_B1")
        result = self.validate([foreign, other_site, self.cancel, self.create, self.cancel])
        self.assertEqual([r["reason"] for r in result["quarantine"]],
                         ["scope_mismatch", "scope_mismatch", "invalid_sale_revision", "invalid_sale_revision"])
        self.assertEqual(sum(result["sale_units"].values()), self.create["payload"]["quantity"])

    def test_signed_stock_and_human_approval_schema(self):
        VALIDATOR.check_schema(SCHEMA)
        invalid_movement = copy.deepcopy(self.events[0])
        invalid_movement["payload"]["quantity_signed"] = -1
        approval = dict(self.create, event_id="approval", type="purchase_order_approved", operation="approve",
                        payload=dict(purchase_order_id="po1", product_id="SIM_PRODUCT", quantity=1,
                                     currency="MAD", approved_by="human1", approval_mode="human", revision=1))
        automatic = copy.deepcopy(approval)
        automatic["payload"]["approval_mode"] = "automatic"
        result = self.validate([invalid_movement, approval, automatic])
        self.assertEqual(result["accepted"], [approval])
        self.assertEqual([r["reason"] for r in result["quarantine"]], ["invalid_movement_sign", "schema_violation"])

    def test_batch_bound(self):
        with patch("pipelines.generators.events.MAX_BATCH", 2):
            with self.assertRaisesRegex(ValueError, "batch limit"):
                self.validate(self.events)


if __name__ == "__main__":
    unittest.main()
