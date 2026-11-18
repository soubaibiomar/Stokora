"""Static core checks, without services or credential files."""

import json
import os
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[3]


class CoreConfigTests(unittest.TestCase):
    def render(self, **settings):
        env = dict(os.environ, POSTGRES_IMAGE="postgres:16.16-bookworm",
                   POSTGRES_PASSWORD_FILE="../../config/secrets/static-bootstrap",
                   CORE_SECRET_DIR="../../config/secrets/static-cdc")
        env.update(settings)
        return subprocess.run(
            ["docker", "compose", "-f", "infra/compose/compose.yaml", "-f",
             "infra/compose/core.yml", "--profile", "core", "config", "--format", "json"],
            env=env, cwd=ROOT, capture_output=True, text=True, check=False,
        )

    def test_core_is_private_and_bounded(self):
        result = self.render()
        self.assertEqual(result.returncode, 0, result.stderr)
        model = json.loads(result.stdout)
        services = model["services"]
        self.assertEqual(set(services), {"postgres", "kafka", "connect"})
        self.assertTrue(model["networks"]["data"]["internal"])
        for service in services.values():
            self.assertEqual(set(service["networks"]), {"data"})
            self.assertEqual(service["profiles"], ["core"])
            self.assertFalse(service.get("ports"))
            self.assertFalse(service.get("privileged"))
            self.assertIn("no-new-privileges:true", service["security_opt"])
            self.assertIn("test", service["healthcheck"])
        self.assertEqual(sum(float(service["cpus"]) for service in services.values()), 3)
        self.assertLessEqual(sum(int(service["mem_limit"]) for service in services.values()), 3072 * 1024**2)

    def test_storage_and_secret_scope(self):
        result = self.render()
        self.assertEqual(result.returncode, 0, result.stderr)
        model = json.loads(result.stdout)
        for name, expected in {
            "postgres": {"postgres_password", "cdc_password"},
            "kafka": {"broker_jaas", "broker_admin", "broker_observer"},
            "connect": {"connect_jaas", "connect_database"},
        }.items():
            self.assertEqual({mount["source"] for mount in model["services"][name]["secrets"]}, expected)
        for name, path in (("postgres", "/var/lib/postgresql/data"), ("kafka", "/var/lib/kafka/data")):
            self.assertIn((f"{name}_data", path, "volume"), {
                (mount["source"], mount["target"], mount["type"])
                for mount in model["services"][name]["volumes"]
            })

    def test_missing_core_secret_directory_is_refused(self):
        result = self.render(CORE_SECRET_DIR="")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CORE_SECRET_DIR", result.stderr)


if __name__ == "__main__":
    unittest.main()
