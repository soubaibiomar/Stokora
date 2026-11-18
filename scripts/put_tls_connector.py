"""Publish the versioned connector contract with mandatory hostname verification."""

import argparse
import json
from pathlib import Path

from scripts.smoke_cdc import Core


def apply(core):
    path = Path(__file__).resolve().parents[1] / "infra/kafka/connectors/sales.json"
    config = json.loads(path.read_text(encoding="utf-8"))
    config.update({"database.sslmode": "verify-full", "database.sslrootcert": "/tls/ca.pem"})
    core.connect("connectors/sales/config", "-X", "PUT", "-H", "Content-Type: application/json",
                 "--data-binary", "@-", sql=json.dumps(config))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    args = parser.parse_args()
    core = Core(args.project)
    core.prefix[-2:-2] = ["-f", "infra/compose/core-tls.yml"]
    apply(core)
