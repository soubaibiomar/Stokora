"""Generate deterministic, explicitly synthetic transactions and reference tables."""

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import tempfile

from pipelines.generators.events import validate_batch

EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)
SOURCE = "stokora_simulator"
SCOPES = {
    "SIM_TENANT_A": ("SIM_SITE_A1", "SIM_SITE_A2"),
    "SIM_TENANT_B": ("SIM_SITE_B1", "SIM_SITE_B2"),
}


def generate(seed):
    if type(seed) is not int or not 0 <= seed <= 2**32 - 1:
        raise ValueError("Seed must be an integer in [0, 2^32-1]")
    rng = random.Random(seed)
    scenario = f"sim-v1-{seed}"
    events, stocks, suppliers = [], [], []
    for tenant, sites in SCOPES.items():
        for site in sites:
            provenance = {
                "tenant_id": tenant, "site_id": site, "source": SOURCE,
                "is_simulated": True, "synthetic_scenario_id": scenario,
            }
            opening = rng.randint(50, 100)
            quantity = rng.randint(1, 10)
            unit_price = rng.randint(100, 1500)
            sale = dict(sale_id=f"{scenario}-{site}-sale", revision=1,
                        product_id="SIM_PRODUCT", quantity=quantity,
                        unit_price_minor=unit_price, currency="MAD")
            stock = dict(product_id="SIM_PRODUCT", unit="each")
            stocks.append(dict(provenance, **stock, stock_initial=opening, currency="MAD",
                               business_date=EPOCH.date().isoformat()))
            suppliers.append(dict(provenance, product_id="SIM_PRODUCT", supplier_id="SIM_SUPPLIER",
                                  lead_time_days=rng.randint(2, 7), currency="MAD"))

            sequence = (
                ("stock_movement", "record", dict(stock, quantity_signed=opening, reason="opening")),
                ("sale_created", "create", sale),
                ("stock_movement", "record", dict(stock, quantity_signed=-quantity, reason="sale")),
                ("sale_updated", "update", dict(sale, revision=2, quantity=quantity + 1)),
                ("stock_movement", "record", dict(stock, quantity_signed=-1, reason="sale")),
                ("sale_cancelled", "cancel", dict(sale_id=sale["sale_id"], revision=3, reason="void")),
                ("stock_movement", "record", dict(stock, quantity_signed=quantity + 1, reason="return")),
            )
            for kind, operation, payload in sequence:
                ordinal = len(events) + 1
                origin = EPOCH + timedelta(minutes=ordinal)
                events.append(dict(
                    provenance, event_id=f"{scenario}-{ordinal}", type=kind, operation=operation,
                    version=1, payload=payload,
                    origin_timestamp=origin.isoformat().replace("+00:00", "Z"),
                    ingestion_timestamp=(origin + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
                ))
    for tenant, sites in SCOPES.items():
        batch = (event for event in events if event["tenant_id"] == tenant)
        if validate_batch(batch, tenant_id=tenant, allowed_sites=sites)["quarantine"]:
            raise ValueError("Generated transactions violated the contract")
    return {
        "events": events,
        "initial_inventory": stocks,
        "suppliers": suppliers,
        "manifest": {
            "seed": seed, "generator_version": 1, "synthetic_scenario_id": scenario,
            "is_simulated": True, "source": SOURCE, "currency": "MAD",
            "events": len(events), "inventory_rows": len(stocks), "supplier_rows": len(suppliers),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.absolute()
    try:
        if output.exists() or output.is_symlink():
            raise ValueError("Output already exists. Choose a fresh directory.")
        scenario = generate(args.seed)
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".scenario-", dir=output.parent) as temporary:
            staging = Path(temporary) / "result"
            staging.mkdir()
            for name, contents in scenario.items():
                (staging / f"{name}.json").write_text(
                    json.dumps(contents, indent=2) + "\n", encoding="utf-8")
            staging.rename(output)
    except ValueError as error:
        parser.exit(2, f"{error}\n")
    except OSError:
        parser.exit(2, "Unable to publish simulation output safely.\n")
    print(json.dumps(scenario["manifest"], sort_keys=True))


if __name__ == "__main__":
    main()
