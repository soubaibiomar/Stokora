"""Validate and reconcile a bounded, single-tenant batch against the v1 contract."""

from datetime import datetime
import json
from pathlib import Path

from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "contracts/events/v1.schema.json"
SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA)
MAX_BATCH = 10_000


def apply_sale(event, sales):
    payload = event["payload"]
    key = (event["site_id"], event["source"], event["synthetic_scenario_id"], payload["sale_id"])
    previous = sales.get(key)
    kind = event["type"]
    if kind == "sale_created":
        if previous is not None:
            return "invalid_sale_revision"
        sales[key] = payload
        return None
    if (previous is None or previous["quantity"] == 0
            or payload["revision"] != previous["revision"] + 1):
        return "invalid_sale_revision"
    if kind == "sale_cancelled":
        sales[key] = dict(previous, revision=payload["revision"], quantity=0)
    else:
        if any(payload[field] != previous[field] for field in ("currency", "product_id")):
            return "immutable_sale_identity"
        sales[key] = payload
    return None


def validate_batch(records, *, tenant_id, allowed_sites):
    """Return accepted events, redacted quarantine and net units per scoped sale.

    The caller derives scope from a trusted identity. Lifetime is one ordered batch.
    """
    if not tenant_id or not allowed_sites:
        raise ValueError("Trusted tenant and permitted sites are required")
    accepted, quarantine, seen, sales = [], [], {}, {}
    duplicates = 0
    for position, event in enumerate(records, start=1):
        if position > MAX_BATCH:
            raise ValueError("Event batch limit exceeded")
        reason = None
        if not isinstance(event, dict) or event.get("version") != 1:
            reason = "unsupported_version_or_envelope"
        elif not VALIDATOR.is_valid(event):
            reason = "schema_violation"
        elif event["tenant_id"] != tenant_id or event["site_id"] not in allowed_sites:
            reason = "scope_mismatch"
        else:
            try:
                # The schema requires Z, while Python 3.10 needs an explicit UTC offset.
                origin = datetime.fromisoformat(event["origin_timestamp"].removesuffix("Z") + "+00:00")
                ingestion = datetime.fromisoformat(event["ingestion_timestamp"].removesuffix("Z") + "+00:00")
                if ingestion < origin:
                    reason = "invalid_timestamp_order"
            except ValueError:
                reason = "invalid_timestamp"

        if reason is None:
            key = (event["site_id"], event["source"], event["event_id"])
            logical_event = {name: value for name, value in event.items() if name != "ingestion_timestamp"}
            if key in seen:
                if seen[key] == logical_event:
                    duplicates += 1
                    continue
                reason = "event_id_conflict"
            elif event["type"].startswith("sale_"):
                reason = apply_sale(event, sales)
            elif event["type"] == "stock_movement":
                movement, quantity = event["payload"]["reason"], event["payload"]["quantity_signed"]
                if (movement == "sale" and quantity > 0) or (
                    movement in {"opening", "receipt", "return"} and quantity < 0
                ):
                    reason = "invalid_movement_sign"
        if reason:
            quarantine.append({"record": position, "reason": reason})
        else:
            seen[key] = logical_event
            accepted.append(event)

    return {
        "accepted": accepted,
        "quarantine": quarantine,
        "duplicates": duplicates,
        "sale_units": {key: payload["quantity"] for key, payload in sales.items()},
    }
