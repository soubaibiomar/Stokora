"""Deployment-owned policy for the STEP 06 synthetic fixture only."""

from dataclasses import asdict, dataclass
from datetime import date
import hashlib
import json

VERSION = "step06-overlap-v1"
OPERATION = "replace_historical_cell"
CDC_NAMESPACE = "cdc:" + hashlib.sha256(json.dumps(
    ["MkU3OEVBNTcwNTJENDM2Qk", "pXxvbp-2R6awzONTXwsJZg", "stokora.operational.sales_events"],
    separators=(",", ":"),
).encode()).hexdigest()


@dataclass(frozen=True)
class Approval:
    id: str
    version: str
    approver: str
    valid_from: date
    valid_until: date
    revoked: bool
    operation: str
    source: tuple
    target: tuple
    day: date
    quantity: int


# The Spark container mounts this module read-only; --mapping supplies only requests.
APPROVALS = (
    Approval("fixture-jan06", VERSION, "fixture-owner", date(2026, 1, 1), date(2027, 1, 1),
             False, OPERATION,
             ("c9af2b22-00b3-473e-bff4-0ace224872f9", "m5_fixture", "SIM_M5", "FIX_A", "FIX_P",
              "FIX_A:FIX_P:2024-01-06"),
             (CDC_NAMESPACE, "SIM_A", "SIM_SITE", "SIM_PRODUCT", "snapshot"),
             date(2024, 1, 6), 2),
)
AUTHORITY = frozenset({
    ("fixture-owner", OPERATION,
     ("c9af2b22-00b3-473e-bff4-0ace224872f9", "m5_fixture", "SIM_M5", "FIX_A", "FIX_P",
      "FIX_A:FIX_P:2024-01-06"),
     (CDC_NAMESPACE, "SIM_A", "SIM_SITE", "SIM_PRODUCT", "snapshot")),
})


def fingerprint():
    contents = json.dumps([VERSION, [asdict(a) for a in APPROVALS], sorted(AUTHORITY)],
                          default=str, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(contents.encode()).hexdigest()


def authorize(approval_id, batch_id, historical, initial, as_of, *, approvals=APPROVALS, authority=AUTHORITY):
    source = (batch_id, *(historical[field] for field in ("dataset", "tenant", "site", "product", "event_id")))
    target = tuple(initial[field] for field in ("dataset", "tenant", "site", "product", "event_id"))
    matches = [a for a in approvals if a.id == approval_id]
    if len(matches) != 1:
        raise ValueError("overlap_approval_denied")
    a = matches[0]
    if (a.version != VERSION or a.revoked or not a.valid_from <= as_of < a.valid_until
            or a.operation != OPERATION or (a.approver, a.operation, source, target) not in authority
            or (a.source, a.target) != (source, target)
            or historical["simulated"] is not True or initial["simulated"] is not True
            or not a.day == historical["day"] == initial["day"]
            or not a.quantity == historical["quantity"] == initial["quantity"]):
        raise ValueError("overlap_approval_denied")
    return a.id
