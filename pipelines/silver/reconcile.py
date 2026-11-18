"""Build canonical fixture sales and retain Bronze references for every decision."""

from collections import defaultdict
import csv
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json
import re

from pipelines.adapters.m5 import DIMENSIONS
from pipelines.silver.overlap_policy import authorize
from pipelines.streaming.merge_bronze import valid_id, valid_state

GRANT = {"SIM_A": "SIM_SITE", "SIM_B": "SIM_SITE"}
EPOCH = date(1970, 1, 1)
FIRST_DAY = date(1900, 1, 1)
LAST_DAY = date(2100, 12, 31)


def batch_sales(rows, version):
    current = {}
    for row in rows:
        if (row.record_type == "load" and row.stage_status == "committed"
                and row.dataset == "m5_fixture" and row.is_simulated is True and row.schema_version == 1):
            old = current.get(row.source_table)
            if old is None or (row.ingested_at, row.ingestion_run_id) > (old.ingested_at, old.ingestion_run_id):
                current[row.source_table] = row
    if set(current) != {"sales", "calendar", "prices"}:
        raise ValueError("incomplete_batch_manifest")
    if current["sales"].source_columns != DIMENSIONS + ["d_1", "d_2", "d_3"]:
        raise ValueError("unsupported_fixture_sales_schema")
    calendar, prices, sales, rejects = defaultdict(list), defaultdict(list), [], []
    for row in sorted(rows, key=lambda r: (r.source_table, r.source_line or 0, r.source_file)):
        load = current.get(row.source_table)
        if load is None or row.source_hash != load.source_hash or row.record_type == "load":
            continue
        ref = f"{row.source_file}:{row.source_hash}:{row.source_line}"
        base = dict(dataset="m5_fixture", tenant="SIM_M5", source_ref=ref, bronze_version=version,
                    refreshed_source=row.ingested_at, simulated=True)
        if (row.dataset != load.dataset or row.is_simulated is not True or row.schema_version != load.schema_version
                or row.origin != load.origin or row.source_file != load.source_file):
            rejects.append(dict(base, reason="batch_reference_scope"))
            continue
        rejected = row.record_type != "raw" or row.stage_status != "accepted"
        if rejected and row.source_table != "calendar":
            rejects.append(dict(base, reason=row.reason or "unaccepted_bronze_record"))
            continue
        if rejected:
            try:
                values = next(csv.reader([row.raw], strict=True))
                if len(values) != len(load.source_columns):
                    raise ValueError("ambiguous field layout")
                key = values[load.source_columns.index("d")]
                if not re.fullmatch(r"d_[1-9][0-9]{0,3}", key):
                    raise ValueError("invalid day key")
            except (csv.Error, IndexError, StopIteration, TypeError, ValueError):
                # An unscoped rejection could contradict any date in the active manifest.
                raise ValueError("calendar_rejected_key_unrecoverable") from None
            text_day = values[load.source_columns.index("date")]
            week = values[load.source_columns.index("wm_yr_wk")]
            calendar[key].append((None, week, text_day, ref))
            rejects.append(dict(base, event_id=key, reason=row.reason or "unaccepted_bronze_record"))
            continue
        try:
            values = next(csv.reader([row.raw], strict=True))
            data = dict(zip(load.source_columns, values, strict=True))
            if row.source_table == "calendar":
                key = data["d"]
                text_day, week = data["date"], data["wm_yr_wk"]
                try:
                    day = date.fromisoformat(text_day)
                    if (not FIRST_DAY <= day <= LAST_DAY or day.isoformat() != text_day
                            or not re.fullmatch(r"d_[1-9][0-9]{0,3}", key) or not re.fullmatch(r"[0-9]{1,9}", week)):
                        raise ValueError("invalid calendar attributes")
                except ValueError:
                    calendar[key].append((None, week, text_day, ref))
                    raise
                calendar[key].append((day, week, text_day, ref))
            elif row.source_table == "prices":
                key = data["store_id"], data["item_id"], data["wm_yr_wk"]
                price = Decimal(data["sell_price"]) if data["sell_price"] else None
                if price is not None and (not price.is_finite() or price <= 0):
                    raise ValueError("invalid price")
                prices[key].append((price, ref))
            else:
                sales.append((base, data))
        except (ValueError, KeyError, csv.Error, ArithmeticError):
            rejects.append(dict(base, reason="batch_reference_or_schema"))
    facts = defaultdict(list)
    for base, data in sales:
        site, product = data["store_id"], data["item_id"]
        for key, quantity in data.items():
            if not key.startswith("d_"):
                continue
            refs = calendar.get(key, ())
            values = {(day, week) for day, week, _, _ in refs}
            cell = dict(base, site=site, product=product, event_id=key)
            if not refs or len(values) != 1 or any(day is None for day, _ in values):
                reason = ("calendar_reference_missing" if not refs else
                          "calendar_reference_conflict" if len(values) != 1 else "calendar_reference_invalid")
                candidates = sorted({(text_day, week, ref) for _, week, text_day, ref in refs})
                rejects.append(dict(cell, reason=reason, detail=json.dumps({"cell": key, "calendar": candidates})))
                continue
            if (not valid_id(site) or not valid_id(product) or not quantity.isascii()
                    or not quantity.isdecimal() or len(quantity) > 9):
                rejects.append(dict(base, site=site, product=product, event_id=key, reason="batch_invalid_or_missing_cell"))
                continue
            day, week = next(iter(values))
            calendar_refs = sorted({ref for _, _, _, ref in refs})
            price_options = prices.get((site, product, week), ())
            price_values = {price for price, _ in price_options}
            if len(price_values) > 1:
                rejects.append(dict(cell, day=day, reason="price_reference_conflict",
                                    detail=json.dumps(sorted({ref for _, ref in price_options}))))
                continue
            price = next(iter(price_values), None)
            price_refs = sorted({ref for _, ref in price_options})
            event = f"{site}:{product}:{day}"
            facts[event].append(dict(base, site=site, product=product, day=day, event_id=event,
                                     quantity=int(quantity), price=price, currency="USD", op="batch",
                                     detail=json.dumps({"calendar": calendar_refs, "price": price_refs, "cell": key})))
    accepted = []
    for group in facts.values():
        first = group[0]
        if any(r["quantity"] != first["quantity"] for r in group):
            rejects.extend(dict(row, reason="conflicting_batch_grain") for row in group)
        else:
            accepted.append(first)
    return accepted, rejects


def cdc_sales(rows, version, binding):
    if not binding:
        raise ValueError("CDC Bronze requires a verified source binding")
    epoch = hashlib.sha256(json.dumps(binding, separators=(",", ":")).encode()).hexdigest()
    receipts = {r.receipt_id: r for r in rows if r.record_kind == "receipt"}
    events, rejects, groups = [], [], defaultdict(list)
    for row in rows:
        base = dict(dataset="cdc:" + epoch, tenant=row.tenant_id, site=row.site_id, simulated=True,
                    event_id=row.business_event_id, source_ref=row.ledger_id, bronze_version=version,
                    refreshed_source=row.ingested_at)
        if row.record_kind == "receipt":
            if row.disposition == "quarantine":
                rejects.append(dict(base, reason=row.reason))
            continue
        try:
            receipt = receipts[row.receipt_id]
            if (receipt.disposition != "accepted" or row.event_uid != receipt.event_uid
                    or row.raw_value != receipt.raw_value or row.raw_key != receipt.raw_key
                    or row.payload_sha256 != hashlib.sha256(row.raw_value).hexdigest()
                    or row.source_epoch != epoch or row.schema_version != 1
                    or [row.kafka_cluster_id, row.kafka_topic_id, row.kafka_topic] != binding
                    or GRANT.get(row.tenant_id) != row.site_id):
                raise ValueError("provenance")
            payload = json.loads(row.raw_value)
            before, after = payload["before"], payload["after"]
            key = dict(tenant_id=row.tenant_id, site_id=row.site_id, event_id=row.business_event_id)
            if row.operation not in ("c", "r", "u", "d") or payload["op"] != row.operation:
                raise ValueError("operation")
            if not (valid_state(before, key) if row.operation in ("u", "d") else before is None):
                raise ValueError("before")
            if not (valid_state(after, key) if row.operation != "d" else after is None):
                raise ValueError("after")
            state = after or before
            day = EPOCH + timedelta(days=state["business_date"])
            if not FIRST_DAY <= day <= LAST_DAY or state["product_id"] != "SIM_PRODUCT":
                raise ValueError("unknown product or invalid business date")
            event = dict(base, op=row.operation, product=state["product_id"], day=day,
                         currency=state["currency"], quantity=after["quantity"] if after else 0,
                         price=Decimal(state["unit_price_minor"]) / 100,
                         source_ms=row.source_time_ms, detail=row.receipt_id)
            groups[(row.tenant_id, row.site_id, row.business_event_id)].append((
                (row.source_lsn, row.kafka_partition, row.kafka_offset), row.event_uid, event, before, after,
            ))
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
            rejects.append(dict(base, reason="invalid_cdc_lineage_or_state"))
    latest = []
    for mutations in groups.values():
        seen, state, current = set(), None, None
        for _, uid, event, before, after in sorted(mutations, key=lambda item: item[0]):
            if uid in seen:
                continue
            seen.add(uid)
            if before != state or (event["op"] in ("u", "d") and state is None):
                rejects.append(dict(event, reason="conflicting_cdc_transition"))
                continue
            events.append(event)
            state, current = after, event
        if current is not None:
            latest.append(current)
    return events, latest, rejects


def reconcile(batch, events, latest, mapping, *, source_table_id, as_of):
    """A trusted fixture mapping replaces one entire historical cell with one sale."""
    batch_by_id = {r["event_id"]: r for r in batch}
    latest_by_id = {(r["tenant"], r["site"], r["event_id"]): r for r in latest}
    original = defaultdict(list)
    for row in events:
        if row["op"] in ("c", "r"):
            original[(row["tenant"], row["site"], row["event_id"])].append(row)
    used_batch, used_cdc = set(), set()
    current = []
    if not isinstance(mapping, (list, tuple)):
        raise ValueError("unverified_synthetic_overlap")
    for link in mapping:
        if (not isinstance(link, dict) or link.keys() != {"batch_event_id", "cdc_key", "approval_id"}
                or not isinstance(link["batch_event_id"], str) or not isinstance(link["approval_id"], str)
                or not isinstance(link["cdc_key"], list) or len(link["cdc_key"]) != 3
                or not all(valid_id(field) for field in link["cdc_key"])):
            raise ValueError("unverified_synthetic_overlap")
        batch_id = link["batch_event_id"]
        key = tuple(link["cdc_key"])
        historical, starts, last = batch_by_id.get(batch_id), original[key], latest_by_id.get(key)
        if batch_id in used_batch or key in used_cdc or not (historical and last) or len(starts) != 1:
            raise ValueError("unverified_synthetic_overlap")
        if any(last[field] != starts[0][field] for field in ("dataset", "product", "day")):
            raise ValueError("overlap_approval_denied")
        authorize(link["approval_id"], source_table_id, historical, starts[0], as_of)
        used_batch.add(batch_id)
        used_cdc.add(key)
        if last["op"] != "d":
            current.append(dict(last, overlap_ref=historical["source_ref"]))
    current.extend(r for r in batch if r["event_id"] not in used_batch)
    current.extend(r for key, r in latest_by_id.items() if key not in used_cdc and r["op"] != "d")
    return current
