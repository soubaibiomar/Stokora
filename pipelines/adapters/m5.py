"""Validate and unpivot local M5 CSV files. Never accesses the network."""

import argparse
from contextlib import closing, contextmanager
import csv
from datetime import date
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import tempfile

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/data/fixtures/m5"
SIZE_LIMIT = 1024**3
LINE_LIMIT = 64 * 1024
DIMENSIONS = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]
FACT_COLUMNS = (
    "source_dataset", "store_id", "product_id", "business_date", "quantity",
    "unit", "sell_price", "currency", "is_simulated", "source_release",
)
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,100}")
PRICE = re.compile(r"[0-9]{1,8}(?:\.[0-9]{1,8})?")
RULES = "https://www.kaggle.com/competitions/m5-forecasting-accuracy/rules"


def require_real_m5_clearance():
    """The approved rights decision is BLOCKED pending documented owner clearance."""
    raise InputError("Real M5 rights and provenance are not cleared.", code="RIGHTS_BLOCKED")


class InputError(ValueError):
    """An input failed its contract, with a message safe for local evidence."""

    def __init__(self, message, *, code="INVALID_INPUT", source_file=None):
        super().__init__(message)
        self.code = code
        self.source_file = source_file


@contextmanager
def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        def lines():
            while line := handle.readline(LINE_LIMIT + 1):
                if len(line) > LINE_LIMIT:
                    raise InputError(f"{path.name}: physical CSV line exceeds limit")
                yield line

        reader = csv.DictReader(lines(), strict=True)
        try:
            columns = reader.fieldnames
        except csv.Error:
            raise InputError(f"{path.name}: invalid headers", code="SCHEMA_ERROR",
                             source_file=path.name) from None
        if not columns or len(columns) > 2000 or len(columns) != len(set(columns)):
            raise InputError(f"{path.name}: invalid headers", code="SCHEMA_ERROR", source_file=path.name)
        yield reader


def files_for(source, sales_name):
    files = [source / name for name in ("calendar.csv", "sell_prices.csv", sales_name)]
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise InputError(f"Missing or non-regular {path.name}. See data/README.md or use --fixture.")
        size = path.stat().st_size
        if not size:
            raise InputError("Required CSV is empty.", code="EMPTY_INPUT", source_file=path.name)
        if size > SIZE_LIMIT:
            raise InputError(f"{path.name}: file must be between 1 byte and 1 GiB")
    return files


def inspect(files):
    metadata = []
    grains = ("day", "store/item/week", "store/item series across days")
    for path, grain in zip(files, grains, strict=True):
        digest = file_hash(path)
        with rows(path) as reader:
            columns = reader.fieldnames
        metadata.append(dict(file=path.name, sha256=digest, bytes=path.stat().st_size,
                             grain=grain, columns=columns))
    return metadata


def require_columns(reader, required):
    if not set(required) <= set(reader.fieldnames):
        raise InputError("CSV is missing required columns")


def require_width(row):
    if None in row or None in row.values():
        raise InputError("CSV row has an unexpected number of fields")


def identifier(value):
    if not IDENTIFIER.fullmatch(value):
        raise InputError("Invalid source identifier")
    return value


def number(value):
    if not re.fullmatch(r"[0-9]{1,9}", value):
        raise InputError("Invalid nonnegative integer")
    return int(value)


def calendar_index(path):
    days = {}
    dates = set()
    with rows(path) as reader:
        require_columns(reader, ("d", "date", "wm_yr_wk"))
        for row in reader:
            require_width(row)
            day = row["d"]
            if not re.fullmatch(r"d_[1-9][0-9]{0,3}", day):
                raise InputError("Invalid calendar day code")
            try:
                business_day = date.fromisoformat(row["date"])
            except ValueError:
                raise InputError("Invalid calendar date") from None
            if day in days or business_day in dates or business_day.isoformat() != row["date"]:
                raise InputError("Duplicate calendar key or invalid date format")
            days[day] = (business_day, number(row["wm_yr_wk"]))
            dates.add(business_day)
            if len(days) > 2500:
                raise InputError("Calendar row limit exceeded")
    return days


def price_rows(path, valid_weeks, profile):
    with rows(path) as reader:
        require_columns(reader, ("store_id", "item_id", "wm_yr_wk", "sell_price"))
        for row in reader:
            require_width(row)
            week = number(row["wm_yr_wk"])
            value = row["sell_price"]
            if week not in valid_weeks:
                profile["price_weeks_missing_calendar"] += 1
            if not value:
                profile["missing_price_values"] += 1
            elif not PRICE.fullmatch(value) or Decimal(value) <= 0:
                raise InputError("Invalid sell price")
            profile["price_rows"] += 1
            entry = identifier(row["store_id"]), identifier(row["item_id"]), week, value or None
            if week in valid_weeks:
                yield entry


def unpivot(files, destination, total_days, simulated):
    calendar = calendar_index(files[0])
    columns = [f"d_{day}" for day in range(1, total_days + 1)]
    profile = dict.fromkeys((
        "calendar_rows", "price_rows", "series", "facts", "zero_sales", "missing_sales",
        "missing_calendar_days", "missing_price_values", "price_weeks_missing_calendar",
        "zero_sales_without_price", "positive_sales_without_price",
    ), 0)
    profile["calendar_rows"] = len(calendar)
    profile["missing_calendar_days"] = sum(day not in calendar for day in columns)
    if profile["missing_calendar_days"]:
        raise InputError("Calendar coverage incomplete for the sales window")
    days = [calendar[day] for day in columns]
    if any((right[0] - left[0]).days != 1 for left, right in zip(days, days[1:])):
        raise InputError("Calendar dates must follow the daily sales window")
    products, stores, seen = set(), set(), set()
    dataset = "m5_fixture" if simulated else "m5"
    sample = []
    with closing(sqlite3.connect(destination / "price-index.sqlite")) as db:
        db.execute("""CREATE TABLE price (
            store TEXT, product TEXT, week INTEGER, value TEXT,
            PRIMARY KEY (store, product, week)
        ) WITHOUT ROWID""")
        try:
            db.executemany("INSERT INTO price VALUES (?, ?, ?, ?)",
                           price_rows(files[1], {week for _, week in calendar.values()}, profile))
            db.commit()
        except sqlite3.IntegrityError:
            raise InputError("Duplicate store/item/week price key") from None

        with rows(files[2]) as reader, (destination / "sales.csv").open(
            "w", encoding="utf-8", newline=""
        ) as out:
            if reader.fieldnames != DIMENSIONS + columns:
                raise InputError("Unexpected sales headers or day range")
            writer = csv.writer(out)
            writer.writerow(FACT_COLUMNS)
            for row in reader:
                require_width(row)
                store, product = identifier(row["store_id"]), identifier(row["item_id"])
                if (store, product) in seen:
                    raise InputError("Duplicate store/item sales series")
                seen.add((store, product))
                if len(seen) > 50_000:
                    raise InputError("Sales series limit exceeded")
                products.add(product)
                stores.add(store)
                weekly_prices = dict(db.execute(
                    "SELECT week, value FROM price WHERE store=? AND product=?", (store, product)
                ))
                profile["series"] += 1
                for column, (day, week) in zip(columns, days, strict=True):
                    raw = row[column]
                    if raw == "":
                        profile["missing_sales"] += 1
                    quantity = number(raw) if raw else None
                    price = weekly_prices.get(week)
                    if quantity == 0:
                        profile["zero_sales"] += 1
                        profile["zero_sales_without_price"] += price is None
                    if quantity and price is None:
                        profile["positive_sales_without_price"] += 1
                    fact = (dataset, store, product, day.isoformat(), quantity, "each", price,
                            "USD", str(simulated).lower(), files[2].stem)
                    writer.writerow(fact)
                    if len(sample) < 5:
                        sample.append(dict(zip(FACT_COLUMNS, fact, strict=True)))
                    profile["facts"] += 1
    (destination / "price-index.sqlite").unlink()
    if not seen:
        raise InputError("No sales series found")
    profile.update(products=len(products), stores=len(stores),
                   first_date=days[0][0].isoformat(), last_date=days[-1][0].isoformat())
    return profile, sample


def adapt(source, output, *, fixture=False, evaluation=False, retrieved_on=None):
    if fixture:
        if source is not None or evaluation or retrieved_on is not None:
            raise InputError("--fixture cannot be combined with licensed input options")
        source = FIXTURE
        total_days = 3
    else:
        if source is None:
            raise InputError("Provide --source for authorized local files or explicitly use --fixture.")
        total_days = 1941 if evaluation else 1913
    sales_name = "sales_train_evaluation.csv" if evaluation else "sales_train_validation.csv"
    files = files_for(source, sales_name)
    if not fixture and retrieved_on is None:
        raise InputError("Provide --retrieved-on for the local Kaggle acquisition.")
    if output.is_symlink():
        raise InputError("Output must not be a symbolic link.")
    output = output.resolve()
    if not fixture and output.is_relative_to(ROOT):
        local = ROOT / "data/local"
        if output == local or not output.is_relative_to(local):
            raise InputError("Licensed outputs inside the repository must be under data/local.")
    if output.exists() or output.is_symlink():
        raise InputError("Output already exists. Choose a fresh directory.")
    metadata = inspect(files)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".m5-", dir=output.parent) as temporary:
        staging = Path(temporary) / "result"
        staging.mkdir()
        profile, sample = unpivot(files, staging, total_days, fixture)
        if inspect(files) != metadata:
            raise InputError("Source files changed during import")
        manifest = {
            "adapter_version": 1,
            "source_dataset": "m5_fixture" if fixture else "m5",
            "is_simulated": fixture,
            "retrieved_on": retrieved_on.isoformat() if retrieved_on else None,
            "source_terms": "Project-authored synthetic fixture" if fixture else RULES,
            "fact_grain": ["source_dataset", "store_id", "product_id", "business_date"],
            "inputs": metadata,
            "profile": profile,
            "fact_sha256": file_hash(staging / "sales.csv"),
        }
        if fixture:
            manifest["sample"] = sample
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        staging.rename(output)
    return profile


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--evaluation", action="store_true")
    parser.add_argument("--retrieved-on", type=date.fromisoformat)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        if not args.fixture:
            require_real_m5_clearance()
        profile = adapt(args.source, args.output, fixture=args.fixture,
                        evaluation=args.evaluation, retrieved_on=args.retrieved_on)
    except InputError as error:
        parser.exit(2, f"Input rejected: {error}\n")
    except (OSError, UnicodeError, csv.Error, sqlite3.Error):
        parser.exit(2, "Import failed. Check UTF-8 CSV structure, access and free disk space.\n")
    print(json.dumps(profile, sort_keys=True))


if __name__ == "__main__":
    main()
