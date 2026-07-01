#!/usr/bin/env python3
"""
generate_source.py — (re)generate the legacy "before" raw extracts under data/raw/.

These files stand in for the flat-file landing area that the Ab Initio graphs in
`ts-python-abinitio-etl` read from. The layouts mirror that repo's DML record
definitions exactly:

    customers.dat     comma-delimited   (customer.dml + customer_address.dml)
    orders.dat        pipe-delimited    (order extract, see account_balance.dml style)
    transactions.dat  pipe-delimited    (transaction_detail.dml, flattened header)

The generator is deterministic (fixed seed), so it is idempotent: re-running it
reproduces byte-identical files. That makes the "before" state durable and the
source->target reconciliation reproducible. The committed files in data/raw/ are
the output of this script; regenerate them with `make seed`.
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

FIRST_NAMES = [
    "John",
    "Jane",
    "Bob",
    "Alice",
    "Charlie",
    "Diana",
    "Edward",
    "Fiona",
    "George",
    "Helen",
    "Ivan",
    "Julia",
    "Kevin",
    "Laura",
    "Mike",
    "Nina",
    "Oscar",
    "Paula",
    "Quinn",
    "Rachel",
]
LAST_NAMES = [
    "Smith",
    "Doe",
    "Johnson",
    "Williams",
    "Brown",
    "Martinez",
    "Garcia",
    "Lee",
    "Wilson",
    "Anderson",
    "Taylor",
    "Thomas",
    "Moore",
    "Jackson",
    "White",
    "Harris",
    "Clark",
    "Lewis",
    "Young",
    "King",
]
CITIES = [
    ("New York", "NY", "10001"),
    ("Chicago", "IL", "60601"),
    ("Houston", "TX", "77001"),
    ("Phoenix", "AZ", "85001"),
    ("Philadelphia", "PA", "19101"),
    ("San Antonio", "TX", "78201"),
    ("San Diego", "CA", "92101"),
    ("Dallas", "TX", "75201"),
    ("San Jose", "CA", "95101"),
    ("Austin", "TX", "73301"),
]
STREETS = [
    "Main St",
    "Oak Ave",
    "Pine Rd",
    "Elm St",
    "Maple Dr",
    "Cedar Ln",
    "Birch Way",
    "Spruce Ct",
    "Walnut Pl",
    "Ash Blvd",
]
CUSTOMER_STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE", "PENDING"]
ORDER_STATUSES = ["SHIPPED", "DELIVERED", "PROCESSING", "CANCELLED"]
ORDER_DATES = [f"2024-01-{d:02d}" for d in range(15, 25)]
MERCHANTS = [
    ("MERCHANT_A", "Retail"),
    ("MERCHANT_B", "Electronics"),
    ("MERCHANT_C", "Grocery"),
    ("MERCHANT_D", "Services"),
]
SKUS = ["SKU-100", "SKU-200", "SKU-300", "SKU-400", "SKU-500"]
# txn_type 1 = sale, 2 = refund. channel may be blank -> Ab Initio DML default
# is null("UNKNOWN"), i.e. a blank channel is read as the literal "UNKNOWN".
TXN_CHANNELS = ["WEB", "STORE", "APP", "WEB", ""]

# Customer CDC pipeline inputs (run_customer_cdc.ksh / customer_cdc.pset).
# The CDC graph compares two customer-master snapshots (PREVIOUS_SNAPSHOT_PATH vs
# CURRENT_SNAPSHOT_PATH) by key and a row hash over HASH_COLUMNS to emit
# INSERT/UPDATE/DELETE. The master record carries exactly the pset's HASH_COLUMNS
# (customer_id,name,address,phone,email,status), pipe-delimited so the free-text
# address may itself contain commas. These two files are the durable "before" for
# the CDC reconciliation, the analogue of the snapshot landing area.
CDC_STATUSES = ["ACTIVE", "ACTIVE", "INACTIVE", "PENDING"]
# common keys whose CURRENT record differs from PREVIOUS -> CDC UPDATE class.
CDC_UPDATED_STATUS = "SUSPENDED"


def _cdc_master_record(i: int, *, updated: bool = False) -> str:
    """One customer-master row (customer_id|name|address|phone|email|status).

    Deterministic in ``i`` so the previous/current snapshots agree value-for-value
    on unchanged keys. ``updated=True`` mutates a HASH_COLUMNS field (status) so
    the row hash differs from the previous snapshot -> a CDC UPDATE.
    """
    cid = 1001 + i
    first = FIRST_NAMES[i % len(FIRST_NAMES)]
    last = LAST_NAMES[(i * 3) % len(LAST_NAMES)]
    name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}{cid}@example.com"
    city, state, zc = CITIES[i % len(CITIES)]
    street = f"{100 + i} {STREETS[i % len(STREETS)]}"
    address = f"{street}, {city}, {state} {zc}"
    phone = f"555-{cid:04d}"
    status = CDC_UPDATED_STATUS if updated else CDC_STATUSES[i % len(CDC_STATUSES)]
    return f"{cid}|{name}|{address}|{phone}|{email}|{status}"


def generate_customer_snapshot_previous(base: int) -> list[str]:
    """Previous customer-master snapshot: keys 1001..(1001+base-6).

    Omits the last 5 base keys (they appear only in current -> CDC INSERTs) and
    keeps the first 3 keys that current omits (-> CDC DELETEs).
    """
    return [_cdc_master_record(i) for i in range(0, base - 5)]


def generate_customer_snapshot_current(base: int) -> list[str]:
    """Current customer-master snapshot: keys 1004..(1001+base-1).

    Drops the first 3 base keys (-> DELETEs vs previous), adds the last 5 (->
    INSERTs), and mutates a deterministic subset of the shared keys (cid % 7 == 0)
    so their hash differs from previous (-> UPDATEs). Every other shared key is
    byte-identical to previous and must be classified as unchanged.
    """
    rows = []
    for i in range(3, base):
        cid = 1001 + i
        rows.append(_cdc_master_record(i, updated=(cid % 7 == 0)))
    return rows


def generate_customers(n: int) -> list[str]:
    rng = random.Random(101)
    rows = []
    for i in range(n):
        cid = 1001 + i
        first = FIRST_NAMES[i % len(FIRST_NAMES)]
        last = LAST_NAMES[(i * 3) % len(LAST_NAMES)]
        email = f"{first.lower()}.{last.lower()}{cid}@example.com"
        city, state, zc = CITIES[i % len(CITIES)]
        street = f"{rng.randint(100, 999)} {STREETS[i % len(STREETS)]}"
        status = CUSTOMER_STATUSES[rng.randrange(len(CUSTOMER_STATUSES))]
        rows.append(
            f"{cid},{first},{last},{email},{street},{city},{state},{zc},{status}"
        )
    return rows


def generate_orders(n: int, customer_ids: list[int]) -> list[str]:
    rng = random.Random(202)
    rows = []
    for i in range(n):
        oid = f"ORD-2024-{i + 1:04d}"
        cid = rng.choice(customer_ids)
        date = rng.choice(ORDER_DATES)
        status = ORDER_STATUSES[rng.randrange(len(ORDER_STATUSES))]
        item_count = rng.randint(1, 6)
        unit = rng.choice([19.99, 24.99, 29.99, 49.99, 79.98, 9.99])
        amount = round(unit * item_count, 2)
        rows.append(f"{oid}|{cid}|{date}|{status}|{item_count}|{amount:.2f}|USD")
    return rows


def generate_transactions(n: int, customer_ids: list[int]) -> list[str]:
    rng = random.Random(303)
    rows = []
    for i in range(n):
        tid = f"TXN{i + 1:03d}"
        ts = f"2024-01-{rng.randint(15, 24):02d} {rng.randint(8, 18):02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}"
        cid = rng.choice(customer_ids)
        txn_type = rng.choice([1, 1, 1, 2])
        merchant, category = rng.choice(MERCHANTS)
        qty = rng.randint(1, 3)
        line_total = round(rng.choice([10.0, 25.5, 49.99, 75.0, 199.99]), 2)
        amount = round(line_total * qty, 2)
        sku = rng.choice(SKUS)
        channel = TXN_CHANNELS[rng.randrange(len(TXN_CHANNELS))]
        rows.append(
            f"{tid}|{ts}|{cid}|{txn_type}|{merchant}|{category}|{amount:.2f}|1|{sku}|{qty}|{line_total:.2f}|{channel}"
        )
    return rows


def write_file(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(
        f"  wrote {len(rows):>4} rows -> {path.relative_to(path.parent.parent.parent)}"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--customers", type=int, default=50)
    ap.add_argument("--orders", type=int, default=80)
    ap.add_argument("--transactions", type=int, default=40)
    args = ap.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Generating legacy raw extracts into {RAW_DIR}")

    customers = generate_customers(args.customers)
    customer_ids = [1001 + i for i in range(args.customers)]
    orders = generate_orders(args.orders, customer_ids)
    transactions = generate_transactions(args.transactions, customer_ids)
    cdc_previous = generate_customer_snapshot_previous(args.customers)
    cdc_current = generate_customer_snapshot_current(args.customers)

    write_file(RAW_DIR / "customers.dat", customers)
    write_file(RAW_DIR / "orders.dat", orders)
    write_file(RAW_DIR / "transactions.dat", transactions)
    write_file(RAW_DIR / "customer_snapshot_previous.dat", cdc_previous)
    write_file(RAW_DIR / "customer_snapshot_current.dat", cdc_current)
    print("Done. These files are the durable 'before' state for reconciliation.")


if __name__ == "__main__":
    main()
