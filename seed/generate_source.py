#!/usr/bin/env python3
"""
generate_source.py — (re)generate the legacy "before" raw extracts under data/raw/.

These files stand in for the flat-file landing area that the Ab Initio graphs in
`ts-python-abinitio-etl` read from. The layouts mirror that repo's DML record
definitions exactly:

    customers.dat     comma-delimited   (customer.dml + customer_address.dml)
    orders.dat        pipe-delimited    (order extract, see account_balance.dml style)
    transactions.dat  pipe-delimited    (transaction_detail.dml, flattened header)
    vectors_inp.dat   pipe-delimited    (emp_data_process_v4.mp Input File, vector_inp.dml)
    emp_data.dat      pipe-delimited    (emp_data_process_v4.mp Input File-2)

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


EMP_DEPTS = ["HR", "FIN", "IT", "OPS"]
EMP_RATINGS = ["A", "B", "C", "D"]
EMP_LOCATIONS = ["London", "Leeds", "Bristol", "Manchester", "Glasgow"]
EMP_SALARIES = ["450", "600.50", "725", "800.25", "950", "1100", "1250.75", "1500"]


def generate_emp_vectors(n: int) -> tuple[list[str], list[str]]:
    """vectors_inp.dat rows ``dept|emp_id,emp_id,...|rating,rating,...``.

    Each row carries one dept and a vector of emp_ids (shuffled within the row;
    the graph's Reformat re-sorts them). emp_ids increase across rows so the
    Dedup Sorted ``check_sort`` on {emp_id} holds, and some rows repeat the
    previous row's last emp_id so Dedup Sorted (keep first) has work to do.
    Returns (rows, distinct emp_ids in order).
    """
    rng = random.Random(404)
    rows, ids = [], []
    next_id = 1001
    for i in range(n):
        size = rng.randint(1, 5)
        emp_ids = [f"E{next_id + k}" for k in range(size)]
        next_id += size
        if ids and i % 4 == 3:
            emp_ids.insert(0, ids[-1])
        ids.extend(e for e in emp_ids if e not in ids)
        ratings = [rng.choice(EMP_RATINGS) for _ in emp_ids]
        pairs = list(zip(emp_ids, ratings))
        rng.shuffle(pairs)
        dept = EMP_DEPTS[rng.randrange(len(EMP_DEPTS))]
        rows.append(
            f"{dept}|{','.join(e for e, _ in pairs)}|{','.join(r for _, r in pairs)}"
        )
    return rows, ids


def generate_emp_data(emp_ids: list[str]) -> list[str]:
    """emp_data.dat rows ``emp_name|location|salary|emp_id``.

    Exercises the Join/Dedup rules: every 7th vector emp_id has no emp_data row
    (dropped by the inner join), a few emp_data rows have no vector entry, one
    emp_id has two emp_data rows (join fan-out resolved by Dedup keep-first),
    and some names carry padding that the Join's string_lrtrim removes.
    """
    rng = random.Random(505)
    rows = []
    for i, eid in enumerate(emp_ids):
        if i % 7 == 6:
            continue
        name = f"{FIRST_NAMES[i % len(FIRST_NAMES)]} {LAST_NAMES[i % len(LAST_NAMES)]}"
        if i % 5 == 0:
            name = f"  {name} "
        loc = EMP_LOCATIONS[rng.randrange(len(EMP_LOCATIONS))]
        rows.append(f"{name}|{loc}|{rng.choice(EMP_SALARIES)}|{eid}")
        if i == 3:
            rows.append(f"{name} Dup|{loc}|1|{eid}")
    for k in range(3):
        rows.append(f"Orphan Emp{k}|London|999|E9{k:03d}")
    rng.shuffle(rows)
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
    ap.add_argument("--emp-vectors", type=int, default=16)
    args = ap.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Generating legacy raw extracts into {RAW_DIR}")

    customers = generate_customers(args.customers)
    customer_ids = [1001 + i for i in range(args.customers)]
    orders = generate_orders(args.orders, customer_ids)
    transactions = generate_transactions(args.transactions, customer_ids)

    write_file(RAW_DIR / "customers.dat", customers)
    write_file(RAW_DIR / "orders.dat", orders)
    write_file(RAW_DIR / "transactions.dat", transactions)
    emp_vectors, emp_ids = generate_emp_vectors(args.emp_vectors)
    write_file(RAW_DIR / "vectors_inp.dat", emp_vectors)
    write_file(RAW_DIR / "emp_data.dat", generate_emp_data(emp_ids))
    print("Done. These files are the durable 'before' state for reconciliation.")


if __name__ == "__main__":
    main()
