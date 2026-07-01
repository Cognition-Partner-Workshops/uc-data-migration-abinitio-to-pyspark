#!/usr/bin/env python3
"""
generate_source.py — (re)generate the legacy "before" raw extracts under data/raw/.

These files stand in for the flat-file landing area that the Ab Initio graphs in
`ts-python-abinitio-etl` read from. The layouts mirror that repo's DML record
definitions exactly:

    customers.dat     comma-delimited   (customer.dml + customer_address.dml)
    orders.dat        pipe-delimited    (order extract, see account_balance.dml style)
    transactions.dat  pipe-delimited    (transaction_detail.dml, flattened header)

    customer_snapshot_current.dat   comma-delimited  (customer master, CDC source)
    customer_snapshot_previous.dat  comma-delimited  (customer master, CDC target)

The two customer_snapshot_* files are the snapshot area the customer-CDC graph
reads (`graphs/cdc_processor.py`, `psets/pset_templates/customer_cdc.pset`,
`scripts/run_customer_cdc.ksh`): the graph compares the *current* snapshot
against the *previous* snapshot to emit INSERT/UPDATE/DELETE. Both are laid out
as the combined customer.dml + customer_address.dml record
(customer_id,name,street,city,state,zip,phone,email,status), and are generated
so the pair contains a known, reproducible set of inserts, updates and deletes.

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


# --------------------------------------------------------------- customer CDC
# The customer-CDC graph compares two customer-master snapshots. We generate the
# pair with a known, reproducible delta:
CDC_INSERTS = 5  # ids present in current but not previous -> INSERT
CDC_DELETES = 5  # ids present in previous but not current -> DELETE
# and a deterministic subset of the shared ids differs (older status in the
# previous snapshot) -> UPDATE. This is durable "before" state, not run output.


def _phone(rng: random.Random) -> str:
    return f"{rng.randint(200, 989)}-{rng.randint(200, 999)}-{rng.randint(1000, 9999)}"


def _master_record(cid: int, i: int, rng: random.Random) -> dict:
    first = FIRST_NAMES[i % len(FIRST_NAMES)]
    last = LAST_NAMES[(i * 3) % len(LAST_NAMES)]
    city, state, zc = CITIES[i % len(CITIES)]
    return {
        "customer_id": cid,
        "name": f"{first} {last}",
        "street": f"{rng.randint(100, 999)} {STREETS[i % len(STREETS)]}",
        "city": city,
        "state": state,
        "zip": zc,
        "phone": _phone(rng),
        "email": f"{first.lower()}.{last.lower()}{cid}@example.com",
        "status": CUSTOMER_STATUSES[rng.randrange(len(CUSTOMER_STATUSES))],
    }


def _fmt_master(r: dict) -> str:
    # customer.dml + customer_address.dml, comma-delimited. name has no comma;
    # address is kept as its street/city/state/zip sub-fields (common_address.dml).
    return (
        f"{r['customer_id']},{r['name']},{r['street']},{r['city']},"
        f"{r['state']},{r['zip']},{r['phone']},{r['email']},{r['status']}"
    )


def generate_customer_snapshots(n: int) -> tuple[list[str], list[str]]:
    """Return (current_rows, previous_rows) for the customer-master CDC snapshots.

    current  = ids 1001..1000+n.
    previous = the shared ids (all but the last CDC_INSERTS current ids) plus
    CDC_DELETES extra previous-only ids; a deterministic subset of the shared ids
    carries an older status so it is detected as an UPDATE.
    """
    rng = random.Random(404)
    current = {1001 + i: _master_record(1001 + i, i, rng) for i in range(n)}

    previous: dict[int, dict] = {}
    both_ids = [1001 + i for i in range(n - CDC_INSERTS)]
    for idx, cid in enumerate(both_ids):
        rec = dict(current[cid])
        if idx % 5 == 0:
            # UPDATE: previous holds the pre-change status (guaranteed to differ
            # from the current domain of ACTIVE/INACTIVE/PENDING).
            rec["status"] = "SUSPENDED"
        previous[cid] = rec
    for j in range(CDC_DELETES):
        cid = 1000 + n + 1 + j
        previous[cid] = _master_record(cid, n + j, rng)

    current_rows = [_fmt_master(current[c]) for c in sorted(current)]
    previous_rows = [_fmt_master(previous[c]) for c in sorted(previous)]
    return current_rows, previous_rows


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

    cdc_current, cdc_previous = generate_customer_snapshots(args.customers)

    write_file(RAW_DIR / "customers.dat", customers)
    write_file(RAW_DIR / "orders.dat", orders)
    write_file(RAW_DIR / "transactions.dat", transactions)
    write_file(RAW_DIR / "customer_snapshot_current.dat", cdc_current)
    write_file(RAW_DIR / "customer_snapshot_previous.dat", cdc_previous)
    print("Done. These files are the durable 'before' state for reconciliation.")


if __name__ == "__main__":
    main()
