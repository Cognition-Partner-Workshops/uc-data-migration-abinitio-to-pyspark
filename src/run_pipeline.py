#!/usr/bin/env python3
"""
run_pipeline.py — orchestrate the converted PySpark pipeline for one namespace.

This is the PySpark analogue of the Ab Initio KornShell orchestration
(`scripts/run_daily_orders.ksh`, `scripts/run_customer_cdc.ksh`): it runs the
converted jobs in dependency order and writes their outputs under
``out/<namespace>/``.

The jobs converted **on main** are the customer and orders pipelines. The
transactions pipeline (``curated/transactions``) is converted live in the demo
(see the playbook and the migration map); the customer-CDC pipeline remains the
next wave.

Usage:
    python -m src.run_pipeline --namespace dev
"""

from __future__ import annotations

import argparse

from src.common.spark import build_spark
from src.jobs import curated_transactions, mart_daily_orders, stg_customers, stg_orders


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--namespace", "-n", default="dev", help="isolated output namespace"
    )
    args = ap.parse_args()
    ns = args.namespace

    spark = build_spark(f"pipeline-{ns}")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print(f"[pipeline] namespace={ns}")
        print("  staging.customers   <- ", stg_customers.run(spark, ns))
        print("  staging.orders      <- ", stg_orders.run(spark, ns))
        print("  marts.daily_orders  <- ", mart_daily_orders.run(spark, ns))
        print("  curated.transactions<- ", curated_transactions.run(spark, ns))
        print("[pipeline] done")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
