#!/usr/bin/env python3
"""
run_pipeline.py — orchestrate the converted PySpark pipeline for one namespace.

This is the PySpark analogue of the Ab Initio KornShell orchestration
(`scripts/run_daily_orders.ksh`, `scripts/run_customer_cdc.ksh`): it runs the
converted jobs in dependency order and writes their outputs under
``out/<namespace>/``.

The customer, orders and customer-CDC pipelines are wired in here. The
transactions pipeline is the remaining live-conversion target (see the playbook
and the migration map) and is intentionally not wired in yet.

Usage:
    python -m src.run_pipeline --namespace dev
"""

from __future__ import annotations

import argparse

from src.common.spark import build_spark
from src.jobs import cdc_customers, mart_daily_orders, stg_customers, stg_orders


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
        print("  curated.customer_cdc<- ", cdc_customers.run(spark, ns))
        print("[pipeline] done")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
