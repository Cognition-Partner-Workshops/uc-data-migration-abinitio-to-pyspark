#!/usr/bin/env python3
"""
run_pipeline.py — orchestrate the converted PySpark pipeline for one namespace.

This is the PySpark analogue of the Ab Initio KornShell orchestration
(`scripts/run_daily_orders.ksh`, `scripts/run_customer_cdc.ksh`,
`run_cust_txn_summary.ksh`): it runs the
converted jobs in dependency order and writes their outputs under
``out/<namespace>/``.

The jobs converted **on main** are the customer and orders pipelines. The
transactions and customer-CDC pipelines are the work converted live in the demo
(see the playbook and the migration map) and are intentionally not wired in here.

`run_cust_txn_summary.ksh` maps to the cust_txn_summary step: `AI_SERIAL`
(input/output serial dir) becomes `data/raw/` in and `out/<namespace>/` out,
`PROCESS_DATE` defaults to today (YYYYMMDD) exactly as the wrapper does, and
`set -e` / `|| exit 1` become an uncaught exception (non-zero exit).

Usage:
    python -m src.run_pipeline --namespace dev [--process-date 20260115]
"""

from __future__ import annotations

import argparse
import os
from datetime import date

from src.common.spark import build_spark
from src.jobs import cust_txn_summary, mart_daily_orders, stg_customers, stg_orders


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--namespace", "-n", default="dev", help="isolated output namespace"
    )
    ap.add_argument(
        "--process-date",
        default=os.environ.get("PROCESS_DATE") or date.today().strftime("%Y%m%d"),
        help="PROCESS_DATE passed to cust_txn_summary (YYYYMMDD; default today)",
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
        print(
            "  marts.cust_txn_summary <- ",
            cust_txn_summary.run(spark, ns, process_date=args.process_date),
        )
        print("[pipeline] done")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
