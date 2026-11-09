#!/usr/bin/env python3
"""
run_pipeline.py — orchestrate the converted PySpark pipeline for one namespace.

This is the PySpark analogue of the Ab Initio KornShell orchestration
(`scripts/run_daily_orders.ksh`, `scripts/run_customer_cdc.ksh`): it runs the
converted jobs in dependency order and writes their outputs under
``out/<namespace>/``.

The jobs converted **on main** are the customer and orders pipelines. The
transactions and customer-CDC pipelines are the work converted live in the demo
(see the playbook and the migration map) and are intentionally not wired in here.

``emp_data_process_v4.mp`` is driven by its generated KornShell wrapper, which
exports the graph parameters before ``mp run``::

    export PROCESS_COUNT;PROCESS_COUNT=1
    export PROCESS_DATE;PROCESS_DATE=$(date +%Y%m%d)   # $[(date("YYYYMMDD"))now1()]

The same parameters are accepted here as ``--process-count`` / ``--process-date``
(falling back to the ``PROCESS_COUNT`` / ``PROCESS_DATE`` environment variables,
then to the graph defaults).

Usage:
    python -m src.run_pipeline --namespace dev
    PROCESS_DATE=20260611 python -m src.run_pipeline -n dev --process-count 0
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime

from src.common.spark import build_spark
from src.jobs import emp_data_process_v4, mart_daily_orders, stg_customers, stg_orders


def _process_date(value: str) -> str:
    datetime.strptime(value, "%Y%m%d")
    return value


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--namespace", "-n", default="dev", help="isolated output namespace"
    )
    ap.add_argument(
        "--process-date",
        type=_process_date,
        default=os.environ.get("PROCESS_DATE", datetime.now().strftime("%Y%m%d")),
        help="emp_data_process_v4 PROCESS_DATE (YYYYMMDD; default: today)",
    )
    ap.add_argument(
        "--process-count",
        default=os.environ.get("PROCESS_COUNT", "1"),
        help="emp_data_process_v4 PROCESS_COUNT: enables the Get_Count subgraph",
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
            f"  emp_data_process_v4 (PROCESS_DATE={args.process_date}, "
            f"PROCESS_COUNT={args.process_count})"
        )
        emp_outputs = emp_data_process_v4.run(
            spark, ns, args.process_date, args.process_count
        )
        for name, path in emp_outputs.items():
            print(f"    {name:<26} <-  {path}")
        print("[pipeline] done")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
