"""Staging job: daily orders.

Ab Initio source: `scripts/run_daily_orders.ksh` phase 1-3 (extract -> CDC ->
staging load) reads the order extract and lands typed staging rows in
`STAGING.ORDERS`.

PySpark equivalent: read orders.dat via the DML-derived schema and write the
staging table. order_date is parsed to a real DATE (Ab Initio date("YYYY-MM-DD")).
All source rows are preserved — staging is a typed mirror of the extract.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table


def transform(spark: SparkSession) -> DataFrame:
    orders = dml.trimmed(dml.read_orders(spark))
    return orders.select(
        "order_id",
        "customer_id",
        F.to_date("order_date", "yyyy-MM-dd").alias("order_date"),
        F.upper(F.col("order_status")).alias("order_status"),
        "item_count",
        "amount",
        "currency",
    )


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark)
    return write_table(df, namespace, "staging", "orders")
