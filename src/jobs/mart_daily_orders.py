"""Mart job: daily order summary.

Ab Initio source: `scripts/run_daily_orders.ksh` phase 4 (production rollover)
aggregates the staged orders into the daily reporting table.

PySpark equivalent: read the staging orders table and aggregate by order_date to
produce order_count, total_amount, and total_items. This is the set-based
GROUP BY that replaces the legacy rollover graph.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.io import layer_path, write_table


def transform(spark: SparkSession, namespace: str) -> DataFrame:
    staged = spark.read.parquet(layer_path(namespace, "staging", "orders"))
    return (
        staged.groupBy("order_date")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("total_amount"),
            F.sum("item_count").alias("total_items"),
        )
        .orderBy("order_date")
    )


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark, namespace)
    return write_table(df, namespace, "marts", "daily_orders")
