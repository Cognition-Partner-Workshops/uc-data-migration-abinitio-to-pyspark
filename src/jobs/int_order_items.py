"""Intermediate job: explode order-item vectors into one row per item.

Ab Initio source: `dml/order_items.dml`. The record carries two *parallel
repeating vectors* both sized by a single `item_count` field:

    string(",")[item_count]  item_names;
    decimal(",")[item_count] item_quantities;

In Ab Initio this "normalize" pattern (a `normalize` component driven by the
vector length) fans each order out to one output row per element, pairing
`item_names[i]` with `item_quantities[i]` positionally.

PySpark equivalent: `arrays_zip` the two vectors, then `posexplode` so each
element becomes a row while `pos` preserves the source ordering. Because the
reader (`dml.read_order_items`) slices both vectors to exactly `item_count`, the
fan-out is bounded to `item_count` per order — never more, never fewer — which is
the reconciled completeness contract.

Source-parity notes (reproduced faithfully, not "improved" — see the PR):
- order_status has NO null(...) default in order_items.dml, so a blank is carried
  through as the empty string it is read as; we do not invent a default and do not
  coerce it to NULL. (Contrast transaction_detail.dml's channel = null("UNKNOWN").)
- order_status is NOT upper-cased here — the order_items DML declares no such rule
  (that transform belongs to the separate orders extract / stg_orders graph).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table


def transform(spark: SparkSession) -> DataFrame:
    src = dml.read_order_items(spark)
    zipped = src.withColumn("pair", F.arrays_zip("item_names", "item_quantities"))
    exploded = zipped.select(
        "order_id",
        "item_count",
        "order_status",
        F.posexplode("pair").alias("item_seq", "pair"),
    )
    return exploded.select(
        "order_id",
        # 1-based sequence preserving the source vector order.
        (F.col("item_seq") + F.lit(1)).alias("item_seq"),
        F.col("pair.item_names").alias("item_name"),
        F.col("pair.item_quantities").alias("item_quantity"),
        "item_count",
        # Blank order_status preserved as empty string (no DML default). Guard
        # against any NULL sneaking in so the source-parity rule holds explicitly.
        F.coalesce(F.col("order_status"), F.lit("")).alias("order_status"),
    )


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark)
    return write_table(df, namespace, "intermediate", "order_items")
