"""Curated job: transaction detail.

Ab Initio source
----------------
Record format : ``dml/transaction_detail.dml``
Orchestration : ``scripts/run_daily_orders.ksh`` (the daily batch that lands the
                transaction extract alongside orders; extract -> staging -> load)
Extract       : ``data/raw/transactions.dat``, pipe-delimited flattened header

The DML record is not flat. It declares, in order:

* scalar header fields (``txn_id``, ``txn_timestamp``, ``customer_id``, ``txn_type``);
* an embedded record ``merchant_info`` (``merchant_name`` with a ``null("")``
  default, ``merchant_category``, ``amount``);
* ``item_count`` followed by a record vector ``record[item_count] ... end line_items``;
* a **conditional** record ``if (txn_type == 2) ... end refund_details``;
* ``string("\\n", null("UNKNOWN")) channel``.

PySpark equivalent
------------------
* nested record  -> ``StructType`` column (``merchant_info``, ``refund_details``);
* record vector  -> ``ArrayType(StructType)`` then ``explode`` to flatten, so the
  curated grain is **one row per transaction line item**;
* ``null("X")``  -> the default is applied explicitly, because Ab Initio reads a
  blank delimited field as an empty string and substitutes the DML default —
  it never yields NULL.

Source quirks reproduced (flagged, not corrected — see the conversion playbook)
------------------------------------------------------------------------------
1. The extract is the *flattened header* form of the record: it carries exactly
   one ``sku|quantity|line_total`` triple per transaction regardless of the
   ``item_count`` field. The vector is therefore rebuilt as a single-element
   array; ``item_count`` is carried through from the extract as-is rather than
   being recomputed from the array, so any divergence stays visible instead of
   being silently normalised.
2. The extract carries no columns for the conditional ``refund_details`` record,
   so refunds (``txn_type = 2``) land with a present-but-empty struct and sales
   with NULL. The conditional record's presence/absence is reproduced; its
   values cannot be, because they are not in the extract.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table


def _dml_default(column: str, default: str):
    """Apply a DML ``null("<default>")`` substitution.

    Ab Initio reads a blank delimited field as an empty string and substitutes
    the DML default; Spark's CSV reader would otherwise leave it blank or coerce
    it to NULL. Both cases are mapped to the declared default here.
    """
    col = F.trim(F.col(column))
    return F.when(col.isNull() | (col == ""), F.lit(default)).otherwise(col)


def transform(spark: SparkSession) -> DataFrame:
    txns = dml.trimmed(dml.read_transactions(spark))

    nested = txns.select(
        F.col("txn_id"),
        F.to_timestamp("txn_timestamp", "yyyy-MM-dd HH:mm:ss").alias("txn_timestamp"),
        F.col("customer_id"),
        F.col("txn_type"),
        # merchant_info: embedded DML record -> struct. merchant_name carries the
        # DML null("") default, so a blank merchant name is "" and never NULL.
        F.struct(
            _dml_default("merchant_name", dml.MERCHANT_NAME_DML_DEFAULT).alias(
                "merchant_name"
            ),
            F.col("merchant_category").alias("merchant_category"),
            F.col("amount").alias("amount"),
        ).alias("merchant_info"),
        # item_count is carried through from the extract unmodified (quirk 1).
        F.col("item_count"),
        # line_items: record[item_count] -> array<struct>. The flattened extract
        # supplies one triple per transaction, hence a single-element array.
        F.array(
            F.struct(
                F.col("sku").alias("sku"),
                F.col("quantity").alias("quantity"),
                F.col("line_total").alias("line_total"),
            )
        ).alias("line_items"),
        # refund_details: conditional record, present only when txn_type == 2.
        # The extract has no columns for its fields (quirk 2).
        F.when(
            F.col("txn_type") == dml.TXN_TYPE_REFUND,
            F.struct(
                F.lit(None).cast("string").alias("original_txn_id"),
                F.lit(None).cast("string").alias("refund_reason"),
            ),
        ).alias("refund_details"),
        _dml_default("channel", dml.CHANNEL_DML_DEFAULT).alias("channel"),
    )

    # Flatten the record vector: one curated row per line item, with its 1-based
    # position preserved so the vector order in the source record is not lost.
    flattened = nested.select(
        "*", F.posexplode("line_items").alias("line_item_pos", "line_item")
    )

    return flattened.select(
        "txn_id",
        "txn_timestamp",
        "customer_id",
        "txn_type",
        "merchant_info",
        "item_count",
        (F.col("line_item_pos") + F.lit(1)).alias("line_item_seq"),
        F.col("line_item.sku").alias("sku"),
        F.col("line_item.quantity").alias("quantity"),
        F.col("line_item.line_total").alias("line_total"),
        "refund_details",
        "channel",
    )


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark)
    return write_table(df, namespace, "curated", "transactions")
