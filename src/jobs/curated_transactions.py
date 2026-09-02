"""Curated job: transaction detail.

Ab Initio source: the transactions detail graph bound to
`dml/transaction_detail.dml` (orchestrated alongside the daily orders load in
`scripts/run_daily_orders.ksh`). The DML is a nested record:

    record
      decimal txn_id; datetime txn_timestamp; decimal customer_id; decimal txn_type;
      record { string(null("")) merchant_name; string merchant_category;
               decimal("10.2") amount; }                     end merchant_info;
      decimal item_count;
      record[item_count] { string sku; decimal quantity;
                           decimal("8.2") line_total; }      end line_items;
      if (txn_type == 2) record { ... }                      end refund_details;
      string("\\n", null("UNKNOWN")) channel;
    end;

The legacy extract lands the record flattened (one line item per physical row,
see `src/common/dml.py::TRANSACTION_SCHEMA`). This job rebuilds the DML shape —
`merchant_info` as a struct and `line_items` as an array — then flattens it back
to one curated row per line item (the Ab Initio *normalize* pattern ->
`posexplode`), applying the DML `null(...)` defaults value-for-value:

    merchant_name  null("")        -> blank stays the empty string, never NULL
    channel        null("UNKNOWN") -> blank/NULL becomes the literal 'UNKNOWN'

Source-faithful quirks reproduced (not endorsed):
    * txn_id is declared decimal in the DML but the extract carries 'TXN001'-style
      keys; it is kept as a string, as the extract has it.
    * the conditional `refund_details` record (txn_type == 2) is not present in the
      flattened extract, so no refund columns are emitted; txn_type is passed through
      so the class is still identifiable downstream.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table

TXN_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"  # DML datetime("YYYY-MM-DD HH24:MI:SS")
CHANNEL_DEFAULT = "UNKNOWN"  # DML string("\n", null("UNKNOWN")) channel
MERCHANT_NAME_DEFAULT = ""  # DML string(",", null("")) merchant_name


def nested(spark: SparkSession) -> DataFrame:
    """Read the flattened extract and rebuild the transaction_detail.dml record
    shape: scalar header fields, a `merchant_info` struct and a `line_items`
    array (one element per physical extract row)."""
    raw = dml.trimmed(dml.read_transactions(spark))
    return raw.select(
        "txn_id",
        F.to_timestamp("txn_timestamp", TXN_TIMESTAMP_FORMAT).alias("txn_timestamp"),
        "customer_id",
        "txn_type",
        F.struct(
            F.coalesce(F.col("merchant_name"), F.lit(MERCHANT_NAME_DEFAULT)).alias(
                "merchant_name"
            ),
            F.col("merchant_category"),
            F.col("amount"),
        ).alias("merchant_info"),
        "item_count",
        F.array(
            F.struct(
                F.col("sku"),
                F.col("quantity"),
                F.col("line_total"),
            )
        ).alias("line_items"),
        F.coalesce(F.nullif(F.col("channel"), F.lit("")), F.lit(CHANNEL_DEFAULT)).alias(
            "channel"
        ),
    )


def transform(spark: SparkSession) -> DataFrame:
    """Flatten the nested record to one curated row per line item."""
    return (
        nested(spark)
        .select(
            "txn_id",
            "txn_timestamp",
            "customer_id",
            "txn_type",
            F.col("merchant_info.merchant_name").alias("merchant_name"),
            F.col("merchant_info.merchant_category").alias("merchant_category"),
            F.col("merchant_info.amount").alias("amount"),
            "item_count",
            F.posexplode("line_items").alias("line_pos", "line_item"),
            "channel",
        )
        .select(
            "txn_id",
            "txn_timestamp",
            "customer_id",
            "txn_type",
            "merchant_name",
            "merchant_category",
            "amount",
            "item_count",
            (F.col("line_pos") + 1).cast("int").alias("line_seq"),
            F.col("line_item.sku").alias("sku"),
            F.col("line_item.quantity").alias("quantity"),
            F.col("line_item.line_total").alias("line_total"),
            "channel",
        )
    )


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark)
    return write_table(df, namespace, "curated", "transactions")
