"""Curated job: transaction detail.

Ab Initio source: ``dml/transaction_detail.dml`` (read by the transactions graph,
orchestrated alongside ``scripts/run_daily_orders.ksh``). The DML record is a
nested layout:

    record
      decimal(",") txn_id;
      datetime("YYYY-MM-DD HH24:MI:SS")(",") txn_timestamp;
      decimal(",") customer_id;
      decimal(",") txn_type;
      record                                   -- merchant_info sub-record
        string(",", null("")) merchant_name;   --   blank -> "" (DML default)
        string(",") merchant_category;
        decimal("10.2", ",") amount;
      end merchant_info;
      decimal(",") item_count;
      record[item_count]                       -- variable-length line_items
        string(",") sku;
        decimal(",") quantity;
        decimal("8.2", ",") line_total;
      end line_items;
      if (txn_type == 2)                        -- conditional refund_details
        record
          decimal(",") original_txn_id;
          string(",") refund_reason;
        end refund_details;
      string("\n", null("UNKNOWN")) channel;   -- blank -> "UNKNOWN" (DML default)
    end;

PySpark equivalent: read transactions.dat via the DML-derived schema
(``src/common/dml.py``), reproduce the two ``null(...)`` defaults value-for-value,
and rebuild the nested merchant_info / line_items / refund_details structures for
the curated ``curated/transactions`` table.

The conversion is intentionally faithful — where the physical extract diverges
from the DML it is reproduced (and flagged), not "corrected". See the
SOURCE-FAITHFUL comments below and the PR description.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table


def transform(spark: SparkSession) -> DataFrame:
    txns = dml.trimmed(dml.read_transactions(spark))

    # DML: string("\n", null("UNKNOWN")) channel
    # A blank channel in the extract is read by Ab Initio as the literal
    # "UNKNOWN", never NULL. Spark's CSV reader would coerce the blank field to
    # NULL, so reproduce the DML default deliberately (coalesce/empty handling).
    channel = F.when(
        F.col("channel").isNull() | (F.length(F.trim(F.col("channel"))) == 0),
        F.lit("UNKNOWN"),
    ).otherwise(F.col("channel"))

    # DML: string(",", null("")) merchant_name
    # The merchant_name default is the empty string (NOT "UNKNOWN"): a blank
    # merchant stays "" and must never become NULL. Reproduced value-for-value.
    merchant_name = F.coalesce(F.col("merchant_name"), F.lit(""))

    # DML: record merchant_info { merchant_name, merchant_category, amount }
    merchant_info = F.struct(
        merchant_name.alias("merchant_name"),
        F.col("merchant_category").alias("merchant_category"),
        F.col("amount").alias("amount"),
    ).alias("merchant_info")

    # DML: record[item_count] line_items { sku, quantity, line_total }
    # SOURCE-FAITHFUL: the physical extract is flattened and carries exactly one
    # line item per transaction (item_count is always 1), so the variable-length
    # group is reproduced as a single-element array — the nested contract is
    # preserved without inventing rows. Flagged: extract does not carry >1 item.
    line_items = F.array(
        F.struct(
            F.col("sku").alias("sku"),
            F.col("quantity").alias("quantity"),
            F.col("line_total").alias("line_total"),
        )
    ).alias("line_items")

    # DML: if (txn_type == 2) record refund_details { original_txn_id, refund_reason }
    # SOURCE-FAITHFUL: the flattened extract does NOT carry the refund payload,
    # so refund_details is present-but-null for refunds (txn_type == 2) and
    # absent (NULL) otherwise — reproducing the DML's conditional presence.
    # Flagged as a divergence (missing refund columns), not remediated.
    refund_details = (
        F.when(
            F.col("txn_type") == 2,
            F.struct(
                F.lit(None).cast("long").alias("original_txn_id"),
                F.lit(None).cast("string").alias("refund_reason"),
            ),
        )
        .otherwise(F.lit(None))
        .alias("refund_details")
    )

    return txns.select(
        F.col("txn_id"),
        F.to_timestamp("txn_timestamp", "yyyy-MM-dd HH:mm:ss").alias("txn_timestamp"),
        F.col("customer_id"),
        F.col("txn_type"),
        merchant_info,
        F.col("item_count"),
        line_items,
        refund_details,
        channel.alias("channel"),
    )


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark)
    return write_table(df, namespace, "curated", "transactions")
