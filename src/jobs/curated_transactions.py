"""Curated job: transaction detail.

Ab Initio source
----------------
- Graph orchestration: ``scripts/run_daily_orders.ksh`` (the daily batch wrapper
  that runs the extract -> transform -> load phases via ``air sandbox run``).
- Record layout: ``dml/transaction_detail.dml`` — a *nested* record:

      record
        decimal(",") txn_id; datetime(...) txn_timestamp;
        decimal(",") customer_id; decimal(",") txn_type;
        record  merchant_name(null("")), merchant_category, amount  end merchant_info;
        decimal(",") item_count;
        record[item_count]  sku, quantity, line_total  end line_items;   // variable array
        if (txn_type == 2) record original_txn_id, refund_reason end refund_details;
        string("\\n", null("UNKNOWN")) channel;
      end;

PySpark equivalent
------------------
Read ``transactions.dat`` via the DML-derived schema, rebuild the two nested DML
structures (``merchant_info`` struct and the variable-length ``line_items`` array)
and then **flatten** them: ``explode`` the ``line_items`` array to one row per line
item (the DML ``record[item_count]``), mirroring how a downstream Ab Initio
normalize/reformat would unnest the array before loading a flat curated table.

Source-faithful DML defaults (reproduced value-for-value, **not** "improved"):
- ``channel``  — DML ``null("UNKNOWN")``: a blank channel in the extract is the
  literal ``UNKNOWN``, never NULL. Spark's CSV reader would coerce the blank to
  NULL, so the default is re-applied deliberately (see the conversion playbook's
  channel worked example).
- ``merchant_name`` — DML ``null("")``: a blank merchant name is the empty
  string, never NULL. Reproduced with ``coalesce(..., '')``.

Note: ``refund_details`` (the ``if (txn_type == 2)`` conditional sub-record) is
not present in the flattened flat-file extract, so it is not reconstructed here.
This is faithful to the extract as landed; flagged rather than fabricated.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table

# DML null(...) substitution defaults, reproduced exactly.
CHANNEL_DEFAULT = (
    "UNKNOWN"  # transaction_detail.dml: string("\n", null("UNKNOWN")) channel
)
MERCHANT_NAME_DEFAULT = (
    ""  # transaction_detail.dml: string(",", null("")) merchant_name
)


def _dml_default(col: str, default: str):
    """Reproduce an Ab Initio DML ``null(default)`` substitution.

    Ab Initio reads a blank delimited field as an empty string and substitutes
    ``default``; Spark may surface it as NULL or "". Treat both as blank.
    """
    trimmed = F.trim(F.col(col))
    return F.when(trimmed.isNull() | (trimmed == ""), F.lit(default)).otherwise(trimmed)


def transform(spark: SparkSession) -> DataFrame:
    txns = dml.read_transactions(spark)

    # Rebuild the nested DML records: merchant_info (record) and the
    # variable-length line_items array (record[item_count]). The extract is
    # already flattened to one line item per transaction (item_count = 1), so the
    # array carries a single element; modelling it as an array keeps the
    # conversion faithful to the DML and makes the flatten explicit.
    typed = (
        txns.withColumn("txn_type", F.col("txn_type").cast("int"))
        .withColumn(
            "txn_timestamp", F.to_timestamp("txn_timestamp", "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("channel", _dml_default("channel", CHANNEL_DEFAULT))
        .withColumn(
            "merchant_info",
            F.struct(
                _dml_default("merchant_name", MERCHANT_NAME_DEFAULT).alias(
                    "merchant_name"
                ),
                F.col("merchant_category").alias("merchant_category"),
                F.col("amount").alias("amount"),
            ),
        )
        .withColumn(
            "line_items",
            F.array(
                F.struct(
                    F.col("sku").alias("sku"),
                    F.col("quantity").alias("quantity"),
                    F.col("line_total").alias("line_total"),
                )
            ),
        )
    )

    # Flatten: one output row per line item (DML record[item_count]).
    flattened = typed.withColumn("line_item", F.explode("line_items"))

    return flattened.select(
        "txn_id",
        "txn_timestamp",
        "customer_id",
        "txn_type",
        F.col("merchant_info.merchant_name").alias("merchant_name"),
        F.col("merchant_info.merchant_category").alias("merchant_category"),
        F.col("merchant_info.amount").alias("amount"),
        "item_count",
        F.col("line_item.sku").alias("sku"),
        F.col("line_item.quantity").alias("quantity"),
        F.col("line_item.line_total").alias("line_total"),
        "channel",
    )


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark)
    return write_table(df, namespace, "curated", "transactions")
