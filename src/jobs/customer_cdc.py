"""Curated job: customer CDC (Change Data Capture).

Ab Initio source: the customer-CDC graph ``graphs/cdc_processor.py`` (the
``CDCProcessor`` "Compare Records by Key" component), driven by
``psets/pset_templates/customer_cdc.pset`` and orchestrated by
``scripts/run_customer_cdc.ksh`` (step 2, "compare with previous snapshot").

PySpark equivalent: read the *current* and *previous* customer-master snapshots,
compute a per-row MD5 hash over the PSET ``HASH_COLUMNS``, ``full_outer`` join on
the key, and classify every key as INSERT / UPDATE / DELETE — exactly the legacy
component's set semantics. The curated output is the change set (the delta), one
row per changed key, matching what ``CDCProcessor.process`` returns
(``inserts`` + ``updates`` + ``deletes``; UNCHANGED keys are not emitted).

Source-parity notes (reproduced faithfully, NOT "fixed" — flagged for a separate
business decision):
  * ``HASH_COLUMNS`` mirrors the PSET **exactly**, including the key column
    ``customer_id`` itself. Including the key in the compare hash is redundant for
    UPDATE detection (the join is already by key) but the legacy graph does it, so
    we reproduce it verbatim.
  * The legacy ``_row_hash`` is ``md5("||".join(cols.astype(str)))`` in PSET
    column order. We reproduce the MD5, the ``||`` separator and the column order.
    ``concat_ws`` skips NULLs whereas pandas ``astype(str)`` would stringify them;
    the snapshots are NULL-free, but we ``coalesce`` to "" so the two schemes stay
    value-for-value identical rather than relying on that.
  * The customer DMLs (``customer.dml`` / ``customer_address.dml`` /
    ``common_address.dml``) declare **no** ``null(...)`` defaults, unlike the
    transactions ``channel`` — so there is no DML default to substitute here.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table

# --- customer_cdc.pset contract (mirrored EXACTLY) -------------------------
# define KEY_COLUMNS  customer_id
# define HASH_COLUMNS customer_id,name,address,phone,email,status
KEY_COLUMNS = ["customer_id"]
HASH_COLUMNS = ["customer_id", "name", "address", "phone", "email", "status"]

# The customer-master attributes carried through to the CDC output (the non-key
# HASH_COLUMNS, i.e. the compared business attributes).
ATTR_COLUMNS = [c for c in HASH_COLUMNS if c not in KEY_COLUMNS]


def customer_master(df: DataFrame) -> DataFrame:
    """Project a raw snapshot into the customer-master record the CDC compares.

    ``address`` is the consolidated street/city/state/zip (``common_address.dml``
    address_t), matching how the customer master presents a single address field.
    """
    return df.select(
        "customer_id",
        "name",
        F.concat_ws(", ", "street", "city", "state", "zip").alias("address"),
        "phone",
        "email",
        "status",
    )


def row_hash() -> F.Column:
    """Legacy row hash: md5 of the PSET HASH_COLUMNS cast to string, joined with
    ``||`` in PSET order (graphs/cdc_processor.py ``_row_hash``)."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in HASH_COLUMNS]
    return F.md5(F.concat_ws("||", *parts))


def transform(spark: SparkSession) -> DataFrame:
    current = customer_master(dml.read_customer_snapshot(spark, "current")).withColumn(
        "_hash", row_hash()
    )
    previous = customer_master(
        dml.read_customer_snapshot(spark, "previous")
    ).withColumn("_hash", row_hash())

    src = current.select(
        "customer_id",
        *[F.col(c).alias(f"src_{c}") for c in ATTR_COLUMNS],
        F.col("_hash").alias("src_hash"),
    )
    tgt = previous.select(
        "customer_id",
        *[F.col(c).alias(f"tgt_{c}") for c in ATTR_COLUMNS],
        F.col("_hash").alias("tgt_hash"),
    )

    joined = src.join(tgt, "customer_id", "full_outer")

    # Legacy CDCProcessor semantics on the key sets of a full_outer join:
    #   INSERT: key in source, not in target      (tgt side null)
    #   DELETE: key in target, not in source       (src side null)
    #   UPDATE: key in both, row hash changed
    change_type = (
        F.when(F.col("tgt_hash").isNull(), F.lit("INSERT"))
        .when(F.col("src_hash").isNull(), F.lit("DELETE"))
        .when(F.col("src_hash") != F.col("tgt_hash"), F.lit("UPDATE"))
        .otherwise(F.lit("UNCHANGED"))
    )

    classified = joined.withColumn("change_type", change_type)

    # Emit the source (post-change) attributes for INSERT/UPDATE and the target
    # (pre-change) attributes for DELETE — mirroring which DataFrame each class is
    # sourced from in CDCProcessor.process.
    is_delete = F.col("change_type") == "DELETE"
    delta = classified.where(F.col("change_type") != "UNCHANGED").select(
        "customer_id",
        "change_type",
        *[
            F.when(is_delete, F.col(f"tgt_{c}")).otherwise(F.col(f"src_{c}")).alias(c)
            for c in ATTR_COLUMNS
        ],
        F.when(is_delete, F.col("tgt_hash"))
        .otherwise(F.col("src_hash"))
        .alias("row_hash"),
    )
    return delta.orderBy("change_type", "customer_id")


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark)
    return write_table(df, namespace, "curated", "customer_cdc")
