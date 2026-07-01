"""Curated job: customer master CDC (Change Data Capture).

Ab Initio source: the customer CDC pipeline —
``graphs/cdc_processor.py`` (the ``CDCProcessor`` "Compare Records by Key"
component), ``psets/pset_templates/customer_cdc.pset`` (KEY_COLUMNS / HASH_COLUMNS
/ snapshot paths), driven by ``scripts/run_customer_cdc.ksh``. It compares the
current customer-master snapshot against the previous one and emits
INSERT / UPDATE / DELETE change records.

PySpark equivalent: a ``full_outer`` join on the key plus a per-row MD5 hash of
the HASH_COLUMNS — the Compare-Records-by-Key → Delta-MERGE analog from the
migration map. The classification is:

    key in current only .................. INSERT (emit the current record)
    key in previous only ................. DELETE (emit the previous record)
    key in both, row hash differs ........ UPDATE (emit the current record)
    key in both, row hash equal .......... unchanged (dropped, as the legacy does)

Source-faithful reproduction (not "improved"), flagged where the legacy has a
quirk:

* The row hash is ``md5( "||".join( str(v) for v in HASH_COLUMNS ) )`` — the exact
  algorithm, separator, column set and column *order* of ``CDCProcessor._row_hash``
  (``hashlib.md5("||".join(row.values).encode()).hexdigest()``). ``concat_ws``
  reproduces the ``"||"`` join; every value is cast to string first to mirror the
  legacy ``.astype(str)``.
* HASH_COLUMNS in ``customer_cdc.pset`` *includes the key column* ``customer_id``.
  Hashing the key alongside the compare columns is redundant for change detection
  (the key is constant within a matched pair) — but it is what the legacy graph
  does, so we reproduce it rather than "cleaning it up". Changing the hash input
  would be a deliberate, separate decision, not a side effect of conversion.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table

INSERT = "INSERT"
UPDATE = "UPDATE"
DELETE = "DELETE"


def _row_hash(cols: list[str]) -> Column:
    """MD5 of the HASH_COLUMNS joined by '||' — the exact CDCProcessor row hash.

    Values are cast to string (legacy ``.astype(str)``) and NULLs coalesced to
    the empty string so ``concat_ws`` — which otherwise skips NULLs — matches the
    pandas join value-for-value.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return F.md5(F.concat_ws("||", *parts))


def transform(spark: SparkSession) -> DataFrame:
    key = dml.CUSTOMER_MASTER_KEY_COLUMNS[0]
    hash_cols = dml.CUSTOMER_MASTER_HASH_COLUMNS
    master_cols = [f.name for f in dml.CUSTOMER_MASTER_SCHEMA.fields]

    current = dml.trimmed(dml.read_customer_snapshot_current(spark)).withColumn(
        "_hash", _row_hash(hash_cols)
    )
    previous = dml.trimmed(dml.read_customer_snapshot_previous(spark)).withColumn(
        "_hash", _row_hash(hash_cols)
    )

    cur = current.select(
        [F.col(c).alias(f"c_{c}") for c in master_cols]
        + [F.col("_hash").alias("c_hash")]
    )
    prev = previous.select(
        [F.col(c).alias(f"p_{c}") for c in master_cols]
        + [F.col("_hash").alias("p_hash")]
    )

    joined = cur.join(prev, cur[f"c_{key}"] == prev[f"p_{key}"], "full_outer")

    in_current = F.col(f"c_{key}").isNotNull()
    in_previous = F.col(f"p_{key}").isNotNull()
    operation = (
        F.when(in_current & ~in_previous, F.lit(INSERT))
        .when(~in_current & in_previous, F.lit(DELETE))
        .when(F.col("c_hash") != F.col("p_hash"), F.lit(UPDATE))
        .otherwise(F.lit(None))  # unchanged
    )

    changes = joined.withColumn("cdc_operation", operation).where(
        F.col("cdc_operation").isNotNull()
    )

    # INSERT/UPDATE carry the current record; DELETE carries the previous record.
    projected = [
        F.when(F.col("cdc_operation") == DELETE, F.col(f"p_{c}"))
        .otherwise(F.col(f"c_{c}"))
        .alias(c)
        for c in master_cols
    ]
    return changes.select(*projected, "cdc_operation").orderBy("cdc_operation", key)


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark)
    return write_table(df, namespace, "curated", "customer_cdc")
