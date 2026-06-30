"""Staging job: customer master.

Ab Initio source: the customer snapshot graph (`scripts/run_customer_cdc.ksh`
step 1) reads the customer flat file using `customer.dml` + `customer_address.dml`
and lands a typed staging record.

PySpark equivalent: read customers.dat via the DML-derived schema, normalise the
status domain, and write the staging table. The conversion is intentionally
faithful — no rows are dropped or deduplicated here, matching the legacy
snapshot's pass-through behaviour.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table


def transform(spark: SparkSession) -> DataFrame:
    customers = dml.trimmed(dml.read_customers(spark))
    return customers.select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        F.concat_ws(", ", "street", "city", "state", "zip").alias("full_address"),
        "state",
        # Faithful pass-through of the source status domain (ACTIVE/INACTIVE/PENDING).
        F.upper(F.col("status")).alias("status"),
    )


def run(spark: SparkSession, namespace: str) -> str:
    df = transform(spark)
    return write_table(df, namespace, "staging", "customers")
