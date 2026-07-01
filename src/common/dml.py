"""Ab Initio DML record layouts expressed as PySpark schemas + readers.

Each function maps one legacy DML record definition (from the
`ts-python-abinitio-etl` repo's `dml/` directory) to a PySpark ``StructType`` and
reads the corresponding flat file from ``data/raw/`` using the same delimiter the
DML declares. This is the direct analogue of an Ab Initio *input file* component
bound to a ``.dml`` record format.

DML -> Spark type rules used here:
    decimal(...)  -> LongType for ids/counts, DecimalType(p, s) for money
    string(...)   -> StringType
    date(...)     -> DateType
    datetime(...) -> TimestampType

Delimiters come straight from the DML:
    customer.dml / customer_address.dml -> comma-delimited
    order extract / account_balance.dml -> pipe-delimited
    transaction_detail.dml              -> pipe-delimited (flattened header)
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

RAW_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "raw"


# customer.dml + customer_address.dml, comma-delimited
CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

# order extract, pipe-delimited (account_balance.dml-style decimal("|") layout)
ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", LongType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("item_count", IntegerType(), True),
        StructField("amount", DecimalType(12, 2), True),
        StructField("currency", StringType(), True),
    ]
)

# Customer-master snapshot for the CDC pipeline (run_customer_cdc.ksh /
# customer_cdc.pset). The record carries exactly the pset's HASH_COLUMNS in
# declared order — customer_id,name,address,phone,email,status — and is
# pipe-delimited so the free-text address may contain commas. This is the input
# both the previous- and current-snapshot components bind to.
CUSTOMER_MASTER_SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

# transaction_detail.dml, pipe-delimited flattened header
TRANSACTION_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), False),
        StructField("txn_timestamp", StringType(), True),
        StructField("customer_id", LongType(), True),
        StructField("txn_type", IntegerType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DecimalType(12, 2), True),
        StructField("item_count", IntegerType(), True),
        StructField("sku", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("line_total", DecimalType(12, 2), True),
        StructField("channel", StringType(), True),
    ]
)


def _read_delimited(
    spark: SparkSession, filename: str, schema: StructType, sep: str
) -> DataFrame:
    path = str(RAW_DIR / filename)
    return (
        spark.read.option("sep", sep)
        .option("header", "false")
        # Ab Initio reads blank fields as empty strings, not nulls. Disable
        # Spark's default empty-string->null coercion so the null("...") DML
        # defaults are applied deliberately in the conversion, not implicitly.
        .option("emptyValue", "")
        .option("nullValue", "\u0000")
        .schema(schema)
        .csv(path)
    )


def read_customers(spark: SparkSession) -> DataFrame:
    """Read customers.dat per customer.dml + customer_address.dml (comma-delimited)."""
    return _read_delimited(spark, "customers.dat", CUSTOMER_SCHEMA, ",")


def read_orders(spark: SparkSession) -> DataFrame:
    """Read orders.dat per the order extract layout (pipe-delimited)."""
    return _read_delimited(spark, "orders.dat", ORDER_SCHEMA, "|")


def read_transactions(spark: SparkSession) -> DataFrame:
    """Read transactions.dat per transaction_detail.dml (pipe-delimited)."""
    return _read_delimited(spark, "transactions.dat", TRANSACTION_SCHEMA, "|")


# Column order matters: it is the customer_cdc.pset HASH_COLUMNS list, and the CDC
# row hash concatenates these values in exactly this order.
CUSTOMER_MASTER_HASH_COLUMNS = [
    "customer_id",
    "name",
    "address",
    "phone",
    "email",
    "status",
]
CUSTOMER_MASTER_KEY_COLUMNS = ["customer_id"]


def read_customer_snapshot_previous(spark: SparkSession) -> DataFrame:
    """Read the previous customer-master snapshot (CDC PREVIOUS_SNAPSHOT_PATH)."""
    return _read_delimited(
        spark, "customer_snapshot_previous.dat", CUSTOMER_MASTER_SCHEMA, "|"
    )


def read_customer_snapshot_current(spark: SparkSession) -> DataFrame:
    """Read the current customer-master snapshot (CDC CURRENT_SNAPSHOT_PATH)."""
    return _read_delimited(
        spark, "customer_snapshot_current.dat", CUSTOMER_MASTER_SCHEMA, "|"
    )


def trimmed(df: DataFrame) -> DataFrame:
    """Trim string columns — Ab Initio fixed/delimited reads do not pad here, but
    this guards against stray whitespace from hand-edited extracts."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df
