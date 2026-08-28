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


# transaction_detail.dml declares nested sub-records that the flat extract carries
# in flattened-header form: an embedded `merchant_info` record, a `line_items`
# vector sized by `item_count`, and a conditional `refund_details` record emitted
# only when `txn_type == 2`. The nesting is rebuilt in the conversion (see
# src/jobs/curated_transactions.py) rather than parsed from the file.
MERCHANT_INFO_STRUCT = StructType(
    [
        # string(",", null("")) merchant_name — the DML default is the empty
        # string, so a blank merchant name is "" and never NULL.
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DecimalType(12, 2), True),
    ]
)

LINE_ITEM_STRUCT = StructType(
    [
        StructField("sku", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("line_total", DecimalType(12, 2), True),
    ]
)

REFUND_DETAILS_STRUCT = StructType(
    [
        StructField("original_txn_id", StringType(), True),
        StructField("refund_reason", StringType(), True),
    ]
)

# DML null(...) substitutions declared in transaction_detail.dml.
CHANNEL_DML_DEFAULT = "UNKNOWN"  # string("\n", null("UNKNOWN")) channel
MERCHANT_NAME_DML_DEFAULT = ""  # string(",", null("")) merchant_name

# txn_type values the DML's conditional record keys off: 2 selects refund_details.
TXN_TYPE_REFUND = 2


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


def trimmed(df: DataFrame) -> DataFrame:
    """Trim string columns — Ab Initio fixed/delimited reads do not pad here, but
    this guards against stray whitespace from hand-edited extracts."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df
