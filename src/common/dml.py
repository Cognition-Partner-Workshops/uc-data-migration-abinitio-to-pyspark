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
    ArrayType,
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

# order_items.dml, comma-delimited with two *parallel repeating vectors*:
#
#   record
#     decimal(",") order_id;
#     decimal(",") item_count;
#     string(",")[item_count]  item_names;
#     decimal(",")[item_count] item_quantities;
#     string("\n") order_status;
#   end;
#
# Both vectors are sized by the single item_count field, so item_names[i] pairs
# positionally with item_quantities[i]. This cannot be read with the fixed-width
# StructType CSV reader (the column count varies per row with item_count), so
# read_order_items() below parses it from raw text. This StructType documents the
# parsed record shape that reader returns (one row per order, vectors intact).
ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), False),
        StructField("item_count", IntegerType(), True),
        StructField("item_names", ArrayType(StringType()), True),
        StructField("item_quantities", ArrayType(LongType()), True),
        StructField("order_status", StringType(), True),
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


def read_order_items(spark: SparkSession) -> DataFrame:
    """Read order_items.dat per order_items.dml, returning one row per order with
    the two parallel repeating vectors intact (item_names, item_quantities).

    The record is variable-width (its token count depends on item_count), so the
    fixed-schema CSV reader cannot describe it. We parse the comma-delimited text
    directly, reproducing the DML record contract exactly:

    - ``item_count`` (field 2) governs how many tokens each vector consumes, so we
      slice exactly ``item_count`` names then exactly ``item_count`` quantities,
      starting right after the two header fields. This bounds any downstream
      explode to item_count — never more, never fewer.
    - ``order_status`` is the newline-terminated tail, i.e. the final token.
    - ``order_status`` has **no** ``null(...)`` default in the DML (unlike the
      ``channel = null("UNKNOWN")`` field in transaction_detail.dml). A blank is
      therefore preserved as the empty string it is read as — we deliberately do
      *not* substitute a default and, by splitting the raw text ourselves, do not
      let Spark's CSV reader coerce the blank to NULL.
    """
    path = str(RAW_DIR / "order_items.dat")
    lines = spark.read.text(path).where(F.length(F.trim(F.col("value"))) > 0)
    toks = lines.select(F.split(F.col("value"), ",").alias("t"))
    parsed = toks.select(
        F.col("t").getItem(0).cast(LongType()).alias("order_id"),
        F.col("t").getItem(1).cast(IntegerType()).alias("item_count"),
        F.col("t").alias("t"),
    )
    # item_names  = t[2 : 2 + item_count]   (Spark slice is 1-based -> start = 3)
    # item_quantities = t[2 + item_count : 2 + 2*item_count]
    # order_status = last token (newline-terminated tail)
    return parsed.select(
        "order_id",
        "item_count",
        F.expr("slice(t, 3, item_count)").alias("item_names"),
        F.expr(
            "transform(slice(t, 3 + item_count, item_count), x -> cast(x as bigint))"
        ).alias("item_quantities"),
        F.element_at(F.col("t"), F.size(F.col("t"))).alias("order_status"),
    )


def trimmed(df: DataFrame) -> DataFrame:
    """Trim string columns — Ab Initio fixed/delimited reads do not pad here, but
    this guards against stray whitespace from hand-edited extracts."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df
