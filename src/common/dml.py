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
    emp_data_process_v4.mp layouts      -> pipe-delimited, parsed from the DML
                                           text embedded in the graph
                                           (``struct_from_dml``)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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


def _read_delimited(
    spark: SparkSession,
    filename: str,
    schema: StructType,
    sep: str,
    *,
    strict: bool = False,
) -> DataFrame:
    """Read a delimited extract from ``data/raw/``.

    ``strict=True`` mirrors an Ab Initio input component with
    ``reject-threshold = Abort on first reject`` and no quoting: quote characters
    are data, and any record that does not parse against the DML aborts the read.
    """
    path = str(RAW_DIR / filename)
    reader = (
        spark.read.option("sep", sep)
        .option("header", "false")
        # Ab Initio reads blank fields as empty strings, not nulls. Disable
        # Spark's default empty-string->null coercion so the null("...") DML
        # defaults are applied deliberately in the conversion, not implicitly.
        .option("emptyValue", "")
        .option("nullValue", "\u0000")
    )
    if strict:
        reader = reader.option("quote", "").option("mode", "FAILFAST")
    return reader.schema(schema).csv(path)


def read_customers(spark: SparkSession) -> DataFrame:
    """Read customers.dat per customer.dml + customer_address.dml (comma-delimited)."""
    return _read_delimited(spark, "customers.dat", CUSTOMER_SCHEMA, ",")


def read_orders(spark: SparkSession) -> DataFrame:
    """Read orders.dat per the order extract layout (pipe-delimited)."""
    return _read_delimited(spark, "orders.dat", ORDER_SCHEMA, "|")


def read_transactions(spark: SparkSession) -> DataFrame:
    """Read transactions.dat per transaction_detail.dml (pipe-delimited)."""
    return _read_delimited(spark, "transactions.dat", TRANSACTION_SCHEMA, "|")


# ---------------------------------------------------------------------------
# Embedded-DML parser (record formats pasted from a graph's .mp / generated .ksh)
# ---------------------------------------------------------------------------

# A delimited ``decimal("|")`` has no declared precision; it holds whatever the
# extract carries. Widest exact Spark decimal with room for fractional values.
UNBOUNDED_DECIMAL = DecimalType(38, 10)

_DML_TOKEN = re.compile(r'"(?:\\.|[^"\\])*"|[A-Za-z_][A-Za-z0-9_]*|\d+|\S')


@dataclass(frozen=True)
class DmlField:
    name: str
    spark_type: DataType
    delimiter: str | None
    fmt: str | None = None
    default: str | None = None


@dataclass(frozen=True)
class DmlRecord:
    fields: tuple[DmlField, ...]

    @property
    def struct(self) -> StructType:
        return StructType(
            [StructField(f.name, f.spark_type, True) for f in self.fields]
        )

    @property
    def field_delimiter(self) -> str:
        """The single inter-field delimiter of a flat delimited record."""
        delims = {f.delimiter for f in self.fields[:-1]}
        if len(delims) != 1 or None in delims:
            raise ValueError(f"not a single-delimiter flat record: {delims}")
        return delims.pop()


def _unescape(literal: str) -> str:
    body = literal[1:-1]
    # .mp files escape '|' as '\|' and newlines as '\\n'.
    body = body.replace("\\\\", "\\").replace("\\|", "|")
    return body.encode("utf-8").decode("unicode_escape")


class _DmlParser:
    def __init__(self, text: str):
        text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
        text = re.sub(r"//[^\n]*", " ", text)
        self.toks = _DML_TOKEN.findall(text)
        self.i = 0

    def peek(self) -> str | None:
        return self.toks[self.i] if self.i < len(self.toks) else None

    def take(self, expected: str | None = None) -> str:
        tok = self.peek()
        if tok is None or (expected is not None and tok != expected):
            raise ValueError(f"DML parse error at token {self.i}: {tok!r}")
        self.i += 1
        return tok

    def args(self) -> list[str]:
        self.take("(")
        out: list[str] = []
        depth = 0
        while True:
            tok = self.take()
            if tok == ")" and depth == 0:
                return out
            depth += tok == "("
            depth -= tok == ")"
            out.append(tok)

    def record(self) -> DmlRecord:
        self.take("record")
        fields: list[DmlField] = []
        while self.peek() != "end":
            fields.append(self.field())
        self.take("end")
        return DmlRecord(tuple(fields))

    def field(self) -> DmlField:
        kind = self.peek()
        if kind == "record":
            inner = self.record()
            self.take("[")
            while self.take() != "]":
                pass
            name = self.take()
            self._finish()
            return DmlField(name, ArrayType(inner.struct, True), None)
        kind = self.take()
        args = self.args()
        literals = [_unescape(a) for a in args if a.startswith('"')]
        fmt = None
        if kind in ("date", "datetime"):
            fmt = literals[0] if literals else None
            delim_args = self.args() if self.peek() == "(" else []
            literals = [_unescape(a) for a in delim_args if a.startswith('"')]
            delimiter = literals[0] if literals else None
        else:
            delimiter = literals[0] if literals else None
        default = None
        if "null" in args:
            j = args.index("null")
            default = _unescape(args[j + 2]) if args[j + 2].startswith('"') else None
        spark_type: DataType = {
            "string": StringType(),
            "decimal": UNBOUNDED_DECIMAL,
            "integer": LongType(),
            "date": DateType(),
            "datetime": TimestampType(),
        }[kind]
        name = self.take()
        self._finish()
        return DmlField(name, spark_type, delimiter, fmt, default)

    def _finish(self) -> None:
        if self.peek() == "=":
            self.take("=")
            while self.peek() != ";":
                self.take()
        self.take(";")


def struct_from_dml(text: str) -> DmlRecord:
    """Parse an Ab Initio DML ``record ... end`` into a :class:`DmlRecord`.

    Supports the subset used by the converted graphs: delimited ``string``,
    ``decimal``, ``integer``, ``date("fmt")(delim)``, ``datetime("fmt")(delim)``,
    ``null("X")`` defaults, ``= NULL`` initialisers and nested
    ``record ... end[int] name`` vectors (-> ``ArrayType(StructType)``).
    """
    return _DmlParser(text).record()


# ---------------------------------------------------------------------------
# emp_data_process_v4.mp record formats (verbatim from the graph)
# ---------------------------------------------------------------------------

# $AI_DML/vector_inp.dml is referenced by the graph's Input File but was not
# supplied with it. Layout inferred from the Reformat transform that reads it
# (in.dept, in.emp_id, in.rating; emp_id/rating are comma-separated lists) and
# from the Normalize/Reformat out formats.
VECTOR_INP_DML = """record
string("|") dept;
string("|") emp_id;
string("\\n") rating;
end"""

# Input File-2 read_metadata (emp_data.dat)
EMP_DATA_DML = """record
string("|") emp_name;
string("|") location;
decimal("|") salary;
string("\\n") emp_id;
end"""

# Reformat out0_metadata: the op_vec built by the vector transform.
EMP_OP_VEC_DML = """record
        record
        string("\\t") dept;
        string("\\t") emp_id;
        string("\\n") rating;
        end[int] op_vec;
string("") rank = NULL;
end;"""

# Reformat-1 out0_metadata (vectors_op.dat)
EMP_VECTORS_OP_DML = """record
        date("YYYYMMDD")("|") process_date;
        datetime("YYYYMMDD HH24:MI:SS")("|") timestamp;
        string("|") emp_id;
        string("|") emp_name;
        string("|") location;
        string("|") dept;
        decimal("|") cum_salary;
        string("\\n") rating;
end"""

# Get_Count Rollup write_metadata (count_file.dat)
EMP_COUNT_DML = """record
        decimal("\\n") total_count;
end"""

VECTOR_INP = struct_from_dml(VECTOR_INP_DML)
EMP_DATA = struct_from_dml(EMP_DATA_DML)


def read_vectors_inp(spark: SparkSession) -> DataFrame:
    """Read vectors_inp.dat per the (inferred) vector_inp.dml (pipe-delimited)."""
    return _read_delimited(
        spark,
        "vectors_inp.dat",
        VECTOR_INP.struct,
        VECTOR_INP.field_delimiter,
        strict=True,
    )


def read_emp_data(spark: SparkSession) -> DataFrame:
    """Read emp_data.dat per Input File-2's embedded DML (pipe-delimited)."""
    return _read_delimited(
        spark, "emp_data.dat", EMP_DATA.struct, EMP_DATA.field_delimiter, strict=True
    )


def trimmed(df: DataFrame) -> DataFrame:
    """Trim string columns — Ab Initio fixed/delimited reads do not pad here, but
    this guards against stray whitespace from hand-edited extracts."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df
