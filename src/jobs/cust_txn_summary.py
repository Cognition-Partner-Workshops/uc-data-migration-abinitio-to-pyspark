"""Mart job: customer transaction summary.

Ab Initio source: `cust_txn_summary.mp` (wrapper `run_cust_txn_summary.ksh`),
kept for review under `legacy/cust_txn_summary/`. Graph flow:

    Input_File (txn.dat) -> Clean_Txn (Reformat) -> Valid_Amount (Filter by
    Expression: amount > 0) -> Sort_Cust {cust_id} -> Rollup_Cust {cust_id}
    -> Output_Summary (cust_summary.dat)
    Valid_Amount.deselect -> Output_Rejects (txn_rejects.dat)

PySpark equivalent:
    Input_File       -> dml.read_cust_txn (DML-derived StructType, FAILFAST)
    Clean_Txn        -> select (string_lrtrim / string_upcase / first_defined)
    Valid_Amount     -> where(amount > 0) / where(NOT amount > 0)
    Sort + Rollup    -> groupBy(cust_id).agg(count, sum, max), ordered by cust_id
    Output_Summary   -> marts/cust_txn_summary
    Output_Rejects   -> intermediate/cust_txn_rejects
    Clean_Txn.out0   -> intermediate/cust_txn_clean (added, not a graph output: it
                        exposes the reformat so its defaults can be reconciled)

Source-faithful quirks (reproduced, not endorsements):
    - The graph declares a MIN_AMOUNT parameter (default "0") but Valid_Amount
      hard-codes `amount > 0`; the parameter is unused, so it is here too.
    - PROCESS_DATE is passed by the wrapper but never referenced by the graph.
    - Rollup sums `amount` across currencies (e.g. EUR and GBP rows for one
      customer would be added together); no FX conversion is applied.
    - channel is neither trimmed nor upcased (only blank -> "UNKNOWN"), whereas
      currency is trimmed and upcased.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import write_table

# Graph-level PSET parameter; declared by the graph but not used by Valid_Amount.
MIN_AMOUNT = "0"


def valid_amount():
    """Valid_Amount select_expr, hard-coded in the graph as `amount > 0`."""
    return F.col("amount") > F.lit(0)


def clean(spark: SparkSession) -> DataFrame:
    """Input_File + Clean_Txn reformat (out0_metadata == read_metadata)."""
    txn = dml.read_cust_txn(spark)
    return txn.select(
        # out.txn_id :: string_lrtrim(in.txn_id);
        F.trim(F.col("txn_id")).alias("txn_id"),
        "cust_id",
        "txn_date",
        "amount",
        # out.currency :: string_upcase(string_lrtrim(in.currency));
        F.upper(F.trim(F.col("currency"))).alias("currency"),
        # DML `channel = NULL("")` makes an empty field NULL, then
        # out.channel :: first_defined(in.channel, "UNKNOWN");
        F.coalesce(F.nullif(F.col("channel"), F.lit("")), F.lit("UNKNOWN")).alias(
            "channel"
        ),
    )


def transform(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Return (clean, summary, rejects) DataFrames."""
    cleaned = clean(spark)
    valid = cleaned.where(valid_amount())
    # Filter by Expression routes records whose select_expr is not true to deselect.
    rejects = cleaned.where(~valid_amount())
    summary = (
        valid.groupBy("cust_id")
        .agg(
            # out.txn_count :: count(in.txn_id);
            F.count("txn_id").alias("txn_count"),
            # out.total_amount :: sum(in.amount);
            F.sum("amount").alias("total_amount"),
            # out.last_txn_date :: max(in.txn_date);
            F.max("txn_date").alias("last_txn_date"),
        )
        # Sort_Cust {cust_id} feeds a sorted Rollup, so output is in key order.
        .orderBy("cust_id")
    )
    return cleaned, summary, rejects


def run(spark: SparkSession, namespace: str, process_date: str | None = None) -> str:
    """Run the graph into `namespace`. process_date is accepted for parity with
    the wrapper's `-PROCESS_DATE` argument; the graph does not use it."""
    cleaned, summary, rejects = transform(spark)
    write_table(cleaned, namespace, "intermediate", "cust_txn_clean")
    write_table(rejects, namespace, "intermediate", "cust_txn_rejects")
    return write_table(summary, namespace, "marts", "cust_txn_summary")
