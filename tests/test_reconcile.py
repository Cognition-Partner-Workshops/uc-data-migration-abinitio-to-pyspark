"""End-to-end test: run the converted pipeline, then assert reconciliation passes.

This exercises the same loop the demo does: build the PySpark tables for an
isolated namespace and prove every source->target control on `main` passes
(customers/orders/cust_txn_summary), with the live-conversion control (transactions) correctly
SKIPping until that pipeline is converted.
"""

from __future__ import annotations

import shutil
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.common.io import OUT_ROOT, write_table
from src.common.spark import build_spark
from src.jobs import cust_txn_summary, mart_daily_orders, stg_customers, stg_orders
from verify.reconcile import Reconciler

NS = "pytest"


@pytest.fixture(scope="module", autouse=True)
def built_namespace():
    spark = build_spark("pytest-build")
    spark.sparkContext.setLogLevel("ERROR")
    stg_customers.run(spark, NS)
    stg_orders.run(spark, NS)
    mart_daily_orders.run(spark, NS)
    cust_txn_summary.run(spark, NS, process_date="20260115")
    spark.stop()
    yield
    shutil.rmtree(OUT_ROOT / NS, ignore_errors=True)


def _results():
    rec = Reconciler(NS)
    rec.run()
    return {r.name: r for r in rec.results}


def test_outputs_exist():
    for layer, table in [
        ("staging", "customers"),
        ("staging", "orders"),
        ("marts", "daily_orders"),
        ("marts", "cust_txn_summary"),
        ("intermediate", "cust_txn_rejects"),
        ("intermediate", "cust_txn_clean"),
    ]:
        assert (OUT_ROOT / NS / layer / table).exists()


def test_main_controls_pass():
    results = _results()
    for name in [
        "customers_completeness",
        "orders_completeness",
        "orders_control_total",
        "orders_daily_parity",
        "cust_txn_completeness",
        "cust_txn_control_total",
        "cust_txn_summary_parity",
        "cust_txn_rejects_parity",
        "cust_txn_reformat_parity",
    ]:
        assert results[name].status == "PASS", f"{name}: {results[name].detail}"


def test_transactions_control_skips_until_converted():
    results = _results()
    assert results["transactions_channel_parity"].status == "SKIP"


def test_cust_txn_summary_golden_values():
    """Hand-derived from txn.dat + cust_txn_summary.mp: amount <= 0 (T004, T006)
    is rejected, so customers 103/104 drop out; EUR is summed as-is."""
    spark = build_spark("pytest-golden")
    try:
        summary = [
            tuple(r)
            for r in spark.read.parquet(
                str(OUT_ROOT / NS / "marts" / "cust_txn_summary")
            )
            .orderBy("cust_id")
            .collect()
        ]
        rejects = spark.read.parquet(
            str(OUT_ROOT / NS / "intermediate" / "cust_txn_rejects")
        )
        clean = spark.read.parquet(
            str(OUT_ROOT / NS / "intermediate" / "cust_txn_clean")
        )
        reject_ids = sorted(r.txn_id for r in rejects.collect())
        channels = {r.txn_id: r.channel for r in clean.collect()}
        currencies = {r.txn_id: r.currency for r in clean.collect()}
    finally:
        spark.stop()
    assert summary == [
        (101, 3, Decimal("155.50"), date(2026, 1, 9)),
        (102, 2, Decimal("86.00"), date(2026, 1, 11)),
        (105, 1, Decimal("999.99"), date(2026, 1, 12)),
        (106, 1, Decimal("42.00"), date(2026, 1, 15)),
    ]
    assert reject_ids == ["T004", "T006"]
    assert channels["T003"] == "UNKNOWN" and channels["T007"] == "UNKNOWN"
    assert channels["T001"] == "web"  # channel is not upcased in the graph
    assert currencies["T001"] == "GBP" and currencies["T002"] == "GBP"
    assert "T009" in channels  # txn_id trimmed from " T009 "


def test_cust_txn_reformat_parity_catches_null_channel():
    """Negative control: dropping the NULL("") -> "UNKNOWN" default must FAIL."""
    ns = "pytest_neg"
    spark = build_spark("pytest-neg")
    try:
        cleaned, _, _ = cust_txn_summary.transform(spark)
        broken = cleaned.withColumn(
            "channel",
            F.when(F.col("channel") == "UNKNOWN", F.lit(None)).otherwise(
                F.col("channel")
            ),
        )
        write_table(broken, ns, "intermediate", "cust_txn_clean")
    finally:
        spark.stop()
    try:
        rec = Reconciler(ns)
        rec.run()
        result = {r.name: r for r in rec.results}["cust_txn_reformat_parity"]
        assert result.status == "FAIL"
        assert result.metrics["blank_channels"] == 2
    finally:
        shutil.rmtree(OUT_ROOT / ns, ignore_errors=True)
