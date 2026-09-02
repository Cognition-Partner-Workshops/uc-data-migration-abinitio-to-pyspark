"""End-to-end test: run the converted pipeline, then assert reconciliation passes.

This exercises the same loop the demo does: build the PySpark tables for an
isolated namespace and prove every source->target control on `main` passes
(customers/orders/transactions), and that the transactions controls correctly
SKIP in a namespace where that pipeline has not been built.
"""

from __future__ import annotations

import shutil

import pytest

from src.common.io import OUT_ROOT
from src.common.spark import build_spark
from src.jobs import (
    curated_transactions,
    mart_daily_orders,
    stg_customers,
    stg_orders,
)
from verify.reconcile import Reconciler

NS = "pytest"
NS_NO_TXN = "pytest-no-txn"
TRANSACTION_CONTROLS = [
    "transactions_completeness",
    "transactions_control_total",
    "transactions_row_parity",
    "transactions_channel_domain_parity",
    "transactions_channel_parity",
]


@pytest.fixture(scope="module", autouse=True)
def built_namespace():
    spark = build_spark("pytest-build")
    spark.sparkContext.setLogLevel("ERROR")
    stg_customers.run(spark, NS)
    stg_orders.run(spark, NS)
    mart_daily_orders.run(spark, NS)
    curated_transactions.run(spark, NS)
    stg_customers.run(spark, NS_NO_TXN)
    spark.stop()
    yield
    shutil.rmtree(OUT_ROOT / NS, ignore_errors=True)
    shutil.rmtree(OUT_ROOT / NS_NO_TXN, ignore_errors=True)


def _results(ns: str = NS):
    rec = Reconciler(ns)
    rec.run()
    return {r.name: r for r in rec.results}


def test_outputs_exist():
    for layer, table in [
        ("staging", "customers"),
        ("staging", "orders"),
        ("marts", "daily_orders"),
        ("curated", "transactions"),
    ]:
        assert (OUT_ROOT / NS / layer / table).exists()


def test_main_controls_pass():
    results = _results()
    for name in [
        "customers_completeness",
        "orders_completeness",
        "orders_control_total",
        "orders_daily_parity",
    ]:
        assert results[name].status == "PASS", f"{name}: {results[name].detail}"


def test_transactions_controls_pass():
    results = _results()
    for name in TRANSACTION_CONTROLS:
        assert results[name].status == "PASS", f"{name}: {results[name].detail}"


def test_transactions_channel_default_applied():
    """DML `string("\\n", null("UNKNOWN")) channel`: blank source channels must
    surface as the literal 'UNKNOWN' (the playbook's worked example)."""
    spark = build_spark("pytest-channel")
    curated = spark.read.parquet(str(OUT_ROOT / NS / "curated" / "transactions"))
    domain = {r["channel"] for r in curated.select("channel").distinct().collect()}
    assert None not in domain and "" not in domain
    assert "UNKNOWN" in domain


def test_transactions_controls_skip_until_converted():
    results = _results(NS_NO_TXN)
    for name in TRANSACTION_CONTROLS:
        assert results[name].status == "SKIP", f"{name}: {results[name].detail}"
