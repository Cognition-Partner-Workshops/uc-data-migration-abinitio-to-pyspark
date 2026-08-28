"""End-to-end test: run the converted pipeline, then assert reconciliation passes.

This exercises the same loop the demo does: build the PySpark tables for an
isolated namespace and prove every source->target control for the converted
pipelines passes (customers/orders/transactions). Controls for pipelines that are
not converted yet SKIP rather than fail.
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


@pytest.fixture(scope="module", autouse=True)
def built_namespace():
    spark = build_spark("pytest-build")
    spark.sparkContext.setLogLevel("ERROR")
    stg_customers.run(spark, NS)
    stg_orders.run(spark, NS)
    mart_daily_orders.run(spark, NS)
    curated_transactions.run(spark, NS)
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
        ("curated", "transactions"),
    ]:
        assert (OUT_ROOT / NS / layer / table).exists()


def test_orders_and_customers_controls_pass():
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
    for name in [
        "transactions_channel_parity",
        "transactions_completeness",
        "transactions_control_total",
        "transactions_channel_domain_parity",
        "transactions_merchant_default_parity",
        "transactions_refund_parity",
    ]:
        assert results[name].status == "PASS", f"{name}: {results[name].detail}"


def test_no_control_fails():
    assert not any(r.status == "FAIL" for r in _results().values())
