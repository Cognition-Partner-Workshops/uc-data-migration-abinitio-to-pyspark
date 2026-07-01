"""End-to-end test: run the converted pipeline, then assert reconciliation passes.

This exercises the same loop the demo does: build the PySpark tables for an
isolated namespace and prove every source->target control passes — the
customers/orders pipelines plus the converted transactions pipeline
(completeness, control total, and the merchant/channel DML default parity).
"""

from __future__ import annotations

import shutil

import pytest

from src.common.io import OUT_ROOT
from src.jobs import (
    curated_transactions,
    mart_daily_orders,
    stg_customers,
    stg_orders,
)
from src.common.spark import build_spark
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
    """The transactions pipeline is now converted, so its controls must PASS —
    completeness, control total, and the merchant/channel DML default parity."""
    results = _results()
    for name in [
        "transactions_completeness",
        "transactions_control_total",
        "transactions_merchant_default_parity",
        "transactions_channel_parity",
    ]:
        assert results[name].status == "PASS", f"{name}: {results[name].detail}"
