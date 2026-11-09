"""End-to-end test: run the converted pipeline, then assert reconciliation passes.

This exercises the same loop the demo does: build the PySpark tables for an
isolated namespace and prove every source->target control on `main` passes
(customers/orders), with the live-conversion control (transactions) correctly
SKIPping until that pipeline is converted.
"""

from __future__ import annotations

import shutil

import pytest

from src.common.io import OUT_ROOT
from src.jobs import (
    emp_data_process_v4,
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
    emp_data_process_v4.run(spark, NS, "20260611", "1")
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
        ("intermediate", "emp_v4_scan"),
        ("curated", "emp_vectors_op"),
        ("curated", "emp_count"),
        ("files", "vectors_op.dat"),
        ("files", "count_file.dat"),
    ]:
        assert (OUT_ROOT / NS / layer / table).exists()


def test_main_controls_pass():
    results = _results()
    for name in [
        "customers_completeness",
        "orders_completeness",
        "orders_control_total",
        "orders_daily_parity",
        "emp_v4_completeness",
        "emp_v4_control_total",
        "emp_v4_vectors_op_parity",
        "emp_v4_flat_file_parity",
    ]:
        assert results[name].status == "PASS", f"{name}: {results[name].detail}"


def test_transactions_control_skips_until_converted():
    results = _results()
    assert results["transactions_channel_parity"].status == "SKIP"
