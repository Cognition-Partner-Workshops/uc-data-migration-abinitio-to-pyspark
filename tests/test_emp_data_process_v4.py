"""emp_data_process_v4: DML parsing, graph semantics, and its reconciliation controls."""

from __future__ import annotations

import shutil
from decimal import Decimal

import pytest
from pyspark.sql.types import ArrayType, DateType, DecimalType, TimestampType

from src.common import dml
from src.common.io import OUT_ROOT, write_table
from src.common.spark import build_spark
from src.jobs import emp_data_process_v4 as job
from verify.reconcile import Reconciler

TS = "20260611 09:30:00"


@pytest.fixture(scope="module")
def spark():
    s = build_spark("pytest-emp-v4")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _frames(spark, vectors, emp_data):
    v = spark.createDataFrame(vectors, dml.VECTOR_INP.struct)
    e = spark.createDataFrame(
        [(n, loc, Decimal(sal), eid) for n, loc, sal, eid in emp_data],
        dml.EMP_DATA.struct,
    )
    return v, e


def _run(spark, vectors, emp_data, **kw):
    v, e = _frames(spark, vectors, emp_data)
    scanned, out0, out1 = job.transform(v, e, "20260611", TS, **kw)
    return scanned.orderBy("scan_seq").collect(), out0.collect(), out1


def test_dml_parses_embedded_formats():
    assert dml.EMP_DATA.field_delimiter == "|"
    assert [f.name for f in dml.EMP_DATA.fields] == [
        "emp_name",
        "location",
        "salary",
        "emp_id",
    ]
    assert dml.EMP_DATA.fields[-1].delimiter == "\n"
    op_vec = dml.struct_from_dml(dml.EMP_OP_VEC_DML).struct["op_vec"].dataType
    assert isinstance(op_vec, ArrayType)
    assert op_vec.elementType.fieldNames() == ["dept", "emp_id", "rating"]
    out = dml.struct_from_dml(dml.EMP_VECTORS_OP_DML)
    types = {f.name: f.spark_type for f in out.fields}
    assert isinstance(types["process_date"], DateType)
    assert isinstance(types["timestamp"], TimestampType)
    assert isinstance(types["cum_salary"], DecimalType)
    assert out.fields[0].fmt == "YYYYMMDD"


def test_dml_null_default():
    rec = dml.struct_from_dml(
        'record string("|") a = NULL; string("\\n", null("X")) b; end'
    )
    assert [f.default for f in rec.fields] == [None, "X"]


def test_vector_sort_normalize_join_scan(spark):
    scanned, out0, _ = _run(
        spark,
        [("HR", "E3,E1,E2", "B,A,C")],
        [
            (" ann ", "London", "900", "E1"),
            ("bob", "Leeds", "800", "E2"),
            ("cy", "Bath", "500", "E3"),
        ],
    )
    assert [r.emp_id for r in scanned] == ["E1", "E2", "E3"]
    assert [r.rating for r in scanned] == ["A", "C", "B"]
    assert [r.cum_salary for r in scanned] == [900, 1700, 2200]
    assert scanned[0].emp_name == "ANN" and scanned[0].location == "UK London"
    assert [r.emp_id for r in out0] == ["E3"]
    assert out0[0].timestamp.strftime("%Y%m%d %H:%M:%S") == TS


def test_threshold_is_strictly_greater_than_2000(spark):
    _, out0, out1 = _run(
        spark,
        [("HR", "E1,E2", "A,A")],
        [("a", "L", "2000", "E1"), ("b", "L", "0.01", "E2")],
    )
    assert [r.emp_id for r in out0] == ["E2"]
    assert [r.emp_id for r in out1.collect()] == ["E1"]


def test_inner_join_and_dedup_keep_first(spark):
    scanned, _, _ = _run(
        spark,
        [("HR", "E1,E2", "A,B"), ("IT", "E2,E9", "C,D")],
        [("first", "L", "1", "E2"), ("second", "L", "5", "E2"), ("x", "L", "1", "E7")],
    )
    # E1/E9 have no emp_data (inner join); E2 keeps its first in0/in1 match.
    assert [(r.emp_id, r.emp_name, r.dept) for r in scanned] == [("E2", "FIRST", "HR")]


def test_dedup_check_sort_aborts_on_unsorted_input(spark):
    with pytest.raises(job.AbInitioAbort):
        _run(
            spark,
            [("HR", "E5", "A"), ("HR", "E1", "A")],
            [("a", "L", "1", "E1"), ("b", "L", "1", "E5")],
        )


def test_fewer_ratings_than_emp_ids_aborts(spark):
    with pytest.raises(job.AbInitioAbort):
        _run(spark, [("HR", "E1,E2", "A")], [("a", "L", "1", "E1")])


def test_rollup_count_suppressed_on_empty_flow(spark):
    _, _, out1 = _run(spark, [("HR", "E1", "A")], [("a", "L", "5000", "E1")])
    assert job.rollup_count(out1).collect() == []


@pytest.mark.parametrize(
    "value,expected",
    [("1", True), ("yes", True), ("", False), ("0", False), ("False", False)],
)
def test_process_count_condition(value, expected):
    assert job.is_condition_true(value) is expected


@pytest.fixture
def ns_cleanup():
    names = []
    yield names
    for ns in names:
        shutil.rmtree(OUT_ROOT / ns, ignore_errors=True)


def _emp_results(ns):
    rec = Reconciler(ns)
    rec.run()
    return {r.name: r for r in rec.results if r.name.startswith("emp_v4_")}


def test_reconcile_passes_with_and_without_count(spark, ns_cleanup):
    ns = "pytest_emp_v4"
    ns_cleanup.append(ns)
    out = job.run(spark, ns, "20260611", "1", TS)
    assert "curated.emp_count" in out
    spark.stop()
    results = _emp_results(ns)
    assert len(results) == 4
    for name, r in results.items():
        assert r.status == "PASS", f"{name}: {r.detail}"

    spark2 = build_spark("pytest-emp-v4-nocount")
    out = job.run(spark2, ns, "20260611", "0", TS)
    spark2.stop()
    assert "curated.emp_count" not in out
    assert not (OUT_ROOT / ns / "curated" / "emp_count").exists()
    for name, r in _emp_results(ns).items():
        assert r.status == "PASS", f"{name}: {r.detail}"


def test_reconcile_fails_on_tampered_target(ns_cleanup):
    ns = "pytest_emp_v4_bad"
    ns_cleanup.append(ns)
    s = build_spark("pytest-emp-v4-bad")
    job.run(s, ns, "20260611", "1", TS)
    from pyspark.sql import functions as F

    tampered = s.read.parquet(str(OUT_ROOT / ns / "curated" / "emp_vectors_op"))
    tampered = tampered.withColumn(
        "location", F.regexp_replace("location", "^UK ", "")
    ).localCheckpoint()
    write_table(tampered, ns, "curated", "emp_vectors_op")
    s.stop()
    results = _emp_results(ns)
    assert results["emp_v4_vectors_op_parity"].status == "FAIL"
    assert results["emp_v4_completeness"].status == "PASS"
