"""Curated job: emp_data_process_v4 (employee vector -> cumulative salary).

Ab Initio source: graph ``emp_data_process_v4.mp`` (graph parameters
``PROCESS_DATE``, ``PROCESS_COUNT``), deployed via its generated KornShell
wrapper. All components run on the serial ``o_p_file`` layout, so record order
is the file order of each input; this job carries explicit sequence columns to
reproduce that order wherever the graph depends on it.

Ab Initio component                         -> PySpark step
  Input File  (vectors_inp.dat, vector_inp.dml)   -> dml.read_vectors_inp
  Reformat    (build + vector_sort op_vec)        -> build_op_vec
  Normalize   (Normalize.empdata.xfr)             -> normalize_op_vec
  Input File-2 (emp_data.dat, embedded DML)       -> dml.read_emp_data
  Partition by Key / Partition by Key-1 {emp_id}  -> Spark join shuffle (no-op on
                                                     the serial layout)
  Join {emp_id}, inner, hash, driving in0         -> join_emp
  Dedup Sorted {emp_id}, keep first, check-sort   -> dedup_sorted_first
  Scan {dept} (hash-scan), cum_salary = sum       -> scan_cum_salary
  Reformat-1 output_index (cum_salary > 2000)     -> split_by_output_index
    out0 -> o/p file  vectors_op.dat              -> curated/emp_vectors_op
    out1 -> Get_Count subgraph (if $PROCESS_COUNT)
            Rollup {} count(emp_id)               -> curated/emp_count
  LKP: F1/F2/F3 (lookup files)                    -> not referenced by any
                                                     transform; not converted
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common import dml
from src.common.io import OUT_ROOT, layer_path, write_table

ORDER_COLS = ["_in0_seq", "_vec_pos", "_in1_seq"]
OP_VEC_TYPE = dml.struct_from_dml(dml.EMP_OP_VEC_DML).struct["op_vec"].dataType
CUM_SALARY_THRESHOLD = 2000


class AbInitioAbort(RuntimeError):
    """A condition that makes the legacy graph abort (reject-threshold = abort)."""


def is_condition_true(value: str) -> bool:
    """Ab Initio conditional-component semantics for ``$PROCESS_COUNT``: the
    component is disabled when the condition is empty, 0 or false."""
    return value.strip().lower() not in ("", "0", "false", "f")


def build_op_vec(vectors: DataFrame) -> DataFrame:
    """Reformat: split emp_id/rating on ',' into a vector of {dept, emp_id,
    rating} records and ``vector_sort(temp_vec, {emp_id; rating})``."""
    emp_vec = F.split(F.col("emp_id"), ",")
    rating_vec = F.split(F.col("rating"), ",")
    short = vectors.where(F.size(rating_vec) < F.size(emp_vec)).limit(1).collect()
    if short:
        # rating_vec[k] past the end is a DML vector-index error -> reject -> abort.
        raise AbInitioAbort(f"Reformat: fewer ratings than emp_ids in {short[0]}")
    op_vec = F.transform(
        emp_vec,
        lambda e, k: F.struct(
            F.col("dept").alias("dept"),
            e.alias("emp_id"),
            rating_vec[k].alias("rating"),
        ),
    )
    # Every element shares the row's dept, so sorting the struct is exactly
    # vector_sort on {emp_id; rating}.
    return vectors.select(
        "_in0_seq", F.array_sort(op_vec).cast(OP_VEC_TYPE).alias("op_vec")
    )


def normalize_op_vec(df: DataFrame) -> DataFrame:
    """Normalize: one output record per op_vec element, in vector order."""
    return df.select("_in0_seq", F.posexplode("op_vec").alias("_vec_pos", "v")).select(
        "_in0_seq", "_vec_pos", "v.dept", "v.emp_id", "v.rating"
    )


def join_emp(in0: DataFrame, in1: DataFrame) -> DataFrame:
    """Join {emp_id}, inner, transform Join-7.xfr."""
    return (
        in0.alias("in0")
        .join(in1.alias("in1"), F.col("in0.emp_id") == F.col("in1.emp_id"), "inner")
        .select(
            F.col("in0.emp_id").alias("emp_id"),
            # string_lrtrim strips leading/trailing blanks (Spark trim = spaces).
            F.trim(F.col("in1.emp_name")).alias("emp_name"),
            F.concat(F.lit("UK "), F.col("in1.location")).alias("location"),
            F.col("in0.dept").alias("dept"),
            F.col("in1.salary").alias("salary"),
            F.col("in0.rating").alias("rating"),
            *[F.col(c) for c in ORDER_COLS],
        )
    )


def dedup_sorted_first(df: DataFrame, *, check_sort: bool = True) -> DataFrame:
    """Dedup Sorted {emp_id}, keep first, check_sort = True."""
    if check_sort:
        prev = F.lag("emp_id").over(Window.orderBy(*ORDER_COLS))
        bad = (
            df.withColumn("_prev", prev)
            .where(F.col("_prev") > F.col("emp_id"))
            .limit(1)
            .collect()
        )
        if bad:
            # Source-faithful: the legacy graph aborts on the first out-of-order
            # record rather than deduplicating unsorted input.
            raise AbInitioAbort(f"Dedup Sorted: input not sorted on emp_id at {bad[0]}")
    w = Window.partitionBy("emp_id").orderBy(*ORDER_COLS)
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def scan_cum_salary(df: DataFrame, process_date: str) -> DataFrame:
    """Scan {dept} (hash-scan, arrival order): running SUM(salary) per dept,
    emp_name upper-cased, process_date from ``$PROCESS_DATE``. ``scan_seq`` keeps
    the arrival order for reconciliation."""
    running = (
        Window.partitionBy("dept")
        .orderBy(*ORDER_COLS)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return df.select(
        F.row_number().over(Window.orderBy(*ORDER_COLS)).alias("scan_seq"),
        F.to_date(F.lit(process_date), "yyyyMMdd").alias("process_date"),
        "emp_id",
        F.upper("emp_name").alias("emp_name"),
        "location",
        "dept",
        F.sum("salary").over(running).cast(dml.UNBOUNDED_DECIMAL).alias("cum_salary"),
        "rating",
    )


def split_by_output_index(
    scanned: DataFrame, job_timestamp: str
) -> tuple[DataFrame, DataFrame]:
    """Reformat-1: ``if (in.cum_salary > 2000) 0 else 1``. out0 adds
    ``timestamp = now1()`` (one value per job run); out1 is a pass-through."""
    to_out0 = F.col("cum_salary") > CUM_SALARY_THRESHOLD
    out0 = (
        scanned.where(to_out0)
        .orderBy("scan_seq")
        .select(
            "process_date",
            F.to_timestamp(F.lit(job_timestamp), "yyyyMMdd HH:mm:ss").alias(
                "timestamp"
            ),
            "emp_id",
            "emp_name",
            "location",
            "dept",
            "cum_salary",
            "rating",
        )
    )
    out1 = scanned.where(~to_out0).drop("scan_seq")
    return out0, out1


def rollup_count(out1: DataFrame) -> DataFrame:
    """Get_Count/Rollup {} count(in.emp_id), output_without_input = Never: an
    empty out1 flow produces no count record at all (not a 0)."""
    agg = out1.agg(F.count("emp_id").cast("long").alias("total_count"))
    return agg.where(F.lit(out1.limit(1).count() > 0))


def fmt_decimal(col: Column) -> Column:
    """Delimited decimal("|") text: no trailing fractional zeros / bare '.'."""
    s = col.cast("string")
    return F.regexp_replace(F.regexp_replace(s, r"(\.\d*?)0+$", "$1"), r"\.$", "")


def to_delimited(df: DataFrame) -> DataFrame:
    """Render rows exactly as the legacy pipe-delimited, newline-terminated file."""
    cols = []
    for field in df.schema.fields:
        c = F.col(field.name)
        kind = field.dataType.typeName()
        if kind == "date":
            c = F.date_format(c, "yyyyMMdd")
        elif kind == "timestamp":
            c = F.date_format(c, "yyyyMMdd HH:mm:ss")
        elif kind == "decimal" or kind == "long":
            c = fmt_decimal(c)
        cols.append(F.coalesce(c.cast("string"), F.lit("")))
    return df.select(F.concat_ws("|", *cols).alias("value"))


def params_path(namespace: str) -> str:
    return str(OUT_ROOT / namespace / "files" / "emp_data_process_v4.params.json")


def write_flat_file(df: DataFrame, namespace: str, name: str) -> str:
    path = str(OUT_ROOT / namespace / "files" / name)
    to_delimited(df).coalesce(1).write.mode("overwrite").text(path)
    return path


def transform(
    vectors: DataFrame,
    emp_data: DataFrame,
    process_date: str,
    job_timestamp: str,
    *,
    check_sort: bool = True,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Run the graph's phase 0. Returns (scanned, out0, out1)."""
    vectors = vectors.withColumn("_in0_seq", F.monotonically_increasing_id())
    emp_data = emp_data.withColumn("_in1_seq", F.monotonically_increasing_id())
    normalized = normalize_op_vec(build_op_vec(vectors))
    deduped = dedup_sorted_first(join_emp(normalized, emp_data), check_sort=check_sort)
    scanned = scan_cum_salary(deduped, process_date).cache()
    out0, out1 = split_by_output_index(scanned, job_timestamp)
    return scanned, out0, out1


def run(
    spark: SparkSession,
    namespace: str,
    process_date: str,
    process_count: str = "1",
    job_timestamp: str | None = None,
) -> dict[str, str]:
    job_timestamp = job_timestamp or datetime.now().strftime("%Y%m%d %H:%M:%S")
    scanned, out0, out1 = transform(
        dml.read_vectors_inp(spark),
        dml.read_emp_data(spark),
        process_date,
        job_timestamp,
    )
    written = {
        "intermediate.emp_v4_scan": write_table(
            scanned, namespace, "intermediate", "emp_v4_scan"
        ),
        "curated.emp_vectors_op": write_table(
            out0, namespace, "curated", "emp_vectors_op"
        ),
        "files/vectors_op.dat": write_flat_file(
            spark.read.parquet(layer_path(namespace, "curated", "emp_vectors_op")),
            namespace,
            "vectors_op.dat",
        ),
    }
    count_paths = {
        "curated.emp_count": layer_path(namespace, "curated", "emp_count"),
        "files/count_file.dat": str(OUT_ROOT / namespace / "files" / "count_file.dat"),
    }
    if is_condition_true(process_count):
        counts = rollup_count(out1)
        written["curated.emp_count"] = write_table(
            counts, namespace, "curated", "emp_count"
        )
        written["files/count_file.dat"] = write_flat_file(
            counts, namespace, "count_file.dat"
        )
    else:
        # Disabled conditional component: count_file.dat is not produced. Clear
        # any output left by an earlier run of this namespace.
        for path in count_paths.values():
            shutil.rmtree(path, ignore_errors=True)
    scanned.unpersist()
    params = {
        "PROCESS_DATE": process_date,
        "PROCESS_COUNT": process_count,
        "job_timestamp": job_timestamp,
    }
    with open(params_path(namespace), "w", encoding="utf-8") as fh:
        json.dump(params, fh, indent=2)
    written["files/emp_data_process_v4.params.json"] = params_path(namespace)
    return written
