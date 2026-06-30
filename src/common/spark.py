"""Local SparkSession factory.

In the Ab Initio world the runtime was the Co>Operating System (a licensed,
cluster-bound engine). The PySpark equivalent is a SparkSession; for the
workshop/demo it runs entirely locally (`local[*]`) with no cluster, so the
conversion and its reconciliation can be exercised on a laptop or in CI.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(app_name: str = "abinitio-to-pyspark") -> SparkSession:
    """Build (or get) a local SparkSession tuned for small, deterministic runs."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
