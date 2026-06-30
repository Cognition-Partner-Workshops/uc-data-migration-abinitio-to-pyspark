"""Namespaced output paths for concurrent-safe, repeatable runs.

Every run writes under ``out/<namespace>/...`` so multiple runs (``NS=dev``,
``NS=child1``, an attendee's name, ...) never collide, and the durable "before"
data in ``data/raw/`` is never touched. This mirrors the SAS->Databricks demo's
catalog-namespace pattern, but on the local filesystem instead of Unity Catalog.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
OUT_ROOT = REPO_ROOT / "out"


def layer_path(namespace: str, layer: str, table: str) -> str:
    """Return the output path for a table in a layer of a namespace.

    layer is one of: staging, intermediate, marts, curated.
    """
    return str(OUT_ROOT / namespace / layer / table)


def write_table(df: DataFrame, namespace: str, layer: str, table: str) -> str:
    """Write a DataFrame as parquet to its namespaced layer path (overwrite)."""
    path = layer_path(namespace, layer, table)
    df.write.mode("overwrite").parquet(path)
    return path
