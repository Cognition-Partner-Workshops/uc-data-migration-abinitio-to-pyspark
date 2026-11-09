#!/usr/bin/env python3
"""
reconcile.py — source -> target reconciliation report for the Ab Initio -> PySpark
migration.

Why this exists
---------------
The point of the migration is not to produce *some* PySpark output — it is to
produce output we can *trust* reproduces what the legacy Ab Initio graphs would
have produced. Because there is no live Co>Operating System runtime here, "trust"
is established the way an Ab Initio analyst established it: deterministic
reconciliation controls (row counts, control totals, per-key parity, domain
coverage) between the raw source extracts and the converted PySpark tables.

Each control reads the **source** straight from ``data/raw/`` (via the DML-derived
readers) and the **target** from ``out/<namespace>/`` (the converted parquet
tables), then compares them. A control returns FAIL on any divergence, SKIP when
a prerequisite (e.g. the live-converted transactions curated table) has not been
produced yet, and PASS otherwise. The script exits non-zero if any control FAILs,
so it doubles as a CI / pre-merge gate.

Controls present on ``main`` cover the customer and orders pipelines and the
``cust_txn_summary.mp`` graph (``cust_txn_*``). Converting a
new program (transactions, customer-CDC) adds its matching controls here — see
.workshop/playbooks/abinitio-to-pyspark-conversion.devin.md and
.agents/skills/abinitio-to-pyspark-conversion/SKILL.md for the contract.

Usage:
    python verify/reconcile.py --namespace dev
    python verify/reconcile.py --namespace run1 --report reconciliation_report.md
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Make 'src' importable when run as a script (python verify/reconcile.py).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F

from src.common import dml
from src.common.io import OUT_ROOT, layer_path
from src.common.spark import build_spark


@dataclass
class CheckResult:
    name: str
    status: str  # PASS | FAIL | SKIP
    detail: str = ""
    metrics: dict = field(default_factory=dict)


class Reconciler:
    def __init__(self, namespace: str):
        self.ns = namespace
        self.spark = build_spark(f"reconcile-{namespace}")
        self.spark.sparkContext.setLogLevel("ERROR")
        self.results: list[CheckResult] = []

    def _target_exists(self, layer: str, table: str) -> bool:
        return (Path(layer_path(self.ns, layer, table))).exists()

    def _read_target(self, layer: str, table: str):
        return self.spark.read.parquet(layer_path(self.ns, layer, table))

    # ------------------------------------------------------------------ checks
    def check_customers_completeness(self):
        """Staging customers must equal the source customer population (no loss)."""
        expected = dml.read_customers(self.spark).count()
        if not self._target_exists("staging", "customers"):
            self.results.append(
                CheckResult(
                    "customers_completeness",
                    "SKIP",
                    "staging.customers not produced yet",
                )
            )
            return
        actual = self._read_target("staging", "customers").count()
        ok = expected == actual
        self.results.append(
            CheckResult(
                "customers_completeness",
                "PASS" if ok else "FAIL",
                f"source customers = {expected}, staging customers = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_orders_completeness(self):
        """Staging orders must equal the source order population (no loss/fan-out)."""
        expected = dml.read_orders(self.spark).count()
        if not self._target_exists("staging", "orders"):
            self.results.append(
                CheckResult(
                    "orders_completeness", "SKIP", "staging.orders not produced yet"
                )
            )
            return
        actual = self._read_target("staging", "orders").count()
        ok = expected == actual
        self.results.append(
            CheckResult(
                "orders_completeness",
                "PASS" if ok else "FAIL",
                f"source orders = {expected}, staging orders = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_orders_control_total(self):
        """Total order amount in the daily mart must tie out to the source extract."""
        if not self._target_exists("marts", "daily_orders"):
            self.results.append(
                CheckResult(
                    "orders_control_total",
                    "SKIP",
                    "marts.daily_orders not produced yet",
                )
            )
            return
        src_total = dml.read_orders(self.spark).agg(F.sum("amount")).collect()[0][0]
        mart_total = (
            self._read_target("marts", "daily_orders")
            .agg(F.sum("total_amount"))
            .collect()[0][0]
        )
        ok = src_total == mart_total
        self.results.append(
            CheckResult(
                "orders_control_total",
                "PASS" if ok else "FAIL",
                f"source SUM(amount) = {src_total}, mart SUM(total_amount) = {mart_total}",
                {"expected": str(src_total), "actual": str(mart_total)},
            )
        )

    def check_orders_daily_parity(self):
        """Per order_date, the mart's count and total must match the source — per
        value, not just in aggregate."""
        if not self._target_exists("marts", "daily_orders"):
            self.results.append(
                CheckResult(
                    "orders_daily_parity", "SKIP", "marts.daily_orders not produced yet"
                )
            )
            return
        src = (
            dml.read_orders(self.spark)
            .groupBy("order_date")
            .agg(F.count("*").alias("s_count"), F.sum("amount").alias("s_total"))
        )
        mart = self._read_target("marts", "daily_orders").select(
            F.col("order_date").cast("string").alias("order_date"),
            "order_count",
            "total_amount",
        )
        joined = src.join(mart, "order_date", "full_outer")
        mismatches = joined.where(
            (F.col("s_count") != F.col("order_count"))
            | (F.col("s_total") != F.col("total_amount"))
            | F.col("s_count").isNull()
            | F.col("order_count").isNull()
        )
        n_bad = mismatches.count()
        ok = n_bad == 0
        self.results.append(
            CheckResult(
                "orders_daily_parity",
                "PASS" if ok else "FAIL",
                f"{n_bad} order_date(s) diverge between source and mart",
                {"mismatched_dates": n_bad},
            )
        )

    def check_transactions_channel_parity(self):
        """Live-converted control: the curated transactions table must apply the
        DML default channel = null("UNKNOWN") — a blank source channel becomes the
        literal 'UNKNOWN', never NULL. SKIPs until the transactions pipeline is
        converted (see the playbook's worked example)."""
        if not self._target_exists("curated", "transactions"):
            self.results.append(
                CheckResult(
                    "transactions_channel_parity",
                    "SKIP",
                    "curated.transactions not produced yet (live conversion target)",
                )
            )
            return
        nulls = (
            self._read_target("curated", "transactions")
            .where(F.col("channel").isNull() | (F.trim(F.col("channel")) == ""))
            .count()
        )
        ok = nulls == 0
        self.results.append(
            CheckResult(
                "transactions_channel_parity",
                "PASS" if ok else "FAIL",
                f"{nulls} transaction(s) with NULL/blank channel "
                f"(expected the DML default 'UNKNOWN')",
                {"null_channels": nulls},
            )
        )

    # ------------------------------------------------ cust_txn_summary.mp
    # Expected values are derived here straight from data/raw/txn.dat and the
    # graph's DML/transform rules, independently of src/jobs/cust_txn_summary.py.
    def _cust_txn_expected_clean(self):
        raw = dml.read_cust_txn(self.spark)
        return raw.select(
            F.trim("txn_id").alias("txn_id"),
            "cust_id",
            "txn_date",
            "amount",
            F.upper(F.trim("currency")).alias("currency"),
            F.when(F.col("channel").isNull() | (F.col("channel") == ""), "UNKNOWN")
            .otherwise(F.col("channel"))
            .alias("channel"),
        )

    def _skip_unless(self, name: str, layer: str, table: str) -> bool:
        if self._target_exists(layer, table):
            return False
        self.results.append(
            CheckResult(name, "SKIP", f"{layer}.{table} not produced yet")
        )
        return True

    @staticmethod
    def _diff_count(a, b) -> int:
        return a.exceptAll(b).count() + b.exceptAll(a).count()

    def check_cust_txn_completeness(self):
        """Every source txn lands in exactly one of summary (via txn_count) or
        rejects, and the summary has one row per customer with a valid txn."""
        name = "cust_txn_completeness"
        if self._skip_unless(name, "marts", "cust_txn_summary") or self._skip_unless(
            name, "intermediate", "cust_txn_rejects"
        ):
            return
        src = dml.read_cust_txn(self.spark)
        n_src = src.count()
        n_cust = src.where(F.col("amount") > 0).select("cust_id").distinct().count()
        summary = self._read_target("marts", "cust_txn_summary")
        n_summary_txn = summary.agg(F.sum("txn_count")).collect()[0][0] or 0
        n_summary_rows = summary.count()
        n_rejects = self._read_target("intermediate", "cust_txn_rejects").count()
        ok = n_src == n_summary_txn + n_rejects and n_cust == n_summary_rows
        self.results.append(
            CheckResult(
                name,
                "PASS" if ok else "FAIL",
                f"source txns = {n_src}, summarised = {n_summary_txn} + rejected = "
                f"{n_rejects}; customers = {n_cust}, summary rows = {n_summary_rows}",
                {
                    "source": n_src,
                    "summarised": n_summary_txn,
                    "rejected": n_rejects,
                    "customers": n_cust,
                    "summary_rows": n_summary_rows,
                },
            )
        )

    def check_cust_txn_control_total(self):
        """SUM(amount) of source txns passing `amount > 0` ties out to the summary
        SUM(total_amount), and the full source total = summary + rejects."""
        name = "cust_txn_control_total"
        if self._skip_unless(name, "marts", "cust_txn_summary") or self._skip_unless(
            name, "intermediate", "cust_txn_rejects"
        ):
            return
        src = dml.read_cust_txn(self.spark)
        src_valid = src.where(F.col("amount") > 0).agg(F.sum("amount")).collect()[0][0]
        src_all = src.agg(F.sum("amount")).collect()[0][0]
        summ = (
            self._read_target("marts", "cust_txn_summary")
            .agg(F.sum("total_amount"))
            .collect()[0][0]
        )
        rej = (
            self._read_target("intermediate", "cust_txn_rejects")
            .agg(F.sum("amount"))
            .collect()[0][0]
            or 0
        )
        ok = src_valid == summ and src_all == summ + rej
        self.results.append(
            CheckResult(
                name,
                "PASS" if ok else "FAIL",
                f"source SUM(amount > 0) = {src_valid}, summary SUM(total_amount) = "
                f"{summ}; source SUM(all) = {src_all}, rejects SUM = {rej}",
                {"expected": str(src_valid), "actual": str(summ)},
            )
        )

    def check_cust_txn_summary_parity(self):
        """Per cust_id, txn_count / total_amount / last_txn_date match the source
        rollup value-for-value."""
        name = "cust_txn_summary_parity"
        if self._skip_unless(name, "marts", "cust_txn_summary"):
            return
        expected = (
            dml.read_cust_txn(self.spark)
            .where(F.col("amount") > 0)
            .groupBy("cust_id")
            .agg(
                F.count("*").cast("long").alias("txn_count"),
                F.sum("amount").alias("total_amount"),
                F.max("txn_date").alias("last_txn_date"),
            )
        )
        actual = self._read_target("marts", "cust_txn_summary").select(
            "cust_id",
            F.col("txn_count").cast("long"),
            "total_amount",
            "last_txn_date",
        )
        n_bad = self._diff_count(expected, actual)
        self.results.append(
            CheckResult(
                name,
                "PASS" if n_bad == 0 else "FAIL",
                f"{n_bad} customer summary row(s) diverge between source and mart",
                {"mismatched_rows": n_bad},
            )
        )

    def check_cust_txn_rejects_parity(self):
        """The rejects file holds exactly the cleaned txns failing `amount > 0`."""
        name = "cust_txn_rejects_parity"
        if self._skip_unless(name, "intermediate", "cust_txn_rejects"):
            return
        expected = self._cust_txn_expected_clean().where(~(F.col("amount") > 0))
        actual = self._read_target("intermediate", "cust_txn_rejects")
        n_bad = self._diff_count(expected, actual.select(*expected.columns))
        self.results.append(
            CheckResult(
                name,
                "PASS" if n_bad == 0 else "FAIL",
                f"{n_bad} reject row(s) diverge (expected {expected.count()} "
                f"txn(s) with amount <= 0)",
                {"mismatched_rows": n_bad},
            )
        )

    def check_cust_txn_reformat_parity(self):
        """Clean_Txn reformat parity: trimmed txn_id, trimmed+upcased currency, and
        the DML `NULL("")` + first_defined(channel, "UNKNOWN") default — a blank
        source channel must become the literal 'UNKNOWN', never NULL/blank."""
        name = "cust_txn_reformat_parity"
        if self._skip_unless(name, "intermediate", "cust_txn_clean"):
            return
        expected = self._cust_txn_expected_clean()
        actual = self._read_target("intermediate", "cust_txn_clean")
        n_bad = self._diff_count(expected, actual.select(*expected.columns))
        blank_channels = actual.where(
            F.col("channel").isNull() | (F.col("channel") == "")
        ).count()
        ok = n_bad == 0 and blank_channels == 0
        self.results.append(
            CheckResult(
                name,
                "PASS" if ok else "FAIL",
                f"{n_bad} cleaned row(s) diverge; {blank_channels} NULL/blank "
                f"channel(s) (expected the default 'UNKNOWN')",
                {"mismatched_rows": n_bad, "blank_channels": blank_channels},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_customers_completeness()
        self.check_orders_completeness()
        self.check_orders_control_total()
        self.check_orders_daily_parity()
        self.check_transactions_channel_parity()
        self.check_cust_txn_completeness()
        self.check_cust_txn_control_total()
        self.check_cust_txn_summary_parity()
        self.check_cust_txn_rejects_parity()
        self.check_cust_txn_reformat_parity()
        self.spark.stop()
        return all(r.status != "FAIL" for r in self.results)

    def render(self) -> str:
        lines = [
            f"# Reconciliation Report — namespace `{self.ns}`",
            "",
            "Source (`data/raw/`) -> target (`out/"
            + self.ns
            + "/`) controls proving the",
            "converted PySpark tables reproduce the legacy Ab Initio extract's intent.",
            "FAIL blocks the migration; SKIP means a prerequisite (e.g. the live-converted",
            "transactions pipeline) has not been produced yet.",
            "",
            "| Control | Result | Detail |",
            "|---|---|---|",
        ]
        for r in self.results:
            lines.append(f"| `{r.name}` | {r.status} | {r.detail} |")
        n_fail = sum(1 for r in self.results if r.status == "FAIL")
        n_skip = sum(1 for r in self.results if r.status == "SKIP")
        n_pass = sum(1 for r in self.results if r.status == "PASS")
        lines += ["", f"**{n_pass} passed, {n_fail} failed, {n_skip} skipped.**", ""]
        return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--namespace", "-n", default="dev")
    ap.add_argument("--report", help="optional path to write the markdown report")
    args = ap.parse_args()

    if not (OUT_ROOT / args.namespace).exists():
        print(
            f"WARNING: no output found for namespace '{args.namespace}'. "
            f"Run `make demo-up NS={args.namespace}` first.",
            file=sys.stderr,
        )

    rec = Reconciler(args.namespace)
    ok = rec.run()
    report = rec.render()
    print(report)
    if args.report:
        Path(args.report).write_text(report, encoding="utf-8")
        print(f"(report written to {args.report})")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
