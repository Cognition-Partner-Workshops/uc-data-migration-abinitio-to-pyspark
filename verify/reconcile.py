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

Controls present on ``main`` cover the customer and orders pipelines. Converting a
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

from pyspark.sql import functions as F  # noqa: E402

from src.common import dml  # noqa: E402
from src.common.io import OUT_ROOT, layer_path  # noqa: E402
from src.common.spark import build_spark  # noqa: E402


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

    def check_transactions_completeness(self):
        """Curated transactions (flattened line items) must equal the source
        line-item population — the explode of the DML ``line_items[item_count]``
        array must neither drop a transaction nor fan out beyond the array."""
        if not self._target_exists("curated", "transactions"):
            self.results.append(
                CheckResult(
                    "transactions_completeness",
                    "SKIP",
                    "curated.transactions not produced yet (live conversion target)",
                )
            )
            return
        # Each source extract row is one flattened line item (item_count = 1),
        # so the source line-item population is the raw row count and must equal
        # SUM(item_count); the curated table (post-explode) must match both.
        src = dml.read_transactions(self.spark)
        expected_rows = src.count()
        expected_line_items = src.agg(F.sum("item_count")).collect()[0][0]
        target = self._read_target("curated", "transactions")
        actual = target.count()
        distinct_txns = target.select("txn_id").distinct().count()
        ok = (
            actual == expected_rows == expected_line_items
            and distinct_txns == expected_rows
        )
        self.results.append(
            CheckResult(
                "transactions_completeness",
                "PASS" if ok else "FAIL",
                f"source rows = {expected_rows}, SUM(item_count) = {expected_line_items}, "
                f"curated rows = {actual}, distinct txns = {distinct_txns}",
                {
                    "expected_rows": expected_rows,
                    "expected_line_items": expected_line_items,
                    "actual": actual,
                    "distinct_txns": distinct_txns,
                },
            )
        )

    def check_transactions_control_total(self):
        """Line-item economics must tie out: curated SUM(line_total * quantity)
        must equal the source SUM(merchant_info.amount) transaction total."""
        if not self._target_exists("curated", "transactions"):
            self.results.append(
                CheckResult(
                    "transactions_control_total",
                    "SKIP",
                    "curated.transactions not produced yet (live conversion target)",
                )
            )
            return
        src_total = (
            dml.read_transactions(self.spark).agg(F.sum("amount")).collect()[0][0]
        )
        tgt_total = (
            self._read_target("curated", "transactions")
            .agg(F.sum(F.col("line_total") * F.col("quantity")))
            .collect()[0][0]
        )
        ok = src_total == tgt_total
        self.results.append(
            CheckResult(
                "transactions_control_total",
                "PASS" if ok else "FAIL",
                f"source SUM(amount) = {src_total}, "
                f"curated SUM(line_total*quantity) = {tgt_total}",
                {"expected": str(src_total), "actual": str(tgt_total)},
            )
        )

    def check_transactions_merchant_default_parity(self):
        """The curated table must apply the DML default merchant_name = null("") —
        a blank source merchant name becomes the empty string, never NULL."""
        if not self._target_exists("curated", "transactions"):
            self.results.append(
                CheckResult(
                    "transactions_merchant_default_parity",
                    "SKIP",
                    "curated.transactions not produced yet (live conversion target)",
                )
            )
            return
        nulls = (
            self._read_target("curated", "transactions")
            .where(F.col("merchant_name").isNull())
            .count()
        )
        ok = nulls == 0
        self.results.append(
            CheckResult(
                "transactions_merchant_default_parity",
                "PASS" if ok else "FAIL",
                f"{nulls} transaction(s) with NULL merchant_name "
                f"(expected the DML default '' for blanks)",
                {"null_merchant_names": nulls},
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

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_customers_completeness()
        self.check_orders_completeness()
        self.check_orders_control_total()
        self.check_orders_daily_parity()
        self.check_transactions_completeness()
        self.check_transactions_control_total()
        self.check_transactions_merchant_default_parity()
        self.check_transactions_channel_parity()
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
