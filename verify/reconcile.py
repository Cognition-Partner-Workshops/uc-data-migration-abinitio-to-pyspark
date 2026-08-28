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
a prerequisite (e.g. a table from a pipeline that is not converted yet) has not
been produced, and PASS otherwise. The script exits non-zero if any control FAILs,
so it doubles as a CI / pre-merge gate.

Controls here cover the customer, orders, and transaction-detail pipelines.
Converting a new program (customer-CDC) adds its matching controls here — see
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

    def _transactions_target(self):
        """Curated transactions, or None if the pipeline has not produced it yet."""
        if not self._target_exists("curated", "transactions"):
            return None
        return self._read_target("curated", "transactions")

    def _skip_transactions(self, name: str) -> None:
        self.results.append(
            CheckResult(name, "SKIP", "curated.transactions not produced yet")
        )

    def check_transactions_completeness(self):
        """Every source transaction must appear in the curated table exactly once
        per line item — no row loss and no fan-out beyond the DML's line_items
        vector. The extract carries one line-item triple per record, so the
        curated row count must equal the source row count and the distinct
        txn_id count must equal the source transaction population."""
        target = self._transactions_target()
        if target is None:
            self._skip_transactions("transactions_completeness")
            return
        src = dml.read_transactions(self.spark)
        expected_txns = src.count()
        actual_rows = target.count()
        actual_txns = target.select("txn_id").distinct().count()
        ok = expected_txns == actual_rows == actual_txns
        self.results.append(
            CheckResult(
                "transactions_completeness",
                "PASS" if ok else "FAIL",
                f"source transactions = {expected_txns}, curated rows = {actual_rows}, "
                f"curated distinct txn_id = {actual_txns}",
                {
                    "expected": expected_txns,
                    "rows": actual_rows,
                    "distinct_txn_id": actual_txns,
                },
            )
        )

    def check_transactions_control_total(self):
        """SUM(merchant_info.amount) over transactions (counted once per txn, not
        once per exploded line item) must tie out to the source extract, and
        SUM(line_total) over the flattened line items must tie out too."""
        target = self._transactions_target()
        if target is None:
            self._skip_transactions("transactions_control_total")
            return
        src = dml.read_transactions(self.spark)
        src_amount, src_line_total = src.agg(
            F.sum("amount"), F.sum("line_total")
        ).collect()[0]
        tgt_amount = (
            target.where(F.col("line_item_seq") == 1)
            .agg(F.sum("merchant_info.amount"))
            .collect()[0][0]
        )
        tgt_line_total = target.agg(F.sum("line_total")).collect()[0][0]
        ok = src_amount == tgt_amount and src_line_total == tgt_line_total
        self.results.append(
            CheckResult(
                "transactions_control_total",
                "PASS" if ok else "FAIL",
                f"SUM(amount): source {src_amount} vs curated {tgt_amount}; "
                f"SUM(line_total): source {src_line_total} vs curated {tgt_line_total}",
                {
                    "src_amount": str(src_amount),
                    "tgt_amount": str(tgt_amount),
                    "src_line_total": str(src_line_total),
                    "tgt_line_total": str(tgt_line_total),
                },
            )
        )

    def check_transactions_channel_domain_parity(self):
        """Per-value parity for the channel domain: the curated count for every
        channel value must equal the source count, with a blank source channel
        counted as the DML default 'UNKNOWN'. A total that ties out can still hide
        a single misclassified value, which is what this control catches."""
        target = self._transactions_target()
        if target is None:
            self._skip_transactions("transactions_channel_domain_parity")
            return
        raw_channel = F.trim(F.col("channel"))
        src = (
            dml.read_transactions(self.spark)
            .select(
                F.when(
                    raw_channel.isNull() | (raw_channel == ""),
                    F.lit(dml.CHANNEL_DML_DEFAULT),
                )
                .otherwise(raw_channel)
                .alias("channel")
            )
            .groupBy("channel")
            .agg(F.count("*").alias("s_count"))
        )
        tgt = (
            target.select("txn_id", "channel")
            .distinct()
            .groupBy("channel")
            .agg(F.count("*").alias("t_count"))
        )
        joined = src.join(tgt, "channel", "full_outer")
        mismatches = joined.where(
            F.col("s_count").isNull()
            | F.col("t_count").isNull()
            | (F.col("s_count") != F.col("t_count"))
        )
        n_bad = mismatches.count()
        detail = ", ".join(
            f"{r['channel']}: source {r['s_count']} vs curated {r['t_count']}"
            for r in mismatches.collect()
        )
        self.results.append(
            CheckResult(
                "transactions_channel_domain_parity",
                "PASS" if n_bad == 0 else "FAIL",
                f"{n_bad} channel value(s) diverge"
                + (f" ({detail})" if detail else ""),
                {"mismatched_channels": n_bad},
            )
        )

    def check_transactions_merchant_default_parity(self):
        """transaction_detail.dml declares string(",", null("")) merchant_name, so a
        blank merchant name is the empty string — never NULL. Reproduce, don't
        let Spark's CSV NULL coercion through."""
        target = self._transactions_target()
        if target is None:
            self._skip_transactions("transactions_merchant_default_parity")
            return
        nulls = target.where(F.col("merchant_info.merchant_name").isNull()).count()
        self.results.append(
            CheckResult(
                "transactions_merchant_default_parity",
                "PASS" if nulls == 0 else "FAIL",
                f"{nulls} row(s) with NULL merchant_name "
                f"(expected the DML default '' for blanks)",
                {"null_merchant_names": nulls},
            )
        )

    def check_transactions_refund_parity(self):
        """The DML's conditional record `if (txn_type == 2) ... end refund_details`
        must be present for exactly the refund transactions and absent for all
        others. Only the record's presence is reconcilable: the flat extract
        carries no columns for its fields (flagged in the job)."""
        target = self._transactions_target()
        if target is None:
            self._skip_transactions("transactions_refund_parity")
            return
        src = dml.read_transactions(self.spark)
        expected_refunds = src.where(F.col("txn_type") == dml.TXN_TYPE_REFUND).count()
        txn_level = target.select("txn_id", "txn_type", "refund_details").distinct()
        actual_refunds = txn_level.where(F.col("refund_details").isNotNull()).count()
        leaked = txn_level.where(
            (F.col("txn_type") != dml.TXN_TYPE_REFUND)
            & F.col("refund_details").isNotNull()
        ).count()
        ok = expected_refunds == actual_refunds and leaked == 0
        self.results.append(
            CheckResult(
                "transactions_refund_parity",
                "PASS" if ok else "FAIL",
                f"source txn_type=2 = {expected_refunds}, curated with refund_details = "
                f"{actual_refunds}, non-refunds carrying refund_details = {leaked}",
                {
                    "expected_refunds": expected_refunds,
                    "actual_refunds": actual_refunds,
                    "leaked": leaked,
                },
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_customers_completeness()
        self.check_orders_completeness()
        self.check_orders_control_total()
        self.check_orders_daily_parity()
        self.check_transactions_channel_parity()
        self.check_transactions_completeness()
        self.check_transactions_control_total()
        self.check_transactions_channel_domain_parity()
        self.check_transactions_merchant_default_parity()
        self.check_transactions_refund_parity()
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
            "FAIL blocks the migration; SKIP means a prerequisite (a table from a",
            "pipeline that is not converted yet) has not been produced.",
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
