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
import hashlib
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

    # -------------------------------------------------- customer CDC (live target)
    @staticmethod
    def _legacy_row_hash(values: list[str]) -> str:
        """The exact CDCProcessor row hash: md5 of the HASH_COLUMNS joined by '||'.

        Mirrors ``ts-python-abinitio-etl`` ``cdc_processor.py``
        (``hashlib.md5("||".join(row.values).encode()).hexdigest()`` over
        ``df[cols].astype(str)``). This is the independent legacy oracle the Spark
        job's hash is reconciled against — recomputed here from the raw snapshots,
        not from the converted job, so the two implementations must agree.
        """
        return hashlib.md5("||".join(values).encode()).hexdigest()

    def _read_snapshot(self, filename: str) -> dict[str, list[str]]:
        """Read a pipe-delimited customer-master snapshot from data/raw/ into
        {customer_id: [HASH_COLUMNS values]} — the source side of the CDC check."""
        cols = dml.CUSTOMER_MASTER_HASH_COLUMNS
        rows: dict[str, list[str]] = {}
        text = (dml.RAW_DIR / filename).read_text(encoding="utf-8")
        for line in text.splitlines():
            if not line:
                continue
            fields = line.split("|")
            record = dict(zip(cols, fields))
            key = record["customer_id"].strip()
            rows[key] = [record[c].strip() for c in cols]
        return rows

    def _cdc_expected(self) -> dict[str, set]:
        """Recompute INSERT/UPDATE/DELETE key sets from the raw snapshots using the
        legacy row hash — the oracle for the CDC parity controls."""
        current = self._read_snapshot("customer_snapshot_current.dat")
        previous = self._read_snapshot("customer_snapshot_previous.dat")
        cur_keys, prev_keys = set(current), set(previous)
        inserts = cur_keys - prev_keys
        deletes = prev_keys - cur_keys
        updates = {
            k
            for k in (cur_keys & prev_keys)
            if self._legacy_row_hash(current[k]) != self._legacy_row_hash(previous[k])
        }
        return {
            "inserts": inserts,
            "updates": updates,
            "deletes": deletes,
            "current": cur_keys,
            "previous": prev_keys,
        }

    def _cdc_actual(self) -> dict[str, set]:
        """The Spark job's change set, grouped into key sets per operation."""
        rows = (
            self._read_target("curated", "customer_cdc")
            .select(F.col("customer_id").cast("string").alias("k"), "cdc_operation")
            .collect()
        )
        actual: dict[str, set] = {"INSERT": set(), "UPDATE": set(), "DELETE": set()}
        for r in rows:
            actual[r["cdc_operation"]].add(r["k"])
        return actual

    def check_customer_cdc_completeness(self):
        """No silent row loss / fan-out: exactly one change record per genuinely
        new key (INSERT) and per removed key (DELETE), and no key is emitted under
        more than one operation."""
        if not self._target_exists("curated", "customer_cdc"):
            self.results.append(
                CheckResult(
                    "customer_cdc_completeness",
                    "SKIP",
                    "curated.customer_cdc not produced yet (live conversion target)",
                )
            )
            return
        exp = self._cdc_expected()
        act = self._cdc_actual()
        n_out = len(act["INSERT"]) + len(act["UPDATE"]) + len(act["DELETE"])
        distinct = act["INSERT"] | act["UPDATE"] | act["DELETE"]
        ok = (
            len(act["INSERT"]) == len(exp["inserts"])
            and len(act["DELETE"]) == len(exp["deletes"])
            and len(distinct) == n_out  # no key under two operations / no fan-out
        )
        self.results.append(
            CheckResult(
                "customer_cdc_completeness",
                "PASS" if ok else "FAIL",
                f"source only-current = {len(exp['inserts'])}, only-previous = "
                f"{len(exp['deletes'])}; job INSERT = {len(act['INSERT'])}, "
                f"DELETE = {len(act['DELETE'])}, distinct change keys = "
                f"{len(distinct)} of {n_out} records",
                {
                    "inserts": len(act["INSERT"]),
                    "deletes": len(act["DELETE"]),
                    "records": n_out,
                },
            )
        )

    def check_customer_cdc_control_total(self):
        """Control total: SUM(customer_id) over the whole change set ties out to the
        same sum over the independently-recomputed changed keys."""
        if not self._target_exists("curated", "customer_cdc"):
            self.results.append(
                CheckResult(
                    "customer_cdc_control_total",
                    "SKIP",
                    "curated.customer_cdc not produced yet (live conversion target)",
                )
            )
            return
        exp = self._cdc_expected()
        expected_keys = exp["inserts"] | exp["updates"] | exp["deletes"]
        exp_total = sum(int(k) for k in expected_keys)
        act_total = (
            self._read_target("curated", "customer_cdc")
            .agg(F.sum("customer_id"))
            .collect()[0][0]
        )
        act_total = int(act_total) if act_total is not None else 0
        ok = exp_total == act_total
        self.results.append(
            CheckResult(
                "customer_cdc_control_total",
                "PASS" if ok else "FAIL",
                f"source SUM(changed customer_id) = {exp_total}, "
                f"job SUM(customer_id) = {act_total}",
                {"expected": exp_total, "actual": act_total},
            )
        )

    def check_customer_cdc_parity(self):
        """Per-class parity: the INSERT/UPDATE/DELETE key sets the job produces must
        match, value-for-value, the sets recomputed from the raw snapshots with the
        legacy '||' MD5 row hash. This is what proves the row hash — including the
        pset's key-in-hash quirk — was reproduced exactly, not just that totals
        happen to tie out."""
        if not self._target_exists("curated", "customer_cdc"):
            self.results.append(
                CheckResult(
                    "customer_cdc_parity",
                    "SKIP",
                    "curated.customer_cdc not produced yet (live conversion target)",
                )
            )
            return
        exp = self._cdc_expected()
        act = self._cdc_actual()
        diffs = {
            "INSERT": (exp["inserts"] ^ act["INSERT"]),
            "UPDATE": (exp["updates"] ^ act["UPDATE"]),
            "DELETE": (exp["deletes"] ^ act["DELETE"]),
        }
        n_bad = sum(len(v) for v in diffs.values())
        ok = n_bad == 0
        detail = (
            f"INSERT {len(act['INSERT'])}/{len(exp['inserts'])}, "
            f"UPDATE {len(act['UPDATE'])}/{len(exp['updates'])}, "
            f"DELETE {len(act['DELETE'])}/{len(exp['deletes'])} "
            f"(job/source); {n_bad} key(s) misclassified"
        )
        self.results.append(
            CheckResult(
                "customer_cdc_parity",
                "PASS" if ok else "FAIL",
                detail,
                {"misclassified": n_bad},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_customers_completeness()
        self.check_orders_completeness()
        self.check_orders_control_total()
        self.check_orders_daily_parity()
        self.check_transactions_channel_parity()
        self.check_customer_cdc_completeness()
        self.check_customer_cdc_control_total()
        self.check_customer_cdc_parity()
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
