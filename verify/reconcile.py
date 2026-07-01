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
from src.jobs import customer_cdc  # noqa: E402

# customer_cdc.pset contract, transcribed here so reconcile verifies the converted
# job against the PSET *independently* — it re-derives the expected INSERT/UPDATE/
# DELETE from the raw snapshots rather than trusting the job's classification.
#   define KEY_COLUMNS  customer_id
#   define HASH_COLUMNS customer_id,name,address,phone,email,status
CDC_KEY = ["customer_id"]
CDC_HASH = ["customer_id", "name", "address", "phone", "email", "status"]
CDC_ATTRS = [c for c in CDC_HASH if c not in CDC_KEY]
CDC_OUT_COLS = ["customer_id"] + CDC_ATTRS + ["row_hash"]


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

    # ------------------------------------------------------- customer CDC checks
    def _cdc_master(self, which: str):
        """Independent reconstruction of the customer-master snapshot the CDC
        compares — the address consolidation + PSET row hash, re-derived here from
        the raw snapshot rather than imported from the job."""
        raw = dml.read_customer_snapshot(self.spark, which)
        master = raw.select(
            "customer_id",
            "name",
            F.concat_ws(", ", "street", "city", "state", "zip").alias("address"),
            "phone",
            "email",
            "status",
        )
        parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in CDC_HASH]
        return master.withColumn("_h", F.md5(F.concat_ws("||", *parts)))

    def _cdc_frames(self):
        if getattr(self, "_cdc_cache", None) is None:
            cur = self._cdc_master("current").cache()
            prv = self._cdc_master("previous").cache()
            self._cdc_cache = (cur, prv)
        return self._cdc_cache

    def _cdc_output(self):
        return self._read_target("curated", "customer_cdc")

    def _cdc_exists(self) -> bool:
        return self._target_exists("curated", "customer_cdc")

    @staticmethod
    def _expected_rows(frame):
        """Project a master frame to the CDC output column contract."""
        return frame.select("customer_id", *CDC_ATTRS, F.col("_h").alias("row_hash"))

    def check_customer_cdc_completeness(self):
        """Every key in the source (current) ∪ target (previous) population must be
        accounted for by the CDC output plus the (unchanged) remainder — no row
        loss, no fan-out."""
        if not self._cdc_exists():
            self.results.append(
                CheckResult(
                    "customer_cdc_completeness",
                    "SKIP",
                    "curated.customer_cdc not produced yet",
                )
            )
            return
        cur, prv = self._cdc_frames()
        out = self._cdc_output()
        union_keys = (
            cur.select("customer_id")
            .union(prv.select("customer_id"))
            .distinct()
            .count()
        )
        in_both = (
            cur.select("customer_id")
            .join(prv.select("customer_id"), "customer_id", "inner")
            .count()
        )
        n_ins = out.where(F.col("change_type") == "INSERT").count()
        n_upd = out.where(F.col("change_type") == "UPDATE").count()
        n_del = out.where(F.col("change_type") == "DELETE").count()
        unchanged = in_both - n_upd
        accounted = n_ins + n_upd + unchanged + n_del
        ok = accounted == union_keys and unchanged >= 0
        self.results.append(
            CheckResult(
                "customer_cdc_completeness",
                "PASS" if ok else "FAIL",
                f"source∪target keys = {union_keys}, accounted "
                f"(INSERT {n_ins} + UPDATE {n_upd} + UNCHANGED {unchanged} + "
                f"DELETE {n_del}) = {accounted}",
                {"union_keys": union_keys, "accounted": accounted},
            )
        )

    def check_customer_cdc_control_total(self):
        """Control total: INSERT + UPDATE + UNCHANGED must tie out to the source
        (current) population, and DELETE to the target-only (previous) population."""
        if not self._cdc_exists():
            self.results.append(
                CheckResult(
                    "customer_cdc_control_total",
                    "SKIP",
                    "curated.customer_cdc not produced yet",
                )
            )
            return
        cur, prv = self._cdc_frames()
        out = self._cdc_output()
        src_rows = cur.count()
        target_only = (
            prv.select("customer_id")
            .join(cur.select("customer_id"), "customer_id", "left_anti")
            .count()
        )
        in_both = (
            cur.select("customer_id")
            .join(prv.select("customer_id"), "customer_id", "inner")
            .count()
        )
        n_ins = out.where(F.col("change_type") == "INSERT").count()
        n_upd = out.where(F.col("change_type") == "UPDATE").count()
        n_del = out.where(F.col("change_type") == "DELETE").count()
        unchanged = in_both - n_upd
        ok = (n_ins + n_upd + unchanged == src_rows) and (n_del == target_only)
        self.results.append(
            CheckResult(
                "customer_cdc_control_total",
                "PASS" if ok else "FAIL",
                f"INSERT+UPDATE+UNCHANGED = {n_ins + n_upd + unchanged} "
                f"(source rows = {src_rows}); DELETE = {n_del} "
                f"(target-only rows = {target_only})",
                {"src_rows": src_rows, "target_only": target_only},
            )
        )

    def _cdc_class_parity(self, change_type: str, expected):
        """Value-for-value parity for one CDC class: the output rows of that class
        must equal the independently-derived expected rows exactly (every attribute
        AND the PSET row hash)."""
        out = (
            self._cdc_output()
            .where(F.col("change_type") == change_type)
            .select(*CDC_OUT_COLS)
        )
        expected = expected.select(*CDC_OUT_COLS)
        missing = expected.exceptAll(out).count()
        extra = out.exceptAll(expected).count()
        n_exp = expected.count()
        ok = missing == 0 and extra == 0
        return ok, n_exp, missing, extra

    def check_customer_cdc_insert_parity(self):
        """INSERT parity: keys in source not target, emitting the source values."""
        if not self._cdc_exists():
            self.results.append(
                CheckResult(
                    "customer_cdc_insert_parity",
                    "SKIP",
                    "curated.customer_cdc not produced yet",
                )
            )
            return
        cur, prv = self._cdc_frames()
        expected = self._expected_rows(
            cur.join(prv.select("customer_id"), "customer_id", "left_anti")
        )
        ok, n_exp, missing, extra = self._cdc_class_parity("INSERT", expected)
        self.results.append(
            CheckResult(
                "customer_cdc_insert_parity",
                "PASS" if ok else "FAIL",
                f"{n_exp} expected INSERT rows; {missing} missing, {extra} unexpected "
                f"(value-for-value incl. row hash)",
                {"expected": n_exp, "missing": missing, "extra": extra},
            )
        )

    def check_customer_cdc_delete_parity(self):
        """DELETE parity: keys in target not source, emitting the prior values."""
        if not self._cdc_exists():
            self.results.append(
                CheckResult(
                    "customer_cdc_delete_parity",
                    "SKIP",
                    "curated.customer_cdc not produced yet",
                )
            )
            return
        cur, prv = self._cdc_frames()
        expected = self._expected_rows(
            prv.join(cur.select("customer_id"), "customer_id", "left_anti")
        )
        ok, n_exp, missing, extra = self._cdc_class_parity("DELETE", expected)
        self.results.append(
            CheckResult(
                "customer_cdc_delete_parity",
                "PASS" if ok else "FAIL",
                f"{n_exp} expected DELETE rows; {missing} missing, {extra} unexpected "
                f"(value-for-value incl. row hash)",
                {"expected": n_exp, "missing": missing, "extra": extra},
            )
        )

    def check_customer_cdc_update_parity(self):
        """UPDATE parity: keys in both whose PSET row hash changed, emitting the
        source values. Also asserts the job's HASH_COLUMNS equals the PSET compare
        columns exactly — i.e. the row hash is over customer_id,name,address,phone,
        email,status in that order."""
        if not self._cdc_exists():
            self.results.append(
                CheckResult(
                    "customer_cdc_update_parity",
                    "SKIP",
                    "curated.customer_cdc not produced yet",
                )
            )
            return
        if customer_cdc.HASH_COLUMNS != CDC_HASH or customer_cdc.KEY_COLUMNS != CDC_KEY:
            self.results.append(
                CheckResult(
                    "customer_cdc_update_parity",
                    "FAIL",
                    f"job compare columns {customer_cdc.HASH_COLUMNS} / key "
                    f"{customer_cdc.KEY_COLUMNS} do not match the PSET "
                    f"{CDC_HASH} / {CDC_KEY}",
                )
            )
            return
        cur, prv = self._cdc_frames()
        changed_keys = (
            cur.alias("c")
            .join(prv.alias("p"), "customer_id", "inner")
            .where(F.col("c._h") != F.col("p._h"))
            .select("customer_id")
        )
        expected = self._expected_rows(cur.join(changed_keys, "customer_id", "inner"))
        ok, n_exp, missing, extra = self._cdc_class_parity("UPDATE", expected)
        self.results.append(
            CheckResult(
                "customer_cdc_update_parity",
                "PASS" if ok else "FAIL",
                f"{n_exp} expected UPDATE rows (hash over PSET compare columns); "
                f"{missing} missing, {extra} unexpected (value-for-value incl. row hash)",
                {"expected": n_exp, "missing": missing, "extra": extra},
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
        self.check_customer_cdc_insert_parity()
        self.check_customer_cdc_delete_parity()
        self.check_customer_cdc_update_parity()
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
