# Playbook: Convert one Ab Initio graph to a verified PySpark job

> **Facilitator / presenter:** this file is the source for a **Devin Playbook**.
> Copy its contents into your Devin organization (Settings → Playbooks → *Create
> a new Playbook*) so sessions can invoke it as `!convert-abinitio-to-pyspark`.
> See [Creating Playbooks](https://docs.devin.ai/product-guides/creating-playbooks).
> The repo-specific commands (make targets, namespaces, harness paths) are kept
> in the companion Skill at
> `.agents/skills/abinitio-to-pyspark-conversion/SKILL.md`, which Devin auto-loads
> when working in this repo.

## Overview

Convert **one** Ab Initio graph (and its DML, PSET, and KornShell wrapper) into a
runnable, **verified** PySpark job. The outcome is a PR containing the converted
job, its reconciliation controls, and a reconciliation report that proves the
output ties out to the legacy source. The value is consistency: every graph is
converted the same way and every conversion is gated by a parity check against
the Ab Initio source extract.

## The one principle: the Ab Initio source is the source of truth

A migration reproduces the legacy numbers faithfully — it does not improve them.
If a graph has a quirk (a DML `null("UNKNOWN")` default, a packed-decimal field,
a CDC hash that excludes a column, a partition rounding rule), reproduce it and
**flag it** — never silently "correct" it. Remediating a legacy behaviour is a
separate, deliberate decision made with the business, not a side effect of
conversion. This is why "looks reasonable" review is not enough and why every
conversion is gated by a parity check against the source.

## Required from user

- **Ab Initio graph** — the unit to convert, in the source estate
  (`ts-python-abinitio-etl`), e.g. the transactions pipeline
  (`graphs/`, `dml/transaction_detail.dml`, `scripts/run_daily_orders.ksh`) or
  the customer CDC pipeline (`graphs/cdc_processor.py`,
  `psets/pset_templates/customer_cdc.pset`, `scripts/run_customer_cdc.ksh`).
- **Target job(s)** — the PySpark job(s) to produce under `src/jobs/`, plus the
  curated output table(s), e.g. `curated/transactions`.
- **Namespace** — an isolated build space so concurrent runs do not collide,
  e.g. `dev` (outputs land in `out/dev/...`).

## Procedure

1. Read the graph end to end, plus every DML it binds, the PSET parameters, and
   the KornShell wrapper. Identify inputs (input-file components + `.dml` record
   format → a DML-derived `StructType` reader), transforms (reformat, join,
   rollup, partition-by-key, the CDC compare-by-key component), outputs (output
   files / `STAGING.*` loads → namespaced parquet tables), and every filter and
   business rule (each select expression, `null(...)` default, dedup, reject
   condition — these define the scope contract you reconcile against).
2. Map each Ab Initio construct to its PySpark equivalent: DML record → schema;
   reformat → `select`/`withColumn`; join component → `DataFrame.join`; rollup →
   `groupBy().agg()`; partition-by-key (`m_partition`) → `repartition`; the
   CDC compare-by-key + row hash → a `full_outer` join on keys with an MD5/SHA2
   hash of the compare columns producing INSERT/UPDATE/DELETE; `null("X")` DML
   default → `coalesce`/empty-string handling that yields exactly `X`.
3. Write the job(s) under `src/jobs/` preserving the legacy logic **exactly** —
   mirror every default and rule value-for-value, including DML `null(...)`
   substitutions and the CDC hash column list. Where the source has a quirk,
   reproduce it and add a short comment noting it is source-faithful (not an
   endorsement).
4. Add or extend reconciliation controls in `verify/reconcile.py`: at minimum a
   **completeness** check (no silent row loss vs the source population), a
   **control total** (a SUM that must tie out), and a **parity** check for every
   default/mapping/CDC class against the source. (See the Skill for exactly where
   controls live and how they are run.)
5. Build and verify into the requested namespace, then run the reconciliation
   report. (Commands are in the Skill.)
6. Close the loop: if any control fails, investigate **against the Ab Initio
   source** — do not relax the check to make it pass. Correct the job and re-run
   until the build and the reconciliation report are green.
7. Deliver a PR that includes the new/changed job, the reconciliation controls,
   and the report output, so a reviewer sees the parity evidence, not just the
   code. CI re-runs every control on the PR.

## Specifications (postconditions)

- The converted job(s) build cleanly into the requested namespace.
- Every reconciliation control passes: completeness, control total(s), and a
  parity check for each default/mapping/CDC class in the graph.
- The PR contains the job, the controls, and the reconciliation report.
- Any source quirk reproduced is explicitly flagged in code and in the PR — never
  silently changed.

## Advice and pointers

- Parity is per-value, not aggregate: a conversion can produce a correct total
  while an individual class (e.g. a single channel value or CDC delta type) is
  wrong. Compare each class to the source contract.
- A control that is hard to make pass is usually telling you the conversion
  diverged — re-read the DML and the graph before touching the control.
- Ab Initio reads blank delimited fields as empty strings, not NULLs; the
  `null(...)` DML clause is what substitutes a default. Spark's CSV reader
  coerces empty strings to NULL by default — handle this deliberately so the DML
  defaults are reproduced, not lost.

### Worked example: the transaction channel default divergence

A real defect this loop caught, and the canonical illustration of "source is
truth":

- `dml/transaction_detail.dml` declares `string("\n", null("UNKNOWN")) channel` —
  a **blank** channel in the extract is read by Ab Initio as the literal
  `UNKNOWN`, never NULL.
- An early PySpark conversion read transactions with Spark's default CSV options,
  which coerce the blank channel field to `NULL`. Row counts and the amount
  control total still tied out, so "looks reasonable" review passed it.
- The `transactions_channel_parity` control compares the curated table's channel
  domain to the source contract and **fails**: `6 transaction(s) with NULL/blank
  channel (expected the DML default 'UNKNOWN')`.
- The fix is to reproduce the DML default — treat blank/NULL channel as
  `UNKNOWN` via `coalesce` + empty-string handling — not to relax the control.
  "Looks reasonable" review never catches this; the source-parity check always
  does.

## Forbidden actions

- Do **not** "improve", clean up, or modernise legacy logic during conversion —
  reproduce it faithfully and flag anomalies for a separate decision.
- Do **not** relax, delete, or hard-code a reconciliation control to make a build
  go green. Fix the job, not the check.
- Do **not** write into another run's namespace or the durable `data/raw/` source.
- Do **not** convert more than the one graph in scope for this session.

## Parallel fan-out

Each Ab Initio graph is independent, so conversions parallelise cleanly: run one
session per graph (each with its own namespace), or one orchestrator session that
spawns a child per graph and monitors them to green. Because this playbook fixes
the procedure and the reconciliation contract, every session's output is
consistent and independently verified — the same review bar applied N times in
parallel instead of once in series.
