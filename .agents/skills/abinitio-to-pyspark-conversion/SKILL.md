---
name: abinitio-to-pyspark-conversion
description: Repo mechanics for converting an Ab Initio graph to a verified PySpark job in this repo — run/reconcile commands, namespaces, where the DML schemas and reconciliation controls live. Supplements the general !convert-abinitio-to-pyspark playbook.
---

## When to use this

Use this skill whenever you are converting an Ab Initio graph into a PySpark job
**in this repository**. It is the repo-specific companion to the general procedure
in the `!convert-abinitio-to-pyspark` playbook
(`.workshop/playbooks/abinitio-to-pyspark-conversion.devin.md`): the playbook says
*what* to do and *why* (source-parity principle, procedure, forbidden actions);
this skill says *how* to do it here (exact commands, paths, namespaces).

## Layout

- Ab Initio source estate (read-only): the `ts-python-abinitio-etl` repo (graphs,
  `dml/`, `psets/`, `scripts/*.ksh`).
- Legacy raw extracts (the durable "before"): `data/raw/*.dat`, regenerated
  deterministically by `seed/generate_source.py` (`make seed`).
- DML → PySpark schemas + delimited readers: `src/common/dml.py`. Add the schema
  for a new record layout here.
- Converted jobs: `src/jobs/` (one module per staging/intermediate/mart/curated
  table), orchestrated by `src/run_pipeline.py`.
- Namespaced I/O helper: `src/common/io.py` (writes under `out/<NS>/<layer>/...`).
- Reconciliation controls: `verify/reconcile.py` — each control reads source from
  `data/raw/` and target from `out/<NS>/`, returns PASS/FAIL/SKIP, and the script
  exits non-zero on any FAIL.

## Namespaces (isolated, concurrent-safe)

Every run is namespaced by `NS`. Outputs land under
`out/<NS>/staging | intermediate | marts | curated`, so multiple runs (`NS=dev`,
`NS=child1`, an attendee's name, …) never collide and the durable "before" data in
`data/raw/` is never touched. Always build into the namespace you were given;
never write into another run's namespace or into `data/raw/`.

## Build and verify

```bash
make demo-up   NS=<ns>   # seed (idempotent) + run pipeline + reconcile
make reconcile NS=<ns>   # source -> target reconciliation report (verify/reconcile.py)
make test                # pytest: end-to-end pipeline + reconciliation
make demo-down NS=<ns>   # drop only that namespace's outputs (raw untouched)
```

- `make run NS=<ns>` runs `src/run_pipeline.py` (the converted jobs in order).
- `make reconcile NS=<ns>` runs `verify/reconcile.py`, which exits non-zero on any
  failed control and prints an attachable markdown report.
- `python verify/reconcile.py --namespace <ns> --report reconciliation_report.md`
  writes the report to a file for attaching to the PR.

## Adding reconciliation controls for a new graph

For each graph you convert, add a `check_<thing>` method to `verify/reconcile.py`
(and call it from `run()`), covering at minimum:

- **completeness** — target row count equals the documented in-scope source
  population (no silent row loss, no fan-out);
- **control total** — a SUM (e.g. total transaction amount) that ties out to the
  source extract;
- **parity** — every DML default / mapping / CDC delta class matches the source
  value-for-value (e.g. the `transactions_channel_parity` control that enforces
  the `null("UNKNOWN")` channel default).

Each control should `SKIP` (not FAIL) until its target table exists, so the
harness is forward-looking and the SKIP→PASS transition is visible in the report
when the conversion lands.

## Close the loop

If a control fails, investigate against the Ab Initio source (the graph + DML) —
**do not** relax, delete, or hard-code the control to make it pass. Fix the job
and re-run `make demo-up NS=<ns>` until the pipeline and the reconciliation report
are green, then open a PR that includes the reconciliation report output.
