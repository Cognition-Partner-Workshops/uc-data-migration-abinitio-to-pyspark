# Ab Initio → PySpark — Migration Target

Target-state PySpark project for migrating the legacy Ab Initio ETL estate in
[`ts-python-abinitio-etl`](https://github.com/Cognition-Partner-Workshops/ts-python-abinitio-etl)
to runnable, **verified** PySpark. Each Ab Initio graph maps to a PySpark job, and
every conversion is gated by a **source → target reconciliation harness** that
proves the output reproduces the legacy extract — runnable entirely locally
(`local[*]`), no cluster or licensed runtime required.

This is the platform-agnostic Spark counterpart to
[`uc-data-migration-abinitio-to-databricks`](https://github.com/Cognition-Partner-Workshops/uc-data-migration-abinitio-to-databricks)
(the Databricks Lakehouse target).

## Quick Start

```bash
pip install -r requirements.txt -r verify/requirements.txt

make demo-up NS=dev     # generate legacy raw data + run the pipeline + reconcile
make reconcile NS=dev   # re-run the source -> target reconciliation report
make test               # pytest: end-to-end pipeline + reconciliation
make demo-down NS=dev   # drop this namespace's outputs (raw data untouched)
```

`make demo-up` prints a reconciliation report; a non-zero exit means a control
failed. Outputs land under `out/<NS>/` so multiple runs never collide.

## Repository Structure

```
├── data/raw/                     # legacy "before" flat-file extracts (durable)
│   ├── customers.dat             #   comma-delimited (customer.dml + customer_address.dml)
│   ├── orders.dat                #   pipe-delimited  (order extract)
│   ├── transactions.dat          #   pipe-delimited  (transaction_detail.dml)
│   ├── customer_snapshot_previous.dat  # pipe-delimited customer-master (CDC previous)
│   └── customer_snapshot_current.dat   # pipe-delimited customer-master (CDC current)
├── seed/generate_source.py       # deterministic (re)generator for data/raw/  (make seed)
├── src/
│   ├── common/
│   │   ├── spark.py              # local SparkSession factory
│   │   ├── dml.py                # Ab Initio DML -> PySpark StructType + readers
│   │   └── io.py                 # namespaced output paths (out/<NS>/...)
│   ├── jobs/                     # converted PySpark jobs (one per table)
│   │   ├── stg_customers.py      #   customer snapshot graph
│   │   ├── stg_orders.py         #   daily orders extract -> staging
│   │   ├── mart_daily_orders.py  #   orders production rollover -> daily mart
│   │   └── cdc_customers.py      #   customer CDC (compare-by-key + row hash)
│   └── run_pipeline.py           # orchestrator (PySpark analogue of the .ksh wrappers)
├── verify/reconcile.py           # source -> target reconciliation harness (CI gate)
├── tests/test_reconcile.py       # end-to-end pytest
├── .workshop/playbooks/          # portable Devin Playbook source (copied into the org)
├── .agents/skills/               # repo Skill: how to convert/verify here (auto-loaded)
├── docs/ABINITIO_TO_PYSPARK_MIGRATION_MAP.md
└── Makefile
```

## The verification loop (why this repo exists)

The point of the migration is not to produce *some* PySpark output — it is to
produce output we can **trust** reproduces what the legacy Ab Initio graphs would
have produced. Because there is no live Co>Operating System runtime here, trust is
established with deterministic reconciliation controls between the raw source
extracts and the converted tables:

| Control | Proves |
|---|---|
| `customers_completeness` | staging customers = source customers (no row loss) |
| `orders_completeness` | staging orders = source orders (no loss / fan-out) |
| `orders_control_total` | mart `SUM(total_amount)` ties out to source `SUM(amount)` |
| `orders_daily_parity` | per `order_date`, count + total match the source |
| `transactions_channel_parity` | curated channel applies the DML `null("UNKNOWN")` default (live-conversion target) |
| `customer_cdc_completeness` | one change record per new/removed key, no fan-out or double-count |
| `customer_cdc_control_total` | `SUM(customer_id)` over the change set ties out to the recomputed changed keys |
| `customer_cdc_parity` | INSERT/UPDATE/DELETE key sets match the legacy `"||"` MD5 row-hash classification |

`verify/reconcile.py` exits non-zero on any FAIL, so it doubles as the CI gate.

## Conversion Playbook & Skill

The reusable Ab Initio → PySpark **conversion procedure** is a
[Devin Playbook](https://docs.devin.ai/product-guides/creating-playbooks). Its
source lives at `.workshop/playbooks/abinitio-to-pyspark-conversion.devin.md`.

**Facilitator / demo presenter:** before running, copy that file's contents into
your Devin organization (Settings → Playbooks → *Create a new Playbook*) so
sessions can invoke it as `!convert-abinitio-to-pyspark`. The playbook is portable
(the general procedure, the source-parity principle, forbidden actions); it is not
auto-loaded from the repo — registering it in the org is what makes it available
across sessions.

The repo-specific mechanics (the `make demo-up` / `make reconcile` commands,
namespaces, and where the DML schemas and reconciliation controls live) are kept
in a [Skill](https://docs.devin.ai/product-guides/skills) at
`.agents/skills/abinitio-to-pyspark-conversion/SKILL.md`, which Devin
auto-discovers and loads when working in this repo.

### What is converted on `main` vs live

`main` carries the durable **before**-state — the customer, orders and
customer-CDC pipelines already converted (the CDC job reproduces the legacy
compare-by-key + `"||"` MD5 row hash and is gated by per-class parity controls),
plus the reconciliation harness, the seed generator, the playbook source, and the
Skill. The work Devin does **live** in the demo is the next wave — the
transactions pipeline (flatten nested line items + reproduce the DML
`null("UNKNOWN")` channel default). See
[`docs/ABINITIO_TO_PYSPARK_MIGRATION_MAP.md`](docs/ABINITIO_TO_PYSPARK_MIGRATION_MAP.md).

## Related Repositories

| Repo | Purpose |
|---|---|
| [`ts-python-abinitio-etl`](https://github.com/Cognition-Partner-Workshops/ts-python-abinitio-etl) | Source Ab Initio estate (graphs, DML, PSETs, CDC, KornShell orchestration) |
| [`uc-data-migration-abinitio-to-databricks`](https://github.com/Cognition-Partner-Workshops/uc-data-migration-abinitio-to-databricks) | Databricks Lakehouse target (dbt + Delta + reconciliation) |
