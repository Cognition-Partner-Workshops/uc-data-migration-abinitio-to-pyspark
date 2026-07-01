# Ab Initio → PySpark Migration Map

Construct-level mapping from the legacy Ab Initio estate
([`ts-python-abinitio-etl`](https://github.com/Cognition-Partner-Workshops/ts-python-abinitio-etl))
to this PySpark target. Use it as the reference when converting a graph.

## Runtime & orchestration

| Ab Initio | PySpark target |
|---|---|
| Co>Operating System (`air sandbox run`) | local SparkSession (`src/common/spark.py`) |
| Graph (`.mp`) | a job module under `src/jobs/` |
| `m_partition` / partition-by-key parallelism | `DataFrame.repartition` / Spark partitions |
| KornShell wrapper (`scripts/*.ksh`) | `src/run_pipeline.py` |
| AutoSys / Control-M schedule | Databricks Workflows / cron / Devin scheduled session |
| PSET (`define KEY value`) | job parameters / namespace (`NS`) |

## DML record layouts → PySpark schemas

DML lives in the source repo's `dml/`; the PySpark equivalents are in
`src/common/dml.py`.

| DML construct | Example | PySpark type |
|---|---|---|
| `decimal(",")` (id / count) | `customer_id`, `item_count` | `LongType` / `IntegerType` |
| `decimal("8.2", "\|")` (money) | `amount`, `balance` | `DecimalType(p, s)` |
| `string(",")` | `first_name`, `channel` | `StringType` |
| `date("YYYY-MM-DD")` | `order_date` | `to_date(...)` → `DateType` |
| `datetime("YYYY-MM-DD HH24:MI:SS")` | `txn_timestamp` | `to_timestamp(...)` → `TimestampType` |
| `string(..., null("UNKNOWN"))` | `channel` default | `coalesce(nullif(trim(col), ''), lit('UNKNOWN'))` |
| `[item_count]` variable array | `item_names[]` | `ArrayType` / `explode` for flatten |
| `packed_decimal` / `zoned_decimal` | `packed_account.dml` | decode to `DecimalType` on read |
| delimiter (`","`, `"\|"`, `";"`) | per record | `spark.read.option("sep", ...)` |

> **Blank ≠ NULL.** Ab Initio reads a blank delimited field as an empty string;
> the `null(...)` clause is what substitutes a default. Spark's CSV reader coerces
> empty strings to NULL by default, so reproduce DML defaults deliberately — see
> the channel worked example in the conversion playbook.

## Transform components → DataFrame operations

| Ab Initio component | PySpark |
|---|---|
| Reformat | `select` / `withColumn` |
| Filter by Expression | `where` / `filter` |
| Join (inner/outer) | `DataFrame.join(..., how=...)` |
| Rollup | `groupBy().agg()` |
| Dedup Sorted | `dropDuplicates` / window `row_number()` |
| Scan (running total) | window function with `rowsBetween` |
| Lookup (hash file) | broadcast `join` |
| **Compare Records by Key (CDC)** | `full_outer` join on keys + row hash → INSERT/UPDATE/DELETE |

## Program-level migration map

| Ab Initio pipeline | PySpark job(s) | Status | Pattern |
|---|---|---|---|
| Customer snapshot (`run_customer_cdc.ksh` step 1) | `src/jobs/stg_customers.py` | on `main` | input-file + reformat → staging |
| Daily orders extract→staging (`run_daily_orders.ksh` ph.1-3) | `src/jobs/stg_orders.py` | on `main` | typed read + date parse |
| Orders production rollover (`run_daily_orders.ksh` ph.4) | `src/jobs/mart_daily_orders.py` | on `main` | rollup → `groupBy().agg()` |
| Transactions detail (`transaction_detail.dml`) | `src/jobs/stg_transactions.py` → `curated/transactions` | **live conversion** | flatten nested line items + DML defaults |
| Customer CDC (`cdc_processor.py`, `customer_cdc.pset`) | `src/jobs/cdc_customers.py` → `curated/customer_cdc` | converted | compare-by-key + row hash → Delta MERGE analog |

"Live conversion" rows are the work Devin does during the demo via
`!convert-abinitio-to-pyspark`; `main` carries the durable before-state plus the
reconciliation harness.

### Customer CDC conversion notes

The CDC graph compares two customer-master snapshots (the pset's
`PREVIOUS_SNAPSHOT_PATH` vs `CURRENT_SNAPSHOT_PATH`, seeded here as
`data/raw/customer_snapshot_previous.dat` and `..._current.dat`) by
`KEY_COLUMNS` and a row hash over `HASH_COLUMNS`, emitting INSERT/UPDATE/DELETE.

Source-faithful reproductions (reproduced exactly, not "improved"):

- **Row hash** = `md5( "||".join( str(v) for v in HASH_COLUMNS ) )` — the exact
  algorithm, `"||"` separator, column set and *order* of
  `CDCProcessor._row_hash`. In PySpark: `md5(concat_ws("||", <cols cast to
  string>))`, verified byte-for-byte against the legacy `hashlib.md5`.
- **Key column in the hash.** `HASH_COLUMNS` in `customer_cdc.pset` *includes*
  the key `customer_id`. That is redundant for change detection but is what the
  legacy graph hashes, so it is reproduced (flagged in `cdc_customers.py`), not
  silently dropped.

Reconciliation controls (`verify/reconcile.py`): `customer_cdc_completeness`,
`customer_cdc_control_total`, and `customer_cdc_parity` — the last recomputes the
INSERT/UPDATE/DELETE key sets from the raw snapshots with the legacy `"||"` MD5
hash and asserts per-class equality with the job output.

## Reconciliation contract

Every conversion is gated by controls in `verify/reconcile.py`:

- **completeness** — no silent row loss vs the source population;
- **control total** — a SUM that ties out to the source extract;
- **parity** — every DML default / mapping / CDC delta class matches the source
  value-for-value.

A control `SKIP`s until its target table exists, then must `PASS`. The script
exits non-zero on any `FAIL`, so it is also the CI / pre-merge gate.
