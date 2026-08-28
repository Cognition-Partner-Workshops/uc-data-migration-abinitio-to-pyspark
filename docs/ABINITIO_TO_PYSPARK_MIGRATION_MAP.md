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
| Transactions detail (`transaction_detail.dml`, `run_daily_orders.ksh`) | `src/jobs/curated_transactions.py` → `curated/transactions` | converted | nested record → struct, `line_items[]` → explode, DML `null(...)` defaults |
| Customer CDC (`cdc_processor.py`, `customer_cdc.pset`) | `src/jobs/cdc_customers.py` | **live conversion** | compare-by-key + row hash → Delta MERGE analog |

"Live conversion" rows are the work Devin does during the demo via
`!convert-abinitio-to-pyspark`; `main` carries the durable before-state plus the
reconciliation harness.

### Source quirks carried forward by the transactions conversion

Reproduced faithfully and flagged in `src/jobs/curated_transactions.py` — each is
a separate business decision, not a conversion fix:

1. `transactions.dat` is the *flattened header* form of the DML record: it carries
   exactly one `sku|quantity|line_total` triple per transaction regardless of the
   `item_count` field, so the `line_items` vector is rebuilt as a single-element
   array and `item_count` is carried through from the extract rather than
   recomputed from the array.
2. The extract carries no columns for the conditional `refund_details` record, so
   its *presence* is reproduced (a struct for `txn_type = 2`, NULL otherwise) but
   its field values cannot be — they are not in the source.

## Reconciliation contract

Every conversion is gated by controls in `verify/reconcile.py`:

- **completeness** — no silent row loss vs the source population;
- **control total** — a SUM that ties out to the source extract;
- **parity** — every DML default / mapping / CDC delta class matches the source
  value-for-value.

A control `SKIP`s until its target table exists, then must `PASS`. The script
exits non-zero on any `FAIL`, so it is also the CI / pre-merge gate.
