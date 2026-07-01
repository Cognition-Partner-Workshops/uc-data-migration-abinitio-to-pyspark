# Reconciliation Report — namespace `dev`

Source (`data/raw/`) -> target (`out/dev/`) controls proving the
converted PySpark tables reproduce the legacy Ab Initio extract's intent.
FAIL blocks the migration; SKIP means a prerequisite (e.g. the live-converted
transactions pipeline) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `customers_completeness` | PASS | source customers = 50, staging customers = 50 |
| `orders_completeness` | PASS | source orders = 80, staging orders = 80 |
| `orders_control_total` | PASS | source SUM(amount) = 8492.04, mart SUM(total_amount) = 8492.04 |
| `orders_daily_parity` | PASS | 0 order_date(s) diverge between source and mart |
| `transactions_channel_parity` | SKIP | curated.transactions not produced yet (live conversion target) |
| `customer_cdc_completeness` | PASS | source only-current = 5, only-previous = 3; job INSERT = 5, DELETE = 3, distinct change keys = 14 of 14 records |
| `customer_cdc_control_total` | PASS | source SUM(changed customer_id) = 14399, job SUM(customer_id) = 14399 |
| `customer_cdc_parity` | PASS | INSERT 5/5, UPDATE 6/6, DELETE 3/3 (job/source); 0 key(s) misclassified |

**7 passed, 0 failed, 1 skipped.**
