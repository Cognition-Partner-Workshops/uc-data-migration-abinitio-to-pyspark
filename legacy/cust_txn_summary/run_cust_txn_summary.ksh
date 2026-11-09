#!/bin/ksh
set -e
export AI_SERIAL=${AI_SERIAL:-/data/serial}
export PROCESS_DATE=${PROCESS_DATE:-$(date +%Y%m%d)}
air sandbox run cust_txn_summary.mp -PROCESS_DATE "$PROCESS_DATE" || exit 1
