#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-./actor_notebook.db}"
RETENTION_DAYS="${RETENTION_DAYS:-180}"
KEEP_MIN_ROWS="${KEEP_MIN_ROWS:-500}"

python - <<'PY' "$DB_PATH" "$RETENTION_DAYS" "$KEEP_MIN_ROWS"
import json
import sqlite3
import sys

from services import data_retention_service
from services import db_schema_service

db_path = sys.argv[1]
retention_days = int(sys.argv[2])
keep_min_rows = int(sys.argv[3])

with sqlite3.connect(db_path) as connection:
    db_schema_service.ensure_schema(connection)
    results = data_retention_service.prune_data_core(
        connection,
        retention_days=retention_days,
        keep_min_rows_per_table=keep_min_rows,
    )
    connection.commit()

print(json.dumps({"db_path": db_path, "retention_days": retention_days, "keep_min_rows": keep_min_rows, "results": results}, indent=2))
PY
