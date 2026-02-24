#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-./actor_notebook.db}"

python - <<'PY' "$DB_PATH"
import sqlite3
import sys

from services import db_schema_service

db_path = sys.argv[1]
with sqlite3.connect(db_path) as conn:
    db_schema_service.ensure_schema(conn)
print(f"schema migration complete: {db_path}")
PY
