#!/usr/bin/env bash
set -euo pipefail

BACKUP_PATH="${1:-./actor_notebook.db.backup}"

if [ ! -f "$BACKUP_PATH" ]; then
  echo "backup file not found: $BACKUP_PATH" >&2
  exit 1
fi

if [ ! -s "$BACKUP_PATH" ]; then
  echo "backup file is empty: $BACKUP_PATH" >&2
  exit 1
fi

python - <<'PY' "$BACKUP_PATH"
import sqlite3
import sys

path = sys.argv[1]
required_tables = (
    'actor_profiles',
    'sources',
    'question_threads',
    'timeline_events',
)

with sqlite3.connect(path) as connection:
    for table in required_tables:
        connection.execute(f'SELECT COUNT(*) FROM {table}').fetchone()

print(f'backup restore verification passed: {path}')
PY
