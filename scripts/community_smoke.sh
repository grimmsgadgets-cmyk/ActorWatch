#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:8000}"
DB_PATH="${2:-./actor_notebook.db}"

echo "schema migration helper"
./scripts/migrate_sqlite.sh "$DB_PATH" >/dev/null

echo "health"
curl -fsS "$BASE_URL/health" | grep -q '"status":"ok"'

echo "create actor"
ACTOR_ID="$(
  curl -fsS -X POST "$BASE_URL/actors" \
    -H 'Content-Type: application/json' \
    -d '{"display_name":"Smoke Actor"}' | sed -n 's/.*"id":"\([^"]*\)".*/\1/p'
)"
[ -n "$ACTOR_ID" ]

echo "refresh actor"
curl -fsS -X POST "$BASE_URL/actors/$ACTOR_ID/refresh" >/dev/null

echo "stix export"
curl -fsS "$BASE_URL/actors/$ACTOR_ID/stix/export" | grep -q '"type":"bundle"'

echo "environment profile round-trip"
curl -fsS -X POST "$BASE_URL/actors/$ACTOR_ID/environment-profile" \
  -H 'Content-Type: application/json' \
  -d '{"query_dialect":"generic","field_mapping":{},"default_time_window_hours":24}' >/dev/null
curl -fsS "$BASE_URL/actors/$ACTOR_ID/environment-profile" | grep -q '"actor_id"'

echo "smoke checks passed for actor: $ACTOR_ID"
