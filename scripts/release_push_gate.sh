#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/7] Verify sqlite migration helper"
./scripts/migrate_sqlite.sh /tmp/actorwatch-release-gate.db >/dev/null
rm -f /tmp/actorwatch-release-gate.db

echo "[2/7] Build app container image"
docker compose build app >/dev/null

echo "[3/7] Start app stack and verify boot"
docker compose up -d ollama app >/dev/null

# Wait for app to report healthy from inside container.
for _ in $(seq 1 40); do
  if docker compose exec -T app python -c "import http.client; c=http.client.HTTPConnection('127.0.0.1',8000,timeout=3); c.request('GET','/health'); r=c.getresponse(); print(r.status); print(r.read().decode()); assert r.status==200" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

docker compose exec -T app python -c "import http.client; c=http.client.HTTPConnection('127.0.0.1',8000,timeout=3); c.request('GET','/health'); r=c.getresponse(); body=r.read().decode(); assert r.status==200 and '\"status\":\"ok\"' in body; print('health ok')"

echo "[4/7] Verify security headers are present"
docker compose exec -T app python -c "import http.client; c=http.client.HTTPConnection('127.0.0.1',8000,timeout=5); c.request('GET','/'); r=c.getresponse(); h={k.lower():v for k,v in r.getheaders()}; required=['content-security-policy','x-content-type-options','referrer-policy','x-frame-options','permissions-policy']; missing=[k for k in required if k not in h]; assert not missing, f'missing headers: {missing}'; print('headers ok')"

echo "[5/7] Verify 413 request-size protection"
docker compose exec -T app python -c "import http.client,json; body=json.dumps({'source_type':'x','ttp_list':['A']*200000}); c=http.client.HTTPConnection('127.0.0.1',8000,timeout=15); c.request('POST','/actors/test-actor/observations',body=body,headers={'Content-Type':'application/json'}); r=c.getresponse(); b=r.read().decode('utf-8','replace'); assert r.status==413, f'expected 413 got {r.status}: {b}'; print('413 ok')"

echo "[6/7] Verify 429 rate limiting on heavy write path"
docker compose exec -T app python - <<'PY'
import http.client

codes = []
for _ in range(20):
    conn = http.client.HTTPConnection('127.0.0.1', 8000, timeout=5)
    conn.request('POST', '/actors/test-actor/refresh')
    resp = conn.getresponse()
    codes.append(resp.status)
    resp.read()

assert any(code == 429 for code in codes), f'expected at least one 429, got {codes}'
print('429 ok', codes)
PY

echo "[7/7] Verify STIX export endpoint responds with bundle"
docker compose exec -T app python - <<'PY'
import http.client

conn = http.client.HTTPConnection('127.0.0.1', 8000, timeout=5)
conn.request('POST', '/actors', body='{"display_name":"Gate Actor"}', headers={'Content-Type': 'application/json'})
resp = conn.getresponse()
body = resp.read().decode('utf-8', 'replace')
assert resp.status in (200, 201), f'create actor failed: {resp.status} {body}'
actor_id = body.split('"id":"', 1)[1].split('"', 1)[0]

conn = http.client.HTTPConnection('127.0.0.1', 8000, timeout=5)
conn.request('GET', f'/actors/{actor_id}/stix/export')
resp = conn.getresponse()
body = resp.read().decode('utf-8', 'replace')
assert resp.status == 200 and '"type":"bundle"' in body, f'stix export failed: {resp.status} {body}'
print('stix export ok')
PY

echo "Release push gate passed."
