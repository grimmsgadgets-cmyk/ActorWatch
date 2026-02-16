#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/5] Build app container image"
docker compose build app >/dev/null

echo "[2/5] Start app stack and verify boot"
docker compose up -d ollama app >/dev/null

# Wait for app to report healthy from inside container.
for _ in $(seq 1 40); do
  if docker compose exec -T app python -c "import http.client; c=http.client.HTTPConnection('127.0.0.1',8000,timeout=3); c.request('GET','/health'); r=c.getresponse(); print(r.status); print(r.read().decode()); assert r.status==200" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

docker compose exec -T app python -c "import http.client; c=http.client.HTTPConnection('127.0.0.1',8000,timeout=3); c.request('GET','/health'); r=c.getresponse(); body=r.read().decode(); assert r.status==200 and '\"status\":\"ok\"' in body; print('health ok')"

echo "[3/5] Verify security headers are present"
docker compose exec -T app python -c "import http.client; c=http.client.HTTPConnection('127.0.0.1',8000,timeout=5); c.request('GET','/'); r=c.getresponse(); h={k.lower():v for k,v in r.getheaders()}; required=['content-security-policy','x-content-type-options','referrer-policy','x-frame-options','permissions-policy']; missing=[k for k in required if k not in h]; assert not missing, f'missing headers: {missing}'; print('headers ok')"

echo "[4/5] Verify 413 request-size protection"
docker compose exec -T app python -c "import http.client,json; body=json.dumps({'source_type':'x','ttp_list':['A']*200000}); c=http.client.HTTPConnection('127.0.0.1',8000,timeout=15); c.request('POST','/actors/test-actor/observations',body=body,headers={'Content-Type':'application/json'}); r=c.getresponse(); b=r.read().decode('utf-8','replace'); assert r.status==413, f'expected 413 got {r.status}: {b}'; print('413 ok')"

echo "[5/5] Verify 429 rate limiting on heavy write path"
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

echo "Release push gate passed."
