# Actor Tracker

## Product Roadmap
- See `ROADMAP.md` for the phased plan covering Community and paid Client editions.

## Release Push Gate
- Deterministic pre-push gate (no full pytest required in sandbox):
```bash
bash scripts/release_push_gate.sh
```
- This validates:
  - container build
  - app boot
  - security headers
  - `413` body-size limits
  - `429` write-path rate limiting

## Test Matrix

### Default Unit Lane (offline, deterministic)
- Purpose: fast local validation of core logic and security controls without external network dependency.
- Includes:
  - notebook/data logic tests
  - SSRF policy tests (`_validate_outbound_url`, redirect re-validation)
  - XSS escaping checks for raw HTML endpoints
- Command:
```bash
.venv/bin/pytest -q
```

### Online Integration Lane (opt-in)
- Purpose: validate real outbound HTTP behavior and TestClient integration in a network-enabled environment.
- Guard: runs only when `ACTORTRACKER_ONLINE_TESTS=1`.
- Command:
```bash
ACTORTRACKER_ONLINE_TESTS=1 .venv/bin/pytest -q tests/test_integration_online.py
```

### Syntax Check
- Command:
```bash
.venv/bin/python -m py_compile app.py tests/conftest.py tests/test_notebook.py tests/test_integration_online.py
```
