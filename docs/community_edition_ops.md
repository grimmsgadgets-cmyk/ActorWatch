# Community Edition Ops

## Migration Toolchain

- `scripts/migrate_sqlite.sh`: run schema migration against a local SQLite file.
- `scripts/community_smoke.sh`: end-to-end API smoke checks for local runs.
- `scripts/prune_data.sh`: retention-based pruning for historical high-volume tables.

Example:

```bash
./scripts/migrate_sqlite.sh ./actor_notebook.db
./scripts/community_smoke.sh http://127.0.0.1:8000
./scripts/prune_data.sh ./actor_notebook.db
```

## API Versioning Policy

- Versioning model: URI versioning (`/api/v1/...`) for future breaking API surfaces.
- Current compatibility mode: legacy unversioned paths remain supported for community adoption.
- Breaking change process:
  1. Introduce equivalent `/api/v{n+1}` endpoint.
  2. Mark legacy endpoint as deprecated in release notes.
  3. Keep legacy endpoint for at least two minor releases.

## Deprecation Policy

- Deprecations are announced in `docs/CHANGELOG.md`.
- API response headers for deprecated endpoints should include:
  - `Deprecation: true`
  - `Sunset: <RFC 3339 date>`
- Community edition target: minimum 90-day deprecation window.

## Security/Quality Gate (Community Baseline)

- Unit/integration tests: `pytest -q`
- Static checks: `ruff check`
- Security scans: `bandit -r app.py services pipelines routes`, `pip-audit -r requirements.txt`
- Release push gate: `scripts/release_push_gate.sh`

## STIX Interop

- Export: `GET /actors/{actor_id}/stix/export`
- Import: `POST /actors/{actor_id}/stix/import`
- Scope:
  - IOC indicators
  - analyst observations (as STIX note objects)

## Sample Data

- Reference sample STIX bundle:
  - `docs/samples/stix_bundle_minimal.json`
