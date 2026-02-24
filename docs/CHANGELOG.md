# Changelog

## 0.2.1 - 2026-02-24

### Added
- Dependabot configuration in `.github/dependabot.yml` for pip and GitHub Actions updates.
- CODEOWNERS baseline in `.github/CODEOWNERS`.
- Maintainer setup guide in `docs/maintainer_setup.md` for branch protection and trust guardrails.
- Release validation workflow in `.github/workflows/release.yml`.
- Version bump helper script `scripts/bump_version.sh`.

### Changed
- README now links maintainer setup and release/dependency maintenance paths.

## 0.2.0 - 2026-02-24

### Added
- `SECURITY.md` with local-first deployment guidance and vulnerability reporting expectations.
- Contributor workflow templates:
  - `.github/ISSUE_TEMPLATE/bug_report.yml`
  - `.github/ISSUE_TEMPLATE/feature_request.yml`
  - `.github/pull_request_template.md`
- API contract regression tests in `tests/test_api_contracts.py`.
- Data retention service in `services/data_retention_service.py`.
- Data pruning script `scripts/prune_data.sh`.

### Changed
- README now references contributor and security guidance.
- Release/smoke scripts include stronger baseline checks.
- SQLite schema now records a schema version in `schema_meta`.

### Notes
- Community Edition remains local-first by default.

## 0.1.1 - 2026-02-16

### Added
- Architecture roadmap and execution plan in `ROADMAP.md`.
- Rate-limiting design record in `docs/ADR-0001-rate-limiting.md`.
- Security headers middleware (CSP-lite, `nosniff`, referrer policy, frame deny, permissions policy).
- Deterministic request-size limits on body-reading write routes (413 enforcement).
- Write-path rate limiting with `429` and `Retry-After` support.
- Deterministic release gate script: `scripts/release_push_gate.sh`.
- Minimal CI workflow: `.github/workflows/ci-lite.yml` (compile + lint, optional pytest).

### Changed
- FastAPI startup lifecycle migrated from deprecated `on_event('startup')` to lifespan handler.
- "What have they been up to recently?" synthesis improved:
  - `Who was targeted` now uses industry-targeting synthesis.
  - `Damage seen` now synthesizes damage outcomes from source-linked text and timeline summaries.

### Notes
- Full `pytest` is intentionally not required as a local push gate in constrained sandbox environments.
- Use `scripts/release_push_gate.sh` as the deterministic pre-push check.
