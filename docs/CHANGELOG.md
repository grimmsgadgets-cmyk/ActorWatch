# Changelog

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
