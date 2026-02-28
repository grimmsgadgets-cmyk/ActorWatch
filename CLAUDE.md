# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Role

Act as a senior software architect and developer. Prioritize correctness, minimal surface area, and architectural coherence. Before writing code, understand the existing pattern in the surrounding layer. Prefer targeted changes over broad refactors unless explicitly asked.

---

## Commands

### Run tests
```bash
uv run pytest -q
```

### Run a single test file
```bash
uv run pytest tests/test_notebook.py -q
```

### Run a single test by name
```bash
uv run pytest tests/test_notebook.py::test_function_name -q
```

### Lint
```bash
uv run ruff check app.py services pipelines routes tests
```

### Security scan
```bash
uv run bandit -q -r app.py services pipelines routes
```

### Run app locally (Docker, standard)
```bash
docker compose up --build
```

Access at `http://localhost:8000`. App publishes on `127.0.0.1:8000` only.

### Online/integration tests (opt-in, requires network)
```bash
ACTORTRACKER_ONLINE_TESTS=1 uv run pytest tests/test_integration_online.py -q
```

---

## Architecture

### Layer responsibilities

| Layer | Location | Rule |
|---|---|---|
| Composition root | `app.py` | Wires deps, owns lifecycle hooks, registers routers. No business logic. |
| Route handlers | `routes/` | Thin: parse request, call service/pipeline, return response. No heavy logic. |
| Domain services | `services/` | Business rules, DB operations, orchestration helpers. |
| Data pipelines | `pipelines/` | Compute-heavy extraction, notebook synthesis, feed ingest internals. |

### Dependency injection

All routers are created via factory functions that accept an explicit `deps` dict:

```python
router = create_notebook_router(deps={...})
app.include_router(router)
```

The wiring lives in `services/app_wiring_service.py` and `services/app_dependency_maps_service.py`. When adding a new service dependency to a route, thread it through the `deps` dict — never import and call services directly inside route handlers.

### URL path constants

All route URL patterns are defined in `route_paths.py`. Always use these constants in both route registration and client calls to keep paths consistent.

### Database

- SQLite at `/data/app.db` (Docker volume `actortracker_db`).
- Schema owned by `services/db_schema_service.py` (`ensure_schema()`), which uses `ALTER TABLE IF NOT EXISTS` column guards for forward-compatible migrations.
- The schema version string in `ensure_schema()` should be bumped when the schema changes.

### Key tables

| Table | Purpose |
|---|---|
| `actor_profiles` | Actor identity, `canonical_name` (lowercased dedup key), tracking state, notebook status |
| `sources` | Actor-linked source corpus with quality metadata |
| `actor_feed_state` | Per-actor, per-feed health, checkpointing, backoff |
| `timeline_events`, `question_threads` | Analysis outputs |
| `analyst_observations` | Human-in-the-loop notes with audit trail |
| `notebook_generation_jobs` | Job journal for generation worker lifecycle |

### Background processing

- Auto-refresh loop: a daemon `Thread` (`actor-auto-refresh`) picks tracked actors not recently refreshed and enqueues batches. Tuned via env vars (`AUTO_REFRESH_*`).
- Generation worker pool: started by `generation_service.start_generation_workers_core()` in lifespan. Jobs enqueued via the journal (`notebook_generation_jobs`).
- Both are started in `app_lifespan()` and shut down cleanly on exit.

### LLM integration

- Local Ollama sidecar (`OLLAMA_BASE_URL`, default model `llama3.1:8b`).
- LLM synthesis is **optional**; deterministic paths must produce usable output without it.
- `ENFORCE_OLLAMA_SYNTHESIS=1` makes LLM required (used in Docker dev).
- LLM calls wrapped in `services/llm_facade_service.py` with caching via `services/llm_cache_service.py`.

### Actor state model

Actors have two states:
- **Catalog** (untracked): seeded from MITRE, no ingest runs. Expected idle behavior, not a failure.
- **Tracked**: eligible for auto-refresh, source ingest, and notebook generation.

A fresh install has no tracked actors. MITRE seed (`MITRE_AUTO_SEED_ACTORS=1`) adds untracked catalog entries only.

### Feed categories (defined in `app.py` as `FEED_CATALOG`)

- `ioc`: IOC-bearing sources — primary for IOC extraction and hunt query generation.
- `research`: broader threat research.
- `advisory`: vendor/government advisories.
- `context`: awareness/news (capped intake to protect signal density).

### Security controls

- SSRF prevention: URL policy validation before any outbound fetch; redirect-chain re-validation.
- CSRF: Origin/Referer validation on write paths.
- Rate limiting and request-size enforcement on write endpoints.
- Domain allowlist for outbound fetches (`OUTBOUND_ALLOWED_DOMAINS`).
- Do not remove or bypass these controls.

---

## Known Architectural Debt (Active RCA)

See `CURRENT_ARCH_ROOT_CAUSE.md` and `ARCH_RCA_AND_PYTHONIC_REFACTOR_PLAN.md` for full context. Key issues to be aware of:

1. **`app.py` is still large** (~3100+ lines). Planned extractions to `services/runtime_service.py`, `services/http_guard_service.py`, etc. are in progress. New logic must go into the appropriate service/pipeline — do not add to `app.py`.
2. **`routes/routes_notebook.py` is similarly large** and being split into sub-routers under `routes/notebook_*.py`.
3. **Cache key mismatch** — FIXED. Dashboard was passing explicit `min_confidence_weight=1, source_days=365` which produced a different cache key from what LLM enrichment stored (`None/None`). Dashboard now passes `None` and lets the pipeline apply its own defaults.
4. **Stale job lockout** — FIXED. `recover_stale_running_states_core` now also expires `notebook_generation_jobs` records when it resets an actor profile, so the journal lock cannot block new jobs after recovery.
5. **Running state card population** resolves naturally from (3): stale cache for the correct key now exists and is served during `running` state by `notebook_live.py`.

When modifying the notebook or dashboard code paths, be aware these defects may make test behavior seem correct even when analyst UX is degraded.

---

## Coding Conventions

- Route handlers must remain thin. If logic exceeds ~20 lines, it belongs in a service or pipeline.
- Business logic in `services/`; extraction/synthesis in `pipelines/`.
- Import services at module level; inject via `deps` dict at router factory call sites.
- Do not break existing endpoint response shapes without explicit coordination and a documented migration path.
- Prefer `uv` for all Python tooling. The project uses `pyproject.toml` + `uv.lock`.
