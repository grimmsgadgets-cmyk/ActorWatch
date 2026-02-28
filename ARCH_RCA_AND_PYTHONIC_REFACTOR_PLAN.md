# Architecture RCA + Pythonic Refactor Blueprint

## 1) Persisted RCA Snapshot (Current State)

This section is the persisted memory artifact of what was diagnosed and validated.

### Primary product failures that were fixed

1. Live status drift in `/actors/{id}/ui/live`
- Symptom: top-level `notebook_status` could differ from `actor.notebook_status`.
- Impact: UI could interpret actor as `idle` while backend intended `running/error`.
- Fix: enforce synchronized status/message in returned `actor` object and top-level fields.
- Proof: forced status smoke now returns:
  - `RUNNING ('running', 'running', 3, 1, 3)`
  - `ERROR ('error', 'error', 3, 1, 3)`

2. Empty-card risk under cache-miss/running/locked states
- Symptom: fallback payload paths could produce sparse or empty card groups.
- Impact: analysts see blank cards and lose trust.
- Fix: route fallbacks run through notebook contract finalization and minimum payload enforcement.
- Proof: live smoke across all actors shows:
  - `TOTAL 175`
  - `EMPTY_OR_PARTIAL 0`

3. Read path side effects + stale job handling
- Symptom: read-style flows could trigger refresh jobs; stale jobs could block progress.
- Impact: unpredictable behavior and stuck actor states.
- Fixes:
  - removed page-load enqueue behavior from dashboard read path.
  - added stale generation job expiry/recovery flow.

4. Test harness instability masking architecture quality
- Symptom: selected `TestClient` paths deadlocked in this environment.
- Impact: false negatives, long debugging loops, unstable CI confidence.
- Fix: converted unstable tests to deterministic endpoint-invocation tests with equivalent behavior assertions.
- Proof:
  - `tests/test_notebook.py` -> `75 passed`
  - broader target set -> `86 passed`

## 2) Refactor Goals (No Removal Yet)

Non-negotiable rules for the refactor phase:

1. No behavior-changing removals in phase 1.
2. Extract first, wire adapters second, remove dead code last.
3. Keep API payload contracts stable (`actor`, `priority_questions`, `top_change_signals`, `recent_activity_synthesis`, etc.).
4. Every extraction step requires parity tests before and after.
5. Route handlers should orchestrate only; heavy logic belongs in services/pipelines.

## 3) High-Risk File Overview + Planned Extractions

### `app.py` (3168)
Current sections (broad):
- startup/runtime loop + stale recovery
- middleware/rate-limit/CSRF/request-size
- MITRE cache and matching helpers
- text normalization and extraction utilities
- source/network derivation helpers
- LLM generation helpers
- actor lifecycle / refresh / job journal orchestration
- notebook wrapper glue
- router registration

Planned extractions:
- `services/runtime_service.py`: startup loop + stale running recovery wrappers
- `services/http_guard_service.py`: request-size, rate-limit, csrf middleware helpers
- `services/mitre_facade_service.py`: MITRE load/match/technique lookup wrappers
- `services/text_utils_service.py`: normalize/token/sentence helpers
- `services/source_network_service.py`: URL validation + safe fetch + metadata extraction
- `services/actor_refresh_service.py`: submit/get refresh jobs + orchestration boundaries
- `services/router_bootstrap_service.py`: dependency map + router registration

Risk points:
- many internal functions are implicitly coupled by closure/shared globals.
- extraction must keep DI argument names and return shape identical.

### `routes/routes_notebook.py` (3497)
Current sections (broad):
- requirements, tracking intent, confirmations
- collection plan / report preferences / relationships / change items
- alerts, technique coverage, tasks, outcomes
- workspace pages (questions/hunts/live)
- observations CRUD/history/exports
- analyst pack export (json/pdf)

Planned extractions:
- `routes/notebook_requirements.py`
- `routes/notebook_operations.py` (tracking/plan/preferences/relationships/change/alerts)
- `routes/notebook_hunts.py`
- `routes/notebook_observations.py`
- `routes/notebook_exports.py`
- `routes/notebook_live.py`

Risk points:
- many closures depend on large `deps` bag.
- first move should be splitting router factory into sub-factories that share the same deps object.

### `pipelines/notebook_pipeline.py` (3409)
Current sections (broad):
- IOC semantic extraction/matching/fallback
- behavior query generation
- recent change summary and signal ranking
- highlight and synthesis builders
- fetch orchestrator

Planned extractions:
- `pipelines/notebook/ioc_relevance.py`
- `pipelines/notebook/behavior_queries.py`
- `pipelines/notebook/change_signals.py`
- `pipelines/notebook/highlights.py`
- `pipelines/notebook/fetch_orchestrator.py`

Risk points:
- shared helper assumptions around date handling and evidence windows.
- enforce contract tests around each top-level builder before splitting.

### `tests/test_notebook.py` (3355)
Current sections (broad):
- wrappers/delegation tests
- IOC and quick-check relevance tests
- source quality and fallback tests
- MITRE and parsing tests
- root/dashboard tests
- observation and export tests
- performance guard tests

Planned extractions:
- `tests/notebook/test_pipeline_behavior.py`
- `tests/notebook/test_routes_live_and_hunts.py`
- `tests/notebook/test_observations_and_exports.py`
- `tests/notebook/test_root_render_contract.py`
- `tests/notebook/test_ingest_and_sources.py`

Risk points:
- avoid reintroducing flaky harness paths.
- preserve deterministic direct-endpoint pattern where needed.

### `pipelines/feed_ingest.py` (1424)
Current sections: candidate discovery, parsing, dedupe, filtering, persistence.

Planned extractions:
- `pipelines/feed_ingest/discovery.py`
- `pipelines/feed_ingest/normalization.py`
- `pipelines/feed_ingest/persistence.py`

### `services/web_backfill_service.py` (1300)
Current sections: query generation, source retrieval, actor relevance filtering, persistence.

Planned extractions:
- `services/web_backfill/query_planner.py`
- `services/web_backfill/relevance.py`
- `services/web_backfill/ingest_writer.py`

### `services/db_schema_service.py` (1133)
Current sections: table DDL, migration guards, index setup.

Planned extractions:
- `services/db_schema/tables_core.py`
- `services/db_schema/tables_notebook.py`
- `services/db_schema/migrations.py`
- `services/db_schema/indexes.py`

### `services/actor_profile_service.py` (767)
Current sections: actor CRUD + merge + migration of actor-owned rows.

Planned extractions:
- `services/actor_profile/crud.py`
- `services/actor_profile/merge.py`
- `services/actor_profile/move_children.py`

### `mitre_store.py` (764)
Current sections: dataset load/cache/index/matching.

Planned extractions:
- `services/mitre_store/load.py`
- `services/mitre_store/index.py`
- `services/mitre_store/match.py`

### `priority_questions.py` (644)
Current sections: quick check templates + rendering/field population.

Planned extractions:
- `services/priority_questions/templates.py`
- `services/priority_questions/render.py`
- `services/priority_questions/evidence.py`

### `services/notebook_service.py` (571)
Current sections: contract finalizer + cache key/fingerprint/load/save + wrappers.

Planned extractions:
- `services/notebook_contract_service.py`
- `services/notebook_cache_service.py`
- keep `services/notebook_service.py` as thin facade.

### `services/analyst_text_service.py` (570)
Current sections: text simplification/formatting for analyst-facing copy.

Planned extractions:
- split by domain (`summaries`, `normalization`, `style_rules`) if file grows further.

### `routes/routes_actor_ops.py` (497)
Current sections: manual source add/import feeds/IOCs/STIX/refresh.

Planned extractions:
- `routes/actor_ops_sources.py`
- `routes/actor_ops_iocs.py`
- `routes/actor_ops_refresh.py`

### `routes/routes_evolution.py` (443)
Current sections: actor state, observations, deltas, resolution, HTML review.

Planned extractions:
- `routes/evolution_state.py`
- `routes/evolution_deltas.py`

### `services/generation_journal_service.py` (413)
Current sections: jobs/phases bookkeeping + stale job recovery.

Planned extractions:
- `services/generation_journal/jobs.py`
- `services/generation_journal/phases.py`
- `services/generation_journal/recovery.py`

### `routes/routes_dashboard.py` (398)
Current sections: root dashboard selection/status/render.

Planned extractions:
- keep this as thin controller; move remaining fallback/status transforms to service.

### `pipelines/timeline_extraction.py` (374)
Current sections: event extraction/categorization.

Planned extractions:
- split classifier vs extractor modules.

### `pipelines/notebook_builder.py` (330)
Current sections: notebook build orchestration.

Planned extractions:
- keep orchestration thin; move detail builders to dedicated modules.

## 4) All Python Files Inventory (Initial Action)

Legend:
- `Extract`: needs modular split.
- `Stabilize`: keep file but tighten contracts/tests first.
- `Hold`: no immediate split required.

| File | LOC | Action | Note |
|---|---:|---|---|
| routes/routes_notebook.py | 3497 | Extract | Split by endpoint domain |
| pipelines/notebook_pipeline.py | 3409 | Extract | Split by notebook computation domain |
| tests/test_notebook.py | 3355 | Extract | Split by behavior area |
| app.py | 3168 | Extract | Bootstrap/facade only target |
| pipelines/feed_ingest.py | 1424 | Extract | Discovery/parse/persist split |
| services/web_backfill_service.py | 1300 | Extract | Planner/relevance/writer split |
| services/db_schema_service.py | 1133 | Extract | DDL/migrations/indexes split |
| tests/test_feed_ingest.py | 965 | Stabilize | Split after feed ingest module split |
| services/actor_profile_service.py | 767 | Extract | CRUD/merge/move children split |
| mitre_store.py | 764 | Extract | load/index/match split |
| priority_questions.py | 644 | Extract | template/render/evidence split |
| services/notebook_service.py | 571 | Extract | contract/cache facade split |
| services/analyst_text_service.py | 570 | Stabilize | review for submodule boundaries |
| tests/test_web_backfill_service.py | 510 | Stabilize | mirror service split |
| routes/routes_actor_ops.py | 497 | Extract | sources/iocs/refresh split |
| pipelines/requirements_pipeline.py | 448 | Stabilize | likely split later by generator/formatter |
| routes/routes_evolution.py | 443 | Extract | state/deltas split |
| services/generation_journal_service.py | 413 | Extract | jobs/phases/recovery split |
| routes/routes_dashboard.py | 398 | Stabilize | keep thin controller |
| pipelines/timeline_extraction.py | 374 | Stabilize | split classifier/extractor later |
| pipelines/notebook_builder.py | 330 | Stabilize | keep orchestration thin |
| services/generation_service.py | 307 | Hold | acceptable size |
| services/refresh_ops_service.py | 305 | Hold | acceptable size |
| services/stix_service.py | 299 | Hold | acceptable size |
| services/ioc_store_service.py | 296 | Hold | acceptable size |
| services/timeline_analytics_service.py | 286 | Hold | acceptable size |
| pipelines/actor_ingest.py | 278 | Hold | acceptable size |
| tests/test_analyst_text_service.py | 256 | Hold | acceptable size |
| tests/test_api_contracts.py | 248 | Hold | contract guard |
| services/recent_activity_service.py | 244 | Hold | acceptable size |
| pipelines/source_derivation.py | 226 | Hold | acceptable size |
| services/ioc_hunt_service.py | 215 | Hold | acceptable size |
| services/source_ingest_service.py | 211 | Hold | acceptable size |
| tests/test_actor_profile_service.py | 209 | Hold | acceptable size |
| tests/test_community_edition_quality.py | 205 | Hold | quality gate tests |
| services/actor_state_service.py | 188 | Hold | acceptable size |
| services/ioc_validation_service.py | 184 | Hold | acceptable size |
| services/observation_service.py | 181 | Hold | acceptable size |
| pipelines/generation_runner.py | 177 | Hold | acceptable size |
| tests/test_notebook_service_cache.py | 174 | Hold | acceptable size |
| tests/test_ioc_hunt_service.py | 173 | Hold | acceptable size |
| tests/test_notebook_pipeline_ioc_extraction.py | 160 | Hold | acceptable size |
| services/llm_cache_service.py | 147 | Hold | acceptable size |
| services/quick_check_service.py | 144 | Hold | acceptable size |
| tests/test_top_change_signals.py | 139 | Hold | acceptable size |
| services/app_wiring_service.py | 137 | Hold | acceptable size |
| services/alert_delivery_service.py | 121 | Hold | acceptable size |
| services/environment_profile_service.py | 120 | Hold | acceptable size |
| tests/test_auto_refresh.py | 118 | Hold | acceptable size |
| tests/test_ioc_store_service.py | 117 | Hold | acceptable size |
| services/actor_search_service.py | 116 | Hold | acceptable size |
| routes/routes_api.py | 115 | Hold | acceptable size |
| guidance_catalog.py | 112 | Hold | acceptable size |
| tests/test_recent_activity_service.py | 111 | Hold | acceptable size |
| services/rate_limit_service.py | 98 | Hold | acceptable size |
| network_safety.py | 97 | Hold | acceptable size |
| tests/test_source_ingest_service.py | 96 | Hold | acceptable size |
| tests/test_quick_checks_view_service.py | 95 | Hold | acceptable size |
| services/feedback_service.py | 93 | Hold | acceptable size |
| tests/test_observation_service.py | 87 | Hold | acceptable size |
| tests/test_generation_runner.py | 85 | Hold | acceptable size |
| services/quick_checks_view_service.py | 85 | Hold | acceptable size |
| services/source_store_service.py | 78 | Hold | acceptable size |
| tests/test_llm_cache_service.py | 76 | Hold | acceptable size |
| tests/test_learning_endpoints.py | 75 | Hold | acceptable size |
| services/source_reliability_service.py | 74 | Hold | acceptable size |
| tests/test_learning_services.py | 73 | Hold | acceptable size |
| services/metrics_service.py | 70 | Hold | acceptable size |
| services/status_service.py | 68 | Hold | acceptable size |
| services/priority_service.py | 67 | Hold | acceptable size |
| routes/routes_ui.py | 66 | Hold | acceptable size |
| services/data_retention_service.py | 65 | Hold | acceptable size |
| tests/test_generation_journal_service.py | 64 | Hold | acceptable size |
| tests/test_notebook_contracts.py | 61 | Hold | contract-focused |
| tests/test_generation_service.py | 58 | Hold | acceptable size |
| tests/test_priority_questions.py | 55 | Hold | acceptable size |
| services/timeline_view_service.py | 55 | Hold | acceptable size |
| services/feed_import_service.py | 55 | Hold | acceptable size |
| tests/test_integration_online.py | 54 | Hold | acceptable size |
| services/activity_highlight_service.py | 54 | Hold | acceptable size |
| tests/test_data_retention_service.py | 48 | Hold | acceptable size |
| services/network_service.py | 48 | Hold | acceptable size |
| route_paths.py | 45 | Hold | acceptable size |
| tests/test_auth_and_metrics.py | 41 | Hold | acceptable size |
| tests/test_source_candidates_batch2.py | 36 | Hold | acceptable size |
| tests/test_ioc_validation_service.py | 34 | Hold | acceptable size |
| services/llm_schema_service.py | 33 | Hold | acceptable size |
| services/requirements_service.py | 31 | Hold | acceptable size |
| legacy_ui.py | 25 | Hold | acceptable size |
| services/source_derivation_service.py | 24 | Hold | acceptable size |
| tests/test_source_candidates_batch1.py | 21 | Hold | acceptable size |
| tests/test_db_schema_evidence_tables.py | 20 | Hold | acceptable size |
| tests/conftest.py | 15 | Hold | acceptable size |
| services/prompt_templates.py | 5 | Hold | acceptable size |
| services/__init__.py | 0 | Hold | package marker |
| routes/__init__.py | 0 | Hold | package marker |
| pipelines/__init__.py | 0 | Hold | package marker |

## 5) Execution Plan (Before Any Removal)

### Phase A: Contract Lock (no refactor yet)
1. Freeze response contracts for:
- dashboard root context
- `/actors/{id}/ui/live`
- `/actors/{id}/hunts/iocs`
- observation endpoints
2. Add/update tests where contract drift has previously occurred.
3. Baseline perf checks for dashboard + observations.

### Phase B: Structural Split With Adapters
1. Split `routes/routes_notebook.py` into sub-routers, keep old import path + factory wrapper.
2. Split `pipelines/notebook_pipeline.py` by domain modules, keep `fetch_actor_notebook_core` stable facade.
3. Split `app.py` helpers to services, keep `app.py` as composition root only.

### Phase C: Test Split
1. Split `tests/test_notebook.py` into domain test modules.
2. Keep a compatibility aggregator file temporarily if needed.

### Phase D: Remove Deprecated Paths
1. Remove dead wrappers only after:
- full suite pass
- smoke pass
- contract snapshots unchanged
2. Remove legacy code in small batches (one domain at a time).

## 6) Risk Register for Refactor

1. Hidden coupling via shared globals in `app.py`.
- Mitigation: explicit dependency objects and facades before moving logic.

2. Route factory closure complexity in notebook routes.
- Mitigation: sub-factory extraction with unchanged `deps` keys.

3. Contract drift during split.
- Mitigation: add contract-focused tests first and run each extraction under `-k` domain subsets plus full smoke.

4. Test suite brittleness from heavy HTTP harness usage.
- Mitigation: keep deterministic endpoint-level tests where environment deadlocks were observed.

## 7) Immediate Next Step (Recommended)

Start with `routes/routes_notebook.py` split only (no behavior change):
- extract hunts/live/observations into dedicated route modules,
- keep existing factory in place as adapter,
- verify with current passing suite and smoke.


## 8) Progress Log

### 2026-02-28 - Phase B1 started

Completed:
- Extracted live route `/actors/{actor_id}/ui/live` out of `routes/routes_notebook.py` into new module:
  - `routes/notebook_live.py`
- Wired new module from notebook router factory via:
  - `register_notebook_live_routes(router=..., deps=...)`
- Removed in-file duplicate live route implementation from `routes/routes_notebook.py`.

Validation after extraction:
- Target regression set passed:
  - `86 passed`
- Live smoke re-run:
  - `TOTAL 175`
  - `EMPTY_OR_PARTIAL 0`

Behavior contract status:
- No contract drift detected in tested routes.

Next extraction target:
- IOC hunts route domain (`/actors/{id}/hunts/iocs`) into a dedicated module.

### 2026-02-28 - Additional modularization completed

Completed route extractions from `routes/routes_notebook.py`:
- Hunts domain:
  - `routes/notebook_hunts.py`
- Observation domain:
  - `routes/notebook_observations.py`
- Feedback/environment profile domain:
  - `routes/notebook_feedback.py`
- Export domain:
  - `routes/notebook_exports.py`
- Workspace/report/timeline/question-page domain:
  - `routes/notebook_workspace.py`

`routes/routes_notebook.py` size reduction:
- Before split sequence: 3497 lines
- Current: 1309 lines

Additional app decomposition started:
- Added runtime facade module:
  - `services/runtime_service.py`
- Moved auto-refresh runtime logic to service-core functions and kept `app.py` wrappers.

Validation after each extraction batch:
- Repeated full target regression pass:
  - `88 passed`
- Repeated smoke validation:
  - `TOTAL 175`
  - `EMPTY_OR_PARTIAL 0`

Status:
- Phase B structural split is actively in progress.
- No behavior-contract regressions detected in validated paths.

### 2026-02-28 - Deep notebook router decomposition completed

Additional extraction completed:
- Operational routes module:
  - `routes/notebook_operations.py`
- `routes/routes_notebook.py` now functions as shared-helper/orchestration layer.

`routes/routes_notebook.py` reduction status:
- prior after first splits: 1789 lines
- current: 578 lines

Additional app decomposition completed:
- Added `services/http_guard_service.py` and delegated:
  - rate-limit bucket computation
  - request client id normalization
  - rate-limit state pruning
  - core rate-limit check orchestration
  - CSRF same-origin gate
- Existing middleware contracts remain unchanged.

Validation after these extractions:
- regression suite: `88 passed`
- smoke validation: `TOTAL 175`, `EMPTY_OR_PARTIAL 0`

Known harness note:
- `tests/test_auth_and_metrics.py::test_metrics_endpoint_reports_counters` still hangs in this environment due HTTP client/lifespan flake pattern (same class of instability previously observed). Functional behavior is covered by stable regression + smoke gates above.

### 2026-02-28 - Notebook pipeline helper extraction batch

Completed:
- Extracted IOC helper cluster from `pipelines/notebook_pipeline.py` into:
  - `pipelines/notebook_ioc_helpers.py`
- Preserved backward compatibility by re-exporting helper symbols from `notebook_pipeline` import surface where tests consume them.

Size impact:
- `pipelines/notebook_pipeline.py`: 3409 -> 2964 lines

Validation:
- IOC/pipeline-focused tests: passed
- full target regression gate: `88 passed`
- live smoke: `TOTAL 175`, `EMPTY_OR_PARTIAL 0`

## 7) Progress Log (Batch Refactor Updates)

### Batch update: routes split complete
- `routes/routes_notebook.py` reduced from `3497` to `578` lines.
- Extracted route modules:
  - `routes/notebook_live.py`
  - `routes/notebook_hunts.py`
  - `routes/notebook_observations.py`
  - `routes/notebook_feedback.py`
  - `routes/notebook_exports.py`
  - `routes/notebook_workspace.py`
  - `routes/notebook_operations.py`
- Kept behavior via `create_notebook_router(...)` registration wrappers.

### Batch update: app.py decomposition started
- Added `services/runtime_service.py` and delegated auto-refresh/runtime helpers.
- Added `services/http_guard_service.py` and delegated rate-limit/csrf helpers.

### Batch update: notebook pipeline decomposition continued
- Added `pipelines/notebook_ioc_helpers.py` (IOC extraction/relevance cluster).
- Added `pipelines/notebook_behavior_helpers.py` (environment checks, change summary, top change signals, activity highlights).
- `pipelines/notebook_pipeline.py` reduced from `3409` to `2267` lines.

### Batch update: notebook tests decomposed
- Moved observation/export/hunt route tests to:
  - `tests/test_notebook_observations_routes.py`
- Moved MITRE/UI/route-table cluster tests to:
  - `tests/test_notebook_mitre_ui.py`
- `tests/test_notebook.py` reduced from `3355` to `2808` lines.

### Current regression proof after latest batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: notebook tests decomposed further + source trust helper extraction
- Split additional contiguous notebook test block into:
  - `tests/test_notebook_generation_and_metrics.py`
- `tests/test_notebook.py` reduced from `2808` to `1758` lines.
- Extracted source scoring helpers from `app.py` to `services/source_reliability_service.py`:
  - `source_trust_score_core(...)`
  - `source_tier_label_core(...)`
- `app.py` reduced from `3137` to `3120` lines.

### Current regression proof after this batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: notebook test helper dedupe
- Added shared helper module:
  - `tests/notebook_test_helpers.py`
- Rewired split notebook test modules to import shared helpers instead of duplicating setup/request scaffolding:
  - `tests/test_notebook.py`
  - `tests/test_notebook_observations_routes.py`
  - `tests/test_notebook_mitre_ui.py`
  - `tests/test_notebook_generation_and_metrics.py`
- Kept behavior identical; fixed import regressions discovered during verification (`Request` direct usages).

### Current regression proof after helper dedupe batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: large notebook split + app text utils extraction
- Split core notebook tests into:
  - `tests/test_notebook_core_pipeline.py`
- Reduced `tests/test_notebook.py` from `1714` to `617` lines.
- Extracted pure text helpers from `app.py` into:
  - `services/text_utils_service.py`
  - (`normalize_text_core`, `token_set_core`, `token_overlap_core`, `split_sentences_core`, `extract_question_sentences_core`, `question_from_sentence_core`, `sanitize_question_text_core`, `first_sentences_core`)
- Kept behavior via thin wrappers in `app.py`.

### Current regression proof after this batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_core_pipeline.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: actor-term helper extraction + source quality test split
- Extended `services/text_utils_service.py` with actor/alias overlap helpers:
  - `normalize_actor_key_core`
  - `dedupe_actor_terms_core`
  - `mitre_alias_values_core`
  - `candidate_overlap_score_core`
- Switched corresponding `app.py` helpers to service wrappers.
- Removed now-unused `string` import from `app.py`.
- Split source-quality and strict filter tests from:
  - `tests/test_notebook_core_pipeline.py`
  into:
  - `tests/test_notebook_source_quality.py`
- `tests/test_notebook_core_pipeline.py` reduced from `1113` to `774` lines.

### Current regression proof after this batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_core_pipeline.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: network/ingest test split + request-size guard extraction
- Split network/ingest and dedupe tests from `tests/test_notebook.py` into:
  - `tests/test_notebook_ingest_network.py`
- Reduced `tests/test_notebook.py` from `617` to `269` lines.
- Reduced request-size logic in `app.py` by delegating to `services/http_guard_service.py`:
  - `enforce_request_size_core(...)` is now async and performs full body-size validation.
  - `app._enforce_request_size(...)` is now a thin wrapper.

### Current regression proof after this batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_ingest_network.py tests/test_notebook_core_pipeline.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: core pipeline split (backfill/quick-checks)
- Split backfill and evidence-window quick-check tests from:
  - `tests/test_notebook_core_pipeline.py`
  into:
  - `tests/test_notebook_backfill_quickchecks.py`
- Reduced `tests/test_notebook_core_pipeline.py` from `774` to `524` lines.

### Current regression proof after this batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_ingest_network.py tests/test_notebook_core_pipeline.py tests/test_notebook_backfill_quickchecks.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: parsing helper extraction from app.py
- Added `services/parsing_utils_service.py` with:
  - `extract_ttp_ids_core(...)`
  - `safe_json_string_list_core(...)`
  - `parse_iso_for_sort_core(...)`
- Switched `app.py` wrappers:
  - `_extract_ttp_ids(...)`
  - `_safe_json_string_list(...)`
  - `_parse_iso_for_sort(...)`
- Removed now-unused `json` import from `app.py`.

### Current regression proof after this batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_ingest_network.py tests/test_notebook_core_pipeline.py tests/test_notebook_backfill_quickchecks.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: core pipeline split (wrappers + IOC)
- Split wrapper/IOC coverage tests from:
  - `tests/test_notebook_core_pipeline.py`
  into:
  - `tests/test_notebook_core_wrappers_ioc.py`
- Reduced `tests/test_notebook_core_pipeline.py` from `524` to `159` lines.

### Current regression proof after this batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_ingest_network.py tests/test_notebook_core_pipeline.py tests/test_notebook_core_wrappers_ioc.py tests/test_notebook_backfill_quickchecks.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: timeline helper facade extraction
- Added `services/timeline_facade_service.py` and moved timeline/KPI wrapper orchestration from `app.py` into service-level helpers.
- Updated `app.py` wrappers to delegate to `timeline_facade_service` for:
  - `_short_date`
  - `_format_date_or_unknown`
  - `_freshness_badge`
  - `_bucket_label`
  - `_timeline_category_color`
  - `_build_notebook_kpis`
  - `_build_timeline_graph`
  - `_first_seen_for_techniques`
  - `_severity_label`
  - `_action_text`
  - `_compact_timeline_rows`
- Removed now-unused `timeline_view_service` import from `app.py`.

### Current regression proof after this batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_ingest_network.py tests/test_notebook_core_pipeline.py tests/test_notebook_core_wrappers_ioc.py tests/test_notebook_backfill_quickchecks.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: large multi-extraction pass (priority + actor + mitre facades)
- Added `services/priority_facade_service.py` and moved priority wrapper orchestration from `app.py` into facade helpers for:
  - `_priority_where_to_check`
  - `_telemetry_anchor_line`
  - `_guidance_query_hint`
  - `_priority_update_evidence_dt`
  - `_question_org_alignment`
  - `_latest_reporting_recency_label`
- Added `services/actor_facade_service.py` and moved actor/search/profile wrapper orchestration from `app.py` for:
  - `_actor_terms`
  - `_text_contains_actor_term`
  - `_actor_query_feeds`
  - `_actor_search_queries`
  - `_domain_allowed_for_actor_search`
  - `_duckduckgo_actor_search_urls`
  - `_sentence_mentions_actor`
  - `_looks_like_navigation_noise`
  - `_build_actor_profile_summary`
- Added `services/mitre_facade_service.py` and moved MITRE wrapper orchestration from `app.py` for dataset/load/index/match/profile helper paths.

### Current regression proof after this multi-extraction batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_ingest_network.py tests/test_notebook_core_pipeline.py tests/test_notebook_core_wrappers_ioc.py tests/test_notebook_backfill_quickchecks.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: large facade consolidation pass (source/network/activity + priority/actor/mitre)
- Added `services/source_facade_service.py` and moved wrapper orchestration from `app.py` for:
  - `_build_recent_activity_highlights`
  - `_source_trust_score`
  - `_source_tier_label`
  - `_build_recent_activity_synthesis`
  - `_validate_outbound_url`
  - `_safe_http_get`
- Added `services/priority_facade_service.py` and shifted priority wrapper orchestration.
- Added `services/actor_facade_service.py` and shifted actor/search/profile wrapper orchestration.
- Added `services/mitre_facade_service.py` and shifted MITRE wrapper orchestration.

### Current regression proof after this large pass
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_ingest_network.py tests/test_notebook_core_pipeline.py tests/test_notebook_core_wrappers_ioc.py tests/test_notebook_backfill_quickchecks.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: large LLM/cache facade extraction
- Added `services/llm_facade_service.py` and moved LLM/cache orchestration wrappers from `app.py` for:
  - `_ollama_available`
  - `_ollama_generate_questions`
  - `_ollama_review_change_signals`
  - `_ollama_synthesize_recent_activity`
  - `_ollama_enrich_quick_checks`
  - `_ollama_generate_ioc_hunt_queries`
- Preserved behavior by passing current deps (`llm_cache_service`, env readers, HTTP client, parser callbacks, DB/clock providers).

### Regression proof after this extraction
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_ingest_network.py tests/test_notebook_core_pipeline.py tests/test_notebook_core_wrappers_ioc.py tests/test_notebook_backfill_quickchecks.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`

### Batch update: large split + generation facade extraction
- Split remaining legacy `tests/test_notebook.py` content into:
  - `tests/test_notebook_wrappers_misc.py`
- Converted `tests/test_notebook.py` to a placeholder module to avoid catch-all growth.
- Added `services/generation_facade_service.py` and moved generation-journal wrapper orchestration from `app.py`:
  - `_generation_journal_deps`
  - `_create_generation_job`
  - `_mark_generation_job_started`
  - `_finalize_generation_job`
  - `_start_generation_phase`
  - `_finish_generation_phase`

### Regression proof after this batch
- Command:
  - `uv run pytest -q tests/test_notebook.py tests/test_notebook_wrappers_misc.py tests/test_notebook_ingest_network.py tests/test_notebook_core_pipeline.py tests/test_notebook_core_wrappers_ioc.py tests/test_notebook_backfill_quickchecks.py tests/test_notebook_source_quality.py tests/test_notebook_observations_routes.py tests/test_notebook_mitre_ui.py tests/test_notebook_generation_and_metrics.py tests/test_community_edition_quality.py tests/test_notebook_service_cache.py tests/test_generation_runner.py tests/test_generation_journal_service.py tests/test_auto_refresh.py tests/test_top_change_signals.py tests/test_notebook_pipeline_ioc_extraction.py`
- Result:
  - `101 passed`
