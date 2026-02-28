# ActorWatch Full RCA (System-Level)

Date: 2026-02-27
Scope: End-to-end architecture (ingest, filtering, generation, cache, state, UI, ops)

## Executive Summary
The product is failing for analysts due to a chain of architectural faults, not one bug:

1. Most actors never ingest data because they are untracked.
2. When data exists, dashboard defaults can force cache-miss/zero-source views.
3. UI/state paths intentionally return empty arrays during running/lock conditions.
4. Job lifecycle has no stale-running timeout in journal state, creating long-lived lockout.
5. LLM enrichment times out repeatedly; slow path degrades without strong deterministic substitutes.
6. Ingest relevance/filtering rejects too aggressively for some actors.

Result: analysts frequently see empty or weak cards, long waits, and inconsistent behavior between screens.

---

## RCA-1: Catalog-vs-Tracked Semantics Gap (Not a defect in tracking model itself)

### Evidence
- Runtime DB snapshot:
  - `actors_total=173`
  - `actors_tracked=3`
  - `actors_untracked=170`
  - `actors_with_zero_sources=170`
  - `sources_total=33`

### Root Cause
The platform intentionally has a two-state actor model:
- Catalog actors (untracked) are expected to remain idle.
- Tracked actors are the ones that ingest/refresh.

This is by design and documented/tested:
- Fresh install starts with no tracked actors (`README.md:337`).
- Auto-refresh picks tracked actors (`docs/architecture.md:79`).
- MITRE seed adds untracked catalog entries (`tests/test_actor_profile_service.py:174-209`).

The fault is not the model. The fault is that downstream notebook/card UX can look like a data failure rather than clearly expressing \"this actor is catalog-only and not currently tracked.\"

### Impact
Analysts can interpret expected idle catalog behavior as ingestion failure when status semantics are not explicit enough in card-level output.

### Severity
Medium

---

## RCA-2: Filter and Cache-Key Mismatch (Dashboard can show zero-data by design)

### Evidence
For actor `APT29`:
- Source distribution: 24 sources, all confidence weight `2`.
- Function check:
  - default fetch (`min_conf=2`, `90d`) -> `applied_sources=24`
  - dashboard-like fetch (`min_conf=3`, `90d`, cache-only path) -> `cache_miss=true`, zero-source payload

### Root Cause
Two defaults conflict:
- Dashboard strict default sets `min_confidence_weight=3` and `source_days=90`.
  - Code: `routes/routes_dashboard.py:113-116`
- Notebook pipeline strict default is `min_confidence_weight=2`, `source_days=90`.
  - Code: `pipelines/notebook_pipeline.py:3045-3047`
- Dashboard and live routes fetch with `build_on_cache_miss=False`.
  - Code: `routes/routes_dashboard.py:236-238`
  - Code: `routes/routes_notebook.py:2759-2760`

Because cache key includes filters, if key-specific cache is missing, route can serve placeholder/empty state even though usable data exists under another key.

### Impact
Analyst sees “no data/preparing/running” states despite available source corpus.

### Severity
Critical

---

## RCA-3: Empty-Payload State Model (Intentional empty arrays in core UX path)

### Evidence
When actor is `running`, live endpoint returns empty arrays for core themes:
- `priority_questions: []`
- `top_change_signals: []`
- `recent_activity_synthesis: []`
- Code: `routes/routes_notebook.py:2730-2751`

Dashboard running/idle placeholders also contain empty arrays by design:
- Code: `routes/routes_dashboard.py:117-168`, `170-210`

Tests currently enforce this behavior:
- `tests/test_notebook.py:545-547` expects empty `kpis` and empty `priority_questions` while running.

### Root Cause
State model treats “running” as “blank output allowed,” instead of serving last valid snapshot.

### Impact
Analysts see empty cards during normal refresh windows; trust degrades.

### Severity
Critical

---

## RCA-4: Stale Job Lockout (Running job can persist indefinitely)

### Evidence
Actor `Mustang Panda` has a `running` generation job since `2026-02-27T12:01:35Z` with no `finished_at`.
- `notebook_generation_jobs.status='running'`, `finished_at=NULL`

New refresh requests detect existing active job and refuse enqueue, while repeatedly setting actor status to running:
- Code: `app.py:2403-2419`

`set_actor_notebook_status` always updates `notebook_updated_at`:
- Code: `services/actor_profile_service.py:24-29`

`active_generation_job_for_actor_core` has no stale timeout/TTL logic:
- Code: `services/generation_journal_service.py:232-274`

### Root Cause
Journal “running” state has no expiry/recovery at job table level; active-job gate can block forever.

### Impact
Actor can be stuck in perpetual running state, preventing fresh processing.

### Severity
Critical

---

## RCA-5: Latency Architecture (Synchronous heavy path + repeated long operations)

### Evidence
Recent job durations include:
- ~26s (APT29 auto-refresh)
- ~31s (qilin page load)
- ~114-117s (Mustang Panda repeated page-load jobs)

Generation path performs:
- source import
- timeline build
- question build
in the same request-triggered pipeline worker
- Code: `pipelines/generation_runner.py:48-107`

LLM calls frequently timeout:
- Logs show repeated `ollama_change_signal_review_failed ... timed out`
- Logs show repeated `ollama_recent_activity_synthesis_failed ... timed out`

### Root Cause
Heavy multi-stage processing and LLM operations are not bounded to protect analyst-facing freshness consistently.

### Impact
Slow notebook readiness, repeated running windows, inconsistent enrichment quality.

### Severity
High

---

## RCA-6: Relevance/Funnel Over-Rejection for some actors

### Evidence
`ingest_decisions` shows large reject volume:
- `actor_term_miss` rejected: 196

For `qilin`, feed failures and rejections are substantial relative to accepted sources.

### Root Cause
Current actor-term relevance funnel is strict enough to drop many potentially useful context leads; behavior varies strongly by actor naming/alias quality.

### Impact
Sparse evidence base for certain actors -> weak theme generation.

### Severity
High

---

## RCA-7: UX-Architecture Mismatch (Server-render placeholders vs live updates)

### Evidence
Dashboard root may render idle/running placeholder payloads first, while live endpoint may later show different state/filters.
- Placeholder logic: `routes/routes_dashboard.py:117-210`
- Live endpoint logic separate: `routes/routes_notebook.py:2620+`

### Root Cause
Two different assembly paths produce different semantics and defaults.

### Impact
Analysts observe inconsistent card behavior between initial load and live refresh.

### Severity
High

---

## RCA-8: Contract Fixes were applied at service layer, but route layer bypasses them

### Evidence
`finalize_notebook_contract_core` exists and is applied in notebook service.
- Code: `services/notebook_service.py:95+`, `515-571`

But route placeholders return handcrafted empty payloads directly, bypassing finalized contract output.
- Code: `routes/routes_dashboard.py:117-210`
- Code: `routes/routes_notebook.py:2730-2751`, `2766-2786`

### Root Cause
Contract enforcement point is not the single output boundary for all UI responses.

### Impact
“Guaranteed non-empty by contract” cannot hold system-wide.

### Severity
Critical

---

## RCA-9: Test strategy reinforces degraded UX behavior

### Evidence
Tests validate empty payloads in running state as correct.
- `tests/test_notebook.py:545-547`

No integration test currently enforces “last valid snapshot shown while running” for all core themes.

### Root Cause
Acceptance criteria in tests are aligned to placeholder behavior, not analyst usability guarantee.

### Impact
Regressions toward empty-state UX are not prevented by CI.

### Severity
Medium

---

## RCA-10: Read-path side effects (`GET /` can trigger refresh jobs)

### Evidence
Dashboard render path can enqueue a refresh job on cache miss:
- `routes/routes_dashboard.py:239-243` (`trigger_type='page_load'`)

Job history confirms repeated `page_load` jobs with long runtimes.

### Root Cause
A read operation (dashboard load) has write/queue side effects under cache-miss conditions.

### Impact
Unpredictable background work from normal browsing, queue churn, and higher chance of overlapping/stale job states.

### Severity
High

---

## Consolidated Failure Chain (Why analysts experience dead-in-the-water behavior)

1. Actor exists in catalog-only state (expected) but UX often presents this similar to failure states.
2. Tracked actor enters refresh -> running state may return empty arrays.
3. Dashboard filter/cache combination can force cache_miss placeholder.
4. Long/failed jobs and stale running journal states block forward progress.
5. LLM timeouts reduce synthesis quality.
6. Strict relevance/failure patterns can starve evidence.

Net effect: cards can appear empty, generic, delayed, or inconsistent even when some data is available.

---

## Architectural Corrections Required (not quick fixes)

1. Single output assembler boundary:
- Every UI/API card payload must pass through one final assembler (no route-level empty placeholders).

2. Snapshot-first state model:
- During `running`, serve last complete snapshot + status delta, never empty core themes.

3. Unified default filter policy:
- One global default for confidence/days; dashboard/live/pipeline must match.

4. Cache semantics redesign:
- Cache miss for one filter key must gracefully fall back to nearest valid snapshot before placeholder.

5. Job lifecycle hardening:
- Stale running job TTL + auto-finalize to `error` + requeue allowance.

6. Deterministic evidence tiering:
- LLM optional; deterministic path must always produce usable evidence-tiered cards.

7. Tracking model fix:
- Either auto-track all seeded actors or clearly partition “library actor” vs “tracked actor” UX.

8. CI acceptance criteria update:
- Replace tests that assert empty running payload with tests asserting non-empty snapshot continuity.
