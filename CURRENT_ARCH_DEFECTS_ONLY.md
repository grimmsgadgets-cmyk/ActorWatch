# ActorWatch Architecture Defects Only

Date: 2026-02-27
Scope: Only true defects (intentional behaviors removed)

## Not A Defect (explicitly excluded)
- Untracked actor catalog population is intentional.
  - Fresh install behavior: [README.md](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/README.md:337)
  - Auto-refresh scopes to tracked actors: [docs/architecture.md](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/docs/architecture.md:79)
  - Seeded MITRE actors are untracked by design: [tests/test_actor_profile_service.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/tests/test_actor_profile_service.py:174)

---

## D1: Default filter mismatch across layers
- Defect: Dashboard strict defaults (`min_conf=3`) conflict with notebook pipeline strict defaults (`min_conf=2`).
- Evidence:
  - [routes/routes_dashboard.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/routes/routes_dashboard.py:113)
  - [pipelines/notebook_pipeline.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/pipelines/notebook_pipeline.py:3045)
- Impact: Same actor can appear data-rich in one path and data-empty in another.
- Severity: Critical

## D2: Cache-miss behavior can hide available data
- Defect: Core routes fetch with `build_on_cache_miss=False`, returning placeholders instead of assembling latest available notebook.
- Evidence:
  - [routes/routes_dashboard.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/routes/routes_dashboard.py:236)
  - [routes/routes_notebook.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/routes/routes_notebook.py:2759)
- Impact: Analyst sees empty/preparing view despite usable data in other cache keys/snapshots.
- Severity: Critical

## D3: Running-state payloads intentionally empty in live/read paths
- Defect: Route-level running placeholders emit empty core arrays.
- Evidence:
  - [routes/routes_notebook.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/routes/routes_notebook.py:2730)
  - [routes/routes_dashboard.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/routes/routes_dashboard.py:117)
- Impact: Core themes/cards disappear during normal refresh windows.
- Severity: Critical

## D4: Contract enforcement is not a true output boundary
- Defect: Service-level finalization exists, but route-level placeholders bypass it.
- Evidence:
  - Finalizer path: [services/notebook_service.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/services/notebook_service.py:95)
  - Bypass paths: [routes/routes_dashboard.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/routes/routes_dashboard.py:117), [routes/routes_notebook.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/routes/routes_notebook.py:2730)
- Impact: “Never empty” cannot be guaranteed system-wide.
- Severity: Critical

## D5: Stale running jobs can block refresh indefinitely
- Defect: Active-job gate trusts job table `status in ('queued','running')` without stale timeout/expiry.
- Evidence:
  - Active gate: [services/generation_journal_service.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/services/generation_journal_service.py:232)
  - Submit gate behavior: [app.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/app.py:2402)
- Impact: Actor can remain effectively locked in running state.
- Severity: Critical

## D6: Read-path side effects enqueue background jobs
- Defect: `GET /` dashboard path can enqueue `page_load` refresh jobs on cache-miss.
- Evidence:
  - [routes/routes_dashboard.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/routes/routes_dashboard.py:239)
- Impact: Queue churn and surprise long-running operations from normal browsing.
- Severity: High

## D7: Latency architecture not bounded for analyst-facing continuity
- Defect: Heavy generation stages and enrichment retries/timeouts create long windows without stable assembled snapshots.
- Evidence:
  - Generation orchestration: [pipelines/generation_runner.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/pipelines/generation_runner.py:48)
  - LLM timeout logs observed repeatedly (`ollama_change_signal_review_failed`, `ollama_recent_activity_synthesis_failed`).
- Impact: Slow readiness, prolonged running state, inconsistent quality.
- Severity: High

## D8: Ingest rejection funnel can starve actor evidence
- Defect: Relevance gate rejects large volumes (`actor_term_miss`) with high actor variance; no robust fallback scoring path at assembly boundary.
- Evidence:
  - Ingest decision counts in runtime DB showed dominant `actor_term_miss` rejections.
  - Relevance gating logic: [pipelines/feed_ingest.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/pipelines/feed_ingest.py:1070)
- Impact: Sparse evidence for some actors despite available public reporting.
- Severity: High

## D9: Tests enforce degraded running UX
- Defect: Current tests assert empty arrays while running, encoding undesirable behavior as expected.
- Evidence:
  - [tests/test_notebook.py](/home/sammcgree/Codex_Projects/codex-orchestrator/workspaces/actortracker/tests/test_notebook.py:545)
- Impact: CI allows regressions toward empty-running payloads.
- Severity: Medium

---

## Priority Order
1. D1, D2, D3, D4, D5
2. D6, D7, D8
3. D9

