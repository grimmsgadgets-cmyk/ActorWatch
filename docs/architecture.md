# ThreatCompass Architecture Map

## Goal

Provide an at-a-glance actor notebook for junior analysts with strong defaults:

- high-signal source intake
- freshness-aware change summaries
- low-friction refresh and review workflows

## Runtime Layout

- `app.py`
  - application composition
  - dependency wiring
  - environment configuration
  - lifecycle hooks (startup + auto-refresh loop)
- `routes/`
  - HTTP route handlers only
  - no heavy business logic
- `services/`
  - domain services used by routes/app wiring
  - DB-adjacent operations and orchestration helpers
- `pipelines/`
  - heavier data processing and extraction logic
  - notebook synthesis and feed ingest pipeline internals

## Main Data Flows

1. Actor onboarding
- route: `POST /actors` or UI add form
- service: `create_actor_profile_core`
- behavior:
  - canonical-name dedupe on create
  - duplicate create returns existing actor
  - optional auto-track kickoff

2. Source refresh
- route: `POST /actors/{id}/refresh` or auto-refresh loop
- pipeline: `import_default_feeds_for_actor_core`
- behavior:
  - prioritized feed ordering
  - actor-feed health tracking (`actor_feed_state`)
  - incremental checkpointing by latest successful published timestamp
  - timeout/deadline-aware fetch budget
  - secondary-context intake cap to protect signal density

3. Notebook generation
- pipeline: `run_actor_generation_core` + notebook builders
- behavior:
  - source collection
  - timeline rebuild
  - question and guidance generation
  - status/progress fields persisted to actor profile

## Key Tables

- `actor_profiles`
  - actor identity, tracking state, refresh status metadata
  - includes `canonical_name`
- `sources`
  - actor-linked source corpus and source-quality metadata
- `actor_feed_state`
  - per-actor per-feed health, checkpointing, failures/backoff
- `timeline_events`, `question_threads`, `question_updates`
  - analysis outputs
- `analyst_observations`, `analyst_observation_history`
  - human-in-the-loop notes and audit trail

## Duplicate Handling

- Create-time dedupe by canonical actor name
- Merge API: `POST /actors/{target_actor_id}/merge`
- Startup auto-merge for legacy duplicates (`AUTO_MERGE_DUPLICATE_ACTORS=1`)

## Auto Refresh

- Background loop started in app lifespan
- Picks tracked actors not recently refreshed
- Enqueues small batches to avoid contention
- Tuned by:
  - `AUTO_REFRESH_ENABLED`
  - `AUTO_REFRESH_MIN_INTERVAL_HOURS`
  - `AUTO_REFRESH_LOOP_SECONDS`
  - `AUTO_REFRESH_BATCH_SIZE`

## Contributor Guidance

- Add route behavior in `routes/*` only when request-shaping is needed.
- Keep business rules in `services/*`.
- Put compute-heavy extraction/synthesis in `pipelines/*`.
- Add tests in `tests/*` for:
  - dedupe semantics
  - feed ingest behavior (quality/freshness/backoff/checkpoints)
  - notebook outputs and endpoint contracts
