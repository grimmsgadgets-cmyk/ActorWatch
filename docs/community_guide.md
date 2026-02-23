# Community Edition Guide

## Getting Started in 10 Minutes

1. Install Docker Desktop.
2. Clone the repo and open the project folder.
3. Run `docker compose up --build`.
4. Open `http://localhost:8000`.
5. Add an actor in the left sidebar.
6. Click `Refresh actor` and wait for notebook status to become ready.
7. Review:
   - top at-a-glance strip
   - quick checks
   - recent activity section

## What Each Screen Means

- Sidebar:
  - tracked actors list
  - actor onboarding
  - quick actions (refresh, import feeds, export)
  - duplicate merge controls (when duplicate sets are detected)
- Top status:
  - notebook health chip
  - refresh-health panel (feed backoff + confidence totals)
- Main notebook:
  - Section 1: actor baseline profile and techniques
  - Section 2: recent behavior and validated source-backed changes
  - Quick checks: analyst-first actions

## Common Admin Tasks

1. Force refresh one actor:
   - UI: `Refresh actor` button
   - API: `POST /actors/{actor_id}/refresh`

2. Check refresh health:
   - API: `GET /actors/{actor_id}/refresh/stats`
   - UI: top refresh-health panel

3. Merge duplicate actors:
   - UI: merge controls under `Add actor` panel
   - API: `POST /actors/{target_actor_id}/merge` with `source_actor_id`

4. Import sources manually:
   - UI quick action: `Import established CTI RSS sources`

5. Export analyst pack:
   - UI quick action: `Export analyst pack`
   - API endpoint uses same backend payload
