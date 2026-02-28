# ActorWatch — Product Spec
**Version:** 2026-Q1
**Scope:** Community Edition + Tier 1 (Analyst) + Tier 2 (Team)

This document is the authoritative reference for what each edition includes. It is intended as a handoff document to Claude Code for implementation guidance.

---

## The Non-Negotiable Rule: No Retroactive Locking

**Everything in the repository at the time of initial GitHub release is Community Edition, forever.**

Paid tiers (Tier 1, Tier 2) are built exclusively from net-new features added after the community release. The license gate system does not touch existing code paths. This is both an ethical commitment to early adopters and a practical trust requirement — the CTI community is small and will notice immediately if features are pulled behind a paywall.

If a feature is currently in the codebase and accessible to any GitHub user, it stays in Community Edition. No exceptions.

---

## What ActorWatch Is

ActorWatch is a lightweight, self-hosted threat actor tracking tool and analyst notebook. It is not an enterprise CTI platform. It is designed for solo analysts and small teams who want a fast, persistent reference for tracking threat actors over time — capturing IOCs, activity, analyst notes, and visualizations — without the overhead of tools like OpenCTI or MISP.

**Core loop:** Track actors → ingest sources → annotate with analyst notes → visualize trends → export when needed.

**Deployment model:** Self-hosted via Docker Compose. All data is local. No cloud dependency in any edition. License keys are validated once at startup via a lightweight API call.

---

## Edition Matrix

| Feature | Community | Tier 1: Analyst | Tier 2: Team |
|---|:---:|:---:|:---:|
| **Actor management (add, archive, delete)** | ✅ | ✅ | ✅ |
| **Overview tab** (identity card, TTPs, summary) | ✅ | ✅ | ✅ |
| **Timeline tab** (events, activity density bar) | ✅ | ✅ | ✅ |
| **IOC tab** (table, add, filter, bulk ops) | ✅ | ✅ | ✅ |
| **Notebook tab** (capture, assessment, since-last-review, best practices notes) | ✅ | ✅ | ✅ |
| **Source auto-ingest** (RSS, MITRE, public CTI feeds) | ✅ | ✅ | ✅ |
| **AI-assisted notebook** (priority questions, change signals, LLM synthesis) | ✅ | ✅ | ✅ |
| **Analyst pack export (Markdown)** | ✅ | ✅ | ✅ |
| **One-click IOC enrichment** (VirusTotal, ThreatFox, GreyNoise, AbuseIPDB) | ❌ | ✅ | ✅ |
| **ATT&CK heatmap** (technique coverage visualization) | ❌ | ✅ | ✅ |
| **Actor activity calendar** (GitHub-style heatmap) | ❌ | ✅ | ✅ |
| **IOC volume chart** (stacked bar, by type over time) | ❌ | ✅ | ✅ |
| **PDF analyst pack export** | ❌ | ✅ | ✅ |
| **Multi-user shared watchlists** | ❌ | ❌ | ✅ |
| **Collaborative notebook** (multiple analysts per actor) | ❌ | ❌ | ✅ |
| **IOC correlation across actors** | ❌ | ❌ | ✅ |
| **REST API access** (push/pull IOCs and actor data) | ❌ | ❌ | ✅ |
| **Custom tagging and actor grouping** | ❌ | ❌ | ✅ |

---

## Pricing

| Edition | Price | Channel |
|---|---|---|
| Community | Free | GitHub |
| Tier 1: Analyst | $5–10/month | Lemon Squeezy |
| Tier 2: Team | $25–35/month | Lemon Squeezy |

---

## How Licensing Works

License keys are set via the `ACTORWATCH_LICENSE_KEY` environment variable in `.env` / `docker-compose.yml`. On startup, the app validates the key against the Lemon Squeezy API and stores the resolved tier locally. If validation fails (no key, invalid key, network unreachable), the app falls back to Community Edition behavior — it does not crash.

Key validation is performed:
- Once at app startup
- Once per day in the background (to catch expired or revoked keys without requiring restart)

Feature gates are implemented as a single `feature_enabled(feature_name)` check throughout the codebase. The resolved tier is stored in memory and consulted per-request for gated routes and UI elements.

---

## Community Edition — Feature Detail

### UI Structure

```
┌─────────────────────────────────────────────────────────────┐
│ TOPBAR: ActorWatch logo | Actor name (active) | Export | Settings │
├──────────────┬──────────────────────────────────────────────┤
│  SIDEBAR     │  MAIN CONTENT AREA                          │
│  Actor list  │  [ Overview ] [ Timeline ] [ IOCs ] [ Notebook ] [ Visuals ] │
│  + search    │                                              │
│  + add       │                                              │
└──────────────┴──────────────────────────────────────────────┘
```

### Sidebar
- Search input at top
- Scrollable actor list with: name, status dot (Active/Quiet/Dormant), last updated date, new-data indicator
- "Add Actor" button
- Collapsible to icon-only mode

### Tab 1: Overview
- **Actor identity card**: display name, aliases, origin/nation-state, motivation, target sectors/geographies, attribution confidence, actor status, first/last observed dates, MITRE ATT&CK link
- **Summary**: free-text analyst summary + "what changed" field
- **Top TTPs**: top 5, editable — MITRE technique ID + name + note
- **Associated malware/tooling**: editable tag list
- **Quick defensive checks**: numbered checklist, session-persistent checkboxes
- **Recent reporting**: 3–5 items, title + source + date + URL, manual add

### Tab 2: Timeline
- Events newest-first (toggle to oldest-first)
- Per-event: date, type tag (Campaign / New TTP / New Tool / New IOC Batch / Reporting / Analyst Note / Incident), title, source URL, analyst annotation, "flag as significant" toggle
- Add event form (manual)
- Filter bar: by type, date range, search
- Activity density bar: event count per month above the timeline, click to filter

### Tab 3: IOCs
- Table columns: Type, Value, First seen, Last seen, Confidence, Status, Source, Tags, Notes, Actions
- Actions per row: Copy | Delete | (Enrich — Community shows external links only, no in-app lookup)
- Bulk actions: select multiple → export as CSV or plain text, bulk tag, bulk status change, bulk delete
- Add IOC: single form + bulk paste with auto-detect
- Import: CSV (column mapping) or raw newline-separated list
- Export: CSV, plain text, JSON — all or filtered selection
- Filter: by type, status, confidence, date range, tag, value search

### Tab 4: Notebook
Note types (tabbed within notebook):
- **Capture**: general observations, raw notes, follow-ups
- **Assessment**: analyst judgements, confidence-rated conclusions
- **Since Last Review**: what changed since the analyst last reviewed this actor
- **Best Practices**: detection ideas, hunt hypotheses specific to this actor

Per note: timestamp (auto), analyst handle (from settings), confidence level (Assessment notes only), note body (markdown), optional link to timeline event or IOC, tags. Actions: edit, delete, pin to top, flag as significant.

Quick capture: always-visible input at top of tab for rapid note entry.

### Tab 5: Visuals (Community — limited)
Community edition includes:
- **Activity vs. Reporting correlation**: line chart, analyst-added events vs. public reporting over time

Community does not include: IOC volume chart, activity calendar, ATT&CK heatmap. These surface as locked placeholders with a clear upgrade prompt.

### Settings
- Analyst name/handle
- Default IOC confidence level
- Theme: Dark (default) / Light
- Sidebar default: expanded / collapsed
- Data location / export path

### Export: Analyst Pack (Markdown)
Single-button export generating structured markdown:
- Actor identity card
- Summary
- Top TTPs
- Timeline (filterable: last 30/90/180 days or all)
- IOC table
- Notebook notes (filterable by type)

---

## Tier 1: Analyst — Additional Features

### One-Click IOC Enrichment
"Enrich" button per IOC opens external lookup in a new tab:
- IP → VirusTotal, GreyNoise, AbuseIPDB
- Domain → VirusTotal, URLScan.io, ThreatFox
- Hash → VirusTotal, MalwareBazaar, ThreatFox
- URL → URLScan.io, VirusTotal

These are external link-outs only — no API calls or keys required from the user.

### Visuals Tab (Full)
Unlocks all four Visuals panels:
- **IOC Volume Over Time**: stacked bar chart — IOC count added per month, broken down by type
- **Actor Activity Calendar**: GitHub-style heatmap — each cell = one day, intensity = event/IOC count
- **IOC Type Breakdown**: donut/pie chart of IOC type distribution
- **ATT&CK Coverage Map**: MITRE tactic columns, highlighted techniques attributed to this actor; click a cell to see referencing events and notes

Export any chart as PNG. Charts included in analyst pack export.

### PDF Analyst Pack Export
Full analyst pack export as a formatted PDF in addition to Markdown.

---

## Tier 2: Team — Additional Features

### Multi-User Support
- Multiple named analyst accounts (username + password, local auth)
- Each user has their own handle pre-filled in notebook entries
- Admin account manages user list

### Shared Watchlists
- Actors visible to all team users by default
- Optional private actors (visible only to the creating user)
- Activity feed: see what other analysts have added/changed

### Collaborative Notebook
- All analysts write to the same actor's notebook
- Each entry attributed to the submitting analyst
- Note history: view who wrote what and when

### IOC Correlation Across Actors
- "This IOC is also tracked under [Actor X]" indicator on IOC rows
- Correlation view: search an IOC value across all tracked actors

### REST API
- Authenticated API for pushing/pulling actor and IOC data
- Endpoints: GET /actors, GET /actors/{id}/iocs, POST /actors/{id}/iocs, GET /actors/{id}/timeline
- API key per user, managed from Settings
- Intended for piping IOCs into internal tooling (SIEMs, SOAR, homegrown scripts)

### Custom Tagging and Actor Grouping
- Global tag library (admin-managed)
- Actor groups (e.g., "Chinese APT", "Ransomware", "Active investigations")
- Filter sidebar by group

---

## Build Priority Order

### Phase 1 — Community Edition Foundation
1. Sidebar + actor management (add, list, select, archive, delete, status dot)
2. Overview tab (identity card, TTPs, summary, recent reporting)
3. IOC tab (table, add single/bulk, filter, export, enrichment link-outs)
4. Timeline tab (feed, add event, filter, activity density bar)
5. Notebook tab (4 note types, quick capture, pin, flag)
6. Visuals tab — activity/reporting correlation chart only
7. Analyst pack export (markdown)
8. Settings page
9. Source auto-ingest (RSS, MITRE, public CTI feeds)
10. Empty states and onboarding UX

### Phase 2 — Tier 1 Unlock
11. License key validation system (startup + daily refresh, graceful fallback)
12. Feature gate framework (`feature_enabled(feature_name)`)
13. Locked feature placeholders with upgrade prompt for Community
14. Visuals tab full unlock (IOC volume, activity calendar, ATT&CK heatmap)
15. PDF export
16. Lemon Squeezy integration (webhook to generate key on payment)

### Phase 3 — Tier 2 Unlock
17. Local user account system (username/password, session tokens)
18. Shared watchlists + activity feed
19. Collaborative notebook (multi-author attribution)
20. IOC correlation across actors
21. REST API (API key auth, core endpoints)
22. Custom tagging and actor grouping

---

## UX Principles

- **Empty states must have a call to action.** Never show "No data" without telling the analyst what to do next.
- **Loading states must use skeleton loaders.** Never show a blank panel while data loads.
- **Locked features show a clear upgrade prompt**, not an error. The prompt names the tier required and links to the purchase page.
- **Degraded states are acceptable; broken states are not.** If LLM synthesis is unavailable, show deterministic output. If a license key check fails, fall back to Community — never block the app.
- **Color language is consistent across the app:**
  - Red = Active / High confidence / High severity
  - Amber = Quiet / Medium confidence
  - Grey = Dormant / Low confidence / Expired
  - Green = Confirmed / Revoked (cleared)

---

## What Stays Out of Scope (All Editions)

- Cloud-hosted version (no SaaS infrastructure for any tier)
- Mobile app
- Real-time collaborative editing (websocket sync) — Tier 2 collaboration is read-on-refresh, not live
- Built-in threat intelligence feeds requiring per-user API keys from the user (enrichment is link-outs only)
- Automated enrichment that makes API calls on behalf of the user without explicit action

---

*This spec covers ActorWatch Community, Tier 1 (Analyst), and Tier 2 (Team). Review quarterly as paid tiers develop.*
