# ActorWatch — Community Edition Spec
**Version:** 1.0 Community Edition  
**Purpose:** This document defines the full feature set and UI layout for the ActorWatch Community Edition. It is intended to be handed directly to an AI coding assistant (Claude Code) as a implementation reference.

---

## What ActorWatch Is

ActorWatch is a lightweight, self-hosted threat actor tracking tool and analyst notebook. It is **not** an enterprise CTI platform. It is designed for solo analysts and small teams who want a fast, persistent reference for tracking threat actors over time — capturing IOCs, activity, notes, and visualizations — without the overhead of tools like OpenCTI or MISP.

**Core loop:** Track actors → feed IOCs and activity → annotate with analyst notes → visualize trends over time → export when needed.

---

## Tech Stack Notes

- Preserve whatever stack Codex originally used
- If stack is unknown or needs rebuilding, prefer: **React + Tailwind** frontend, **Node/Express** or **FastAPI** backend, **SQLite** for local persistence
- All data must persist locally — no cloud dependency in community edition
- App should run locally via a simple `npm start` or `python app.py` command

---

## UI Layout

### Overall Structure

```
┌─────────────────────────────────────────────────────────────┐
│ TOPBAR: ActorWatch logo | Actor name (active) | global actions│
├──────────────┬──────────────────────────────────────────────┤
│              │                                              │
│  SIDEBAR     │  MAIN CONTENT AREA                          │
│  Actor list  │  (changes based on active view/tab)         │
│  + search    │                                              │
│  + add       │                                              │
│              │                                              │
└──────────────┴──────────────────────────────────────────────┘
```

### Sidebar (always visible)
- Search input at top
- Scrollable actor list
- Each actor row shows:
  - Actor name
  - Status dot: 🔴 Active / 🟡 Quiet / ⚫ Dormant (analyst-set)
  - Last updated date
  - Unread/new indicator if new data pulled since last visit
- "Add Actor" button at bottom
- Sidebar is collapsible to icon-only mode for more screen space

### Topbar (always visible)
- App name/logo left
- Currently selected actor name center
- Right side: Export button | Settings icon

### Main Content Area — Tab Navigation

Five tabs per actor:

```
[ Overview ] [ Timeline ] [ IOCs ] [ Notebook ] [ Visuals ]
```

---

## Tab Definitions

---

### Tab 1: Overview

The at-a-glance actor profile. Auto-populated where possible, analyst-editable.

**Actor Identity Card**
- Display name (editable)
- All known aliases (comma list, editable) — across MITRE, CrowdStrike, Mandiant, vendor names
- Origin / attributed nation-state (editable)
- Motivation: Espionage / Financial / Hacktivism / Unknown (dropdown)
- Target sectors (tags, editable)
- Target geographies (tags, editable)
- Attribution confidence: High / Medium / Low / Unassessed (dropdown)
- Actor status: Active / Quiet / Dormant (dropdown, drives sidebar dot)
- First observed date
- Last observed date (auto-updated when new activity added)
- MITRE ATT&CK group link (URL field)

**Summary**
- Free-text field: "Who they are in 3-4 sentences" — analyst-written or AI-assisted
- "What changed and why you should care" — analyst-written, updated per review cycle

**Top TTPs (top 5)**
- Simple list: MITRE technique ID + name + brief note
- Editable, add/remove rows

**Associated Malware / Tooling**
- Tag list of known tools, malware families
- Editable

**Quick Defensive Checks**
- Numbered checklist, analyst-editable
- Tied to their TTPs where possible
- Checkboxes are session-persistent (reset on next visit) — these are reference checks, not persistent tasks

**Recent Reporting (3-5 items)**
- Title + source + date + URL
- Auto-pulled if source integration enabled, otherwise manual entry
- "Add report" button

---

### Tab 2: Timeline

Chronological history of everything known about this actor.

**Timeline Feed**
- Events displayed newest-first by default, toggle to oldest-first
- Each event has:
  - Date
  - Event type tag: Campaign / New TTP / New Tool / New IOC Batch / Reporting / Analyst Note / Incident
  - Title / short description
  - Source URL (optional)
  - Analyst annotation field (optional, inline)
  - Flag as significant toggle (starred events surface in Overview)

**Add Event**
- Manual entry form: date, type, title, description, source URL
- Quick-add from IOC tab and Notebook tab (links events together)

**Filter Bar**
- Filter by event type
- Filter by date range
- Search within timeline

**Activity Density Bar**
- Above the timeline: a simple bar chart showing event count per month
- Immediately shows at a glance when the actor was active vs. quiet
- Clicking a bar filters timeline to that month

---

### Tab 3: IOCs

The IOC management table. The core intelligence store.

**IOC Table columns:**
- Type: IP / Domain / Hash (MD5/SHA1/SHA256) / URL / Email / File Name / Registry Key / Other
- Value (the IOC itself)
- First seen date
- Last seen date
- Confidence: High / Medium / Low
- Status: Active / Expired / Revoked
- Source (free text or URL)
- Tags (free text, comma-separated)
- Notes (short inline note)
- Actions: Enrich | Copy | Delete

**Enrichment Links (one-click external lookup)**
Per IOC type, "Enrich" opens relevant tools in new tab:
- IP → VirusTotal, GreyNoise, AbuseIPDB
- Domain → VirusTotal, URLScan.io, ThreatFox
- Hash → VirusTotal, MalwareBazaar, ThreatFox
- URL → URLScan.io, VirusTotal

**Bulk Actions**
- Select multiple IOCs → export selection as CSV or plain text list
- Bulk tag, bulk status change, bulk delete

**Add IOC**
- Single add form (type dropdown + value + fields)
- Bulk paste: paste a list of IOCs, auto-detect type where possible, review before saving

**Import**
- Import from CSV (map columns on import)
- Paste raw list (newline-separated, type auto-detected)

**Export**
- Export all IOCs for this actor: CSV, plain text (one per line), JSON
- Export filtered selection only

**Search / Filter**
- Filter by type, status, confidence, date range, tag
- Search by value

---

### Tab 4: Notebook

The analyst's persistent notes for this actor.

**Note Types (tabbed within notebook):**
- **Capture** — general observations, raw notes, things to follow up
- **Assessment** — analyst judgements, confidence-rated conclusions
- **Since Last Review** — what changed since the analyst last looked at this actor
- **Best Practices** — defensive notes, detection ideas, hunt hypotheses specific to this actor

**Each Note Entry:**
- Timestamp (auto)
- Analyst handle / name (set in settings, pre-filled)
- Confidence level (for Assessment notes): High / Medium / Low / Speculative
- Note body (rich text or markdown)
- Link to timeline event (optional — "this note relates to event X")
- Link to IOC (optional)
- Tags

**Note Actions:**
- Edit, delete, pin to top
- Flag as significant (surfaces in Overview summary)

**Quick Capture**
- Always-visible text input at top of notebook tab for rapid note entry
- One click to save with auto-timestamp

---

### Tab 5: Visuals

Analytics and charts built from the actor's stored data. All generated from local data — no external calls.

**IOC Volume Over Time**
- Bar or line chart: IOC count added per month
- Broken down by IOC type (stacked bars)
- Shows at a glance when the actor was infrastructure-heavy vs. quiet

**Actor Activity Calendar**
- GitHub-style contribution heatmap
- Each cell = one day, color intensity = number of events/IOCs on that day
- Gives immediate visual sense of operational tempo

**IOC Type Breakdown**
- Donut or pie chart: what proportion of their IOCs are IPs vs. domains vs. hashes etc.
- Helps analysts understand actor infrastructure preferences

**TTP Coverage Map**
- Simple MITRE ATT&CK tactic columns (Initial Access, Execution, Persistence, etc.)
- Cells highlight which techniques are attributed to this actor
- Not a full ATT&CK navigator — just a quick reference heatmap
- Click a cell to see which timeline events or notes reference that technique

**Activity vs. Reporting Correlation**
- Line chart: analyst-added events vs. public reporting over time
- Shows whether reporting lags behind observed activity

**Export Visuals**
- Export any chart as PNG
- Include in analyst pack export

---

## Global Features

### Actor Management
- Add actor: name + optional alias, creates empty profile
- Delete actor: requires confirmation, deletes all associated data
- Duplicate actor: clone profile without IOCs/notes (useful for related actors)
- Archive actor: hides from default sidebar view, data preserved

### Export: Analyst Pack
Single button generates a structured export of the full actor record:
- Actor identity card
- Summary
- Top TTPs
- Timeline (filterable: last 30/90/180 days or all)
- IOC table
- Notebook notes (filterable by type)
- Visuals as embedded images
- Output formats: Markdown, PDF (later / Patreon tier)

### Settings
- Analyst name/handle (pre-fills notebook entries)
- Default IOC confidence level
- Theme: Dark (default) / Light
- Sidebar default: expanded / collapsed
- Data location / backup path
- Export path

### Source Integration (basic — auto-pull)
Community edition includes basic auto-pull from free public sources:
- MISP feeds (public)
- ThreatFox (abuse.ch)
- AlienVault OTX (free API key)
- Manual RSS/URL monitor (analyst pastes a blog URL, app checks for updates)

More sources and automated enrichment = Patreon tier.

---

## UI Aesthetic

**Primary theme: Dark**
- Background: deep navy or near-black (#0d1117 range)
- Accent: amber/gold for the HUD feel, or cool blue for clinical mode
- Provide a theme toggle: "Analyst" (amber HUD) vs. "Classic" (clean blue)
- Typography: monospace for IOC values, sans-serif for everything else
- Status dots and confidence tags should use consistent color language:
  - 🔴 Red = Active / High confidence / High severity
  - 🟡 Amber = Quiet / Medium confidence
  - ⚫ Grey = Dormant / Low confidence / Expired
  - 🟢 Green = Confirmed / Revoked (cleared)

**Empty States**
- Every empty panel must have a clear call to action, not just "No data yet"
- Example: empty IOC table → "No IOCs tracked yet. Add one above or import a list."
- Example: empty timeline → "No activity recorded. Add the first event to start tracking."

**Loading States**
- Skeleton loaders for any async data fetch
- Never show a blank panel while loading

---

## What's NOT in Community Edition
(Reserved for Patreon)
- IOC correlation across actors (same IP used by two tracked actors)
- Built-in automated enrichment (API calls to VirusTotal etc. from within the app)
- PDF export of analyst pack
- Multi-user / shared watchlists
- Collaborative notebook
- API access
- Advanced source monitoring / automated refresh scheduling

---

## Build Priority Order

1. Sidebar + actor management (add, list, select, status dot)
2. Overview tab (identity card + TTPs + summary)
3. IOC tab (table, add, filter, enrichment links, export)
4. Timeline tab (feed, add event, activity density bar)
5. Notebook tab (quick capture + note types)
6. Visuals tab (IOC volume chart + activity calendar first)
7. Analyst pack export (markdown first)
8. Settings
9. Source integrations
10. Theme toggle + polish

---

*This spec covers ActorWatch Community Edition v1.0. Patreon tier features are documented separately.*
