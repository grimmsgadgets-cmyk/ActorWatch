# ActorWatch — Architecture Addendum
## ThreatSpire Integration Requirements

**Context:** ActorWatch will exist in two forms:
1. Standalone community edition (current build)
2. A paid embedded module inside ThreatSpire (future)

This addendum must be applied to the current build now to avoid a full rewrite later. These are architectural constraints, not feature changes.

---

## Core Requirement: Build as a Self-Contained Module

ActorWatch must be structured as a self-contained module with clean boundaries — not a monolithic standalone app. Every major system (actors, IOCs, timeline, notebook, visuals, export) should be its own encapsulated unit that ThreatSpire can import and mount independently.

---

## Specific Architectural Requirements

### 1. API-First Design
- All data operations must go through an internal API layer, never direct DB calls from the UI
- Every feature must be accessible via API endpoint, not just via the UI
- This allows ThreatSpire to call ActorWatch functions programmatically
- REST is fine, keep it simple

### 2. Auth Must Be Pluggable
- Community edition: no auth required (single user, local)
- Do NOT hardcode any auth assumption into the core module
- Auth should be an optional middleware layer that wraps the API
- When embedded in ThreatSpire, ThreatSpire's own auth system will handle identity
- Design for: `if (authEnabled) { checkAuth() } else { passthrough() }`

### 3. Data Models Must Be Portable
- All data models (Actor, IOC, TimelineEvent, Note) must be defined in a single shared schema file
- No data logic spread across components
- Export/import must work via JSON at minimum
- Where possible, align IOC and actor data models with **STIX 2.1** field naming conventions — this ensures ThreatSpire can speak to other CTI tools natively
  - Actor → STIX `threat-actor` object
  - IOC → STIX `indicator` object  
  - Timeline event → STIX `sighting` or `report` object
  - Don't need full STIX compliance now, just compatible field names

### 4. No Hardcoded App Assumptions
- App title, branding, color theme must come from a config file, not be hardcoded
- ThreatSpire will rebrand the module with its own design system
- Config file should expose: `appName`, `theme`, `primaryColor`, `logoPath`, `authEnabled`, `baseRoute`

### 5. Routing Must Support a Base Path
- Community edition runs at `/`
- When embedded in ThreatSpire it will run at something like `/modules/actorwatch/`
- All routes must support a configurable `BASE_PATH` prefix
- No hardcoded absolute paths anywhere

### 6. Database Must Be Swappable
- Community edition: SQLite is fine
- ThreatSpire will use its own database (likely PostgreSQL)
- Abstract all DB calls behind a data access layer / repository pattern
- The module should not care what database it's talking to

### 7. Events / Hooks System
- ActorWatch should emit internal events for key actions:
  - `actor.created`, `actor.updated`, `actor.deleted`
  - `ioc.added`, `ioc.bulk_imported`
  - `timeline.event_added`
  - `note.created`
- In standalone mode these events do nothing
- In ThreatSpire these events plug into the platform's notification and workflow system

---

## File Structure Recommendation

```
actorwatch/
├── config.js              # All configurable values
├── api/                   # All API route handlers
│   ├── actors.js
│   ├── iocs.js
│   ├── timeline.js
│   ├── notebook.js
│   └── export.js
├── db/                    # Data access layer (swappable)
│   ├── schema.js          # Shared data models
│   ├── adapters/
│   │   ├── sqlite.js      # Default
│   │   └── postgres.js    # For ThreatSpire
├── services/              # Business logic (no DB or HTTP dependencies)
│   ├── actorService.js
│   ├── iocService.js
│   └── exportService.js
├── events.js              # Internal event emitter
├── middleware/
│   └── auth.js            # Pluggable auth wrapper
└── ui/                    # Frontend (self-contained)
    └── src/
```

---

## What This Means for the Current Build

- Refactor any direct DB calls in UI components to go through the API layer
- Move all data models into `db/schema.js`
- Pull all hardcoded strings (app name, colors, paths) into `config.js`
- Add the event emitter now even if nothing listens to it yet
- Make sure all routes work with a `BASE_PATH` prefix

These changes do not affect any visible features or UI. They are structural only.

---

## ThreatSpire Integration Notes (future reference)

When ActorWatch is embedded in ThreatSpire:
- ThreatSpire mounts ActorWatch at `/modules/actorwatch/`
- ThreatSpire passes its own auth context via middleware
- ThreatSpire swaps the SQLite adapter for its PostgreSQL adapter
- ThreatSpire subscribes to ActorWatch events for notifications and cross-module workflows
- ThreatSpire overrides `config.js` branding values
- Patreon-gated features become ThreatSpire subscription-gated features

---

*This addendum supplements the ActorWatch Community Edition Spec v1.0. Apply these architectural requirements to the current build before adding new features.*
