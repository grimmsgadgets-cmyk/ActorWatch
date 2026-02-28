# ActorWatch — License Key System: Implementation Plan

**For:** Claude Code (implementation reference)
**Status:** Not yet built — Community Edition ships first

---

## Overview

ActorWatch uses a license key system to gate Tier 1 and Tier 2 features. The key is set via environment variable, validated against the Lemon Squeezy API at startup, and stored in memory for the lifetime of the process. If validation fails for any reason, the app falls back silently to Community Edition behavior. The app never crashes due to licensing.

This document defines the full technical design for implementation when Phase 2 begins.

---

## Environment Variable

```
ACTORWATCH_LICENSE_KEY=LS-XXXX-XXXX-XXXX-XXXX
```

Set in `.env` or `docker-compose.yml`. If absent or empty, the app runs as Community Edition. No other configuration required from the user.

---

## Tier Resolution

The resolved tier is one of three string values used throughout the app:

| Resolved tier | Condition |
|---|---|
| `"community"` | No key, invalid key, validation failure, network unreachable |
| `"analyst"` | Valid Tier 1 license key |
| `"team"` | Valid Tier 2 license key |

---

## Validation Logic

### Startup (app lifespan)
1. Read `ACTORWATCH_LICENSE_KEY` from env
2. If absent/empty → set tier to `"community"`, skip validation
3. POST to Lemon Squeezy license validation endpoint with the key
4. On success → parse response, extract tier from product/variant metadata, store in memory
5. On any failure (network error, 4xx, 5xx, timeout, unexpected response shape) → log a warning, set tier to `"community"`, continue startup

### Daily background refresh
- A background thread runs once per day and repeats the same validation logic
- If a previously-valid key expires or is revoked, tier degrades to `"community"` on next refresh
- No restart required

### Implementation location
- New service: `services/license_service.py`
- Called from `app_lifespan()` in `app.py` (startup only — daily refresh started as daemon thread from there)
- Resolved tier stored as a module-level variable in `license_service.py`, accessed via `get_resolved_tier()`

---

## Feature Gate API

A single function used at every gate point:

```python
# services/license_service.py
def feature_enabled(feature: str) -> bool:
    ...
```

Feature names are string constants defined in `license_service.py`:

```python
FEATURE_IOC_ENRICHMENT = 'ioc_enrichment'
FEATURE_ATT&CK_HEATMAP = 'attck_heatmap'
FEATURE_ACTIVITY_CALENDAR = 'activity_calendar'
FEATURE_IOC_VOLUME_CHART = 'ioc_volume_chart'
FEATURE_PDF_EXPORT = 'pdf_export'
FEATURE_MULTI_USER = 'multi_user'
FEATURE_COLLABORATIVE_NOTEBOOK = 'collaborative_notebook'
FEATURE_IOC_CORRELATION = 'ioc_correlation'
FEATURE_API_ACCESS = 'api_access'
FEATURE_CUSTOM_GROUPING = 'custom_grouping'
```

Feature → minimum tier mapping is a static dict in `license_service.py`. Adding a new gated feature requires only adding an entry to this dict.

---

## Gate Points

Feature gates are applied at two layers:

**Route layer (API responses):** Gated routes return HTTP 402 with a JSON body `{"error": "feature_not_available", "feature": "<name>", "required_tier": "<tier>"}` if the feature is not enabled for the current license.

**Template layer (UI):** Locked panels render a placeholder card instead of the feature. Placeholder shows:
- What the feature does (one sentence)
- Which tier unlocks it
- A link to the purchase page (`ACTORWATCH_UPGRADE_URL` env var, defaults to empty)

---

## Lemon Squeezy Integration

Lemon Squeezy handles:
- Payment processing (card, PayPal)
- VAT/tax compliance globally
- License key generation on successful purchase
- Email delivery of the license key to the buyer
- Key activation/deactivation/revocation from the dashboard

No Lemon Squeezy code lives in the app except the validation HTTP call. The app is not the billing system — it only validates.

### Webhook (server-side, separate from the app)
A tiny webhook receiver (separate from ActorWatch itself, can be a Lemon Squeezy "webhook URL") handles:
- `order_created` → activate license key for the correct tier
- `subscription_cancelled` → revoke key (LS handles this automatically)

For initial launch, Lemon Squeezy's built-in license key management handles activation/revocation without a custom webhook. The webhook becomes necessary only if custom provisioning logic is needed.

---

## Graceful Degradation Rules

1. **No key present** → Community Edition, no UI indication that a license system exists (clean UX for Community users)
2. **Key present but validation fails** → Community Edition + a one-time admin warning in the app log. Do not surface to end users.
3. **Key valid but tier unknown** → Community Edition (defensive default)
4. **Network unreachable at startup** → Community Edition until next successful validation (daily retry)
5. **Key expires mid-session** → Degradation happens on next daily refresh, not mid-request

---

## Files to Create / Modify (Phase 2)

| File | Action | Purpose |
|---|---|---|
| `services/license_service.py` | Create | Tier resolution, validation, `feature_enabled()` |
| `app.py` | Modify | Call license validation in lifespan, start daily refresh thread |
| `services/app_wiring_service.py` | Modify | Thread `feature_enabled` into deps maps where needed |
| `routes/routes_*.py` / `routes/notebook_*.py` | Modify | Add `feature_enabled()` gate at gated route handlers |
| `templates/index.html` | Modify | Add locked-feature placeholder component |
| `static/index.js` | Modify | Handle 402 responses, render upgrade prompt |
| `.env.example` | Modify | Add `ACTORWATCH_LICENSE_KEY` and `ACTORWATCH_UPGRADE_URL` |

---

## Testing

- `tests/test_license_service.py` — unit tests for tier resolution logic with mocked HTTP responses
- Each gated route test should include a case for: Community (402 expected), Tier 1 (200 expected where applicable), Tier 2 (200 expected)
- Graceful fallback must be tested: simulate network failure at validation, confirm tier is `"community"` and app starts normally

---

## Do Not Build Yet

This entire system is Phase 2. **Do not implement any part of this until Community Edition has active users.** The licensing system adds complexity and maintenance surface. It is only worth that cost once there is traction to convert.

Community Edition ships first, gets real users, then Phase 2 begins.
