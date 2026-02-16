# Actor Tracker Roadmap

This roadmap is designed to ship two product lines from one codebase:
- Community (free): strong single-team analyst experience
- Client (paid): collaboration, governance, integrations, and support

## Guiding Principles
- One codebase, no long-lived fork.
- Gate paid capability server-side via entitlements.
- Keep core analyst value in Community.
- Monetize scale, workflow, governance, and reliability.

## Phase 0: Community Release Hardening (Now -> 2 weeks)
Goal: Community beta that is safe and stable to run publicly as self-hosted.

1. Security hardening
- Add baseline security headers (CSP-lite, X-Content-Type-Options, Referrer-Policy).
- Add request size limits for uploads/input endpoints.
- Add rate limiting on public-facing write routes.
- Add dependency and image vulnerability scan in CI.

2. Runtime hardening
- Migrate FastAPI `on_event('startup')` to lifespan handlers.
- Add health/readiness separation (`/healthz`, `/readyz`).
- Add structured app logging with request ID.

3. Data safety
- Document backup/restore for SQLite volume.
- Add startup DB integrity check and clear failure mode.
- Add basic retention guidance for source text and timeline data.

4. Release packaging
- Publish a reproducible self-hosted deploy guide.
- Tag `v0.1.0-community-beta`.

Acceptance criteria
- Offline tests pass.
- Online integration lane passes in network-enabled environment.
- Container starts cleanly with no startup warnings.
- Documented recovery procedure tested once.

## Phase 1: Product Foundation for Tiers (2 -> 4 weeks)
Goal: Introduce clean feature gating and plan model without changing core UX.

1. Entitlement model
- Add `plan_tier` (`community`, `team`, `enterprise`) and `entitlements` table.
- Add server-side helper `has_feature(feature_key, tenant_id|actor_id)`.
- Gate routes and actions at backend first, then UI hints.

2. Tenant/account baseline
- Add org/tenant identity abstraction (even if single-tenant initially).
- Add migration path from current local setup.

3. Observability baseline
- Add usage counters for core actions (refresh, source ingest, question updates).
- Add error budget dashboard basics.

Acceptance criteria
- No feature behavior change for existing Community workflows.
- Paid-only feature toggles are enforceable server-side.

## Phase 2: Team Plan (Paid) (4 -> 8 weeks)
Goal: Features teams pay for first.

1. Collaboration
- Multi-user accounts.
- Role-based access control (admin, analyst, viewer).
- Shared notes and assignment on actor work.

2. Workflow
- Notification hooks (Slack/Teams/webhook).
- Case export (PDF/JSON) for handoff.
- Basic approval flow for analyst actions.

3. Integrations (high-demand first)
- Jira or ServiceNow ticket push.
- One SIEM connector for indicator/query handoff.

Acceptance criteria
- 3 pilot users complete analyst workflow end-to-end.
- Audit trail exists for key actions.

## Phase 3: Enterprise Plan (Paid) (8 -> 14 weeks)
Goal: Governance, compliance, scale.

1. Identity and compliance
- SSO/SAML.
- Detailed audit logs with retention controls.
- Scoped API keys and rotation.

2. Advanced automation
- Playbook automation for repeated analyst checks.
- Scheduled report generation and delivery.
- Priority queues by org policy.

3. Reliability/SLA
- Backup automation + restore drills.
- Defined upgrade process and rollback playbook.
- SLOs + on-call runbooks.

Acceptance criteria
- Tenant isolation validated.
- Compliance controls testable and documented.
- Support/SLA workflow operable.

## Feature Gate Matrix (Initial)
| Capability | Community | Team | Enterprise |
|---|---|---|---|
| Actor notebook + timeline + check-first panel | Yes | Yes | Yes |
| Manual source upload | Yes | Yes | Yes |
| Local Ollama support | Yes | Yes | Yes |
| Multi-user & RBAC | No | Yes | Yes |
| SSO/SAML | No | Optional | Yes |
| Audit logs | No | Basic | Full |
| Ticketing integration | No | Yes | Yes |
| SIEM/EDR integrations | No | Basic | Advanced |
| Automation/playbooks | No | Basic | Advanced |
| SLA support | Community docs | Business hours | Priority SLA |

## Immediate Execution Queue (Next 5 Items)
1. Add lifespan startup handler migration.
2. Add security headers middleware.
3. Add lightweight rate limiter on write endpoints.
4. Add `plan_tier` + `entitlements` schema and helper.
5. Add docs: public self-host deploy + backup/restore.
