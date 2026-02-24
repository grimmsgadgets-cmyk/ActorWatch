# Contributing to ActorWatch

Thanks for contributing to ActorWatch Community.

## What Belongs in Community

Community scope includes:
- Core actor notebook and timeline workflows
- Source ingest and IOC handling improvements
- Local-first usability, stability, and security fixes
- Tests, documentation, and developer experience improvements

## Submission Basics

1. Open an issue (or reference an existing one) before large changes.
2. Keep PRs focused and small when possible.
3. Include or update tests for behavior changes.
4. Update docs for new endpoints or user-visible behavior.
5. Keep route handlers thin; put business logic in `services/` or `pipelines/`.

## Local Quality Checks

Run before submitting:

```bash
uv run pytest -q
```

Optional quality checks:

```bash
uv run ruff check app.py services pipelines routes tests
```

## API and Compatibility

- Avoid breaking existing endpoint response shapes unless explicitly coordinated.
- For breaking changes, document migration impact in the PR.

## Security Expectations

- Do not remove existing request-size/rate-limit/CSRF protections on write paths.
- Preserve local-first defaults unless a change is explicitly requested and documented.
- If a change affects trust boundaries, update `SECURITY.md`.
