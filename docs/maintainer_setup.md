# Maintainer Setup (Trust Baseline)

Use this once in GitHub settings to protect `main`.

## 1) Branch Protection (main)

Repository Settings -> Branches -> Add rule for `main`:

- Require a pull request before merging: enabled
- Require approvals: `1` minimum
- Require review from Code Owners: enabled
- Dismiss stale pull request approvals when new commits are pushed: enabled
- Require status checks to pass before merging: enabled
- Required checks:
  - `quality` (from `.github/workflows/ci-lite.yml`)
  - `release-check` (from `.github/workflows/release.yml`, for release-tag validation)
- Require branches to be up to date before merging: enabled
- Restrict who can push to matching branches: enabled (maintainers only)
- Allow force pushes: disabled
- Allow deletions: disabled

## 2) CODEOWNERS

`/.github/CODEOWNERS` is present. Keep it updated as collaborators change.

## 3) Dependabot

`/.github/dependabot.yml` enables weekly updates for:

- pip (`requirements.txt`, `pyproject.toml`)
- GitHub Actions workflows

## 4) Release Discipline

Before tagging:

1. Run `scripts/bump_version.sh <x.y.z>`
2. Fill `docs/CHANGELOG.md` entry for that version
3. Push branch and merge PR
4. Tag: `git tag v<x.y.z> && git push origin v<x.y.z>`

The `release` workflow validates version/changelog alignment and runs tests.
