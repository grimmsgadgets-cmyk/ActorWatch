#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <version>" >&2
  echo "example: $0 0.2.1" >&2
  exit 1
fi

VERSION="$1"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid version: $VERSION (expected semantic version, e.g. 0.2.1)" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TODAY="$(date +%Y-%m-%d)"

python - <<'PY' "$VERSION"
from pathlib import Path
import re
import sys

version = sys.argv[1]
path = Path("pyproject.toml")
text = path.read_text(encoding="utf-8")
updated, count = re.subn(
    r'^(version\s*=\s*")[^"]+(")$',
    rf'\g<1>{version}\2',
    text,
    flags=re.MULTILINE,
)
if count != 1:
    raise SystemExit("failed to update version in pyproject.toml")
path.write_text(updated, encoding="utf-8")
PY

if ! grep -q "^## ${VERSION} -" docs/CHANGELOG.md; then
  tmp="$(mktemp)"
  {
    awk 'NR==1 {print; print ""; print "## '"$VERSION"' - '"$TODAY"'"; print ""; print "### Added"; print "- TODO"; print ""; print "### Changed"; print "- TODO"; print ""; next} {print}' docs/CHANGELOG.md
  } > "$tmp"
  mv "$tmp" docs/CHANGELOG.md
fi

echo "version bumped to ${VERSION}"
echo "updated: pyproject.toml docs/CHANGELOG.md"
