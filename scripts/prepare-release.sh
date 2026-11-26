#!/usr/bin/env bash
#
# prepare-release.sh — prepare a release locally.
#
# Rolls CHANGELOG.md (promotes the [Unreleased] section to the new version) and
# bumps charts/littlered/Chart.yaml to match, so that the version embedded in the
# git tag matches Chart.yaml (required by .github/workflows/publish.yml) and the
# generated GitHub release notes pick up a version-specific changelog section.
#
# Usage:
#   scripts/prepare-release.sh <version> [--commit] [--tag] [--allow-empty]
#
#   <version>       Release version, with or without a leading 'v' (e.g. 0.3.0 or v0.3.0).
#   --commit        git add + commit the CHANGELOG.md and Chart.yaml changes.
#   --tag           Create an annotated git tag v<version> (implies --commit).
#   --allow-empty   Proceed even if the [Unreleased] section has no entries.
#
# The script never pushes. After running with --tag, publish the release with:
#   git push origin main --follow-tags
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHANGELOG="${REPO_ROOT}/CHANGELOG.md"
CHART="${REPO_ROOT}/charts/littlered/Chart.yaml"

die() { echo "ERROR: $*" >&2; exit 1; }

VERSION=""
DO_COMMIT=0
DO_TAG=0
ALLOW_EMPTY=0

for arg in "$@"; do
  case "$arg" in
    --commit) DO_COMMIT=1 ;;
    --tag) DO_TAG=1; DO_COMMIT=1 ;;
    --allow-empty) ALLOW_EMPTY=1 ;;
    -h|--help) sed -n '2,21p' "$0"; exit 0 ;;
    -*) die "unknown flag: $arg" ;;
    *)
      [ -z "$VERSION" ] || die "unexpected extra argument: $arg"
      VERSION="$arg"
      ;;
  esac
done

[ -n "$VERSION" ] || die "missing <version> argument (e.g. 0.3.0)"

# Normalise: strip a leading 'v', then validate strict X.Y.Z (with optional -prerelease).
VERSION="${VERSION#v}"
echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$' \
  || die "version '$VERSION' is not valid semver (expected X.Y.Z[-prerelease])"

[ -f "$CHANGELOG" ] || die "$CHANGELOG not found"
[ -f "$CHART" ] || die "$CHART not found"

grep -qE '^## \[Unreleased\]' "$CHANGELOG" || die "no '## [Unreleased]' section in CHANGELOG.md"
if grep -qE "^## \[${VERSION//./\\.}\]" "$CHANGELOG"; then
  die "CHANGELOG.md already has a section for ${VERSION}"
fi

# Body of the [Unreleased] section, used to guard against an empty release.
UNRELEASED_BODY="$(awk '
  index($0, "## [Unreleased]") == 1 { capture = 1; next }
  capture && /^## / { exit }
  capture { print }
' "$CHANGELOG")"
# Test in-shell, not via `printf | grep -q`: under pipefail, grep -q exiting at the
# first match SIGPIPEs printf on a large body, and the failed pipeline read as "empty".
if [[ ! "$UNRELEASED_BODY" =~ [^[:space:]] ]]; then
  if [ "$ALLOW_EMPTY" -eq 0 ]; then
    die "the [Unreleased] section is empty; add entries or pass --allow-empty"
  fi
  echo "WARNING: releasing with an empty [Unreleased] section" >&2
fi

RELEASE_DATE="$(date -u +%Y-%m-%d)"

# Roll the changelog: keep an empty [Unreleased] at the top, insert the new
# version heading immediately below it.
awk -v ver="$VERSION" -v date="$RELEASE_DATE" '
  !done && $0 == "## [Unreleased]" {
    print
    print ""
    print "## [" ver "] - " date
    done = 1
    next
  }
  { print }
' "$CHANGELOG" > "${CHANGELOG}.tmp"
mv "${CHANGELOG}.tmp" "$CHANGELOG"

# Bump the chart version (publish.yml fails the release if it does not match the tag).
awk -v ver="$VERSION" '
  /^version:/ && !done { print "version: " ver; done = 1; next }
  { print }
' "$CHART" > "${CHART}.tmp"
mv "${CHART}.tmp" "$CHART"

echo "Prepared release v${VERSION} (${RELEASE_DATE}):"
echo "  - CHANGELOG.md: promoted [Unreleased] -> [${VERSION}]"
echo "  - charts/littlered/Chart.yaml: version -> ${VERSION}"

if [ "$DO_COMMIT" -eq 1 ]; then
  git -C "$REPO_ROOT" add CHANGELOG.md charts/littlered/Chart.yaml
  git -C "$REPO_ROOT" commit -m "Release v${VERSION}"
  echo "  - committed"
fi

if [ "$DO_TAG" -eq 1 ]; then
  git -C "$REPO_ROOT" tag -a "v${VERSION}" -m "Release v${VERSION}"
  echo "  - tagged v${VERSION}"
fi

echo
echo "Next steps:"
if [ "$DO_COMMIT" -eq 0 ]; then
  echo "  git add CHANGELOG.md charts/littlered/Chart.yaml && git commit -m 'Release v${VERSION}'"
fi
if [ "$DO_TAG" -eq 0 ]; then
  echo "  git tag -a v${VERSION} -m 'Release v${VERSION}'"
fi
echo "  git push origin main --follow-tags   # triggers .github/workflows/publish.yml"
