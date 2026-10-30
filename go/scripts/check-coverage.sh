#!/usr/bin/env bash
# Fails when the total statement coverage of a Go coverage profile is below the floor.
#
#   scripts/check-coverage.sh [profile] [floor-percent]
#
# profile defaults to coverage.out, the floor to $COVERAGE_FLOOR or DEFAULT_FLOOR below.
# The floor is the measured total (87.2 % when it was set, `go test -race -p 2 -coverprofile`
# over all packages but internal/e2e) minus two points; raise it when coverage rises.
set -euo pipefail

DEFAULT_FLOOR=85.0
profile="${1:-coverage.out}"
floor="${2:-${COVERAGE_FLOOR:-$DEFAULT_FLOOR}}"

if [[ ! -s "$profile" ]]; then
  echo "check-coverage: profile '$profile' is missing or empty" >&2
  exit 2
fi
if ! [[ "$floor" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "check-coverage: floor '$floor' is not a number" >&2
  exit 2
fi

total="$(go tool cover -func="$profile" | awk '/^total:/ { gsub("%", "", $NF); print $NF }')"
if [[ -z "$total" ]]; then
  echo "check-coverage: could not read the total from '$profile'" >&2
  exit 2
fi

echo "total statement coverage: ${total}% (floor ${floor}%)"
if awk -v t="$total" -v f="$floor" 'BEGIN { exit !(t + 0 < f + 0) }'; then
  echo "check-coverage: coverage ${total}% is below the floor of ${floor}%" >&2
  exit 1
fi
