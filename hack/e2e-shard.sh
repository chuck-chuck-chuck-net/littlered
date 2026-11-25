#!/usr/bin/env bash
# Split one e2e mode across several runners by test file (test/e2e/shards.txt).
#
#   hack/e2e-shard.sh count <mode>        number of shards for the mode (1 if unlisted)
#   hack/e2e-shard.sh args <mode> <shard> Ginkgo file filters for that shard
#   hack/e2e-shard.sh matrix [<mode>]     the GitHub Actions matrix, as JSON
#   hack/e2e-shard.sh durations <log>...  spec seconds per test file, from a `make test-e2e` log
#
# The last shard of a mode is the complement of the listed ones, so the shards cover the
# mode exactly once by construction, as long as no two listed files share a name (checked
# here) and every listed file exists (checked here too: a renamed file would otherwise
# silently move to the last shard and unbalance it).
set -euo pipefail
cd "$(dirname "$0")/.."

SHARDS_FILE=test/e2e/shards.txt
MODES="standalone sentinel cluster failover"

die() { echo "e2e-shard: $*" >&2; exit 1; }

# lines <mode>: the "<shard> <files...>" lines for a mode
lines() { awk -v m="$1" '!/^[[:space:]]*(#|$)/ && $1 == m { $1 = ""; sub(/^ /, ""); print }' "$SHARDS_FILE"; }

check() {
  local seen
  seen=$(awk '!/^[[:space:]]*(#|$)/ { for (i = 3; i <= NF; i++) print $1, $i }' "$SHARDS_FILE")
  local dup
  dup=$(sort <<<"$seen" | uniq -d)
  [[ -z "$dup" ]] || die "file listed twice in $SHARDS_FILE: $dup"
  while read -r mode file; do
    [[ -z "$mode" ]] && continue
    [[ " $MODES " == *" $mode "* ]] || die "unknown mode '$mode' in $SHARDS_FILE"
    [[ -f "test/e2e/$file" ]] || die "$SHARDS_FILE lists test/e2e/$file, which does not exist"
  done <<<"$seen"
  # Shards must be numbered 1..N without gaps, so N+1 is the last one.
  for mode in $MODES; do
    local n
    n=$(lines "$mode" | awk '{print $1}' | sort -n | awk '$1 != NR { bad = 1 } END { print bad ? "gap" : NR }')
    [[ "$n" != gap ]] || die "shards of '$mode' in $SHARDS_FILE must be numbered 1, 2, ... without gaps"
  done
}

count() { local n; n=$(lines "$1" | wc -l); echo $((n + 1)); }

# A Ginkgo file filter is a regex over the spec's absolute file path.
filter() { printf '/test/e2e/%s$' "${1//./\\.}"; }

args() {
  local mode=$1 shard=$2 total
  total=$(count "$mode")
  [[ "$shard" =~ ^[0-9]+$ && "$shard" -ge 1 && "$shard" -le "$total" ]] ||
    die "shard must be 1..$total for mode '$mode' (got '$shard')"
  local out=() file
  if [[ "$shard" -lt "$total" ]]; then
    for file in $(lines "$mode" | awk -v s="$shard" '$1 == s { $1 = ""; print }'); do
      out+=("--ginkgo.focus-file=$(filter "$file")")
    done
  else
    for file in $(lines "$mode" | awk '{ $1 = ""; print }'); do
      out+=("--ginkgo.skip-file=$(filter "$file")")
    done
  fi
  echo "${out[*]}"
}

matrix() {
  local modes=${1:-$MODES} mode s n sep=""
  printf '['
  for mode in $modes; do
    n=$(count "$mode")
    for ((s = 1; s <= n; s++)); do
      if [[ "$n" -eq 1 ]]; then
        printf '%s{"mode":"%s","shard":"","name":"%s"}' "$sep" "$mode" "$mode"
      else
        printf '%s{"mode":"%s","shard":"%d","name":"%s %d/%d"}' "$sep" "$mode" "$s" "$mode" "$s" "$n"
      fi
      sep=","
    done
  done
  printf ']\n'
}

# durations: Ginkgo -v prints "• [N seconds]" (or "S [SKIPPED] [N seconds]") per spec,
# followed within a few lines by the spec's file:line.
durations() {
  sed 's/\x1b\[[0-9;]*m//g; s/^[0-9TZ:.-]* //' "$@" | awk '
    /seconds\]$/ && /^(•|S)/ { match($0, /[0-9.]+ seconds/); t = substr($0, RSTART, RLENGTH - 8); want = 12; next }
    want > 0 { want--
      if ($0 ~ /\/test\/e2e\/.*_test\.go:[0-9]+$/) {
        f = $0; sub(/.*\/test\/e2e\//, "", f); sub(/:.*/, "", f); s[f] += t; n[f]++; tot += t; c++; want = 0 } }
    END { for (f in s) printf "%7.0f %3d  %s\n", s[f], n[f], f; printf "%7.0f %3d  total\n", tot, c }' | sort -rn
}

cmd=${1:-}; shift || true
case "$cmd" in
  count) check; count "${1:?mode}" ;;
  args) check; args "${1:?mode}" "${2:?shard}" ;;
  matrix) check; matrix "${1:-}" ;;
  durations) durations "$@" ;;
  *) die "usage: $0 count|args|matrix|durations ..." ;;
esac
