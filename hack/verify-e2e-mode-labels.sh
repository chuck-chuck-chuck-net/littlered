#!/usr/bin/env bash
# Verify the e2e deployment-mode labels PARTITION the suite.
#
# `make test-e2e MODE=<m>` is only trustworthy if every spec carries exactly one mode
# label. An unlabelled spec is invisible to every MODE run — a silently smaller test
# run, which is worse than having no knob at all. A spec labelled twice would run in
# two cuts and inflate them.
#
# Both are caught by one arithmetic check: the per-mode selections must sum to the full
# selection. Ginkgo's --dry-run does the counting, so this needs no cluster.
#
# Run with E2E_ALL=true to include the 'extended' tiers as well.
set -euo pipefail
cd "$(dirname "$0")/.."

MODES="${MODES:-standalone sentinel cluster failover}"
ALL_ARG=""
[[ "${E2E_ALL:-}" == "true" ]] && ALL_ARG="E2E_ALL=true"

count() { # count <make-args...> -> number of selected specs
  make list-e2e "$@" 2>&1 | grep -oE 'Will run [0-9]+' | head -1 | grep -oE '[0-9]+'
}

total=$(count ${ALL_ARG})
sum=0
declare -A per_mode
echo "e2e mode-label partition check${ALL_ARG:+ (including extended)}:"
for m in ${MODES}; do
  n=$(count "MODE=${m}" ${ALL_ARG})
  per_mode[${m}]=${n}
  printf '  %-11s %4d\n' "${m}" "${n}"
  sum=$((sum + n))
done
printf '  %-11s %4d\n' "sum" "${sum}"
printf '  %-11s %4d\n' "suite" "${total}"

if [[ "${sum}" -ne "${total}" ]]; then
  echo
  echo "FAIL: the mode labels do not partition the suite (sum ${sum} != suite ${total})."
  if [[ "${sum}" -lt "${total}" ]]; then
    echo "  $((total - sum)) spec(s) carry NO mode label and would be skipped by every MODE run."
    echo "  Find them by diffing a full listing against the four per-mode listings:"
    echo "    make list-e2e ${ALL_ARG} > /tmp/all.txt"
    echo "    for m in ${MODES}; do make list-e2e MODE=\$m ${ALL_ARG}; done > /tmp/modes.txt"
    echo "  Then label the outermost mode-pure container (see test/e2e/mode_labels_test.go)."
  else
    echo "  Some spec(s) carry MORE THAN ONE mode label and are counted in several cuts."
  fi
  exit 1
fi

echo
echo "OK: every spec carries exactly one deployment-mode label."

# The CI shards of a mode (test/e2e/shards.txt) must add up to the mode. The last shard is
# the complement of the listed ones, so this only fails if a filter matches more or less
# than the file it names, or a listed shard selects nothing.
echo
echo "e2e shard check (test/e2e/shards.txt):"
for m in ${MODES}; do
  shards=$(hack/e2e-shard.sh count "${m}")
  [[ "${shards}" -gt 1 ]] || continue
  ssum=0
  for ((s = 1; s <= shards; s++)); do
    n=$(count "MODE=${m}" "SHARD=${s}" ${ALL_ARG} || echo 0)
    printf '  %-11s %4d\n' "${m} ${s}/${shards}" "${n:-0}"
    if [[ "${n:-0}" -eq 0 ]]; then
      echo "FAIL: ${m} shard ${s} selects no specs."
      exit 1
    fi
    ssum=$((ssum + n))
  done
  if [[ "${ssum}" -ne "${per_mode[${m}]}" ]]; then
    echo "FAIL: the ${m} shards select ${ssum} specs, MODE=${m} selects ${per_mode[${m}]}."
    exit 1
  fi
done
echo "OK: the shards of each mode add up to the mode."
