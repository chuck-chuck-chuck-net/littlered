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
echo "e2e mode-label partition check${ALL_ARG:+ (including extended)}:"
for m in ${MODES}; do
  n=$(count "MODE=${m}" ${ALL_ARG})
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
