#!/usr/bin/env bash
#
# Copyright 2026 The littlered Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# The work-list driver: one work item per Indexed-Job index
# (docs/E2E_SOAK_HARNESS_DESIGN.md §4.2, §9.1, §11 M1).
#
# THE CARDINAL RULE: this script ALWAYS exits 0. A work item that fails must not
# count against the Job's backoffLimit and stop the array — the whole point is
# that item 3 failing still lets items 4 and 5 run. The verdict is RECORDED
# (a result ConfigMap plus a result.json artifact), never propagated.
#
# Every decision — which item this index is, what its timeout is, whether the
# outcome is a pass/fail/timeout, whether the namespace gets parked — is
# delegated to `lrharness`, the CLI over the pure seam in test/harness/worklist.
# This script is deliberately only plumbing.

set -uo pipefail   # NOT -e: a failing work item is an expected outcome here.

log() { printf '[harness %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

# --- always exit 0, whatever happens below ----------------------------------
finish() {
    log "driver finished for index ${JOB_COMPLETION_INDEX:-?} (always exit 0)"
    exit 0
}
trap finish EXIT

# publish_result <idx> <name> <ns> <rc> <timed_out> <started> <finished> <artdir>
#
# Writes the item's result record, publishes it as a labelled ConfigMap for the
# rollup to collect, and then parks or cleans the item's namespace. The
# park/clean decision follows the `parked` field of the record, so the recorded
# verdict and the actual cleanup can never disagree.
publish_result() {
    local idx="$1" name="$2" ns="$3" rc="$4" timed_out="$5"
    local started="$6" finished="$7" artdir="$8"

    local timeout_flag=""
    [ "$timed_out" = "true" ] && timeout_flag="--timed-out"

    # shellcheck disable=SC2086  # timeout_flag is a deliberate optional flag
    lrharness record \
        --array "$LRH_ARRAY" --index "$idx" --name "$name" --namespace "$ns" \
        --exit "$rc" $timeout_flag \
        --started-epoch "$started" --finished-epoch "$finished" \
        --artifact-dir "$artdir" > "${artdir}/result.json"

    local verdict parked
    verdict="$(sed -n 's/.*"verdict":"\([A-Z]*\)".*/\1/p' "${artdir}/result.json")"
    parked="$(sed -n 's/.*"parked":\(true\|false\).*/\1/p' "${artdir}/result.json")"
    log "verdict=${verdict} parked=${parked}"

    # Publish for the rollup. One ConfigMap PER ITEM, never one shared map that
    # every item patches: at parallelism > 1 (design §9.2.2) concurrent patches
    # of one object would conflict and lose results.
    local cm="lrh-${LRH_ARRAY}-$(printf '%03d' "$idx")"
    kubectl -n "$LRH_HARNESS_NAMESPACE" create configmap "$cm" \
        --from-file=result.json="${artdir}/result.json" \
        --dry-run=client -o yaml 2>>"${artdir}/driver.log" \
      | kubectl label --local -o yaml -f - \
            "lr-e2e-harness/array=${LRH_ARRAY}" \
            "lr-e2e-harness/index=${idx}" \
            "lr-e2e-harness/kind=result" 2>>"${artdir}/driver.log" \
      | kubectl apply -f - >>"${artdir}/driver.log" 2>&1 \
      || log "WARNING: could not publish result ConfigMap ${cm} (see driver.log)"

    [ -z "$ns" ] && return 0

    # Fail SAFE, not open: delete only on an explicit "parked":false. If the
    # record could not be written or parsed, `parked` is empty and we must park
    # — an unknown outcome is a reason to keep the evidence, never to destroy it.
    if [ "$parked" != "false" ]; then
        if [ "$parked" != "true" ]; then
            log "WARNING: no verdict could be read from the result record — parking defensively"
        fi
        # Failure handling is "do nothing" (design §4.2): leave the namespace
        # and everything the operator built in it standing for inspection.
        log "PARKING namespace ${ns} for forensics"
        kubectl label ns "$ns" --overwrite \
            "lr-e2e-harness/parked=true" \
            "lr-e2e-harness/array=${LRH_ARRAY}" \
            "lr-e2e-harness/index=${idx}" >>"${artdir}/driver.log" 2>&1
        kubectl annotate ns "$ns" --overwrite \
            "lr-e2e-harness/item=${name}" \
            "lr-e2e-harness/verdict=${verdict}" \
            "lr-e2e-harness/artifacts=${artdir}" >>"${artdir}/driver.log" 2>&1
    else
        # The suite's own AfterSuite already deletes the namespace on success;
        # this is the idempotent belt-and-braces for a crash before AfterSuite.
        log "cleaning namespace ${ns}"
        kubectl delete ns "$ns" --ignore-not-found --wait=false >>"${artdir}/driver.log" 2>&1
    fi
}

: "${LRH_ARRAY:?LRH_ARRAY must be set}"
: "${LRH_WORKLIST:=/worklist/worklist.yaml}"
: "${LRH_ARTIFACTS_ROOT:=/artifacts}"
: "${LRH_REPO:=/workspace/littlered}"
: "${LRH_HARNESS_NAMESPACE:=lr-harness-system}"
IDX="${JOB_COMPLETION_INDEX:?JOB_COMPLETION_INDEX must be set (this must run as an Indexed Job)}"

STARTED_EPOCH="$(date +%s)"
ART_BASE="${LRH_ARTIFACTS_ROOT}/${LRH_ARRAY}"
mkdir -p "$ART_BASE"

# ---------------------------------------------------------------------------
# 1. Resolve this index to one concrete work item.
# ---------------------------------------------------------------------------
if ! PLAN="$(lrharness plan --worklist "$LRH_WORKLIST" --index "$IDX" 2>&1)"; then
    # An unresolvable index is a work-list/Job mismatch, not a test failure.
    # Record it so the rollup shows the gap rather than a silent hole.
    log "FATAL: could not resolve index ${IDX}: ${PLAN}"
    ARTDIR="${ART_BASE}/$(printf '%03d' "$IDX")-unresolved"
    mkdir -p "$ARTDIR"
    printf '%s\n' "$PLAN" > "${ARTDIR}/plan-error.log"
    publish_result "$IDX" "unresolved" "" 1 false "$STARTED_EPOCH" "$(date +%s)" "$ARTDIR"
    exit 0
fi

eval "$PLAN"
log "index ${IDX} -> item '${LRH_ITEM_NAME}' (ns=${LRH_NAMESPACE}, timeout=${LRH_TIMEOUT_SECONDS}s)"

ARTDIR="${ART_BASE}/$(printf '%03d' "$IDX")-${LRH_ITEM_NAME}"
mkdir -p "$ARTDIR"

# ---------------------------------------------------------------------------
# 2. Clean slate. The suite refuses to start if its namespace already exists,
#    so a leftover from an interrupted run of the SAME array is removed. Parked
#    namespaces of OTHER arrays are untouched — the array name is the
#    uniqueness knob (see test/harness/README.md).
# ---------------------------------------------------------------------------
if kubectl get ns "$LRH_NAMESPACE" >/dev/null 2>&1; then
    log "WARNING: ${LRH_NAMESPACE} already exists (leftover from an earlier run of array '${LRH_ARRAY}') — deleting"
    kubectl delete ns "$LRH_NAMESPACE" --ignore-not-found --timeout=5m >>"${ARTDIR}/driver.log" 2>&1
fi

# ---------------------------------------------------------------------------
# 3. Run the work item, bounded by its own deadline.
#
#    SKIP_KIND_SETUP=true      — run against this pre-existing cluster.
#    SKIP_OPERATOR_DEPLOY=true — M1 runs against a pre-deployed, cluster-scoped
#                                operator. Per-item operators are M2.
#    DEBUG_ON_FAILURE=true     — makes the suite collect artifacts AND skip its
#                                own cleanup on failure: exactly the
#                                "leave the broken one standing" half of §4.2.
#    GIT_TAG                   — the tag the Makefile would derive from git.
#                                There is no .git in the image, so the array's
#                                pinned tag is injected on the command line.
# ---------------------------------------------------------------------------
MAKE_ARGS=(
    -C "$LRH_REPO" run-test-e2e
    SKIP_KIND_SETUP=true
    SKIP_OPERATOR_DEPLOY=true
    DEBUG_ON_FAILURE=true
    "TEST_NAMESPACE=${LRH_NAMESPACE}"
)
[ -n "${LRH_GIT_TAG:-}" ]            && MAKE_ARGS+=("GIT_TAG=${LRH_GIT_TAG}")
[ -n "${LITTLERED_REGISTRY:-}" ]     && MAKE_ARGS+=("LITTLERED_REGISTRY=${LITTLERED_REGISTRY}")
[ -n "${LRH_LABEL_FILTER:-}" ]       && MAKE_ARGS+=("LABEL_FILTER=${LRH_LABEL_FILTER}")
[ -n "${LRH_FOCUS:-}" ]              && MAKE_ARGS+=("FOCUS=${LRH_FOCUS}")
[ -n "${LRH_OPERATOR_IMAGE:-}" ]     && MAKE_ARGS+=("OPERATOR_IMAGE=${LRH_OPERATOR_IMAGE}")
[ -n "${LRH_CHAOS_CLIENT_IMAGE:-}" ] && MAKE_ARGS+=("CHAOS_CLIENT_IMAGE=${LRH_CHAOS_CLIENT_IMAGE}")

log "running: make ${MAKE_ARGS[*]}"
TIMED_OUT=false
timeout --kill-after=60s "${LRH_TIMEOUT_SECONDS}s" make "${MAKE_ARGS[@]}" 2>&1 | tee "${ARTDIR}/run.log"
RC="${PIPESTATUS[0]}"
if [ "$RC" -eq 124 ] || [ "$RC" -eq 137 ]; then
    TIMED_OUT=true
fi
FINISHED_EPOCH="$(date +%s)"
log "item '${LRH_ITEM_NAME}' exited ${RC} (timed_out=${TIMED_OUT})"

# ---------------------------------------------------------------------------
# 4. Relocate the suite's debug artifacts onto the durable volume.
#
#    CollectDebugArtifacts writes "./debug-artifacts-<ts>-<spec>" relative to
#    the test binary's CWD — which `go test` sets to the PACKAGE directory — and
#    offers no override. Rather than patch the suite, move them out afterwards.
# ---------------------------------------------------------------------------
shopt -s nullglob
for d in "${LRH_REPO}"/test/e2e/debug-artifacts-* "${LRH_REPO}"/debug-artifacts-*; do
    log "relocating artifacts: $(basename "$d")"
    mv "$d" "${ARTDIR}/" 2>>"${ARTDIR}/driver.log"
done
shopt -u nullglob

# ---------------------------------------------------------------------------
# 5. Record the verdict, then park (on failure) or clean (on success).
# ---------------------------------------------------------------------------
publish_result "$IDX" "$LRH_ITEM_NAME" "$LRH_NAMESPACE" "$RC" "$TIMED_OUT" \
    "$STARTED_EPOCH" "$FINISHED_EPOCH" "$ARTDIR"

exit 0
