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

# publish_result <idx> <name> <ns> <rc> <timed_out> <started> <finished> <artdir> [opns]
#
# Writes the item's result record, publishes it as a labelled ConfigMap for the
# rollup to collect, and then parks or cleans the item's namespaces — BOTH of
# them: the test namespace and (M2) the namespace holding this item's own
# operator. The park/clean decision follows the `parked` field of the record, so
# the recorded verdict and the actual cleanup can never disagree.
publish_result() {
    local idx="$1" name="$2" ns="$3" rc="$4" timed_out="$5"
    local started="$6" finished="$7" artdir="$8" opns="${9:-}"

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

    [ -z "$ns" ] && [ -z "$opns" ] && return 0

    # Fail SAFE, not open: delete only on an explicit "parked":false. If the
    # record could not be written or parsed, `parked` is empty and we must park
    # — an unknown outcome is a reason to keep the evidence, never to destroy it.
    if [ "$parked" != "false" ]; then
        if [ "$parked" != "true" ]; then
            log "WARNING: no verdict could be read from the result record — parking defensively"
        fi
        # Failure handling is "do nothing" (design §4.2): leave the namespaces
        # and everything in them standing for inspection — including this item's
        # OWN operator, still running and still reconciling only its own test
        # namespace (design §3: that is why it is namespace-scoped).
        for parkns in "$ns" "$opns"; do
            [ -z "$parkns" ] && continue
            log "PARKING namespace ${parkns} for forensics"
            kubectl label ns "$parkns" --overwrite \
                "lr-e2e-harness/parked=true" \
                "lr-e2e-harness/array=${LRH_ARRAY}" \
                "lr-e2e-harness/index=${idx}" >>"${artdir}/driver.log" 2>&1
            kubectl annotate ns "$parkns" --overwrite \
                "lr-e2e-harness/item=${name}" \
                "lr-e2e-harness/verdict=${verdict}" \
                "lr-e2e-harness/artifacts=${artdir}" >>"${artdir}/driver.log" 2>&1
        done
    else
        # The suite's own AfterSuite already deletes the test namespace and
        # uninstalls the release on success; this is the idempotent
        # belt-and-braces for a crash before AfterSuite, and it is what actually
        # reaps the operator namespace (helm uninstall does not delete it).
        #
        # ORDER IS LOAD-BEARING: a LittleRed CR carries the operator's finalizer,
        # so the test namespace can only finish terminating while its operator is
        # still alive. Kill the operator first and the namespace hangs in
        # Terminating forever, needing a manual finalizer edit. So: test
        # namespace first, WAITING, then the operator's.
        if [ -n "$ns" ]; then
            log "cleaning namespace ${ns} (waiting: its CRs need the operator to release their finalizer)"
            kubectl delete ns "$ns" --ignore-not-found --timeout=3m >>"${artdir}/driver.log" 2>&1 \
              || log "WARNING: ${ns} did not finish terminating in time — leaving ${opns} standing so its finalizer can still be released"
        fi
        if [ -n "$opns" ] && ! kubectl get ns "$ns" >/dev/null 2>&1; then
            log "cleaning operator namespace ${opns}"
            kubectl delete ns "$opns" --ignore-not-found --wait=false >>"${artdir}/driver.log" 2>&1
        fi
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
log "index ${IDX} -> item '${LRH_ITEM_NAME}' (ns=${LRH_NAMESPACE}, operator ns=${LRH_OPERATOR_NAMESPACE}, timeout=${LRH_TIMEOUT_SECONDS}s)"

ARTDIR="${ART_BASE}/$(printf '%03d' "$IDX")-${LRH_ITEM_NAME}"
mkdir -p "$ARTDIR"

# ---------------------------------------------------------------------------
# 2. Clean slate — but NEVER over parked evidence. The suite refuses to start if
#    its namespace already exists, so an ordinary leftover from an interrupted
#    run of the SAME array is removed. A PARKED namespace is not a leftover: it
#    is the whole product of a failed item (artifacts, and a live operator), and
#    deleting it here is how a set of parked failures once vanished silently.
#
#    The launch gate (`make e2e-harness-parked-check`) already refuses to start
#    an array in that state, so reaching this branch means either the launch was
#    overridden — in which case LRH_ALLOW_PARKED_CLOBBER says so and we comply —
#    or something parked this namespace after launch. Then we leave it standing
#    and let the item fail loudly on the suite's own clean-slate guard: a visible
#    FAIL beats a silent deletion.
#
#    Parked namespaces of OTHER arrays are untouched either way — the array name
#    is the uniqueness knob (see test/harness/README.md).
# ---------------------------------------------------------------------------
BLOCKED=""
for stale in "$LRH_NAMESPACE" "$LRH_OPERATOR_NAMESPACE"; do
    kubectl get ns "$stale" >/dev/null 2>&1 || continue
    parked="$(kubectl get ns "$stale" -o jsonpath='{.metadata.labels.lr-e2e-harness/parked}' 2>/dev/null)"
    if [ "$parked" = "true" ] && [ "${LRH_ALLOW_PARKED_CLOBBER:-false}" != "true" ]; then
        log "REFUSING to delete PARKED namespace ${stale} — it holds a previous item's evidence."
        BLOCKED="${BLOCKED} ${stale}"
        continue
    fi
    if [ "$parked" = "true" ]; then
        log "WARNING: deleting PARKED namespace ${stale} (LRH_ALLOW_PARKED_CLOBBER=true was set at launch)"
    else
        log "WARNING: ${stale} already exists (leftover from an earlier run of array '${LRH_ARRAY}') — deleting"
    fi
    kubectl delete ns "$stale" --ignore-not-found --timeout=5m >>"${ARTDIR}/driver.log" 2>&1
done

# A blocked item is not run AT ALL. Handing a parked namespace to the suite
# would not help: its own clean-slate guard deletes a namespace that holds no
# LittleRed CR and starts anyway, which is the same evidence loss one layer
# down. So record the item as failed-because-blocked and move on — the array
# keeps going, and nothing of the previous run is touched.
if [ -n "$BLOCKED" ]; then
    log "SKIPPING item '${LRH_ITEM_NAME}': parked namespace(s) block it:${BLOCKED}"
    log "  Clear them deliberately with 'make e2e-harness-clean', or relaunch with"
    log "  E2E_HARNESS_ALLOW_PARKED_CLOBBER=true to clobber them."
    {
        echo "Item not run: parked namespace(s) from an earlier run of array '${LRH_ARRAY}' hold evidence:"
        for b in $BLOCKED; do echo "  $b"; done
        echo ""
        echo "Nothing was deleted. Clear them with 'make e2e-harness-clean', or relaunch with"
        echo "E2E_HARNESS_ALLOW_PARKED_CLOBBER=true."
    } > "${ARTDIR}/blocked.txt"
    publish_result "$IDX" "$LRH_ITEM_NAME" "$LRH_NAMESPACE" 1 false \
        "$STARTED_EPOCH" "$(date +%s)" "$ARTDIR" "$LRH_OPERATOR_NAMESPACE"
    exit 0
fi

# Pre-create the operator's namespace LABELLED, rather than letting
# `helm --create-namespace` make it bare. The label is what `make
# e2e-harness-clean` reaps by, so a driver pod that dies mid-item still leaves a
# namespace the array knows how to collect.
kubectl create ns "$LRH_OPERATOR_NAMESPACE" >>"${ARTDIR}/driver.log" 2>&1
kubectl label ns "$LRH_OPERATOR_NAMESPACE" --overwrite \
    "app.kubernetes.io/name=lr-e2e-harness" \
    "lr-e2e-harness/array=${LRH_ARRAY}" \
    "lr-e2e-harness/index=${IDX}" \
    "lr-e2e-harness/role=operator" >>"${ARTDIR}/driver.log" 2>&1

# ---------------------------------------------------------------------------
# 3. Run the work item, bounded by its own deadline.
#
#    SKIP_KIND_SETUP=true      — run against this pre-existing cluster.
#    E2E_OPERATOR_DEPLOY_MODE  — M2: helm-install this item's OWN operator from
#      = pinned                  the array's pre-built, pre-pushed image. NOT a
#                                rebuild (design §1) and NOT the shared
#                                cluster-scoped operator of M1 (design §3).
#    E2E_OPERATOR_NAMESPACE    — that operator's own namespace, one per item.
#    E2E_OPERATOR_SCOPED=true  — and it watches ONLY this item's test namespace,
#                                which is what makes N parked operators able to
#                                coexist without reconciling each other's CRs.
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
    DEBUG_ON_FAILURE=true
    "TEST_NAMESPACE=${LRH_NAMESPACE}"
    E2E_OPERATOR_DEPLOY_MODE=pinned
    "E2E_OPERATOR_NAMESPACE=${LRH_OPERATOR_NAMESPACE}"
    E2E_OPERATOR_SCOPED=true
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
    "$STARTED_EPOCH" "$FINISHED_EPOCH" "$ARTDIR" "$LRH_OPERATOR_NAMESPACE"

exit 0
