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

# ---------------------------------------------------------------------------
# PHASES (design §9.2.4). The same image runs three of them, selected by
# LRH_PHASE:
#
#   item      (default) — one work item, the M1/M2 path. ALWAYS exits 0.
#   setup     — run-scoped, once, BEFORE any item. Every item waits for it.
#   teardown  — run-scoped, once, after the array. REPORT-ONLY.
#
# The run-scoped phases exist now, before M5's resident split-brain monitor
# needs them, so that monitor slots in without restructuring. Unlike a work
# item they do NOT swallow their exit code: there is no verdict to record and
# nothing downstream that can carry on regardless, so a failed setup must be a
# failed Job.
# ---------------------------------------------------------------------------
LRH_PHASE="${LRH_PHASE:-item}"

: "${LRH_ARRAY:?LRH_ARRAY must be set}"
: "${LRH_WORKLIST:=/worklist/worklist.yaml}"
: "${LRH_ARTIFACTS_ROOT:=/artifacts}"
: "${LRH_REPO:=/workspace/littlered}"
: "${LRH_HARNESS_NAMESPACE:=lr-harness-system}"
: "${LRH_LAB:=/lab/lab.yaml}"
: "${LRH_SITE_CREDS:=/site-creds}"

# assemble_kubeconfig <outfile>
#
# Builds the driver pod's multi-context kubeconfig — the hub from this pod's own
# in-cluster ServiceAccount, one context per site from the mounted credential
# Secret — and points KUBECONFIG at it. Prints the site context names.
#
# This is not optional plumbing: the multi-site e2e helpers drive sites BY
# KUBECONTEXT (`kctl` prepends --context), so there is no in-cluster-config
# shortcut for anything but the hub. M0 proved the mechanism; the validation
# lives in `lrharness kubeconfig`.
assemble_kubeconfig() {
    local out="$1"
    lrharness kubeconfig --lab "$LRH_LAB" --creds "$LRH_SITE_CREDS" --out "$out"
}

# --- run-scoped SETUP -------------------------------------------------------
if [ "$LRH_PHASE" = "setup" ]; then
    log "run-scoped SETUP for array '${LRH_ARRAY}'"
    KUBECONFIG=/tmp/kubeconfig
    if ! SITES="$(assemble_kubeconfig "$KUBECONFIG")"; then
        log "FATAL: could not assemble the lab kubeconfig"
        exit 1
    fi
    export KUBECONFIG
    # Reach every site ONCE, for the whole array. Without this each item burns
    # its own timeout rediscovering the same unreachable site — the array
    # equivalent of the launch-time image preflight.
    rc=0
    for site in $SITES; do
        if out="$(kubectl --context "$site" get ns --request-timeout=20s -o name 2>&1)"; then
            log "  ok       site ${site} reachable ($(printf '%s' "$out" | wc -l) namespaces)"
        else
            log "  UNREACHABLE  site ${site}: ${out}"
            rc=1
        fi
    done
    if [ "$rc" -ne 0 ]; then
        log "REFUSING TO RUN THE ARRAY: at least one site is unreachable from this pod."
        log "  Check the site endpoints in the lab inventory — they must be routable FROM"
        log "  HUB PODS, not from a workstation — and re-run 'make e2e-harness-sites-setup'."
        exit 1
    fi
    log "setup complete: ${LRH_ARRAY} may proceed"
    exit 0
fi

# --- run-scoped TEARDOWN ----------------------------------------------------
# REPORT-ONLY, deliberately. A Job cannot schedule a successor, so this one is
# fired by `make e2e-harness-watch` when the array ends (or by hand); that
# trigger is not guaranteed, and a phase that deleted things would therefore be
# a phase that sometimes silently did not. It also keeps the standing rule: the
# harness never destroys forensic evidence unless explicitly told to.
if [ "$LRH_PHASE" = "teardown" ]; then
    log "run-scoped TEARDOWN for array '${LRH_ARRAY}' (report-only, deletes nothing)"
    KUBECONFIG=/tmp/kubeconfig
    if ! SITES="$(assemble_kubeconfig "$KUBECONFIG")"; then
        log "FATAL: could not assemble the lab kubeconfig"
        exit 1
    fi
    export KUBECONFIG
    left=0
    for site in $SITES; do
        # Site-side namespaces are created by the suite (msSetupSites) and carry
        # no harness labels, so they are matched by the reserved name prefix.
        found="$(kubectl --context "$site" get ns -o name --request-timeout=20s 2>/dev/null \
                 | sed -n 's#^namespace/\(lr-harness-.*\)$#\1#p')"
        if [ -n "$found" ]; then
            log "site ${site} still holds harness namespaces (a parked item, or an interrupted run):"
            for ns in $found; do log "    ${ns}"; left=$((left + 1)); done
        else
            log "site ${site}: clean"
        fi
    done
    if [ "$left" -gt 0 ]; then
        log "${left} namespace(s) left standing on the sites. Nothing was deleted — read them,"
        log "  then remove them deliberately: kubectl --context <site> delete ns <name>"
    fi
    exit 0
fi

# --- the work-item phase ----------------------------------------------------
# From here on: ALWAYS exit 0.
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

IDX="${JOB_COMPLETION_INDEX:?JOB_COMPLETION_INDEX must be set (this must run as an Indexed Job)}"

STARTED_EPOCH="$(date +%s)"
ART_BASE="${LRH_ARTIFACTS_ROOT}/${LRH_ARRAY}"
mkdir -p "$ART_BASE"

# ---------------------------------------------------------------------------
# 0. Wait for the run-scoped setup phase, when the array declares one.
#
#    Enforced HERE rather than by whoever launched the array: a Kubernetes Job
#    cannot depend on another Job, and putting the wait in the Makefile would
#    put a workstation back in the run path — the exact coupling §9.3 exists to
#    remove.
# ---------------------------------------------------------------------------
setup_phase_state() {
    local job="$1" type="$2"
    kubectl -n "$LRH_HARNESS_NAMESPACE" get job "$job" \
        -o "jsonpath={.status.conditions[?(@.type==\"${type}\")].status}" 2>/dev/null
}

SETUP_BLOCKED=""
if [ -n "${LRH_SETUP_JOB:-}" ]; then
    log "waiting for the run-scoped setup Job '${LRH_SETUP_JOB}' to complete"
    SETUP_DEADLINE=$(( $(date +%s) + ${LRH_SETUP_TIMEOUT:-900} ))
    while :; do
        if [ "$(setup_phase_state "$LRH_SETUP_JOB" Complete)" = "True" ]; then
            log "run-scoped setup completed"
            break
        fi
        if [ "$(setup_phase_state "$LRH_SETUP_JOB" Failed)" = "True" ]; then
            SETUP_BLOCKED="the run-scoped setup Job '${LRH_SETUP_JOB}' FAILED"
            break
        fi
        if [ "$(date +%s)" -ge "$SETUP_DEADLINE" ]; then
            SETUP_BLOCKED="the run-scoped setup Job '${LRH_SETUP_JOB}' did not complete in time"
            break
        fi
        sleep 5
    done
fi

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

# A failed run-scoped setup blocks every item: the sites it checked are the
# sites the items need. Record the item rather than letting it discover the
# same failure slowly and separately.
if [ -n "$SETUP_BLOCKED" ]; then
    log "SKIPPING item '${LRH_ITEM_NAME}': ${SETUP_BLOCKED}"
    printf '%s\nSee the setup Job log: kubectl -n %s logs job/%s\n' \
        "$SETUP_BLOCKED" "$LRH_HARNESS_NAMESPACE" "${LRH_SETUP_JOB:-?}" > "${ARTDIR}/blocked.txt"
    publish_result "$IDX" "$LRH_ITEM_NAME" "" 1 false "$STARTED_EPOCH" "$(date +%s)" "$ARTDIR"
    exit 0
fi

# ---------------------------------------------------------------------------
# 1b. Multi-site items: assemble the lab kubeconfig and resolve the fixture.
#
#     KUBECONFIG has to be a real FILE with real CONTEXT NAMES, because the
#     multi-site e2e helpers drive sites by kubecontext.
#
#     MS_CONFIG is rendered PER ITEM, not mounted whole. That is forced, not
#     chosen: the operator reads its site kubeconfig Secrets from its OWN
#     namespace (site_registry.go), and every item deploys its own operator into
#     its own namespace (M2) — so `operatorNamespace` cannot be a launch-time
#     constant. The lab inventory IS mounted; only the item-specific field is
#     filled in here. The suite re-reads MS_CONFIG on every call, so a rendered
#     file needs no suite change.
# ---------------------------------------------------------------------------
if [ -n "${LRH_FIXTURE:-}" ]; then
    log "multi-site item: fixture '${LRH_FIXTURE}'"
    if ! assemble_kubeconfig "${ARTDIR}/kubeconfig" >/dev/null; then
        log "FATAL: could not assemble the lab kubeconfig for item '${LRH_ITEM_NAME}'"
        publish_result "$IDX" "$LRH_ITEM_NAME" "" 1 false "$STARTED_EPOCH" "$(date +%s)" "$ARTDIR"
        exit 0
    fi
    export KUBECONFIG="${ARTDIR}/kubeconfig"

    if ! FIXTURE_ENV="$(lrharness fixture \
            --name "$LRH_FIXTURE" \
            --lab "$LRH_LAB" \
            --operator-namespace "$LRH_OPERATOR_NAMESPACE" \
            --msconfig-out "${ARTDIR}/ms-config.yaml" 2>&1 1>"${ARTDIR}/fixture-env.sh")"; then
        log "FATAL: could not resolve fixture '${LRH_FIXTURE}': ${FIXTURE_ENV}"
        printf '%s\n' "$FIXTURE_ENV" > "${ARTDIR}/fixture-error.log"
        publish_result "$IDX" "$LRH_ITEM_NAME" "" 1 false "$STARTED_EPOCH" "$(date +%s)" "$ARTDIR"
        exit 0
    fi
    log "$FIXTURE_ENV"
    # shellcheck disable=SC1090  # generated, validated by `lrharness fixture`
    . "${ARTDIR}/fixture-env.sh"
fi

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
)

# Watch scope: watch exactly this item's test namespace. One rule for BOTH
# single-site and multi-site items — which is the point, not a simplification.
#
# Multi-site used to need a second namespace here. A scoped operator's cache
# holds only its watched namespaces (WATCH_NAMESPACE -> cache DefaultNamespaces,
# ADR-014), but the site kubeconfig Secrets live in the operator's OWN namespace,
# outside that cache; the operator read them THROUGH the cache, so it could not
# resolve any site credential and the stretched cluster never came up. Worse, it
# was a silent deadlock: the failure was laundered into "site unreachable", which
# multi-site treats as unmanaged-not-broken. So the harness passed
# E2E_WATCH_NAMESPACES=<test>,<operator> to widen the scope around the bug.
#
# LR-034 fixed the operator instead: site_registry.go reads that one Secret with
# an UNCACHED reader (the manager's APIReader), and the chart renders a
# `-site-secrets` Role granting `get secrets` in the release namespace, which
# allow mode previously left uncovered. The workaround is therefore removed, and
# removing it is deliberate rather than tidying: a multi-site item under
# allow-mode scoping is exactly the live validation LR-034's own Verification
# notes list as its remaining gap. An array staying green here IS that evidence.
MAKE_ARGS+=(E2E_OPERATOR_SCOPED=true)
# The suite pins its kubeconfig to this context. With MS_CONFIG set it keeps the
# sibling site contexts rather than --minify-ing them away.
[ -n "${KUBECONTEXT:-}" ] && MAKE_ARGS+=("KUBECONTEXT=${KUBECONTEXT}")
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
