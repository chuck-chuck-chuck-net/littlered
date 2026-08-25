# E2E Testing Guide

This guide explains how to run the LittleRed E2E test suite and perform manual chaos testing.

For the full list of test cases and their statuses, see [TEST_CASES.md](TEST_CASES.md).

---

## Overview

E2E tests verify the operator in a real Kubernetes cluster (Kind). They:

1. Create LittleRed CRs across all modes (standalone, sentinel, cluster)
2. Verify Kubernetes resources are created correctly (StatefulSets, Services, ConfigMaps)
3. Verify Redis is functional (PING, SET/GET, replication, cluster routing)
4. Test failover, crash recovery, rolling updates, and security features under load

---

## Prerequisites

- **Go 1.24+**
- **Kind** (Kubernetes in Docker)
- **Docker or Podman**
- **kubectl**

The Makefile manages everything else automatically: cluster creation, image builds, loading images into Kind, operator deployment, and teardown.

---

## Automated E2E Tests

### Quick Start

Run the full suite against a fresh Kind cluster:

```bash
make test-e2e
```

This automatically:
1. Creates a Kind cluster (`littlered-test-e2e`) if it doesn't exist
2. Builds the operator and chaos client images
3. Loads images into the Kind cluster
4. Deploys the operator via `make deploy`
5. Runs all e2e tests (`go test -tags=e2e ./test/e2e/ -timeout 120m`)
6. Tears down the Kind cluster (only if it was created by this run)

### Running Against an Existing Deployment

If the operator is already deployed (e.g., a pre-existing Kind cluster or remote cluster):

```bash
# Reuse existing Kind cluster, redeploy operator
make test-e2e SKIP_KIND_SETUP=true

# Reuse existing cluster and existing operator deployment
make test-e2e SKIP_KIND_SETUP=true SKIP_OPERATOR_DEPLOY=true
```

### Pinning the Kubernetes Context

E2E test runs can take 40+ minutes. If you switch your `kubectl` context in another terminal during a run, the tests will break. Context pinning prevents this by snapshotting the effective kubeconfig into an isolated temp file at suite startup.

```bash
# Pin whatever context is currently active:
make test-e2e KUBECONTEXT_PINNING=true

# Pin a specific context by name:
make test-e2e KUBECONTEXT=kind-littlered-test-e2e
```

When pinning is active, the test process uses `kubectl config view --raw --flatten --minify` to export a self-contained kubeconfig (with embedded certs, no external file references) containing only the selected context. This is written to a temp file and `KUBECONFIG` is set for the process. All kubectl commands and Go client calls automatically use the pinned config.

### Running one deployment mode (`MODE`)

A full run takes roughly 50–85 minutes, which is too long when the work at hand is
mode-specific. `MODE` is the coarse cut for that: it runs one deployment mode and skips
the others.

```bash
make test-e2e MODE=sentinel              # sentinel only (still excludes 'extended')
make test-e2e MODE=cluster E2E_ALL=true  # cluster, including its extended tiers
make list-e2e MODE=sentinel              # preview the selection; no cluster needed
```

Valid values are `standalone`, `sentinel`, `cluster`, `failover`. A typo fails immediately
with the list of valid values rather than silently selecting nothing. `MODE` composes with
the `extended` rule (AND) and with `FOCUS`, so the two together narrow to one context:

```bash
make test-e2e MODE=sentinel FOCUS='Rolling Update'   # 4 specs
```

**Prefer `MODE` over a `FOCUS` regex for this.** Spec names do not cleanly separate modes:
`FOCUS="Sentinel"` also matches `Sentinel and Standalone Chaos Testing` (pulling in
standalone specs), the security tier spells its mode lowercase inside a `Context`, and the
PDB tier has per-mode cases inside shared contexts. `MODE` uses Ginkgo labels attached to
the outermost mode-pure container instead, so the cut is exact.

Every spec carries exactly one mode label, and that is a **checked invariant** — an
unlabelled spec would be invisible to every `MODE` run, which is a silently smaller test
run and worse than having no knob at all:

```bash
make verify-e2e-mode-labels    # per-mode selections must sum to the full selection
```

See `test/e2e/mode_labels_test.go` for the scheme and where to attach the label when adding
a tier.

### Filtering Tests

Use `FOCUS` to run a subset of tests (passed to Ginkgo's `-focus` flag). For whole-mode
selection prefer `MODE` (above); `FOCUS` is for narrowing *within* a mode or onto a named
context:

```bash
# Whole-mode selection: use MODE, not FOCUS (see above)
make test-e2e MODE=standalone
make test-e2e MODE=sentinel
make test-e2e MODE=cluster

# Only sentinel advanced-failover tests (sentinel mode; FOCUS="Failover" would
# be ambiguous — it also matches the failover-mode suite)
make test-e2e FOCUS="Sentinel Advanced Failover"

# Only failover-mode tests (mode: failover)
make test-e2e MODE=failover

# Only kill-9 / crash tests
make test-e2e FOCUS="Kill-9"

# Only security tests
make test-e2e FOCUS="Security"
```

### Labels and running everything

Tiers are also tagged with Ginkgo **labels** (e.g. `reshard`, `pdb`, `security`). Heavy or
opt-in tiers carry the shared label **`extended`**; everything else runs by default. This keeps a
default run fast while making "run absolutely everything" a single switch — so opt-in tiers are
covered by one knob instead of rotting behind scattered per-test flags.

```bash
make test-e2e                          # every tier EXCEPT 'extended' (the default)
make test-e2e-all                      # every tier, INCLUDING 'extended'
make test-e2e E2E_ALL=true             # same as test-e2e-all
make test-e2e LABEL_FILTER='reshard'   # only the labelled tier(s)
make test-e2e LABEL_FILTER='!extended && !security'  # any Ginkgo label expression
```

Convention: when adding a tier that is slow, needs a non-default image, or is otherwise
opt-in, tag it `Label("extended")` — it then runs under `make test-e2e-all`/CI-all and is
skipped by the fast default, with no new flag to remember.

Separately, every spec carries a **deployment-mode** label (`standalone`, `sentinel`,
`cluster`, `failover-mode`) driving the `MODE` knob above. When adding a tier, label its
outermost mode-pure container and run `make verify-e2e-mode-labels`; the mode labels are
orthogonal to the tier labels, so a spec normally has one of each.

### Auth posture of the fixtures

**Sentinel and failover tiers default to auth-ON**, each instance carrying its own Secret
`{crName}-auth` (password derived from the CR name, so it is reproducible from a debug artifact
without reading the Secret back). Per-instance rather than one suite-wide secret on purpose: LR-039
lists a **shared** password among the conditions under which foreign Sentinel gossip is accepted,
so a single secret would have the suite modelling the hazard instead of the mitigation. The Secret
rides as a leading YAML document in the same `kubectl apply` stream as the CR, so it costs no extra
round trip.

**Cluster and standalone tiers stay auth-free**, and that is a statement about what a password
buys, not an omission. In failover mode auth is a genuine mesh-isolation control (a `masterauth`
mismatch aborts the replication handshake before the RDB transfer, closing the path where a stale
`replicaof <ip>` adopts a foreign master after an IP recycle); in sentinel mode it is the only
thing closing the address-adoption path a unique master name leaves open (ADR-015 §9.4). In cluster
mode a password does **not** protect the mesh — the cluster bus has zero password authentication at
every supported version — so defaulting it on there would assert something false about what it
buys. Cluster mode's protection is LR-043's uncached MEET-time address confirmation plus bus-state
attribution.

**No auth coverage is lost.** `security_test.go` still proves password auth *and* TLS in all four
modes, and it deliberately builds its own fixture rather than using these helpers: it is the spec
that proves auth works, so it must not depend on the helpers that assume it.

**Three deliberate auth-free exemptions**, each carrying a `DELIBERATELY AUTH-FREE` block at the
fixture naming its reason, so a later "flip the last stragglers" sweep leaves them alone:

1. `Sentinel Cross-Instance Isolation` (both specs) — they inject a bare
   `PUBLISH __sentinel__:hello` at the sentinel port. Under `requirepass` the connection answers
   `NOAUTH` **before** `sentinelProcessHelloMessage()` ever runs, so the isolation spec would pass
   having tested nothing (it asserts a non-event, and a refused connection is indistinguishable
   from a discarded hello) and its positive control could not land. Auth is also the wrong variable
   to hold fixed here: these specs measure the master *name* closing gossip fusion.
2. `Sentinel Forsaken-Gated Quarantine` (all three tiers) — the same NOAUTH-before-hello problem,
   plus two more. The `Latched` tier is deterministic in a single cycle **only** because
   `quarantineConfigDangerous` (auth off **and** the legacy `mymaster`) sets the attempt budget to
   1; and the `HoldDataPresent` tier stages its permanently-failing sync with a **bogus
   `masterauth`** against a foreign master that has no password.
3. `Failover Mode Minimum Topology` — the deliberate failover no-auth spec: the cheapest tier that
   still exercises bringup, replication, master kill, promotion of the sole replica and data
   survival with no credential anywhere. It calls `failoverCRWithAuth(..., false)`, so the posture
   is stated at the call site rather than by omission.

Auth does not move the durability numbers: re-validated on t3e (`MODE=failover` 25/25,
`MODE=sentinel` 43/43), the failover chaos tiers report 0 MISSING and 0 corruptions on all three
disruption shapes, with availability inside the previously recorded bands (graceful 96.24% against
96.07% recorded; kill-9 87.39% inside the recorded 85.13-95.73%).

### Additional Flags

Pass extra arguments to `go test` via `ARGS`:

```bash
make test-e2e ARGS="-timeout 90m"
```

### Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `SKIP_KIND_SETUP` | `false` | Skip Kind cluster creation (use existing) |
| `SKIP_OPERATOR_DEPLOY` | `false` | Skip operator deployment (use existing) |
| `TEST_NAMESPACE` | `littlered-e2e` | Namespace for test resources (must not be `default`, and must not already exist) |
| `DEBUG_ON_FAILURE` | `false` | Skip cleanup on failure (leave resources for inspection); also enables Ginkgo `--fail-fast` |
| `KIND_CLUSTER` | `littlered-test-e2e` | Kind cluster name |
| `OPERATOR_IMAGE` | `ghcr.io/littlered-operator/littlered:<git-tag>` | Operator image to deploy |
| `CHAOS_CLIENT_IMAGE` | `ghcr.io/littlered-operator/littlered-chaos-client:<git-tag>` | Chaos client image |
| `KUBECONTEXT_PINNING` | `false` | Snapshot current kubeconfig so context switches don't break tests |
| `KUBECONTEXT` | (none) | Pin to a specific named context (implies pinning) |
| `CLUSTER_SHARDS` | `3` | Number of shards for cluster mode tests (minimum 3) |
| `CLUSTER_REPLICAS_PER_SHARD` | `1` | Replicas per shard for cluster tests that use replicas |
| `NON_GRACEFUL_RESTART` | (none) | Advanced: when `true`, pod-restart helpers use a hard/non-graceful kill instead of a graceful delete |
| `MODE` | (none) | Run one deployment mode only: `standalone`, `sentinel`, `cluster`, `failover`. Composes with `FOCUS`/`E2E_ALL`; invalid values fail fast |
| `LABEL_FILTER` | `!extended` | Any Ginkgo label expression; overrides `MODE`/`E2E_ALL` |
| `FOCUS` | (none) | Ginkgo focus filter (regex) |
| `ARGS` | (none) | Extra arguments passed to `go test` |

---

## Test File Organization

| File | Describe block | What it covers |
|------|---------------|----------------|
| `littlered_test.go` | LittleRed | Standalone CRUD, sentinel deployment, rolling updates (standalone + sentinel), sentinel failover |
| `sentinel_advanced_failover_test.go` | Sentinel Advanced Failover | Event-driven labels, polling-only recovery, hybrid (graceful + crash), sentinel pod resilience |
| `kill9_chaos_test.go` | Kill-9 In-Pod Process Crash | Standalone smoke, sentinel master crash, cluster master crash |
| `cluster_functional_test.go` | Cluster Mode Functional Testing | Cluster formation, data routing, 0-replica healing, failover recovery, custom config, cleanup |
| `cluster_rolling_test.go` | Cluster Mode Rolling Update | Rolling update correctness, data preservation, status after update |
| `cluster_chaos_test.go` | Cluster Mode Chaos Testing | Stability baseline, master/replica failure under load, rolling restart, continuous multi-pod failure |
| `sentinel_standalone_chaos_test.go` | Sentinel and Standalone Chaos Testing | Sentinel rapid double failover under load (graceful + crash), standalone pod restart |
| `security_test.go` | LittleRed Security Features | Password authentication enforcement, TLS encryption enforcement |
| `failover_mode_test.go` | Failover Mode / Failover Mode Deadlock Recovery | `mode: failover` (ADR-011): functional (resources, assignment annotations, experimental event), graceful+crash failover (UID-asserted), event-path (<15s) and polling-only tiers, hybrid double-failover (the graduation scenario), kill-9 epoch-gate yield under chaos load, deadlock tiers (total-loss / single-survivor / multi-holder same-lineage), rolling update. Label: `failover-mode` |
| `sentinel_master_name_test.go` | Sentinel Cross-Instance Isolation / master-name admission | `spec.sentinel.masterName` admission specs, and the injected-hello isolation pair with its positive control (ADR-015, LR-039). **Auth-free by decision** — see "Auth posture of the fixtures" |
| `sentinel_quarantine_test.go` | Sentinel Forsaken-Gated Quarantine | Full cycle (capture → `Forsaken`/`Quarantined` → both StatefulSets at 0 → captor's Sentinels heal → release → Rule L reseeds empty), `HoldDataPresent` refusal, `Latched` after the attempt budget (ADR-016, LR-044). **Auth-free by decision** |

> Naming caveat: `sentinel_advanced_failover_test.go` (Describe: *Sentinel Advanced
> Failover*, formerly `failover_test.go`) tests **sentinel-mode** label mechanics and
> predates the failover mode. The failover-**mode** suite lives in
> `failover_mode_test.go` (helpers in `failover_utils_test.go`, ground truth via
> `INFO replication` — `verifyFailoverTopologySync`). Run it alone with
> `LABEL_FILTER='failover-mode'`.

---

## Reliable Cluster Verification (Lessons Learned)

Testing Redis Cluster state transitions is prone to race conditions due to the lag between Kubernetes actions (deleting a pod) and Redis internal state propagation (gossip). Several improvements were made to keep tests robust and non-flaky.

### The "Stale State" Problem

Redis Cluster gossip can take up to 15 seconds (`cluster-node-timeout`) to detect a failed node. If a test checks for health immediately after killing a pod, it might read **stale pre-failure state** from a node that hasn't noticed the failure yet, producing false positives.

### The "Too Fast Operator" Problem

The LittleRed operator uses Kubernetes as its source of truth and detects a missing pod almost instantly. It may `CLUSTER FORGET` a failed node and begin healing before a test's polling loop even sees the node in a `fail` or `pfail` state.

### Verification Best Practices

- **NodeID Tracking**: Always record the `NodeID` of a pod before performing chaos actions. Kubernetes pod names are stable (StatefulSet), but Redis NodeIDs are unique to the instance. Verification must ensure the *specific ID* has been replaced or forgotten.
- **Dynamic Ground Truth**: Helpers like `verifyClusterTopologySync` query the Redis ground truth (`CLUSTER NODES`) **inside** the `Eventually` loop, synchronizing with the cluster's evolution rather than a stale snapshot.
- **Robust Failure Detection**: The `waitForClusterFailureDetected` helper considers a failure "detected" if:
  1. A node is explicitly marked as `fail` or `pfail`
  2. **OR** the specific victim NodeID has disappeared from the mesh
  3. **OR** the total node count has decreased (indicating the operator already cleaned it up)
- **Shard Master Verification**: `waitForShardMasterChange(slot, oldNodeID)` waits until a specific hash slot is owned by a *different* master ID, proving that healing or promotion has actually occurred.

### Key Topology Guarantees

The E2E tests strictly validate the following cluster invariants after any failure:

1. **No Empty Masters**: Every master node must have slots assigned
2. **Correct Replica Count**: Every shard must have the expected number of healthy replicas
3. **Ghost Cleanup**: Nodes that no longer exist in K8s must be forgotten by the cluster gossip
4. **Data Integrity**: Chaos clients verify that every successful write can be read back, even during failover events

---

## Manual Chaos Testing

Manual chaos testing deploys a LittleRed CR and a continuous-load chaos client via Helm, then lets you inject faults while observing behavior in real time.

### Deploy the Test Environment

**Option A: Sentinel Mode** (3 Redis + 3 Sentinels + chaos client)

```bash
helm upgrade --install sentinel-investigation ./charts/sentinel-chaos \
  --set chaosClient.image.tag=$(git rev-parse --short HEAD) \
  --namespace default

# Tear down
helm delete sentinel-investigation
```

**Option B: Cluster Mode** (3 shards × 2 replicas + chaos client)

```bash
helm upgrade --install cluster-investigation ./charts/cluster-chaos \
  --set chaosClient.image.tag=$(git rev-parse --short HEAD) \
  --namespace default

# Tear down
helm delete cluster-investigation
```

### Monitor the System

Open multiple terminal windows (or use `tmux`):

```bash
# Window 1: Operator logs
kubectl logs -n littlered-system -l control-plane=controller-manager --tail=-1 -f

# Window 2: Chaos client — throughput, availability, data integrity
kubectl logs manual-chaos-chaos-client -f

# Window 3: Redis pod logs (one per pod or loop)
while true; do kubectl logs manual-chaos-redis-0 -f; sleep 1; done         # sentinel
while true; do kubectl logs manual-chaos-shard-0-0 -f; sleep 1; done       # cluster (shard 0 master)

# Window 4: Sentinel logs (sentinel mode only)
while true; do kubectl logs manual-chaos-sentinel-0 -f; sleep 1; done

# Window 5: CR status
while true; do kubectl get littlereds.redis.chuck-chuck-chuck.net manual-chaos -o wide; sleep 1; done
```

### Inject Faults

```bash
# Kill the current master (sentinel mode) — find it first:
kubectl get pods -l redis.chuck-chuck-chuck.net/role=master

# Graceful delete (triggers preStop hook and Sentinel FAILOVER)
kubectl delete pod <master-pod>

# Crash delete (force, no preStop — tests hard resilience)
kubectl delete pod <master-pod> --grace-period=0 --force

# Kill the Redis process in-pod (simulates OOM/kill-9)
kubectl exec <pod> -- kill -9 1

# Kill a shard master (cluster mode)
kubectl exec <cluster-pod> -- redis-cli CLUSTER NODES  # find a master
kubectl delete pod <master-cluster-pod> --grace-period=0 --force
```

**Scenarios to try:**
- Kill the master (sentinel) — expect failover within `downAfterMilliseconds`
- Kill master + replica in same shard simultaneously
- Kill 2 of 3 sentinel pods
- Rapid double kill (graceful then crash before the cluster settles)

**Recovery criteria:**
- **Sentinel**: cluster stabilizes, chaos client shows 0 data corruptions, `{name}` service routes to new master
- **Cluster**: all 16384 slots covered, 0 data corruptions, `cluster_state:ok`

### Observe with lrctl

```bash
# Verify topology health
lrctl verify <name>

# Watch CR status
lrctl describe <name>
```

---

## Troubleshooting

### Preflight Image Check Fails

The suite validates that all required images (chaos client, redis) are pullable before running any tests. If this fails:

```
PREFLIGHT FAILURE: image "ghcr.io/.../littlered-chaos-client:abc1234" could not be pulled
```

Build and push (or Kind-load) the missing image:

```bash
make build-images
make push-images   # or: make kind-load
```

### Tests Fail to Connect to Cluster

```bash
kubectl config current-context
kubectl get nodes
```

### Operator Not Running

```bash
kubectl logs -n littlered-system deployment/littlered-operator
kubectl get pods -n littlered-system
```

### Tests Timeout Waiting for Resources

The default timeout is 120 minutes. Increase it:

```bash
make test-e2e ARGS="-timeout 90m"
```

### Inspect Test Resources

```bash
kubectl get littlered -n littlered-e2e
kubectl get pods -n littlered-e2e
kubectl get svc -n littlered-e2e
```

### Clean Up After Interrupted Tests

Tests clean up after themselves, but if interrupted:

```bash
kubectl delete namespace littlered-e2e --ignore-not-found
kind delete cluster --name littlered-test-e2e
```

### Debug Artifacts

On test failure, debug artifacts (pod logs, operator logs, `lrctl` output) are written to:

```
debug-artifacts-<timestamp>-<test-name>/
```

in the project root.
