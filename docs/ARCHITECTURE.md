# LittleRed - Architecture Document

> Technical architecture for the LittleRed Kubernetes operator.

**Document Status**: Active
**Last Updated**: 2026-08-01

---

## Terminology

"Cluster" is overloaded in the Redis world. This project uses these terms consistently:

| Term | Meaning |
|------|---------|
| **instance** | Any LittleRed-managed Redis deployment, regardless of mode |
| **standalone** | A single Redis pod (`mode: standalone`) |
| **sentinel** | The HA mode: 3 Redis pods (1 master + 2 replicas) monitored by 3 sentinel processes (`mode: sentinel`) |
| **sentinels** | The 3 monitoring processes within a sentinel instance specifically |
| **failover** | The operator-managed HA mode: 1 Redis master + N replicas, no Sentinel processes (`mode: failover`, experimental) |
| **Redis Cluster** | The gossip-based sharding mode (`mode: cluster`) |

"Cluster" on its own always refers to Redis Cluster mode. "Sentinel cluster" is avoided throughout because it is ambiguous — it could mean the whole sentinel setup or the sentinel processes themselves. "Instance" is the generic term for any LittleRed deployment.

---

## 1. High-Level Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kubernetes Cluster                          │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  LittleRed Operator                       │   │
│  │  ┌─────────────────┐  ┌─────────────────────────────┐    │   │
│  │  │ Controller      │  │ Sentinel Event Monitor      │    │   │
│  │  │ Manager         │  │ (per sentinel CR) /         │    │   │
│  │  │                 │  │ Failover Master Watcher     │    │   │
│  │  │                 │  │ (per failover CR)           │    │   │
│  │  └────────┬────────┘  └─────────────────────────────┘    │   │
│  │           │                                               │   │
│  │           ▼                                               │   │
│  │  ┌─────────────────────────────────────────────────┐     │   │
│  │  │              Reconcilers                         │     │   │
│  │  │  ┌───────────────┐  ┌───────────────────────┐   │     │   │
│  │  │  │ Standalone    │  │ Sentinel              │   │     │   │
│  │  │  │ Reconciler    │  │ Reconciler            │   │     │   │
│  │  │  └───────────────┘  └───────────────────────┘   │     │   │
│  │  │  ┌───────────────┐  ┌───────────────────────┐   │     │   │
│  │  │  │ Failover      │  │ Cluster               │   │     │   │
│  │  │  │ Reconciler    │  │ Reconciler            │   │     │   │
│  │  │  │ (experimental)│  │                       │   │     │   │
│  │  │  └───────────────┘  └───────────────────────┘   │     │   │
│  │  └─────────────────────────────────────────────────┘     │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              │ watches/manages                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Managed Resources                      │   │
│  │                                                           │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐ │   │
│  │  │ StatefulSets│ │ Services    │ │ ConfigMaps          │ │   │
│  │  │ (Redis,     │ │ (master,    │ │ (redis.conf,        │ │   │
│  │  │  Sentinel,  │ │  headless,  │ │  sentinel.conf)     │ │   │
│  │  │  Cluster)   │ │  sentinel)  │ │                     │ │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────────┘ │   │
│  │                                                           │   │
│  │  ┌─────────────┐ ┌─────────────────────────────────────┐ │   │
│  │  │ Secrets     │ │ ServiceMonitor (optional)           │ │   │
│  │  │ (auth/TLS)  │ │                                     │ │   │
│  │  │ (user-      │ └─────────────────────────────────────┘ │   │
│  │  │  provided)  │                                         │   │
│  │  └─────────────┘                                         │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. CRD Design

### 2.1 API Structure

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
  namespace: default
spec:
  # Mode selection
  mode: standalone | sentinel | cluster | failover  # default: standalone; failover is experimental

  # Image configuration
  image:
    repository: library/redis     # or valkey/valkey
    tag: "8.4.2"
    pullPolicy: IfNotPresent
    pullSecrets: []

  # Redis configuration
  config:
    # Curated settings (typed, validated)
    maxmemory: "1gb"
    maxmemoryPolicy: noeviction
    # Raw config passthrough (escape hatch)
    raw: |
      tcp-keepalive 300
      hz 10

  # Resources: memory limit == request; a CPU request but no CPU limit (Burstable QoS).
  # Redis's CPU use is bounded by its thread count (main thread + io-threads), so
  # size the request to that budget. A CPU limit has no upside — it can only throttle
  # Redis under load, cascading into latency and downstream timeouts. Add one only if
  # your platform mandates Guaranteed QoS.
  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      memory: "1Gi"

  # Authentication
  auth:
    enabled: false
    existingSecret: ""       # Secret containing 'password' key

  # TLS (encryption in transit; see ADR-004 for verification model)
  tls:
    enabled: false
    existingSecret: ""       # Secret containing tls.crt, tls.key, ca.crt
    caCertSecret: ""         # Separate CA cert secret (optional)
    clientAuth: false        # Require client certificates

  # Metrics
  metrics:
    enabled: true
    exporter:
      image: oliver006/redis_exporter:v1.x
      resources: {}
    serviceMonitor:
      enabled: false
      namespace: ""
      labels: {}
      interval: 30s

  # Pod configuration
  podTemplate:
    annotations: {}
    labels: {}
    nodeSelector: {}
    tolerations: []
    affinity: {}
    topologySpreadConstraints: []
    priorityClassName: ""

  # Sentinel mode settings
  sentinel:
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 60000
    parallelSyncs: 1

  # Failover mode settings (experimental, ADR-011)
  failover:
    replicas: 2                  # Data pods = 1 + replicas
    downAfterMilliseconds: 5000  # Probe-evidence window before declaring the master dead
    minReplicasToWrite: 0        # min-replicas-to-write (0 = off)
    allowUnsafeRebootstrapOnDeadlock: false

  # Cluster mode settings
  cluster:
    shards: 3                    # Minimum 3
    replicasPerShard: 1
    clusterNodeTimeout: 15000    # ms
    failoverGracePeriod: 15      # seconds beyond cluster-node-timeout

status:
  phase: Pending | Initializing | Running | Failed | Terminating
  status: ""                     # Human-readable summary (master pod name or phase)
  bootstrapRequired: true        # Sentinel + failover modes
  observedGeneration: 0
  conditions:
    - type: Ready | ConfigValid | Initialized | SentinelReady | FailoverRecovery
      status: "True" | "False"
      reason: ""
      message: ""
      lastTransitionTime: ""
  redis:
    ready: 0
    total: 0
  master:                        # Sentinel + failover modes
    podName: ""
    ip: ""
  replicas:                      # Sentinel + failover modes
    ready: 0
    total: 0
  sentinels:                     # Sentinel mode only
    ready: 0
    total: 0
  failover:                      # Failover mode only (monitoring surfaces)
    masterDownSince: ""
    assignmentEpoch: 0
    transitionSince: ""
  cluster:                       # Cluster mode only
    state: ok | fail | unknown
    nodes:
      - podName: ""
        nodeID: ""
        role: master | replica
        masterNodeID: ""
        slotRanges: ""
    orphanedReplicas:            # Tracked for failover grace period
      - podName: ""
        nodeID: ""
        masterNodeID: ""
        detectedAt: ""
```

### 2.2 CRD Versioning Strategy

| Version | Status | Notes |
|---------|--------|-------|
| `v1alpha1` | Current | Breaking changes allowed |
| `v1beta1` | Future | Stabilized API |
| `v1` | Future | Stable, full compatibility guarantees |

---

## 3. Component Architecture

### 3.1 Standalone Mode

```
┌─────────────────────────────────────────────────────────────┐
│                    Namespace: user-ns                        │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │           StatefulSet: {name}-redis (replicas: 1)       │ │
│  │  ┌──────────────────────────────────────────────────┐  │ │
│  │  │                   Pod: {name}-redis-0              │  │ │
│  │  │  ┌─────────────────┐  ┌────────────────────────┐ │  │ │
│  │  │  │ Container:      │  │ Container:             │ │  │ │
│  │  │  │ redis           │  │ redis-exporter         │ │  │ │
│  │  │  │ Port: 6379      │  │ Port: 9121             │ │  │ │
│  │  │  └─────────────────┘  └────────────────────────┘ │  │ │
│  │  └──────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────┐  ┌────────────────────────────┐ │
│  │ Service: {name}        │  │ ConfigMap: {name}-config   │ │
│  │ Port: 6379, 9121       │  │ - redis.conf               │ │
│  └────────────────────────┘  └────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ ServiceMonitor: {name} (optional)                      │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Sentinel Mode

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Namespace: user-ns                            │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │              StatefulSet: {name}-redis (replicas: 3)           │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │ │
│  │  │ Pod: -0      │  │ Pod: -1      │  │ Pod: -2      │          │ │
│  │  │ (master)     │  │ (replica)    │  │ (replica)    │          │ │
│  │  │ redis:6379   │  │ redis:6379   │  │ redis:6379   │          │ │
│  │  │ export:9121  │  │ export:9121  │  │ export:9121  │          │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘          │ │
│  │  Labels: redis.chuck-chuck-chuck.net/role = master | replica          │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │              StatefulSet: {name}-sentinel (replicas: 3)        │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │ │
│  │  │ Pod: -0      │  │ Pod: -1      │  │ Pod: -2      │          │ │
│  │  │ sentinel     │  │ sentinel     │  │ sentinel     │          │ │
│  │  │ :26379       │  │ :26379       │  │ :26379       │          │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘          │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  ┌─────────────────────┐  ┌─────────────────────┐                   │
│  │ Service:            │  │ Service:            │                   │
│  │ {name}-master       │  │ {name}-replicas     │                   │
│  │ (selector: role=    │  │ (headless, all      │                   │
│  │  master)            │  │  Redis pods)        │                   │
│  └─────────────────────┘  └─────────────────────┘                   │
│                                                                      │
│  ┌─────────────────────┐  ┌─────────────────────────────────────┐   │
│  │ Service:            │  │ ConfigMap: {name}-config            │   │
│  │ {name}-sentinel     │  │ ConfigMap: {name}-sentinel-config   │   │
│  │ (headless)          │  │                                     │   │
│  └─────────────────────┘  └─────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

**Sentinel Mode Resources**:
- StatefulSet: `{name}-redis` (3 pods: 1 master + 2 replicas)
- StatefulSet: `{name}-sentinel` (3 sentinel pods)
- Service: `{name}-master` (ClusterIP, selector `role=master`)
- Service: `{name}-replicas` (headless, all Redis pods)
- Service: `{name}-sentinel` (headless, all Sentinel pods)
- ConfigMap: `{name}-config` (redis.conf + startup script)
- ConfigMap: `{name}-sentinel-config` (sentinel.conf + startup script)

### 3.3 Failover Mode (Experimental)

Same logical topology as sentinel mode minus the Sentinel processes — the
operator itself performs bootstrap, failure detection, failover, and repair
(ADR-011).

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Namespace: user-ns                            │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │      StatefulSet: {name}-redis (replicas: 1 + N, default 3)     │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │ │
│  │  │ Pod: -0      │  │ Pod: -1      │  │ Pod: -2      │          │ │
│  │  │ (master)     │  │ (replica)    │  │ (replica)    │          │ │
│  │  │ redis:6379   │  │ redis:6379   │  │ redis:6379   │          │ │
│  │  │ export:9121  │  │ export:9121  │  │ export:9121  │          │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘          │ │
│  │  Labels: redis.chuck-chuck-chuck.net/role = master | replica          │ │
│  │  Annotations (operator-stamped assignment channel):             │ │
│  │    assigned-role / assigned-master-ip / assignment-epoch        │ │
│  │  Each pod mounts its own annotations via a downward-API volume  │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  ┌─────────────────────┐  ┌─────────────────────┐                   │
│  │ Service: {name}     │  │ Service:            │                   │
│  │ (ClusterIP,         │  │ {name}-replicas     │                   │
│  │  selector:          │  │ (headless, all      │                   │
│  │  role=master)       │  │  Redis pods)        │                   │
│  └─────────────────────┘  └─────────────────────┘                   │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ ConfigMap: {name}-config (redis.conf, no sentinel.conf)     │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  NO Sentinel StatefulSet, NO sentinel Service, NO sentinel PDB       │
└─────────────────────────────────────────────────────────────────────┘
```

**Failover Mode Resources**:
- StatefulSet: `{name}-redis` (`1 + spec.failover.replicas` pods, default 1 master + 2 replicas; parallel pod management)
- Service: `{name}` (ClusterIP, selector `role=master` — the label being the sole writer-selector authority is the point of the mode)
- Service: `{name}-replicas` (headless, all Redis pods)
- ConfigMap: `{name}-config` (redis.conf + startup script; no Sentinel sibling)
- PodDisruptionBudget over the data pods (always ≥ 2 pods)

The **assignment channel**: instead of the sentinel-mode startup wait-loop that
queries Sentinel, the operator stamps `assigned-role` / `assigned-master-ip` /
`assignment-epoch` annotations on each data pod; the pod reads its own
annotations back through a **downward-API volume** and only then starts
`redis-server`. An EmptyDir run-marker records the consumed epoch, so a
kill-9'd ex-master parks instead of reclaiming mastership (the ADR-001 hazard,
epoch-fenced). See [RECONCILIATION_LOOP_FAILOVER.md](RECONCILIATION_LOOP_FAILOVER.md).

### 3.4 Cluster Mode

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Namespace: user-ns                            │
│                                                                      │
│  One StatefulSet per shard: {name}-shard-0 … {name}-shard-(S-1)      │
│  (each replicas: 1 + replicasPerShard)                               │
│  Example: 3 shards, 1 replica/shard = 3 StatefulSets × 2 pods        │
│                                                                      │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐         │
│  │ STS:           │  │ STS:           │  │ STS:           │         │
│  │ {name}-shard-0 │  │ {name}-shard-1 │  │ {name}-shard-2 │         │
│  │ ┌────────────┐ │  │ ┌────────────┐ │  │ ┌────────────┐ │         │
│  │ │ Pod: -0    │ │  │ │ Pod: -0    │ │  │ │ Pod: -0    │ │         │
│  │ │ master     │ │  │ │ master     │ │  │ │ master     │ │         │
│  │ │ 0-5461     │ │  │ │ 5462-10922 │ │  │ │ 10923-16383│ │         │
│  │ │ 6379/16379 │ │  │ │ 6379/16379 │ │  │ │ 6379/16379 │ │         │
│  │ └────────────┘ │  │ └────────────┘ │  │ └────────────┘ │         │
│  │ ┌────────────┐ │  │ ┌────────────┐ │  │ ┌────────────┐ │         │
│  │ │ Pod: -1    │ │  │ │ Pod: -1    │ │  │ │ Pod: -1    │ │         │
│  │ │ replica    │ │  │ │ replica    │ │  │ │ replica    │ │         │
│  │ └────────────┘ │  │ └────────────┘ │  │ └────────────┘ │         │
│  └────────────────┘  └────────────────┘  └────────────────┘         │
│                                                                      │
│  ┌─────────────────────┐  ┌─────────────────────────────────────┐   │
│  │ Service:            │  │ Service: {name}-cluster             │   │
│  │ {name}              │  │ (shared headless, all shard pods,   │   │
│  │ (ClusterIP, client) │  │  for gossip + bus) Ports: 6379,16379│   │
│  └─────────────────────┘  └─────────────────────────────────────┘   │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ ConfigMap: {name}-cluster-config (shared by all shards)     │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

Each shard StatefulSet carries a stable `redis.chuck-chuck-chuck.net/shard`
label, and its pod `{name}-shard-K-0` is that shard's master (pods `-K-1…-K-R`
are its replicas).

**Key design decisions**:
- One StatefulSet per shard (`{name}-shard-K`) for failure-domain isolation; pod
  `-K-0` is the shard master, `-K-1…-K-R` its replicas
- All shards share one headless Service (`{name}-cluster`) and ConfigMap
  (`{name}-cluster-config`)
- No PersistentVolumes: `nodes.conf` is deleted on every start for fresh identity
- Data durability through replication, not disk
- Port 16379 used for cluster bus communication
- `PodManagementPolicy: Parallel` for faster bootstrap

**Per-shard failure-domain isolation.** Because each shard is its own StatefulSet
with a stable `redis.chuck-chuck-chuck.net/shard` label, the operator can keep a
shard's master and replica(s) apart across nodes or zones. This is configured via
`spec.placement.shardAntiAffinity` (`topologyKey`, `whenUnsatisfiable`): the
operator translates it into a per-shard `topologySpreadConstraint` (`maxSkew: 1`,
`labelSelector` scoped to that shard's pods) injected into each shard StatefulSet
and appended to any user-supplied `spec.podTemplate.topologySpreadConstraints`.
This is the ergonomic completion of the per-shard-StatefulSet design (ADR-007,
pillar 3.12); users cannot write the constraint themselves because the shard label
is operator-owned. See `docs/USAGE.md` and `docs/API_SPEC.md` for the field.

---

## 4. Reconciliation Flow

The operator uses a single `LittleRedReconciler` that dispatches based on `spec.mode`.

**Common entry point**: validate spec → add finalizer → dispatch to mode-specific reconciler.

**Requeue strategy**: fast interval (2s default) when not Running, steady interval (10s default) when Running. Intervals are overridable via annotations.

**Resource management**: All resources use Server-Side Apply (SSA) with `client.Apply` and `client.ForceOwnership`, which only manages fields the operator explicitly sets. This preserves external labels/annotations (e.g. `kubectl rollout restart`).

**Watch configuration**: The controller watches the LittleRed CR with `GenerationChangedPredicate` (ignoring status-only updates) and owns ConfigMaps, Services, and StatefulSets. Sentinel and failover modes additionally watch a shared channel fed by their background fast-detection goroutines (the Sentinel event monitor / the failover master watcher).

For detailed reconciliation logic per mode, see:
- [RECONCILIATION_LOOP.md](RECONCILIATION_LOOP.md) — high-level flow
- [RECONCILIATION_LOOP_SENTINEL.md](RECONCILIATION_LOOP_SENTINEL.md) — sentinel healing rules, DetermineRealMaster, kill-9 protection
- [RECONCILIATION_LOOP_FAILOVER.md](RECONCILIATION_LOOP_FAILOVER.md) — failover-mode intent model, death detection, decision table (experimental)
- [RECONCILIATION_LOOP_CLUSTER.md](RECONCILIATION_LOOP_CLUSTER.md) — repair loop, quorum recovery, kill-9 protection

### 4.1 Reconciliation Triggers

| Event | Action |
|-------|--------|
| CR spec change (generation bump) | Full reconciliation |
| Owned resource change (STS, SVC, CM) | Reconciliation via ownership watch |
| Sentinel `+switch-master` event | Reconciliation via channel (sentinel mode) |
| Failover master watcher fires (master unreachable past `downAfterMilliseconds`) | Reconciliation via the same channel (failover mode) |
| Fast requeue timer (2s) | While not Running |
| Steady requeue timer (10s) | While Running |
| CR deleted | Cleanup: remove finalizer, stop sentinel monitor / failover watcher |

---

## 5. Sentinel Mode: Key Mechanisms

### 5.1 Safe Bootstrap

To prevent data loss in pure in-memory mode, LittleRed implements operator-authorized bootstrap:

1. On CR creation, `status.bootstrapRequired = true`.
2. All Redis pods start with a wait-loop querying Sentinel for the current master.
3. Once `redis-0` has a PodIP, the operator issues `SENTINEL MONITOR` to **each sentinel pod individually** (not via the headless service, which would only reach one).
4. `redis-0` sees its own IP in the Sentinel reply → starts as master.
5. Other pods see the master → start as replicas.
6. Once all pods are Running and Sentinel knows all replicas, `bootstrapRequired` is cleared.

### 5.2 Strict IP-Only Identity (ADR-001)

LittleRed enforces **IP-based identity** for all Sentinel and Redis nodes.

**Why**: A restarted pod keeps its hostname but gets a new IP. Since we are pure in-memory, a restarted pod is empty. If we used hostnames, Sentinel would see the empty `redis-0` as the previous master and allow it to accept writes, wiping live replicas via FULLRESYNC.

**Implementation**:
- `sentinel announce-hostnames no` / `sentinel resolve-hostnames no`
- All `SENTINEL MONITOR` commands use Pod IPs
- The operator maps Sentinel-reported IPs to Pod names via the K8s API

### 5.3 Kill-9 / Crash Protection (ADR-001 amendment)

When a Redis process is killed (OOM, kill -9) but the pod stays alive, the container restarts at the same IP with no data. The startup script detects this by querying Sentinel for the stored `run-id` of the master at its own IP:

- If Sentinel knows a `run-id` for a master at this IP → **crash detected**. The script suppresses Redis startup and waits up to 120s for Sentinel's SDOWN timer to fire and failover to complete.
- The pod then joins as a replica of the newly elected master.

### 5.4 Master Service Routing

The `{name}-master` service uses a label selector (`redis.chuck-chuck-chuck.net/role: master`). The operator surgically updates this label on each reconcile cycle:

- **Master known**: the master pod gets `role=master`, all others get `role=replica`.
- **No master** (failover in progress): only the `role=master` label is stripped from whoever had it. Other pods are left untouched to avoid churn.
- **Sentinel unreachable**: labels are left unchanged (stale but stable).

### 5.5 Healing Rules

The sentinel reconciler gathers ground truth from every Redis and Sentinel pod, then applies a prioritized set of healing rules:

| Rule | Trigger | Action |
|------|---------|--------|
| Rule 0 | Sentinel reachable but not monitoring | SENTINEL MONITOR + settings |
| Rule A | Pod terminating or failover active | Skip all healing |
| Ghost Master | Sentinel points at ghost or wrong IP | SENTINEL REMOVE + MONITOR |
| Ghost Replicas | Ghost IPs in s_down in replica list, **and cluster whole** (all Redis reachable) | SENTINEL RESET |
| Rule R | Redis pod not following consensus master | SLAVEOF |

See [RECONCILIATION_LOOP_SENTINEL.md](RECONCILIATION_LOOP_SENTINEL.md) for the full algorithm.

### 5.6 Sentinel Event Monitor

A background goroutine per LittleRed CR subscribes to the Sentinel `+switch-master` pubsub channel. When a failover event is received, it injects a `GenericEvent` into the controller's work queue, triggering an immediate reconciliation without waiting for the next requeue timer.

---

## 6. Cluster Mode: Key Mechanisms

### 6.1 Ground Truth Gathering

On each reconcile, the operator queries every pod for `CLUSTER MYID`, `CLUSTER INFO`, and `CLUSTER NODES`. It builds a `ClusterGroundTruth` containing: node identities, slot assignments, role/replica relationships, partition detection (BFS on the adjacency graph), and ghost node detection (NodeIDs without living K8s pods).

Pods are probed **concurrently**, and each probe carries a hard per-pod deadline (`ClusterProbeTimeout`). During pod churn the K8s pod cache can hand the operator a stale IP belonging to a deleted pod; without these two properties a single dead IP would block the whole gather (and thus the reconcile loop) on dial retries for ~25s, starving the loop of the iterations it needs to converge. See LR-012.

### 6.2 Repair Loop

When the cluster is unhealthy, the operator runs a prioritized repair sequence:

| Step | Trigger | Action |
|------|---------|--------|
| 0. Quorum Recovery | Voting masters ≤ shards/2 | CLUSTER FAILOVER TAKEOVER |
| 1. Heal Partitions | >1 connected component | CLUSTER MEET + orphan tracking/promotion |
| 2. Forget Ghosts | NodeIDs without K8s pods | CLUSTER FORGET (protected if still a replica's master) |
| 3. Recover Slots | Missing shard ranges | CLUSTER ADDSLOTS to intended master |
| 4. Repair Replication | Empty masters, missing replicas | CLUSTER REPLICATE |
| 5. Bootstrap | 0 slots AND 0 replicas | Full MEET + ADDSLOTS + REPLICATE |

See [RECONCILIATION_LOOP_CLUSTER.md](RECONCILIATION_LOOP_CLUSTER.md) for the full algorithm.

### 6.3 Kill-9 / Crash Protection

The cluster startup script detects container restarts via a surviving `nodes.conf` in the emptyDir:

1. If `nodes.conf` exists → crash detected. Poll peers for up to 60s.
2. If peers still see this pod as master with slots after 30s → find own replica and issue `CLUSTER FAILOVER TAKEOVER` to break the deadlock.
3. If still stuck after 60s → enter infinite sleep (liveness probe will restart).
4. Always delete `nodes.conf` before starting to ensure a fresh NodeID.

---

## 7. Failover Mode: Key Mechanisms (Experimental)

Failover mode (ADR-011) is operator-managed HA without Sentinel: one decider
instead of two, eliminating the operator-vs-Sentinel race class (LR-007/008,
LR-013, LR-024) at the accepted cost of coupling failover orchestration to
operator liveness. The full algorithm lives in
[RECONCILIATION_LOOP_FAILOVER.md](RECONCILIATION_LOOP_FAILOVER.md); the load-bearing
mechanisms:

### 7.1 Operator Assignment via Downward API

Pillar 3.6 (a Redis pod does not start `redis-server` until the operator
assigns it a role) is kept, but the Sentinel query loop is replaced: the
operator patches `assigned-role` / `assigned-master-ip` / `assignment-epoch`
annotations onto each data pod, and the pod polls its own annotations through a
downward-API volume. No API-server access from data pods; the kubelet rewrites
the projected file on every annotation change.

### 7.2 Epoch Fencing (Kill-9 / Crash Protection)

Before `exec`, the startup script writes the consumed epoch to an EmptyDir
run-marker (survives a container restart, not a pod replacement). An assignment
is honored only with no marker or a strictly greater epoch — so a kill-9'd
ex-master replays its stale `assigned-role: master` annotation and **parks**
until the operator (with its global view) either fails over to a data-holding
replica or re-authorizes it as master when no data exists anywhere. This
re-owns sentinel mode's run-id yield with the operator as the authority.

### 7.3 Failure Detection (Corroborated)

The reconcile loop is the sole decider (pure predicate `planMasterDeath`):
Kubernetes-authoritative evidence (pod gone/replaced, redis container not-Ready
per kubelet, pod terminating) is acted on immediately; probe evidence requires
operator-unreachability sustained past `downAfterMilliseconds` **and** every
reachable replica reporting `master_link_status:down` — the operator's own dial
alone is never sufficient (a blackholed operator-side route must not kill a
serving master).

### 7.4 The Failover Decision

One pure decision table (`planFailover`) covers bootstrap seeding, normal
failover, and the no-master deadlock matrix: 0 data holders → seed `redis-0`;
holders on one replication lineage → promote the most-complete one (no opt-in);
divergent lineages → refuse unless `failover.allowUnsafeRebootstrapOnDeadlock`.
Execution (stamp intent → promote → flip label → repoint) is idempotent and
resumable from live state.

### 7.5 Failover Master Watcher

A background goroutine per failover-mode CR probes the current master with a
bounded `INFO` every ~1s and injects a `GenericEvent` when a failure streak
crosses `downAfterMilliseconds` — the fast path replacing the sentinel-mode
`+switch-master` subscription. It accelerates reconciliation only; it never
declares death itself.

### 7.6 Master Service Routing

Identical to sentinel mode (§5.4): the `{name}` Service selects on the
operator-managed `role: master` label. In failover mode this label is the
*only* writer-selector authority — there is no second (Sentinel) view to
conflict with it. The label flips only after the promoted pod is observed
reporting `role:master`.

---

## 8. Authentication & TLS

### 8.1 Authentication

- User provides a Secret containing a `password` key
- `spec.auth.existingSecret` references the Secret
- The operator injects `REDIS_PASSWORD` as an environment variable
- Redis is configured with `requirepass` and `masterauth`
- Sentinel is configured with `auth-pass` via `SENTINEL SET`

### 8.2 TLS

- User provides a Secret containing `tls.crt`, `tls.key`, and `ca.crt`
- `spec.tls.existingSecret` references the Secret
- Secrets are mounted at `/tls` in all pods (Redis, Sentinel)
- Redis/Sentinel are configured with `tls-port`, `tls-cert-file`, `tls-key-file`, `tls-ca-cert-file`
- The non-TLS port is disabled (`port 0`)

### 8.3 Operator-to-Pod Connections

The operator connects to Redis/Sentinel pods using `InsecureSkipVerify` for TLS. This provides encryption in transit while relying on the Kubernetes API (not PKI) for identity verification. See [ADR-004](adr/004-tls-insecure-skip-verify.md) for the full rationale.

---

## 9. Configuration Management

### 9.1 Config Layering

```
┌─────────────────────────────────────────────────────────────┐
│                    Configuration Layers                      │
│                                                              │
│  1. Hardcoded base (non-overridable)                        │
│     - daemonize no, dir /data                               │
│                                                              │
│  2. Performance-optimized defaults                          │
│     - maxmemory-policy noeviction                           │
│     - save ""           ← ACTIVELY DISABLE RDB              │
│     - appendonly no     ← ACTIVELY DISABLE AOF              │
│                                                              │
│  3. User config (spec.config.*)                             │
│     - Typed fields (validated)                              │
│     - Raw passthrough (expert mode)                         │
│                                                              │
│  4. Mode-specific (auto-injected)                           │
│     - Sentinel: replicaof, announce-ip                      │
│     - Cluster: cluster-enabled, announce-ip/port/bus-port   │
│     - TLS: tls-port, tls-cert-file, etc.                   │
│     - Auth: requirepass, masterauth                         │
│                                                              │
│  → Final redis.conf written to ConfigMap                    │
└─────────────────────────────────────────────────────────────┘
```

### 9.2 Startup Scripts

Each mode uses a Go `text/template` (with `[[` `]]` delimiters) to generate a startup script injected as the container command. The script handles:
- Sentinel mode: wait-loop for Sentinel authorization, kill-9 detection, master/replica branching
- Failover mode: wait-loop polling the downward-API annotation file for an operator assignment, epoch gate (run-marker) for kill-9 yield, master/replica branching
- Cluster mode: crash detection, yield loop, deadlock breaker, fresh identity guarantee

Pre-stop hooks handle graceful shutdown:
- Sentinel mode: triggers `SENTINEL FAILOVER` if master, waits for handoff
- Failover mode: only sleeps to hold the grace window — the handover itself is operator-led (the reconcile sees the `deletionTimestamp` and promotes; pods make no topology decisions, LR-016)
- Cluster mode: triggers `CLUSTER FAILOVER` to replica if master with slots

---

## 10. Security

### 10.1 Pod Security
- Runs as non-root (uid 999)
- Read-only root filesystem (writable `/data` emptyDir)
- All capabilities dropped

### 10.2 Network Security
- Services are ClusterIP by default (no external exposure)
- TLS available for transport encryption (ADR-004)
- No NetworkPolicies created by operator (user responsibility)

### 10.3 Secret Handling
- Passwords and TLS certs stored in user-managed Secrets, not in the CR
- Operator uses RBAC to read Secrets in the target namespace
- Secrets validated during spec validation (existence + required keys)

---

## 11. Operator Deployment

```
┌─────────────────────────────────────────────────────────────┐
│              Namespace: littlered-system                     │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │             Deployment: littlered-operator              │ │
│  │  ┌─────────────────────────────────────────────────┐   │ │
│  │  │              Pod: operator                       │   │ │
│  │  │  - Controller Manager                            │   │ │
│  │  │  - Health probes: :8081                          │   │ │
│  │  │  - Metrics: :8080                                │   │ │
│  │  │  - Leader election enabled                       │   │ │
│  │  └─────────────────────────────────────────────────┘   │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ServiceAccount, ClusterRole, ClusterRoleBinding             │
│  Service (metrics), ServiceMonitor (optional)                │
└─────────────────────────────────────────────────────────────┘
```

---

## 12. Project Structure

```
littlered/
├── api/
│   └── v1alpha1/
│       ├── littlered_types.go         # CRD struct definitions
│       ├── littlered_defaults.go      # Default values and helpers
│       └── zz_generated.deepcopy.go
├── cmd/
│   ├── littlered/                     # Operator entrypoint
│   ├── lrctl/                         # CLI tool (kubectl plugin)
│   └── littlered-chaos-client/        # Chaos test client
├── config/
│   ├── crd/                           # Generated CRD YAML
│   ├── manager/                       # Operator deployment
│   ├── rbac/                          # RBAC manifests
│   └── samples/                       # Example CRs
├── internal/
│   ├── controller/
│   │   ├── littlered_controller.go    # Main reconciler + sentinel healing
│   │   ├── cluster_reconcile.go       # Cluster mode reconciliation
│   │   ├── failover_reconcile.go      # Failover mode reconciliation (ADR-011)
│   │   ├── failover_plan.go           # Pure seams: planMasterDeath + planFailover
│   │   ├── failover_intent.go         # Intent model (annotations → intent, settledness, re-auth)
│   │   ├── failover_monitor.go        # Background failover-mode master watcher
│   │   ├── resources.go               # Resource builders (CMs, SVCs, STSs, startup scripts)
│   │   ├── resources_failover.go      # Failover-mode builders (STS, config, startup script)
│   │   ├── gatherer.go                # Operator-side Gatherer implementation
│   │   └── sentinel_monitor.go        # Background +switch-master subscriber
│   ├── redis/
│   │   ├── client.go                  # Sentinel client wrapper
│   │   ├── cluster_client.go          # Cluster client wrapper
│   │   ├── replication_state.go       # ReplicationState + DetermineRealMaster
│   │   ├── cluster_state.go           # ClusterGroundTruth + health checks
│   │   └── gather.go                  # GatherReplicationState / GatherClusterGroundTruth
│   └── cli/
│       ├── discovery/                 # Resource discovery for lrctl
│       ├── k8s/                       # K8s exec-based Gatherer for lrctl
│       └── types/                     # Shared types
├── charts/
│   └── littlered/                     # Helm chart
├── test/
│   ├── e2e/                           # End-to-end tests (kind)
│   ├── chaos/                         # Chaos test definitions
│   └── utils/                         # Test utilities (TLS cert generation, etc.)
├── docs/
│   ├── ARCHITECTURE.md                # This document
│   ├── REQUIREMENTS.md
│   ├── SCOPE.md
│   ├── RECONCILIATION_LOOP.md         # High-level reconciliation diagram
│   ├── RECONCILIATION_LOOP_SENTINEL.md
│   ├── RECONCILIATION_LOOP_FAILOVER.md
│   ├── RECONCILIATION_LOOP_CLUSTER.md
│   ├── RECONCILIATION_ALGORITHM_CHANGELOG.md
│   ├── USAGE.md
│   ├── LRCTL.md
│   ├── E2E_TESTING.md
│   └── adr/                           # Architecture Decision Records
│       ├── 001-strict-ip-identity.md
│       ├── 002-remove-startup-ping-check.md
│       ├── 003-low-interference-sentinel-reconciliation.md
│       ├── 003a-deferred-improvements.md
│       ├── 004-tls-insecure-skip-verify.md
│       ├── ...                        # 005-009: recovery rules, per-shard STSs
│       └── 011-failover-mode.md       # Operator-managed HA without Sentinel
├── Makefile
├── go.mod
└── CLAUDE.md
```

---

## 13. Dependencies

| Dependency | Purpose | Version |
|------------|---------|---------|
| controller-runtime | K8s controller framework | latest stable |
| client-go | K8s API client | matches K8s version |
| go-redis/v9 | Redis/Sentinel client | v9.17+ |
| zap | Structured logging | via controller-runtime |

---

## 14. Design Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Admission webhooks? | No (validate in controller) | Operational complexity, cert management burden |
| Leader election? | Yes | Required for HA operator |
| Finalizers? | Yes | Clean shutdown of sentinel monitors |
| Watch Secrets? | No (validated on reconcile) | Simpler; password/cert changes trigger requeue via status check |
| Resource application? | Server-Side Apply | Preserves external fields, no read-modify-write races |
| Watch predicate? | GenerationChangedPredicate | Prevents status-only updates from triggering redundant reconciliation |
| TLS verification? | InsecureSkipVerify | Identity via K8s API, not PKI (ADR-004) |

---

## 15. References

- [ADR-001: Strict IP-Only Identity](adr/001-strict-ip-identity.md)
- [ADR-003: Low-Interference Sentinel Reconciliation](adr/003-low-interference-sentinel-reconciliation.md)
- [ADR-004: TLS InsecureSkipVerify](adr/004-tls-insecure-skip-verify.md)
- [ADR-011: `failover` Mode — Operator-Managed HA without Sentinel](adr/011-failover-mode.md)
- [Reconciliation Algorithm Changelog](RECONCILIATION_ALGORITHM_CHANGELOG.md)
- [CLAUDE.md](../CLAUDE.md) — condensed project overview for LLM onboarding
