# Using LittleRed

This guide shows how to deploy and use Redis instances with the LittleRed operator.

## Prerequisites

- Kubernetes 1.28+
- `kubectl` configured to access your cluster
- Helm 3.x (for Helm installation)

---

## Installing the Operator

### Option 1: Helm (Recommended)

```bash
# From the published OCI chart (recommended)
helm upgrade --install littlered oci://ghcr.io/littlered-operator/charts/littlered \
  -n littlered-system --create-namespace

# For a pinned version, add: --version <version>
```

Or from a source checkout:

```bash
git clone https://github.com/littlered-operator/littlered-operator.git
helm upgrade --install littlered ./littlered-operator/charts/littlered \
  -n littlered-system --create-namespace
```

#### Verify installation

```bash
# Check operator pod is running
kubectl get pods -n littlered-system

# Expected output:
# NAME                                  READY   STATUS    RESTARTS   AGE
# littlered-operator-xxxxxxxxx-xxxxx   1/1     Running   0          30s

# Check CRD is installed
kubectl get crd littlereds.redis.chuck-chuck-chuck.net
```

#### Custom configuration

Create a `values.yaml` file:

```yaml
image:
  repository: ghcr.io/littlered-operator/littlered
  # tag: ""   # defaults to the chart's appVersion — pin only to override

resources:
  limits:
    cpu: 500m
    memory: 256Mi
  requests:
    cpu: 100m
    memory: 128Mi

# For HA setups (multiple operator replicas)
replicas: 2
```

> Leader election is always enabled in the operator, so running multiple
> replicas is safe: only the pod holding the lease reconciles, regardless of
> how the deployment is scaled (via Helm or directly with `kubectl`/`k9s`).

# Optional: spread the operator replicas across nodes (only meaningful with replicas > 1).

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
        control-plane: controller-manager
```

> Leader election (chart default `true`) ensures only one operator pod reconciles at a
> time; the standby takes over on leader loss. `replicas > 1` gives you HA against pod or
> process failure; add `topologySpreadConstraints` (or `affinity`) to also survive a node
> or zone loss.

Install with custom values:

```bash
helm upgrade --install littlered oci://ghcr.io/littlered-operator/charts/littlered \
  -n littlered-system --create-namespace \
  -f values.yaml
```

#### Upgrade

```bash
helm upgrade littlered oci://ghcr.io/littlered-operator/charts/littlered -n littlered-system
```

**Important: Upgrading CRDs**

Helm does not automatically update CRDs on `helm upgrade`. If the LittleRed CRD schema has changed (e.g., new fields like `spec.cluster`), you must apply the CRD manually:

```bash
kubectl apply -f charts/littlered/crds/redis.chuck-chuck-chuck.net_littlereds.yaml
```

#### Uninstall

```bash
helm uninstall littlered -n littlered-system

# CRDs are not deleted automatically. To remove them:
kubectl delete crd littlereds.redis.chuck-chuck-chuck.net
```

### Option 2: ArgoCD

Create an ArgoCD Application:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: littlered-operator
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/littlered-operator/littlered-operator.git
    targetRevision: main          # or a release tag, e.g. v0.3.0
    path: charts/littlered
  destination:
    server: https://kubernetes.default.svc
    namespace: littlered-system
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

---

## Namespace Scoping

By default the operator is **cluster-scoped**: it watches every `LittleRed` CR in
every namespace and ships cluster-wide RBAC (a `ClusterRole` + `ClusterRoleBinding`).
This is unchanged — scoping is entirely opt-in, and leaving both settings below empty
gives you exactly today's behavior.

Scoping lets you restrict which namespaces an operator manages, so more than one
operator can safely share a cluster. It is expressed as *the operator's watch-list*
(which namespaces this operator owns), configured through two **mutually exclusive**
Helm values (set **at most one**):

| Value | Env var set on the operator | Mode | RBAC |
|-------|-----------------------------|------|------|
| `scope.watchNamespaces` | `WATCH_NAMESPACE` | Allow-list — watch only these namespaces | Namespaced `Role` + `RoleBinding` per namespace (least-privilege); no `ClusterRole` |
| `scope.ignoreNamespaces` | `IGNORE_NAMESPACE` | Deny-list — watch all namespaces *except* these | Cluster-wide `ClusterRole` (kept) |
| *(neither set — default)* | *(none)* | Cluster-scoped (all namespaces) | Cluster-wide `ClusterRole` |

The CRD is always installed cluster-wide (from the chart's `crds/`); only the CR
*instances* are namespaced.

### Allow-list mode (`scope.watchNamespaces`)

The operator's informers and reconcilers see **only** the listed namespaces. Because
it needs no cluster-wide reach, the chart drops the `ClusterRole`/`ClusterRoleBinding`
and instead renders the same reconcile permissions as a **`Role` + `RoleBinding` in
each watched namespace**, bound to the operator's ServiceAccount (which stays in the
operator's own namespace). This is the least-privilege deployment.

Use it for a single-tenant or per-team operator ("this operator manages only the
`team-a` namespace"), or to manage a specific set of namespaces:

```bash
helm upgrade --install littlered oci://ghcr.io/littlered-operator/charts/littlered \
  -n littlered-system --create-namespace \
  --set scope.watchNamespaces={team-a,team-b}
```

This sets `WATCH_NAMESPACE="team-a,team-b"` on the operator and renders a
`littlered-manager` `Role` + `RoleBinding` in both `team-a` and `team-b`.

### Deny-list mode (`scope.ignoreNamespaces`)

The operator watches **all namespaces except** the listed ones. This is the "one
global operator, but hands off these" model — it inherently watches cluster-wide, so
it keeps the `ClusterRole` (there is no least-privilege gain to be had, which is
expected for a global operator).

```yaml
# values.yaml
scope:
  ignoreNamespaces:
    - staging
```

This sets `IGNORE_NAMESPACE="staging"` on the operator; the operator reconciles every
namespace but leaves `staging` entirely alone.

### Mutual exclusivity

`scope.watchNamespaces` and `scope.ignoreNamespaces` are mutually exclusive. Setting
both is a **fail-fast error in two places**: the Helm chart refuses to render, and if
both env vars are somehow set the operator exits at startup rather than guessing a
merge.

### The multi-operator partition pattern (staged rollout)

The headline use is running two operators side by side with **zero overlap** — for
example, to stage a new operator version against one namespace without touching the
rest of the cluster:

- A **global** operator with `scope.ignoreNamespaces: [staging]` — manages everything
  except `staging`.
- A **second** operator (e.g. a new version) with `scope.watchNamespaces: [staging]` —
  manages only `staging`.

Their watch-lists are disjoint, so no CR is reconciled by both operators
(no double-reconcile), and each gets its own leader lease (see below). When the new
version is validated in `staging`, promote it to the rest of the cluster.

### Leader election and `POD_NAMESPACE`

The operator's leader-election lease lives in its **own** namespace: the chart wires
the downward-API `POD_NAMESPACE` env var (from `metadata.namespace`) so a scoped
operator's lease never lives cluster-wide. In addition, the lease **ID is derived from
the operator's scope** (mode + namespace set), so two operators with disjoint
watch-lists never contend for the same lease — the unscoped default keeps the original
fixed lease ID, unchanged. No configuration is needed for this; it follows from the
`scope.*` values.

---

## Standalone Mode

A single Redis instance for development or simple caching.

### Deploy

```yaml
# standalone.yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: standalone
```

```bash
kubectl apply -f standalone.yaml
```

### Verify

```bash
# Check status
kubectl get littlered store

# Expected output:
# NAME       MODE         PHASE     READY   AGE
# store   standalone   Running   1       30s

# Check pods
kubectl get pods -l app.kubernetes.io/instance=store

# Check service
kubectl get svc store
```

### Connect

```bash
# Test Redis connection
kubectl exec -it store-redis-0 -c redis -- redis-cli PING
# Output: PONG

# Set and get a value
kubectl exec -it store-redis-0 -c redis -- redis-cli SET hello world
kubectl exec -it store-redis-0 -c redis -- redis-cli GET hello
# Output: world

# Check Redis info
kubectl exec -it store-redis-0 -c redis -- redis-cli INFO server
```

### Connect from your application

```bash
# Service endpoint
store.<namespace>.svc.cluster.local:6379
```

---

## Sentinel Mode

High-availability setup with automatic failover: 3 Redis pods (1 master + 2 replicas) and 3 Sentinel pods.

### Deploy

```yaml
# sentinel.yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
  namespace: apps
spec:
  mode: sentinel
  sentinel:
    masterName: apps.store    # required — see "Isolating Sentinel instances" below
```

`sentinel.masterName` is required and has no default. Read the next section before choosing a
value: it is the only thing separating your instance from every other Sentinel deployment that
can reach it.

```bash
kubectl apply -f sentinel.yaml
```

### Isolating Sentinel instances

Two things protect a sentinel-mode instance from being absorbed by another Redis deployment on
the same pod network. **Do both.** They close different holes, and neither is sufficient alone.

**1. Give every instance a unique master name.**

Use `<namespace>.<name>`. The master name is the *only* isolation Sentinel's gossip protocol
has: a Sentinel receiving a hello message looks the name up and discards the message if it does
not know it, and performs no other check — no instance identifier, no namespace, no
authentication between Sentinels beyond the optional password.

Two instances that share a master name and can reach each other are, protocol-wise, **one
deployment**. The one with the higher config epoch can reassign the other's master to a foreign
Redis pod; that instance's replicas then **flush their datasets** to resynchronise from a
stranger. If both run the same Redis version the merge completes silently — the victim reports
healthy, with a unanimous and self-consistent Sentinel view, while serving someone else's
keyspace. This has happened in production; the analysis is in
`SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md`.

**2. Enable authentication.**

```yaml
spec:
  auth:
    enabled: true
    existingSecret: redis-password
```

Auth is *not* only a client-edge control in Sentinel. The same password is the peer-membership
credential: `sentinel-pass` (falling back to `requirepass`) authenticates Sentinel-to-Sentinel
links in both directions, so an unauthenticated hello from a stranger is rejected outright, and
a replica cannot complete a sync against a foreign master — which is the step that destroys
data.

**Why you need it even with unique names.** A unique name ends *gossip* fusion, but not the
narrower **address-adoption** path: if another instance's master pod dies and Kubernetes recycles
its IP onto *your* master, that instance's Sentinels — still holding the address as their own
master — will connect to your master directly, read its `INFO`, adopt your replicas as theirs,
and can issue `SLAVEOF` to them. No hello is involved, so the name is never consulted. Only
distinct passwords stop it.

Give co-located instances **different** passwords. A platform that templates one shared secret
across every Redis instance in a namespace gets none of this protection.

**Upgrading an existing instance.** Instances created before `masterName` existed keep running
on the historic shared value `mymaster` and surface a `SentinelMasterNameUnscoped` warning
condition:

```bash
kubectl get littlered store -o jsonpath='{.status.conditions[?(@.type=="SentinelMasterNameUnscoped")].message}'
```

Setting the field is a **client-visible change** — Sentinel-aware clients must be reconfigured
in the same maintenance window; clients using the label-routed `{name}` Service are unaffected.
There is no rolling cutover: monitoring one master under two names runs two independent failover
state machines that can promote different replicas. Since a coordinated client reconfiguration
is required anyway, enable authentication in the same window rather than paying for two
outages. Setting `masterName: mymaster` explicitly is accepted (a legacy client may hardcode it)
and silences the warning without changing behaviour or requiring any client change.

### Verify

```bash
# Check status
kubectl get littlered store

# Expected output:
# NAME       MODE       PHASE     READY   AGE
# store   sentinel   Running   3       2m

# Check all pods (3 Redis + 3 Sentinel)
kubectl get pods -l app.kubernetes.io/instance=store

# Expected:
# store-redis-0      2/2     Running
# store-redis-1      2/2     Running
# store-redis-2      2/2     Running
# store-sentinel-0   1/1     Running
# store-sentinel-1   1/1     Running
# store-sentinel-2   1/1     Running

# Check services
kubectl get svc -l app.kubernetes.io/instance=store

# Expected:
# store            ClusterIP   ...   6379/TCP,9121/TCP   (master)
# store-replicas   ClusterIP   ...   6379/TCP,9121/TCP   (all replicas)
# store-sentinel   ClusterIP   ...   26379/TCP           (sentinels)
```

### Check replication

```bash
# Query sentinel for master
kubectl exec -it store-sentinel-0 -- redis-cli -p 26379 SENTINEL get-master-addr-by-name apps.store

# Check master info
kubectl exec -it store-sentinel-0 -- redis-cli -p 26379 SENTINEL master apps.store

# Check replicas
kubectl exec -it store-sentinel-0 -- redis-cli -p 26379 SENTINEL replicas apps.store
```

### Test failover

```bash
# Find current master
kubectl get littlered store -o jsonpath='{.status.master.podName}'

# Kill the master pod
kubectl delete pod store-redis-0 --grace-period=0 --force

# Watch failover (new master elected in ~5-30 seconds)
kubectl get littlered store -w

# Verify new master
kubectl exec -it store-sentinel-0 -- redis-cli -p 26379 SENTINEL get-master-addr-by-name apps.store
```

### Connect from your application

For sentinel-aware clients:

```
Sentinel endpoints: store-sentinel.<namespace>.svc.cluster.local:26379
Master name:        <spec.sentinel.masterName>     # e.g. apps.store
```

The master name your client sends must match the CR exactly — read it from the spec rather
than assuming a convention:

```bash
kubectl get littlered store -o jsonpath='{.spec.sentinel.masterName}'
```

Changing `masterName` later requires reconfiguring these clients in the same window; there is
no overlap period during which both names resolve.

For simple clients (connects to current master):

```
store.<namespace>.svc.cluster.local:6379
```

---

## Cluster Mode

Horizontally scaled setup with automatic sharding across multiple master nodes. Data is distributed using hash slots (16384 total). No PersistentVolumes required - cluster state is stored in the CR status.

### Deploy

```yaml
# cluster.yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: cluster
  cluster:
    shards: 3           # Number of master nodes (minimum 3)
    replicasPerShard: 1 # Replicas per master (0 = no replicas)
```

```bash
kubectl apply -f cluster.yaml
```

### Verify

```bash
# Check status
kubectl get littlered store

# Expected output:
# NAME       MODE      PHASE     READY   AGE
# store   cluster   Running   6       2m

# Check all pods (3 masters + 3 replicas = 6 pods, spread across one
# StatefulSet per shard: store-shard-0, store-shard-1, store-shard-2)
kubectl get pods -l app.kubernetes.io/instance=store

# Expected (pod -K-0 is shard K's master, -K-1 its replica):
# store-shard-0-0   2/2     Running   (shard-0 master)
# store-shard-0-1   2/2     Running   (shard-0 replica)
# store-shard-1-0   2/2     Running   (shard-1 master)
# store-shard-1-1   2/2     Running   (shard-1 replica)
# store-shard-2-0   2/2     Running   (shard-2 master)
# store-shard-2-1   2/2     Running   (shard-2 replica)

# Check services
kubectl get svc -l app.kubernetes.io/instance=store

# Expected:
# store           ClusterIP   ...   6379/TCP,9121/TCP         (client access)
# store-cluster   ClusterIP   None  6379/TCP,16379/TCP        (headless for cluster)
```

### Check cluster health

```bash
# Cluster info
kubectl exec -it store-shard-0-0 -c redis -- redis-cli CLUSTER INFO

# Expected output includes:
# cluster_state:ok
# cluster_slots_assigned:16384
# cluster_slots_ok:16384
# cluster_known_nodes:6
# cluster_size:3

# Cluster nodes (shows all nodes with their roles and slots)
kubectl exec -it store-shard-0-0 -c redis -- redis-cli CLUSTER NODES

# Check slot distribution
kubectl exec -it store-shard-0-0 -c redis -- redis-cli CLUSTER SLOTS
```

### Check cluster state in CR status

```bash
# View stored cluster topology
kubectl get littlered store -o jsonpath='{.status.cluster}' | jq

# Expected output:
# {
#   "state": "ok",
#   "lastBootstrap": "2026-02-03T...",
#   "nodes": [
#     {"podName": "store-shard-0-0", "nodeId": "abc123...", "role": "master", "slotRanges": "0-5460"},
#     {"podName": "store-shard-0-1", "nodeId": "def456...", "role": "replica", "masterNodeId": "abc123..."},
#     ...
#   ]
# }
```

### Test recovery

```bash
# Delete a master pod (shard-0's master)
kubectl delete pod store-shard-0-0

# Watch the operator recover (new pod gets new node ID, operator re-adds slots)
kubectl logs -n littlered-system deployment/littlered-operator -f

# Verify cluster health from a surviving pod (shard-1's master)
kubectl exec -it store-shard-1-0 -c redis -- redis-cli CLUSTER INFO
```

### Connect from your application

Redis Cluster clients automatically discover all nodes:

```
Initial endpoint: store.<namespace>.svc.cluster.local:6379
```

Example with redis-cli:

```bash
# -c flag enables cluster mode (follows redirects)
kubectl exec -it store-shard-0-0 -c redis -- redis-cli -c SET mykey myvalue
kubectl exec -it store-shard-0-0 -c redis -- redis-cli -c GET mykey
```

### Slot distribution

With 3 shards, slots are distributed as:
- Shard 0 (`store-shard-0-0`): slots 0-5460
- Shard 1 (`store-shard-1-0`): slots 5461-10922
- Shard 2 (`store-shard-2-0`): slots 10923-16383

### Important notes

- **Supported topologies**: **3 or more shards** with **0 or more replicas per shard** (default: 3 shards, 1 replica). The minimum of 3 is a Redis Cluster requirement — it needs at least 3 masters. Both counts are validated (`shards ≥ 3`, `replicasPerShard ≥ 0`).
- **In-memory mode**: No persistence. Data will be lost on full cluster restart. By default, 'noeviction' is used, so data is not forgotten when memory is full (Redis will return errors instead).
- **No PVCs**: Cluster state stored in CR status, not nodes.conf.
- **Runtime scaling**: reducing `spec.cluster.shards` is refused (`ShardScaleDownRefused` — the operator never deletes data). Automated shard scale-up (resharding slots onto new shards) is not yet supported; treat the shard count as fixed after creation.

### Upgrading a pre-0.3 cluster

0.3.0 restructures cluster mode from a single StatefulSet (`{name}-cluster`, pods
`{name}-cluster-N`) into **one StatefulSet per shard** (`{name}-shard-K`, pods
`{name}-shard-K-M` where `-K-0` is the shard master and `-K-1…-K-R` its replicas), so
each shard's master and replica(s) can be pinned to separate failure domains (see
[shardAntiAffinity](#spreading-pods-across-nodes-and-failure-domains)).

**The migration is automatic, online, and data-safe — no action is required.** When the
upgraded operator finds a legacy `{name}-cluster` StatefulSet, it migrates the instance
in place to the per-shard layout, on the same running Redis Cluster:

- Each new per-shard pod joins as a **slot-less replica** of the legacy master that owns
  its range and full-syncs; then `{name}-shard-K-0` is promoted by a coordinated
  `CLUSTER FAILOVER` (an atomic ownership flip). Slots are never moved, so every slot
  keeps at least two live copies at every instant — **no data loss**, and no window where
  a shard serves from a single copy.
- **Client connection endpoints do not change.** The client Service `{name}` and the
  headless Service `{name}-cluster` are shard-agnostic and keep fronting the pods
  throughout, so applications are unaffected.
- Progress is reported on `status.cluster.migration` (`phase`, `shardsMoved`,
  `totalShards`) and a `Ready=False` / `MigrationInProgress` condition. The phases are
  `Standup → Meet → Replicate → Failover → Decommission → Complete`. When it reaches
  `Complete`, the operator removes the legacy `{name}-cluster` StatefulSet automatically
  and the instance returns to steady state.

What to keep in mind:

- **Only the workload and pod names change** (`{name}-cluster-N` → `{name}-shard-K-M`).
  Update anything that references them directly — scripts, dashboards, NetworkPolicies,
  `kubectl exec` one-liners.
- The migration is **shape-preserving**: it moves the *same* topology (same `shards`, same
  `replicasPerShard`) onto the new layout. It begins only once the legacy cluster is
  healthy (`cluster_state:ok`, all 16384 slots assigned, all legacy pods Ready, a reachable
  master quorum), and a non-shape-preserving legacy cluster is refused
  (`MigrationUnsupportedTopology`). Change `shards`/`replicasPerShard` only **after** the
  migration completes.
- Reducing `spec.cluster.shards` is always refused (`ShardScaleDownRefused`).

**Pausing the migration (opt-out).** To hold a legacy cluster in its current shape — for a
change-control window, say — set the annotation before (or during) the upgrade:

```bash
kubectl annotate littlered <instance> redis.chuck-chuck-chuck.net/migrate-legacy-sts=hold
```

While held, the operator makes **no changes** and surfaces a `MigrationHeld` condition.
Note the trade-off: holding also **suspends the operator's repair loop** for that instance,
so the legacy cluster keeps serving but is effectively **unmanaged** (no ghost-node healing,
no failover assistance) until you remove the annotation:

```bash
kubectl annotate littlered <instance> redis.chuck-chuck-chuck.net/migrate-legacy-sts-
```

Removing it lets the migration proceed to `Complete`.

---

## Custom Configuration

### With resources and memory policy

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: standalone

  resources:
    requests:
      cpu: "500m"       # CPU request only — set a request, not a limit
      memory: "1Gi"
    limits:
      memory: "1Gi"     # memory limit == request; no CPU limit (Burstable QoS)

  config:
    maxmemory: "900Mi"
    maxmemoryPolicy: noeviction
```

> **Why a CPU request but no CPU limit?** Redis's CPU usage is *bounded by its
> thread count*, not unbounded. Command execution runs on a single main thread;
> enabling `io-threads` adds a fixed number of I/O threads for socket
> reads/writes and protocol parsing (the `io-threads` count *includes* the main
> thread). So Redis will only ever saturate the cores its threads occupy —
> never more. Size the CPU **request** to that thread budget so the scheduler
> reserves the cores Redis can actually use, and set a memory limit equal to the
> memory request to bound the node (Burstable QoS).
>
> A CPU **limit**, by contrast, has no upside. Redis can't exceed its thread
> budget regardless, so a limit can only *throttle* it under load — and a
> throttled Redis just produces rising latency, piled-up requests, and client
> timeouts that cascade into every service that depends on it. Nobody benefits
> from that. Set an explicit CPU limit only if your platform mandates the
> Guaranteed QoS class.
>
> If you enable `io-threads` for higher throughput, raise the CPU request to
> match (e.g. `io-threads: 3` → request ~3–4 CPUs). On Redis 8, `io-threads`
> threads both reads and writes automatically — the old `io-threads-do-reads`
> setting is obsolete.

### With a PodDisruptionBudget

A PodDisruptionBudget (PDB) protects a multi-pod instance from losing too many
pods at once during voluntary disruptions (node drains, upgrades). PDB creation
is opt-in; enable it for any HA instance (sentinel or cluster):

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: sentinel
  sentinel:
    masterName: default.store   # required; unique per pod network
  podDisruptionBudget:
    create: true
    maxUnavailable: 1   # or set minAvailable instead — the two are mutually exclusive
```

> PDBs only make sense for instances with more than one pod. Do not enable one
> for a standalone instance or a cluster with `replicasPerShard: 0` — a
> single-pod PDB would block node drains entirely.

### Spreading pods across nodes and failure domains

`spec.podTemplate` passes the full set of Kubernetes pod-scheduling controls
through to the managed pods **verbatim** — `nodeSelector`, `tolerations`,
`affinity`, `priorityClassName`, and `topologySpreadConstraints`. There is no
LittleRed-specific placement DSL: you express placement with the native
Kubernetes primitives, which cover the full range of requirements (one pod per
node, spread across zones, dedicated node pools, and so on). The operator does
**not** inject a default anti-affinity or spread, and it does **not** augment
your `labelSelector` — what you write is what the pods get.

Select the instance's pods with `app.kubernetes.io/instance: <metadata.name>`.
Note that `app.kubernetes.io/name` is always the constant `littlered` (it is the
*application* name, not the instance name), so a selector keyed on it will match
every LittleRed pod in the cluster — use `app.kubernetes.io/instance`.

**One pod per node** (hard requirement):

```yaml
spec:
  podTemplate:
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: DoNotSchedule      # refuse to schedule rather than co-locate
        labelSelector:
          matchLabels:
            app.kubernetes.io/instance: store
```

**Spread across availability zones** — swap the topology key:

```yaml
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
        labelSelector:
          matchLabels:
            app.kubernetes.io/instance: store
```

Use `whenUnsatisfiable: ScheduleAnyway` for a soft preference (best-effort spread
that still schedules when domains run short), or a `requiredDuringScheduling`
`podAntiAffinity` on `kubernetes.io/hostname` for a strict one-per-node rule.

**Sentinel mode — master/replica domain diversity is fully covered.** The three
Redis data pods form a single StatefulSet; spreading all three across nodes or
zones guarantees the master and its two replicas land in different domains — and
that holds across failover, because a failover only changes *which* pod is master,
it never moves a pod. Apply the spread above to a `mode: sentinel` instance and
you are done.

**Cluster mode — per-shard isolation via `spec.placement.shardAntiAffinity`.**
The instance-wide `spec.podTemplate` constraint above spreads *all* of an
instance's cluster pods evenly across the chosen domains, but it cannot keep an
*individual shard's* master and replica(s) apart: a single shared `labelSelector`
selects every pod of the instance, not one shard's. As of 0.3.0 each shard is its
own StatefulSet (`{name}-shard-K`) whose pods carry a stable, schedule-time
`redis.chuck-chuck-chuck.net/shard` label — but that label is operator-owned, so
you cannot practically write a spread constraint against it yourself. The
`spec.placement.shardAntiAffinity` knob does it for you: the operator injects a
per-shard `topologySpreadConstraint` (`maxSkew: 1`, `labelSelector` scoped to that
shard's pods) into **each** shard StatefulSet, so a shard's master and replica(s)
never share the chosen failure domain.

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: cluster
  cluster:
    shards: 3
    replicasPerShard: 1
  placement:
    shardAntiAffinity:
      topologyKey: kubernetes.io/hostname   # spread a shard's pods across nodes
      whenUnsatisfiable: DoNotSchedule       # hard: refuse to co-locate a shard's pods
```

Two fields:

- **`topologyKey`** (default `kubernetes.io/hostname`) — the node label defining the
  failure domain to spread a shard's pods across. Use `topology.kubernetes.io/zone`
  to spread a shard's pods across availability zones instead of nodes.
- **`whenUnsatisfiable`** (default `ScheduleAnyway`) — `ScheduleAnyway` is a *soft*
  preference: best-effort spread that still schedules a shard's pods when domains
  run short (small/dev/single-node clusters still come up). `DoNotSchedule` is a
  *hard* guarantee that a shard's pods never co-locate — but it can leave pods
  `Pending` when there are fewer failure domains than a shard has pods (e.g.
  `DoNotSchedule` on `kubernetes.io/hostname` with `replicasPerShard: 1` needs at
  least 2 schedulable nodes per shard). The default is soft to match the
  "enable, don't force" philosophy; opt into `DoNotSchedule` for production.

The operator's per-shard constraint is **appended** to any
`spec.podTemplate.topologySpreadConstraints` you supply — both apply, so you can
still layer an instance-wide zone spread (or any other constraint) on top of the
shard-scoped one. `spec.placement.shardAntiAffinity` is **cluster mode only**;
validation rejects it in standalone and sentinel mode (where a single StatefulSet
already covers master/replica domain diversity — see above).

> **Known limitation.** There is no dedicated under-provisioning status condition
> yet — the operator does not count cluster nodes or failure domains. With
> `DoNotSchedule` and too few domains you will see standard `Pending` pods,
> surfaced through the CR's readiness (`status.redis.ready < total`, phase
> `Initializing`), not a "not enough domains" message. What still cannot be pinned
> by scheduling is *role* placement (master vs. replica) within a shard: pod `-K-0`
> starts as the shard master, but roles change on failover, which pod-scheduling
> constraints cannot track. Even spread across domains remains a strong baseline.

### With authentication

In sentinel mode, authentication is **strongly recommended** and is not only a client-edge
control: the same password is Sentinel's peer-membership credential, so it also stops a foreign
Sentinel deployment on the same pod network from talking to yours, and stops your replicas
completing a sync against a foreign master. See "Isolating Sentinel instances" above — it is
the only thing that closes the address-adoption path, which a unique master name does not.
Give co-located instances **different** passwords; one shared platform secret gives no
isolation.

```bash
# Create password secret
kubectl create secret generic redis-password --from-literal=password=mysecretpassword
```

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: standalone
  auth:
    enabled: true
    existingSecret: redis-password
```

```bash
# Connect with password
kubectl exec -it store-redis-0 -c redis -- redis-cli -a mysecretpassword PING
```

### With ServiceMonitor (Prometheus)

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: standalone
  metrics:
    enabled: true
    serviceMonitor:
      enabled: true
      labels:
        release: prometheus  # Match your Prometheus operator selector
```

### Production sentinel setup

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: prod-cache
spec:
  mode: sentinel

  podDisruptionBudget:
    create: true
    maxUnavailable: 1

  resources:
    requests:
      cpu: "1"
      memory: "2Gi"
    limits:
      memory: "2Gi"     # memory limit == request; no CPU limit (Burstable QoS)

  config:
    maxmemory: "1800Mi"
    maxmemoryPolicy: noeviction

  auth:
    enabled: true
    existingSecret: redis-password

  metrics:
    enabled: true
    serviceMonitor:
      enabled: true
      labels:
        release: prometheus

  sentinel:
    masterName: default.prod-cache   # required; unique per pod network
    # quorum defaults to 2 — only set it if you need a different value
    downAfterMilliseconds: 5000
    failoverTimeout: 60000

  podTemplate:
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: DoNotSchedule
        labelSelector:
          matchLabels:
            app.kubernetes.io/instance: prod-cache
```

### Production cluster setup

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: prod-cluster
spec:
  mode: cluster

  cluster:
    shards: 3
    replicasPerShard: 1
    clusterNodeTimeout: 15000

  podDisruptionBudget:
    create: true
    maxUnavailable: 1

  resources:
    requests:
      cpu: "1"
      memory: "2Gi"
    limits:
      memory: "2Gi"     # memory limit == request; no CPU limit (Burstable QoS)

  config:
    maxmemory: "1800Mi"
    maxmemoryPolicy: noeviction

  auth:
    enabled: true
    existingSecret: redis-password

  metrics:
    enabled: true
    serviceMonitor:
      enabled: true
      labels:
        release: prometheus

  podTemplate:
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: DoNotSchedule
        labelSelector:
          matchLabels:
            app.kubernetes.io/instance: prod-cluster
```

---

## Rolling Restarts and Updates

### Overview

When you update a LittleRed resource (e.g., change image version, resource limits, or configuration), the operator triggers a rolling restart of the StatefulSet. The `minReadySeconds` setting controls how long to wait after each pod becomes ready before restarting the next pod.

### Why minReadySeconds Matters

For Redis deployments with high availability (replicas or sentinel), restarting pods too quickly can cause:

- **Cluster mode**: Multiple masters down simultaneously, losing quorum
- **Sentinel mode**: Master restarts before sentinel detects failure and promotes a replica
- **Data unavailability**: Clients can't reach Redis during cascading failures

The `minReadySeconds` setting ensures:
1. Pod is fully ready and healthy
2. Failover (if needed) completes successfully
3. Cluster stabilizes before next pod restarts

### Default Behavior

| Mode | Default minReadySeconds | Reason |
|------|------------------------|--------|
| Cluster with replicas | 30s | Allows automatic failover (cluster-node-timeout + promotion + buffer) |
| Sentinel mode | 35s | Allows sentinel-managed failover (down-after-milliseconds + promotion) |
| Standalone or 0-replica | 0s | No failover mechanism, immediate restart is safe |

### Performing a Rolling Restart

#### Trigger via kubectl

```bash
# Standalone / sentinel: a single StatefulSet
kubectl rollout restart statefulset <name> -n <namespace>

# Cluster mode is one StatefulSet PER SHARD ({name}-shard-K). Roll them one at a time,
# waiting for each to settle, so you never restart two shard masters at once:
kubectl rollout restart statefulset my-cluster-shard-0 -n default
kubectl rollout status  statefulset my-cluster-shard-0 -n default
# …then my-cluster-shard-1, my-cluster-shard-2, and so on.
```

> **Prefer a CR update for cluster mode.** When a pod-template change arrives through the CR
> (below), the operator rolls the shard StatefulSets **one at a time**, waiting for each to
> settle before the next — so only one shard master restarts at a time. A manual
> `kubectl rollout restart` of every shard StatefulSet at once bypasses that serialization.

#### Trigger via CR Update

Change any field that affects the pod template (image, resources, config):

```bash
kubectl patch littlered my-cluster -n default --type merge -p '
spec:
  resources:
    requests:
      memory: "256Mi"
'
```

#### Monitor Rollout Status

```bash
# Watch rollout progress (cluster mode: one StatefulSet per shard, check each)
kubectl rollout status statefulset my-cluster-shard-0 -n default

# Check pod restarts
kubectl get pods -n default -l app.kubernetes.io/instance=my-cluster -w
```

### Customizing minReadySeconds

If you need faster or slower rolling restarts, you can override the default:

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: my-cluster
spec:
  mode: cluster
  cluster:
    shards: 3
    replicasPerShard: 1
  updateStrategy:
    type: RollingUpdate
    minReadySeconds: 45  # Wait 45s between pod restarts
```

**Use cases for customization:**

- **Faster restarts** (e.g., `minReadySeconds: 15`): For clusters with very short cluster-node-timeout or when you've verified failover completes quickly
- **Slower restarts** (e.g., `minReadySeconds: 60`): For large clusters with many keys where replication catch-up takes time
- **Testing** (`minReadySeconds: 0`): For development environments where you want rapid rollouts

**Valid range:** 0-300 seconds

### Example: Safe Production Rolling Restart

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: prod-cluster
spec:
  mode: cluster
  cluster:
    shards: 5
    replicasPerShard: 2
    clusterNodeTimeout: 15000  # 15s default
  updateStrategy:
    type: RollingUpdate
    minReadySeconds: 40  # Conservative: 15s + 10s promotion + 15s buffer
  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      memory: "1Gi"     # memory limit == request; no CPU limit (Burstable QoS)
```

**Rollout timeline for this config:**
- Pod 0 terminates, new pod 0 starts
- New pod 0 becomes ready
- **Wait 40 seconds** (minReadySeconds)
- Repeat for pod 1, 2, 3, etc.
- Total time: ~(40s × 10 pods) = 6-7 minutes for full rollout

### Testing Rolling Restarts

You can test rolling restart behavior with the e2e test suite:

```bash
DEBUG_ON_FAILURE=true make test-e2e-cluster-chaos
```

The `should maintain data integrity during rolling restart` test verifies:
- 0 data corruptions
- ≥95% read availability during rollout
- All slots remain assigned (cluster_slots_assigned:16384)

---

## High-Throughput Tuning (I/O threads)

By default Redis runs its network I/O on the main thread. On a busy instance
that main thread can become the bottleneck long before memory or the network
does. Enabling **I/O threads** lets Redis offload socket reads, writes, and
protocol parsing to additional threads, while command execution stays on the
main thread (so atomicity is preserved). This is a *vertical* throughput lever
and is independent of mode — it applies to standalone, sentinel, and cluster
instances alike.

Enable it via `config.raw` and size the CPU **request** to match the thread
count:

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: cluster
  cluster:
    shards: 3
    replicasPerShard: 1

  resources:
    requests:
      cpu: "4"          # ~ io-threads budget (incl. main) + 1 spare core
      memory: "8Gi"
    limits:
      memory: "8Gi"     # memory limit == request; deliberately NO CPU limit

  config:
    maxmemory: "6gb"    # leave headroom below the memory limit
    raw: |-
      # I/O threads: offload socket reads/writes + protocol parsing.
      # The count INCLUDES the main thread, so "3" = main + 2 helpers.
      # Redis docs suggest (cores - 1); leave at least one spare core.
      io-threads 3

      # Accept-queue depth for bursty connection storms (default 511). The
      # kernel caps this at net.core.somaxconn, so raising it only helps if the
      # node's sysctl is also raised; otherwise the effective backlog is
      # somaxconn. Cheap to set, so a higher ceiling is "free".
      tcp-backlog 4096
```

**Sizing rule of thumb:**

| `io-threads` | Threads doing I/O (incl. main) | Suggested CPU request |
|--------------|-------------------------------|-----------------------|
| unset / `1`  | 1 (main only)                 | ~1 (the default `128m`–`1` range) |
| `3`          | 3                             | ~3–4 (thread budget + a spare core) |
| `7`          | 7                             | ~7–8 |

> **Why no CPU limit here either?** The whole point of I/O threads is to let
> Redis saturate the cores you've given it under peak load. A CPU limit would
> throttle exactly the workload you enabled threading for — turning a
> throughput win into latency and timeouts. Set the CPU *request* to the thread
> budget so the scheduler reserves those cores, and leave the limit off.

**Version note:** On Redis 8+, enabling `io-threads` threads both reads and
writes automatically. The Redis 6.x/7.x `io-threads-do-reads` toggle is obsolete
on Redis 8 and should be omitted — reads are always threaded when `io-threads > 1`.

**When to reach for this:** only when an instance is actually CPU-bound on
network I/O (high ops/sec, large values, or TLS). For most instances the
single-threaded default is faster to reason about and wastes no reserved cores.

---

## Large-Scale Tuning

For installations with hundreds or thousands of instances, you can tune the reconciliation frequency to reduce pressure on the Kubernetes API server and the Redis nodes.

| Field | Default | Description |
|-------|---------|-------------|
| `requeueIntervals.fast` | `2s` | Interval used during initialization, recovery, or when the system is not 'Running'. |
| `requeueIntervals.steadyState` | `30s` | Interval used for periodic health checks once the system is stable ('Running'). |

**Example:**

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: tuned-cache
spec:
  mode: sentinel
  sentinel:
    masterName: default.tuned-cache   # required; unique per pod network
  requeueIntervals:
    fast: "10s"
    steadyState: "1m"
```

---

## Cleanup

```bash
# Delete the LittleRed resource (cleans up all managed resources)
kubectl delete littlered store

# Verify cleanup
kubectl get pods -l app.kubernetes.io/instance=store
# No resources found
```

---

## Troubleshooting

### Check operator logs

```bash
kubectl logs -n littlered-system deployment/littlered-operator
```

### Check LittleRed status and conditions

```bash
kubectl describe littlered store
```

### Check pod events

```bash
kubectl describe pod store-redis-0
```

### Redis logs

```bash
kubectl logs store-redis-0 -c redis
```

### Sentinel logs

```bash
kubectl logs store-sentinel-0
```

### Recovering a sentinel instance captured by another Sentinel deployment

**Symptoms.** The instance sits at `Ready=False` / `phase: Initializing` and `status.master` is
empty. `lrctl verify` names it directly:

```bash
lrctl verify store -n apps
# Sentinel Identity:
#   Master name: mymaster
#   [WARN] This is the historic shared default. ...
#   [FAIL] Evidence of another Sentinel deployment sharing this master name:
#          - monitored master is not one of this instance's pods, and is alive: 10.9.9.9
#          - <pod> reports 8 other sentinels; 2 were deployed
```

Note what that check can and cannot tell you: it reports evidence, and a clean result means
"nothing visible from this vantage", not "isolated" — a deployment yours has not merged with is
invisible by construction.

By hand, the same signals are in the Sentinel reply — a master IP that is **not one of its own
pods**, and more sentinels or replicas than you deployed:

```bash
kubectl exec store-sentinel-0 -c sentinel -- redis-cli -p 26379 SENTINEL master <masterName>
# ip:                  <an address that is not one of this instance's Redis pods>
# num-other-sentinels: 8      <-- you deployed 3, so 2 is the expected value
# num-slaves:          6      <-- you deployed 2
# flags:               master <-- and it looks healthy, so Sentinel will never fail over
```

The Redis pods will be `role:slave` pointing at that foreign address, and their logs will show
repeated `MASTER <-> REPLICA sync: Flushing old data`.

**This means another Sentinel deployment sharing your master name has taken over the instance's
topology.** See "Isolating Sentinel instances" above for how it happens and how to prevent it.

**The data is already gone.** The flush happens about a second after the takeover, long before
anything can react. Recovery restores service on an empty instance — it does not restore data.
The operator deliberately does **not** attempt this automatically: an operator-issued
`SENTINEL MONITOR` starts at config epoch 0 and loses to the other deployment's epoch within
seconds, so it could only loop, and each attempt would wipe the Sentinel replica list.

**Recovery.** Because the outcome is an empty instance either way, the simplest correct fix is to
delete and recreate the CR with a unique `masterName` (and auth) — do that if you can. If you must
repair in place, first make sure the other deployment can no longer reach yours, or it will simply
take over again:

```bash
# 1. Stop the operator fighting the manual repair.
kubectl scale -n littlered-system deployment/littlered-operator --replicas=0

# 2. Point every Sentinel back at this instance's own master.
MASTER_IP=$(kubectl get pod store-redis-0 -o jsonpath='{.status.podIP}')
for i in 0 1 2; do
  kubectl exec store-sentinel-$i -c sentinel -- \
    redis-cli -p 26379 SENTINEL REMOVE <masterName>
  kubectl exec store-sentinel-$i -c sentinel -- \
    redis-cli -p 26379 SENTINEL MONITOR <masterName> $MASTER_IP 6379 2
done

# 3. Promote that pod.
kubectl exec store-redis-0 -c redis -- redis-cli REPLICAOF NO ONE

# 4. Bring the operator back; it relabels and repoints the remaining replicas.
kubectl scale -n littlered-system deployment/littlered-operator --replicas=1
```

**Then fix the cause**, or it recurs on the next pod-IP recycle: give the instance a unique
`masterName` and enable authentication with a password not shared with the neighbouring instance.

### Cluster diagnostics

```bash
# Check cluster state
kubectl exec -it store-shard-0-0 -c redis -- redis-cli CLUSTER INFO

# Check for failed slots
kubectl exec -it store-shard-0-0 -c redis -- redis-cli CLUSTER SLOTS

# View cluster topology
kubectl exec -it store-shard-0-0 -c redis -- redis-cli CLUSTER NODES

# Check stored cluster state in CR
kubectl get littlered store -o jsonpath='{.status.cluster.state}'

# Force cluster re-bootstrap (if cluster is broken)
# 1. Delete all cluster pods
kubectl delete pods -l app.kubernetes.io/instance=store,app.kubernetes.io/component=cluster
# 2. Clear stored state by patching CR
kubectl patch littlered store --type=merge -p '{"status":{"cluster":null}}'
# 3. Operator will re-bootstrap on next reconcile
```
