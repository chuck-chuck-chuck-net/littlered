# LittleRed

A Kubernetes operator for deploying Redis/Valkey as a pure in-memory data store.

LittleRed is built for workloads where persistence is explicitly disabled and never enabled—not even by accident. It provides a full reconciliation engine to manage node identities and cluster membership across restarts and failures: the class of problem where static Helm charts and startup scripts reach their limits.

## Upgrading to v0.3.1

### Sentinel mode: `spec.sentinel.masterName` is now required

We had reports of *Sentinel collisions*: pod restarts on recycled IPs led two unrelated Sentinel instances to merge into one, losing both. A unique `masterName` — use `<namespace>.<name>` — closes most of those cases. Authentication closes the rest, which is why we now **strongly recommend authentication in sentinel mode**; see [Isolating Sentinel instances](docs/USAGE.md#isolating-sentinel-instances) for which cases need which, and `docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md` for the full analysis.

**Existing instances keep running.** The requirement is enforced on new instances; an existing one is only forced to state a value on its next change to `spec.sentinel`, and reports a `SentinelMasterNameUnscoped` warning until then.

Note that `masterName` is part of the configuration of Sentinel-aware clients. If you give an instance a new, unique master name — and enable authentication — you must reconfigure those clients in the same maintenance window. Clients that reach the master through the `{name}` Service are unaffected.

### New `failover` mode (experimental)

Same use case as sentinel mode — a highly available, replicated Redis instance — but managed directly by LittleRed instead of by Sentinel processes. The goal is to explore the robustness of that design, which avoids the "two brains" problem of running Sentinel under LittleRed: two independent failure detectors deciding about the same state. See [docs/RECONCILIATION_LOOP_FAILOVER.md](docs/RECONCILIATION_LOOP_FAILOVER.md).

Failover mode is **experimental**. Its e2e coverage is on parity with sentinel mode, but it has not seen real-world usage yet.

## Upgrading to v0.3.0 (Cluster Mode)

> v0.3.0 restructures **cluster mode** from a single StatefulSet (`{name}-cluster`) into **one StatefulSet per shard** (`{name}-shard-K`, with pods `{name}-shard-K-0` … `-K-R`), so each shard's master and replica(s) can be pinned to separate failure domains. **`standalone` and `sentinel` instances are unaffected.**
>
> **Existing cluster instances migrate automatically — no action required.** When the upgraded operator finds a pre-0.3 `{name}-cluster` StatefulSet, it performs an **online, in-place, data-safe** migration to the per-shard layout: no delete-and-recreate, no data loss, and no change to client connection endpoints (the `{name}` client Service and the `{name}-cluster` headless Service are unchanged). It replicates each shard onto its new pods and hands ownership over with a coordinated failover, so every slot keeps at least two live copies throughout. Progress shows on `status.cluster.migration` and a `Ready=False` / `MigrationInProgress` condition until it reaches `Complete`.
>
> Only the **workload and pod names change** (`{name}-cluster-N` → `{name}-shard-K-M`) — update anything that references them directly (scripts, dashboards, NetworkPolicies). To pause the migration for a maintenance window, set the annotation `redis.chuck-chuck-chuck.net/migrate-legacy-sts: hold` before upgrading. See [Upgrading a pre-0.3 cluster](docs/USAGE.md#upgrading-a-pre-03-cluster).
>
> **New:** per-shard failure-domain isolation via `spec.placement.shardAntiAffinity` (spread each shard's master and replica(s) across nodes/zones). See [USAGE.md](docs/USAGE.md).

## Quick Start

### 1. Install the Operator

```bash
helm upgrade --install littlered oci://ghcr.io/littlered-operator/charts/littlered \
  -n littlered-system --create-namespace
```

This installs the latest release. For a pinned version, add `--version <version>` — see the [releases page](https://github.com/littlered-operator/littlered-operator/releases).

By default the operator is cluster-scoped (watches all namespaces). To scope it to specific namespaces — for multi-tenancy, least-privilege RBAC, or running two operators side by side — see [Namespace Scoping](docs/USAGE.md#namespace-scoping).

### 2. Deploy an Instance

Set `spec.mode` to choose your deployment type:

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: my-store
spec:
  mode: cluster   # standalone | sentinel | cluster | failover (experimental)
  cluster:
    shards: 3            # minimum 3
    replicasPerShard: 1  # 3 x (1 master + 1 replica) = 6 pods
```

```bash
kubectl apply -f my-store.yaml
```

**Choosing a mode.** `cluster` runs a sharded Redis Cluster — horizontal scaling, and each shard survives losing its master. `standalone` is a single Redis pod, for development and for caches that can be rebuilt. `sentinel` runs 3 Redis pods (1 master + 2 replicas) monitored by 3 sentinels; it needs a unique [`spec.sentinel.masterName`](docs/USAGE.md#isolating-sentinel-instances). `failover` (**experimental**) is 1 master + N replicas with no Sentinel processes, the operator deciding failover itself — see [docs/RECONCILIATION_LOOP_FAILOVER.md](docs/RECONCILIATION_LOOP_FAILOVER.md).

Every mode takes the same options for resources, authentication, TLS, metrics, placement and disruption budgets — **[USAGE.md](docs/USAGE.md) is the guide to all of them**, and worth a look before your first production instance.

### 3. Verify Health

```bash
# Install the CLI as a kubectl plugin
make install-plugin

# Check cluster consistency
kubectl lr verify my-store
```

## Key Features

- **Three deployment modes**: `standalone` (single pod), `sentinel` (1 master + 2 replicas monitored by 3 sentinels for automatic failover), and `cluster` (sharded Redis Cluster for horizontal scaling) — plus an **experimental** fourth, `failover` (operator-managed HA without Sentinel).
- **Redis 8.4.2 by default**, compatible with Redis 7.2+.
- **Burstable QoS by default**: memory limit equals request (preventing OOM surprises); a CPU *request* but no CPU *limit*. Redis's CPU use is bounded by its thread count, so a limit can only throttle it under load — size the request to the thread budget instead. Set an explicit CPU limit only if you need Guaranteed QoS.
- **`noeviction` by default**: memory exhaustion returns an error rather than silently dropping data. Explicitly configure a different policy if you need eviction semantics.
- **Per-shard failure-domain isolation (cluster mode)**: `spec.placement.shardAntiAffinity` spreads each shard's master and replica(s) across nodes/zones, so losing a single failure domain can't take out a whole shard.
- **Security**: password authentication and TLS encryption, both via Kubernetes Secrets.
- **Observability**: `redis_exporter` sidecar included by default, with optional `ServiceMonitor` for Prometheus.
- **`lrctl`**: a CLI tool (installable as a `kubectl lr` plugin) for direct state inspection and verification.

> **Current scope:** Cluster mode supports **3 or more shards** with configurable replicas per shard (default: 3 shards, 1 replica per shard).

## Configuration Reference

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: my-store
spec:
  mode: standalone          # standalone | sentinel | cluster | failover (experimental)

  image:
    registry: docker.io
    path: library/redis
    tag: "8.4.2"
    pullPolicy: IfNotPresent

  resources:
    requests: { cpu: 128m, memory: 512Mi }
    limits:   { memory: 512Mi }

  config:
    maxmemoryPolicy: noeviction
    tcpKeepalive: 300
    timeout: 0

  # Mode-specific settings
  sentinel:                 # For mode: sentinel
    masterName: prod.my-store   # REQUIRED; unique per pod network
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 60000

  cluster:                  # For mode: cluster
    shards: 3
    replicasPerShard: 1

  failover:                 # For mode: failover (experimental)
    replicas: 2
    downAfterMilliseconds: 5000

  # Security
  auth:
    enabled: false
    existingSecret: ""      # Secret must have a 'password' key

  tls:
    enabled: false
    existingSecret: ""      # Secret must have 'tls.crt' and 'tls.key'
    caCertSecret: ""        # Optional: separate Secret with 'ca.crt'
    clientAuth: false       # Require client certificates

  # Observability
  metrics:
    enabled: true
    serviceMonitor:
      enabled: false
```

Full field reference: [docs/API_SPEC.md](docs/API_SPEC.md).

## Why LittleRed?

Running Redis or Valkey without persistence creates a lifecycle problem that standard tooling doesn't handle well.

**The risk:** When a non-persistent node restarts, it comes back with an empty dataset. If it returns with its previous identity—same IP or hostname—the cluster may accept it as the authoritative source of truth and trigger a full sync from it, wiping data on healthy replicas.

**The solution:** LittleRed treats every restart as a new entity. It tracks node identities in Kubernetes, not inside Redis. When a pod disappears and a replacement arrives, the operator:

1. Removes the stale identity ("ghost node") from the cluster before the replacement joins.
2. Waits for any in-progress replica promotion to complete before healing the partition.
3. Intervenes with a forced promotion only when the cluster cannot self-recover (e.g., quorum loss).

The core principle is **minimal interference**: trust Sentinel and Cluster Gossip to handle their own state transitions, and intervene only when the cluster lacks the context to heal itself—specifically when it cannot see what the Kubernetes API already knows (that a pod is gone for good).

## Documentation

- [Usage Guide](docs/USAGE.md) — deployment examples for all modes
- [API Reference](docs/API_SPEC.md) — full spec field documentation
- [Architecture](docs/ARCHITECTURE.md) — reconciliation design, ADRs, and [terminology conventions](docs/ARCHITECTURE.md#terminology)
- [E2E Testing](docs/E2E_TESTING.md) — running the test suite and manual chaos testing
- [Test Cases](docs/TEST_CASES.md) — full list of covered scenarios and their status
- [CLI Reference](docs/LRCTL.md) — `lrctl` / `kubectl lr` guide
- [Development Guide](docs/DEVELOPMENT.md) — building from source, custom registry, local Kind workflow

## Behind the Name

The name **LittleRed** is an homage to the fable of the *Little Red Hen*. When we searched for a tool that handled the complexities of pure in-memory Redis lifecycles with technical rigor, we found that the existing solutions were often unmaintained or focused on different problems. Like the hen in the story, we decided to build it ourselves.

## License

Apache License 2.0
