# LittleRed - API Specification

> Detailed Custom Resource Definition schema for LittleRed.

**Document Status**: Active
**Last Updated**: 2026-08-01
**API Version**: `redis.chuck-chuck-chuck.net/v1alpha1`

---

## 1. Resource Overview

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  # ... specification
status:
  # ... status
```

---

## 2. Spec Fields

### 2.1 Core Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `mode` | `string` | No | `standalone` | Deployment mode: `standalone`, `sentinel`, `cluster`, or `failover` (**experimental** — operator-managed HA without Sentinel, see §2.12) |

### 2.2 Image Configuration

Image is composed from three parts: `{registry}/{path}:{tag}`

```yaml
spec:
  image:
    registry: docker.io         # Registry hostname
    path: library/redis         # Image path (without registry or tag)
    tag: "8.4.2"               # Version tag

    pullPolicy: IfNotPresent
    pullSecrets:
      - name: my-registry-secret
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image.registry` | `string` | No | `docker.io` | Registry hostname |
| `image.path` | `string` | No | `library/redis` | Image path (e.g., `library/redis`, `valkey/valkey`) |
| `image.tag` | `string` | No | `8.4.2` | Image version tag |
| `image.pullPolicy` | `string` | No | `IfNotPresent` | `Always`, `IfNotPresent`, `Never` |
| `image.pullSecrets` | `[]LocalObjectReference` | No | `[]` | Pull secret references |

**Resulting image**: `{registry}/{path}:{tag}`

**Default**: `docker.io/library/redis:8.4.2`

**Examples**:

```yaml
# Default: docker.io/library/redis:8.4.2
spec:
  image: {}

# Use Valkey instead of Redis
spec:
  image:
    path: valkey/valkey
    # Result: docker.io/valkey/valkey:8.4.2

# Use a registry mirror (only change registry, keep path)
spec:
  image:
    registry: docker.io
    # Result: docker.io/library/redis:8.4.2

# Mirror + Valkey
spec:
  image:
    registry: docker.io
    path: valkey/valkey
    # Result: docker.io/valkey/valkey:8.4.2

# Different version
spec:
  image:
    tag: "7.2"
    # Result: docker.io/library/redis:7.2

# Harbor proxy cache (different path structure)
spec:
  image:
    registry: harbor.internal
    path: dockerhub-proxy/library/redis
    # Result: harbor.internal/dockerhub-proxy/library/redis:8.4.2
```

**Why fully qualified by default?**

Unqualified images (like `redis` without a registry) are deprecated in modern container runtimes (CRI-O, containerd). Using `docker.io` explicitly ensures portability across all Kubernetes environments.

### 2.3 Redis Configuration

```yaml
spec:
  config:
    # Typed, validated fields
    maxmemory: "1Gi"            # Memory limit for Redis
    maxmemoryPolicy: noeviction # Eviction policy
    timeout: 0                   # Client timeout (0 = disabled)
    tcpKeepalive: 300           # TCP keepalive interval

    # Raw config passthrough (expert mode)
    raw: |
      hz 10
      dynamic-hz yes
      slowlog-log-slower-than 10000
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `config.maxmemory` | `string` | No | (from resources) | Redis maxmemory (e.g., `1Gi`, `512Mi`) |
| `config.maxmemoryPolicy` | `string` | No | `noeviction` | Eviction policy |
| `config.timeout` | `int` | No | `0` | Client idle timeout (seconds, 0=disabled) |
| `config.tcpKeepalive` | `int` | No | `300` | TCP keepalive (seconds) |
| `config.raw` | `string` | No | `""` | Raw redis.conf lines (expert mode) |

**Valid `maxmemoryPolicy` values**:
- `noeviction` - Return errors when memory limit reached (default)
- `allkeys-lru` - Evict any key using LRU
- `allkeys-lfu` - Evict any key using LFU
- `allkeys-random` - Evict any key randomly
- `volatile-lru` - Evict keys with TTL using LRU
- `volatile-lfu` - Evict keys with TTL using LFU
- `volatile-random` - Evict keys with TTL randomly
- `volatile-ttl` - Evict keys with shortest TTL

**Persistence Behavior**:

The operator actively disables persistence by default to ensure pure in-memory performance:
```
save ""           # No RDB snapshots
appendonly no     # No AOF
```

This means:
- No PersistentVolumeClaims are created
- No disk I/O for persistence
- Pod restart = clean slate (by design)

If you need persistence, you can override via `spec.config.raw`, but you're responsible for providing appropriate storage. The operator won't create PVCs for you.

### 2.4 Resources

```yaml
spec:
  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      memory: "1Gi"     # memory limit == request; no CPU limit (Burstable QoS)
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `resources` | `corev1.ResourceRequirements` | No | See below | CPU/memory for Redis container |

**Default resources:**
```yaml
resources:
  requests:
    cpu: "128m"
    memory: "512Mi"
  limits:
    memory: "512Mi"
```

**QoS convention**: LittleRed sets a memory limit equal to the memory request and deliberately omits the CPU limit, placing pods in the **Burstable** QoS class. Redis's CPU consumption is *bounded by its thread count* — one main thread for command execution, plus (if `io-threads` is enabled) a fixed number of I/O threads for socket reads/writes and protocol parsing, the count of which includes the main thread. Redis therefore never uses more CPU than its threads occupy. Size the CPU *request* to that thread budget so the scheduler reserves those cores. Do not set a CPU *limit*: Redis can't exceed its thread budget anyway, so a limit can only throttle it under load, turning into latency spikes, request pile-up, and downstream timeouts. Set an explicit CPU limit only if your platform mandates the Guaranteed QoS class.

**Behavior**: If `config.maxmemory` is not set, the operator auto-calculates it as ~90% of `resources.limits.memory` to leave headroom for Redis overhead (buffers, connections, etc.). User can always override with explicit value.

### 2.5 Authentication

```yaml
spec:
  auth:
    enabled: false              # Enable password auth
    existingSecret: ""          # Secret name (must have 'password' key)
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `auth.enabled` | `bool` | No | `false` | Enable password authentication |
| `auth.existingSecret` | `string` | Conditional | `""` | Secret name containing `password` key |

**Validation**:
- If `auth.enabled=true`, `existingSecret` is required
- Inline passwords are not supported (must use Secret)

### 2.6 TLS

```yaml
spec:
  tls:
    enabled: false              # Enable TLS
    existingSecret: ""          # Secret with tls.crt, tls.key
    caCertSecret: ""            # Optional: Secret with ca.crt
    clientAuth: false           # Require client certificates
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `tls.enabled` | `bool` | No | `false` | Enable TLS encryption |
| `tls.existingSecret` | `string` | Conditional | `""` | Secret with `tls.crt` and `tls.key` |
| `tls.caCertSecret` | `string` | No | `""` | Secret with `ca.crt` for client verification |
| `tls.clientAuth` | `bool` | No | `false` | Require client certificate authentication |

**Validation**:
- If `tls.enabled=true`, `existingSecret` is required
- If `tls.clientAuth=true`, `caCertSecret` is required

### 2.7 Metrics

```yaml
spec:
  metrics:
    enabled: true               # Enable metrics exporter
    exporter:
      # Same pattern as main image: {registry}/{path}:{tag}
      registry: ""              # Empty = inherit from spec.image.registry
      path: oliver006/redis_exporter
      tag: v1.89.0
      resources:
        requests:
          cpu: "50m"
          memory: "64Mi"
        limits:
          memory: "64Mi"    # memory limit == request; no CPU limit
    serviceMonitor:
      enabled: false            # Create ServiceMonitor CR
      namespace: ""             # Override namespace (default: same as CR)
      labels: {}                # Additional labels for ServiceMonitor
      interval: "30s"           # Scrape interval
      scrapeTimeout: "10s"      # Scrape timeout
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `metrics.enabled` | `bool` | No | `true` | Enable redis_exporter sidecar |
| `metrics.exporter.registry` | `string` | No | (inherit) | Registry; empty = use `spec.image.registry` |
| `metrics.exporter.path` | `string` | No | `oliver006/redis_exporter` | Image path |
| `metrics.exporter.tag` | `string` | No | `v1.89.0` | Image tag |
| `metrics.exporter.resources` | `ResourceRequirements` | No | See above | Exporter container resources |
| `metrics.serviceMonitor.enabled` | `bool` | No | `false` | Create ServiceMonitor |
| `metrics.serviceMonitor.namespace` | `string` | No | `""` | ServiceMonitor namespace |
| `metrics.serviceMonitor.labels` | `map[string]string` | No | `{}` | Additional labels |
| `metrics.serviceMonitor.interval` | `string` | No | `30s` | Scrape interval |
| `metrics.serviceMonitor.scrapeTimeout` | `string` | No | `10s` | Scrape timeout |

**Registry inheritance**: If `metrics.exporter.registry` is empty, it inherits from `spec.image.registry`. This means setting a registry mirror once applies to all images:

```yaml
spec:
  image:
    registry: docker.io   # Mirror for all images
  # Exporter automatically uses: docker.io/oliver006/redis_exporter:v1.89.0
```

### 2.8 Update Strategy

```yaml
spec:
  updateStrategy:
    type: RollingUpdate         # Or Recreate
    minReadySeconds: 30         # Wait time before next pod restart (optional)
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `updateStrategy.type` | `string` | No | `RollingUpdate` | `RollingUpdate` or `Recreate` |
| `updateStrategy.minReadySeconds` | `int32` | No | Mode-dependent | Minimum seconds a pod must be ready before the next pod is restarted. **Cluster mode with replicas**: defaults to 30s to allow automatic failover. **Sentinel mode**: defaults to 35s for sentinel-managed failover. **Standalone/0-replica**: defaults to 0s. Range: 0-300. |

### 2.9 Service Configuration

```yaml
spec:
  service:
    type: ClusterIP             # Service type
    annotations: {}             # Service annotations
    labels: {}                  # Additional labels
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `service.type` | `string` | No | `ClusterIP` | `ClusterIP`, `NodePort`, `LoadBalancer` |
| `service.annotations` | `map[string]string` | No | `{}` | Annotations |
| `service.labels` | `map[string]string` | No | `{}` | Additional labels |

### 2.10 Pod Template

```yaml
spec:
  podTemplate:
    annotations: {}             # Pod annotations
    labels: {}                  # Additional pod labels
    nodeSelector: {}            # Node selector
    tolerations: []             # Tolerations
    affinity: {}                # Affinity rules
    priorityClassName: ""       # Priority class
    securityContext:            # Pod security context
      runAsNonRoot: true
      runAsUser: 999
      fsGroup: 999
    topologySpreadConstraints: [] # Topology spread (all modes)
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `podTemplate.annotations` | `map[string]string` | No | `{}` | Pod annotations |
| `podTemplate.labels` | `map[string]string` | No | `{}` | Additional pod labels |
| `podTemplate.nodeSelector` | `map[string]string` | No | `{}` | Node selector |
| `podTemplate.tolerations` | `[]Toleration` | No | `[]` | Tolerations |
| `podTemplate.affinity` | `Affinity` | No | `nil` | Affinity/anti-affinity |
| `podTemplate.priorityClassName` | `string` | No | `""` | Priority class name |
| `podTemplate.securityContext` | `PodSecurityContext` | No | See below | Pod security context |
| `podTemplate.topologySpreadConstraints` | `[]TopologySpreadConstraint` | No | `[]` | Topology spread |

**Default security context**:
```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 999
  runAsGroup: 999
  fsGroup: 999
```

### 2.11 Sentinel-Specific Configuration

Only applicable when `mode: sentinel`:

```yaml
spec:
  sentinel:
    masterName: myns.store      # REQUIRED. Must be unique across every Sentinel
                                # deployment reachable on this pod network.
    quorum: 2                   # Sentinels needed to agree on failure (default: 2)
    downAfterMilliseconds: 30000  # Time before marking master down
    failoverTimeout: 180000     # Failover timeout
    parallelSyncs: 1            # Replicas to sync in parallel
    allowUnsafeRebootstrapOnDeadlock: false  # Break a leaderless deadlock even with data (destructive)

    # Sentinel container resources (separate from Redis)
    resources:
      requests:
        cpu: "100m"
        memory: "64Mi"
      limits:
        memory: "64Mi"    # memory limit == request; no CPU limit
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `sentinel.masterName` | `string` | **Yes** | *none* | The Sentinel master name for this instance. **Must be unique across every Sentinel deployment reachable on the same pod network** — see the warning below. Recommended: `<namespace>.<name>`. Max 128 chars, `^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$` (no comma or whitespace: the Sentinel hello payload is comma-separated and `sentinel.conf` is space-separated). Sentinel-aware clients must be configured with this value. |
| `sentinel.quorum` | `int` | No | `2` | Sentinels needed to agree on failure |
| `sentinel.downAfterMilliseconds` | `int` | No | `30000` | Time to mark master as down |
| `sentinel.failoverTimeout` | `int` | No | `180000` | Failover timeout |
| `sentinel.parallelSyncs` | `int` | No | `1` | Parallel replica syncs |
| `sentinel.allowUnsafeRebootstrapOnDeadlock` | `bool` | No | `false` | Permit the operator to break a leaderless bootstrap deadlock (all Sentinels bare, no master) when **two or more** Redis pods hold data, by force-electing the most-complete pod as master and **discarding** the others. Enable only for caches where data loss is acceptable. With ≥2 data holders and this unset, the operator refuses and waits for manual intervention. Deadlocks with no data, or a single data-holding pod, are always broken automatically and safely regardless of this flag. |
| `sentinel.resources` | `ResourceRequirements` | No | See above | Sentinel container resources |

> **The master name is a security and data-safety boundary, not a label.**
>
> It is the *only* isolation Sentinel's gossip protocol has. A Sentinel receiving a hello
> message looks the master name up and discards the message if it does not know it — and
> performs no other check. There is no instance identifier, no namespace, and no
> authentication between Sentinels beyond the optional password.
>
> Two instances that share a master name and can reach each other are, protocol-wise, **one
> deployment**: the one with the higher config epoch can reassign the other's master to a
> foreign Redis pod, whose replicas then **flush their datasets** to resynchronise from it.
> This has happened in production. See `SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md`.
>
> Use `<namespace>.<name>`, and **enable authentication** (§2.5) — see the isolation notes in
> `USAGE.md`.

**Upgrading an existing instance.** Instances created before this field existed keep running
with the historic shared name `mymaster` and report a `SentinelMasterNameUnscoped` warning
condition. They are only forced to state a name on their next change to `spec.sentinel`.
Setting `masterName: mymaster` explicitly is accepted — a legacy client may hardcode it — and
silences the warning without changing behaviour. **Changing the value is client-visible:**
Sentinel-aware clients must be reconfigured in the same maintenance window (clients using the
label-routed `{name}` Service are unaffected), and there is no rolling cutover — monitoring one
master under two names runs two independent failover state machines that can promote different
replicas.

### 2.12 Failover-Specific Configuration (Experimental)

Only applicable when `mode: failover` (enforced by a CEL rule on the CRD).

> **Experimental**: mode `failover` is operator-managed HA without Sentinel,
> under active validation — see `docs/RECONCILIATION_LOOP_FAILOVER.md` and
> ADR-011 for current status and trade-offs vs `sentinel`. The operator emits a
> warning event (`ExperimentalMode`) on the first reconcile of a failover-mode
> instance. Trade-off: failover orchestration is coupled to operator liveness.

```yaml
spec:
  failover:
    replicas: 2                   # Redis replicas; total data pods = 1 + replicas
    downAfterMilliseconds: 5000   # Sustained-failure window before the operator declares the master down
    minReplicasToWrite: 0         # Rendered as min-replicas-to-write (0 = off)
    allowUnsafeRebootstrapOnDeadlock: false  # Break a diverged-lineage no-master deadlock (destructive)
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `failover.replicas` | `int32` | No | `2` | Number of Redis replicas (min: 1); total data pods = `1 + replicas`. The first mode with a configurable replica count — no quorum math depends on it. |
| `failover.downAfterMilliseconds` | `int` | No | `5000` | Sustained-failure window before the operator declares the master dead on probe evidence and initiates a failover. Kubernetes-authoritative evidence (pod deleted/replaced, redis container not-Ready per kubelet, pod terminating) is acted on immediately, without this window. |
| `failover.minReplicasToWrite` | `int` | No | `0` | Rendered into `redis.conf` as `min-replicas-to-write` (only when > 0): the master stops accepting writes when fewer than this many replicas are connected. Off by default (parity with sentinel mode); setting it trades write availability for a bound on data lost in a failover. |
| `failover.allowUnsafeRebootstrapOnDeadlock` | `bool` | No | `false` | Permit the operator to break a no-master deadlock when the surviving data holders span **divergent replication lineages** (electing any one discards the independent writes on the others). A deadlock with no data, or with all survivors on a single lineage — including a normal post-failover promotion chain — is always broken automatically and safely regardless of this flag (the most-complete holder is promoted, discarding nothing). Enable only for instances where data loss is acceptable (e.g. caches). Note the gate differs from the sentinel-mode field of the same name, which is keyed on the data-holder *count*; this one is keyed on lineage. |

There are no Sentinel-only knobs (`quorum`, `failoverTimeout`, `parallelSyncs`,
sentinel container resources) — the mode has no Sentinel processes.

**Failover mode creates**:
- StatefulSet `{name}-redis` (`1 + replicas` pods; **no** Sentinel StatefulSet, no `sentinel.conf`, no sentinel Service)
- Service `{name}` (ClusterIP, selector `role=master` — routes to the current master) and Service `{name}-replicas` (headless, all data pods)
- A PDB over the data pods (always ≥ 2 pods, so the PDB redundancy rule holds)

### 2.13 Cluster-Specific Configuration

Only applicable when `mode: cluster`:

```yaml
spec:
  cluster:
    shards: 3                   # Number of master shards (minimum 3)
    replicasPerShard: 1         # Replicas per master (0 = no replicas)
    clusterNodeTimeout: 15000   # Node timeout in milliseconds
    failoverGracePeriod: 15     # Extra seconds to wait for natural failover before operator intervenes
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `cluster.shards` | `int` | No | `3` | Number of master shards (min: 3) |
| `cluster.replicasPerShard` | `int` | No | `1` | Replicas per shard (0 = no replicas) |
| `cluster.clusterNodeTimeout` | `int` | No | `15000` | Node timeout in ms |
| `cluster.failoverGracePeriod` | `int` | No | `15` | Extra seconds (on top of `clusterNodeTimeout`) to wait for natural gossip-based failover before the operator force-promotes an orphaned replica. Total wait = `clusterNodeTimeout + failoverGracePeriod`. |

**Cluster mode creates**:
- Total pods: `shards × (1 + replicasPerShard)`, spread across one StatefulSet per shard (`{name}-shard-K`, each `1 + replicasPerShard` pods; pod `-K-0` is the shard master)
- Example: 3 shards with 1 replica = 6 pods (3 masters + 3 replicas)
- 16384 hash slots distributed across masters

**Important notes**:
- No PersistentVolumes required - cluster topology stored in CR status
- Data durability through replication, not disk persistence
- Minimum 3 shards required by Redis Cluster protocol

### 2.14 Placement (Cluster Mode)

Only applicable when `mode: cluster`; rejected by validation in every other mode
(standalone, sentinel, failover). Configures per-shard failure-domain isolation. The operator
translates `shardAntiAffinity` into a per-shard `topologySpreadConstraint`
(`maxSkew: 1`, `labelSelector` scoped to that shard's pods via the operator-owned
`redis.chuck-chuck-chuck.net/shard` label) injected into each shard StatefulSet,
and **appended** to any `spec.podTemplate.topologySpreadConstraints`.

```yaml
spec:
  placement:
    shardAntiAffinity:
      topologyKey: kubernetes.io/hostname   # failure domain to spread a shard's pods across
      whenUnsatisfiable: ScheduleAnyway     # ScheduleAnyway (soft) | DoNotSchedule (hard)
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `placement.shardAntiAffinity` | `ShardAntiAffinity` | No | `nil` | Spread each shard's master and replica(s) across failure domains. Cluster mode only. |
| `placement.shardAntiAffinity.topologyKey` | `string` | No | `kubernetes.io/hostname` | Node label defining the failure domain to spread a shard's pods across. Common alternative: `topology.kubernetes.io/zone`. |
| `placement.shardAntiAffinity.whenUnsatisfiable` | `string` (enum: `DoNotSchedule` \| `ScheduleAnyway`) | No | `ScheduleAnyway` | `ScheduleAnyway` (soft): best-effort spread that never blocks scheduling. `DoNotSchedule` (hard): a shard's pods never co-locate, but can leave pods `Pending` when there are fewer failure domains than a shard has pods. |

> **Note**: There is no under-provisioning status condition — with `DoNotSchedule`
> and too few domains, unschedulable pods surface as standard `Pending` pods
> (visible through readiness: `status.redis.ready < total`, phase `Initializing`).

### 2.15 Requeue Intervals

For tuning large-scale installations to reduce API server pressure:

```yaml
spec:
  requeueIntervals:
    fast: "2s"           # During initialization/recovery
    steadyState: "30s"   # Periodic health checks when Running
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `requeueIntervals.fast` | `Duration` | No | `2s` | Interval during init/recovery |
| `requeueIntervals.steadyState` | `Duration` | No | `30s` | Interval when stable |

### 2.16 PodDisruptionBudget

A [PodDisruptionBudget](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/) (PDB)
protects the Redis pods from voluntary disruptions (node drains, rolling node
upgrades). It is **created by default for redundant deployments**; set
`create: false` to opt out.

A PDB is only meaningful when there is redundancy. A PDB over a single-pod
workload is counter-productive — it can only ever block node drains, never protect
availability — so the operator **never creates one** for such deployments,
regardless of `create`:

| Mode | PDB? |
|------|------|
| `standalone` (1 pod) | ❌ never |
| `sentinel` (1 master + 2 replicas, 3 sentinels) | ✅ |
| `failover` (1 master + ≥1 replicas) | ✅ (always ≥ 2 data pods) |
| `cluster`, `replicasPerShard ≥ 1` | ✅ |
| `cluster`, `replicasPerShard = 0` (single pod per shard) | ❌ never |

```yaml
spec:
  podDisruptionBudget:
    create: true               # Created by default for redundant modes; set false to opt out
    maxUnavailable: 1          # Mutually exclusive with minAvailable
    # minAvailable: 2          # Alternative to maxUnavailable
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `podDisruptionBudget.create` | `*bool` | No | `true` | Whether to create a PDB for the managed StatefulSet(s). Ignored (no PDB created) for single-pod deployments — `standalone` mode and `cluster` mode with `replicasPerShard: 0`. In Sentinel mode, separate PDBs are created for the Redis and Sentinel StatefulSets. In Cluster mode, one PDB per shard is created (`{name}-shard-K-pdb`), only when `replicasPerShard > 0`. |
| `podDisruptionBudget.maxUnavailable` | `IntOrString` | No | `1` | Max pods unavailable during a disruption. Mutually exclusive with `minAvailable`. |
| `podDisruptionBudget.minAvailable` | `IntOrString` | No | — | Min pods that must stay available. Mutually exclusive with `maxUnavailable`. |

When neither `maxUnavailable` nor `minAvailable` is set, the operator defaults to
`maxUnavailable: 1`.

> **Upgrade note:** the default flipped from `false` to `true`. Defaulting is
> enforced by the CRD's OpenAPI schema, so the **new CRD must be applied** for the
> new default to take effect. If you upgrade only the operator image and leave an
> old CRD in place, instances that omit `create` keep the old behavior (no PDB) —
> nothing fails, the feature is simply inert. Note that `helm upgrade` does **not**
> upgrade CRDs in the chart's `crds/` directory, so Helm users must apply the CRD
> manually. To disable the PDB after upgrading, set `create: false` explicitly.

---

## 3. Status Fields

```yaml
status:
  phase: Running                # Overall phase
  status: "store-redis-0"   # Human-readable summary (master pod name when Running)
  observedGeneration: 1         # Last observed generation
  conditions:                   # Detailed conditions
    - type: Ready
      status: "True"
      reason: AllPodsReady
      message: "All pods are ready"
      lastTransitionTime: "2026-01-30T12:00:00Z"

  # Common fields
  redis:
    ready: 1                    # Ready Redis pods
    total: 1                    # Total Redis pods

  # Sentinel + failover modes
  bootstrapRequired: true       # True on creation, cleared after first master elected
  master:
    podName: store-redis-0   # Current master pod
    ip: 10.0.0.5                # Current master IP
  replicas:
    ready: 2
    total: 2

  # Sentinel mode only
  leaderlessSince: "2026-07-09T13:10:37Z"  # Set when a bootstrap deadlock is observed; cleared once a master is known
  ghostMasterStuckSince: "2026-07-31T10:15:00Z"  # Set when a ghost-master failover deadlock is observed; cleared once a master is known
  sentinels:
    ready: 3
    total: 3

  # Failover mode only — monitoring surfaces: every value is re-derived from
  # live state on each reconcile; nothing load-bearing is persisted here
  failover:
    masterDownSince: "2026-08-01T09:00:00Z"  # First observation of the master as unreachable (detection window anchor)
    assignmentEpoch: 4                        # Mirror of the epoch stamped on the pods (authority lives on the pods)
    transitionSince: "2026-08-01T09:00:05Z"   # Last master-intent stamp; anchors the post-transition cooldown

  # Cluster mode only
  cluster:
    state: ok                   # ok, fail, or initializing
    lastBootstrap: "2026-02-03T12:00:00Z"
    nodes:
      - podName: store-shard-0-0
        nodeId: abc123def456...
        role: master
        slotRanges: "0-5460"
      - podName: store-shard-0-1
        nodeId: ghi789jkl012...
        role: replica
        masterNodeId: abc123def456...
    orphanedReplicas:           # Replicas awaiting force-promotion (transient)
      - podName: store-shard-1-1
        nodeId: mno345pqr678...
        masterNodeId: abc123def456...
        detectedAt: "2026-02-03T12:01:00Z"
    wipeDeadlockSince: "2026-07-31T08:00:00Z"  # Set when the total-/partial-wipe deadlock signature is observed; cleared once it no longer holds
```

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `string` | `Pending`, `Initializing`, `Running`, `Failed`, `Terminating` |
| `status` | `string` | Human-readable summary: master pod name when Running, phase otherwise. Shown in `kubectl get littlered` output. |
| `bootstrapRequired` | `bool` | True on creation, cleared after the first master is elected (sentinel + failover modes) |
| `leaderlessSince` | `Time` | Set when the operator first observes a leaderless, all-Sentinels-bare bootstrap deadlock; cleared once a master is known. Gates the leaderless-recovery cooldown (sentinel mode). |
| `ghostMasterStuckSince` | `Time` | Set when the operator first observes a ghost-master failover deadlock: a majority of Sentinels pinned to a dead (ghost) master IP with no promotable replica, so Sentinel aborts every failover `no-good-slave` while living survivors still hold the data. Cleared once a master is known again. Gates the ghost-master-recovery cooldown, so a recent master death gets its full Sentinel election window first (sentinel mode). |
| `observedGeneration` | `int64` | Last processed `.metadata.generation` |
| `conditions` | `[]Condition` | Detailed status conditions |
| `redis.ready` | `int32` | Ready Redis pod count |
| `redis.total` | `int32` | Total Redis pod count |
| `master.podName` | `string` | Current master pod name (sentinel + failover modes; in failover mode this is the operator's intent once *observed* live, empty during transitions) |
| `master.ip` | `string` | Current master pod IP (sentinel + failover modes) |
| `replicas.ready` | `int32` | Ready replica count (sentinel + failover modes) |
| `replicas.total` | `int32` | Total replica count (sentinel + failover modes) |
| `sentinels.ready` | `int32` | Ready sentinel count (sentinel mode) |
| `sentinels.total` | `int32` | Total sentinel count (sentinel mode) |
| `failover.masterDownSince` | `Time` | When the operator first observed the current master as unreachable — the `downAfterMilliseconds` detection-window anchor. Cleared once the master is reachable again or a failover completes. **Monitoring surface only** (failover mode). |
| `failover.assignmentEpoch` | `int64` | Mirror of the monotonic assignment epoch stamped on the data pods' annotations. The authoritative epoch lives on the pods and is re-derived from live state, never read back from status. **Monitoring surface only** (failover mode). |
| `failover.transitionSince` | `Time` | When the operator last stamped a new master intent (bootstrap seed, failover promotion, or unsafe elect). Anchors the short post-transition cooldown that serializes cascading failovers; if lost, at worst one cooldown window is skipped. **Monitoring surface only** (failover mode). |
| `cluster.state` | `string` | Cluster state: `ok`, `fail`, `initializing` (cluster mode) |
| `cluster.lastBootstrap` | `Time` | Timestamp of last full cluster bootstrap (cluster mode) |
| `cluster.nodes` | `[]ClusterNodeState` | Per-node topology for operator-managed recovery (cluster mode) |
| `cluster.nodes[].podName` | `string` | Stable pod name (e.g., `store-shard-0-0`) |
| `cluster.nodes[].nodeId` | `string` | Redis cluster node ID (40-char hex) |
| `cluster.nodes[].role` | `string` | `master` or `replica` |
| `cluster.nodes[].masterNodeId` | `string` | Master's node ID (replicas only) |
| `cluster.nodes[].slotRanges` | `string` | Assigned slot range (masters only, e.g., `0-5460`) |
| `cluster.orphanedReplicas` | `[]OrphanedReplicaInfo` | Replicas whose master is gone, tracked for timeout-based force-promotion (transient, cluster mode) |
| `cluster.orphanedReplicas[].podName` | `string` | Pod name of the orphaned replica |
| `cluster.orphanedReplicas[].nodeId` | `string` | Node ID of the orphaned replica |
| `cluster.orphanedReplicas[].masterNodeId` | `string` | Node ID of the (now gone) master |
| `cluster.orphanedReplicas[].detectedAt` | `Time` | When the orphan was first detected |
| `cluster.wipeDeadlockSince` | `Time` | Set when the operator first observes the total-/partial-wipe deadlock signature: cluster pods stuck not-Ready and crash-looping (redis down, so — pure in-memory — holding no data) while the instance cannot reach a healthy topology. Arms the cooldown before the operator recycles the stuck pods; cleared as soon as the signature no longer holds (cluster mode). |

### 3.1 Condition Types

| Type | Set by controller | Description |
|------|:-----------------:|-------------|
| `Ready` | ✅ | All components are ready and operational |
| `Initialized` | ✅ | Initial setup complete |
| `ConfigValid` | ✅ | Configuration is valid (set `False` on validation failure) |
| `SentinelReady` | ✅ | Sentinel quorum established (sentinel mode; never set in failover mode) |
| `LeaderlessRecovery` | ✅ | Sentinel mode only: reflects a leaderless bootstrap deadlock (every Sentinel bare, no master) and the operator's response — `True` means the instance is deadlocked and needs attention (in cooldown, or refusing because data is present); `False` records a completed recovery |
| `GhostMasterRecovery` | ✅ | Sentinel mode only: reflects a ghost-master failover deadlock — a majority of Sentinels pinned to a dead (ghost) master IP with no promotable replica, so failover aborts `no-good-slave` while living survivors hold the data — and the operator's recovery of it. `True` means deadlocked/needs attention (in cooldown, or refusing because divergent data is present); `False` records a completed recovery |
| `FailoverRecovery` | ✅ | Failover mode only: `True` means the instance needs attention — most importantly the refuse-and-wait state, where the surviving data holders span divergent replication lineages and electing any one would discard independent writes (set `failover.allowUnsafeRebootstrapOnDeadlock` to authorize); `False` records a completed recovery |
| `TLSReady` | — | Reserved for future use (defined but not currently set) |
| `AuthReady` | — | Reserved for future use (defined but not currently set) |
| `ClusterReady` | — | Reserved for future use (defined but not currently set) |

### 3.2 Phase Transitions

```
           create
              │
              ▼
         ┌─────────┐
         │ Pending │ ◄── Waiting for resources
         └────┬────┘
              │
              ▼
       ┌──────────────┐
       │ Initializing │ ◄── Pods starting
       └──────┬───────┘
              │
              ├─────────────────┐
              ▼                 ▼
         ┌─────────┐       ┌────────┐
         │ Running │       │ Failed │ ◄── Validation error,
         └────┬────┘       └────────┘     pod crash, etc.
              │
              ▼ delete
        ┌─────────────┐
        │ Terminating │
        └─────────────┘
```

---

## 4. Complete Examples

### 4.1 Minimal Standalone

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec: {}
```

Uses all defaults: standalone mode, `docker.io/library/redis:8.4.2`, no auth, no TLS, metrics enabled.

### 4.2 Standalone with Custom Resources

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  resources:
    requests:
      cpu: "1"
      memory: "2Gi"
    limits:
      memory: "2Gi"     # memory limit == request; no CPU limit (Burstable QoS)
  config:
    maxmemoryPolicy: allkeys-lfu
```

### 4.3 Standalone with Redis (instead of Valkey)

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  image:
    path: library/redis
    # Result: docker.io/library/redis:8.0
```

### 4.4 Registry Mirror

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  image:
    registry: docker.io
    # Result: docker.io/library/redis:8.4.2
    # Exporter: docker.io/oliver006/redis_exporter:v1.89.0
```

### 4.5 Standalone with Auth

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  auth:
    enabled: true
    existingSecret: my-redis-password
---
apiVersion: v1
kind: Secret
metadata:
  name: my-redis-password
type: Opaque
stringData:
  password: "my-secret-password"
```

### 4.6 Standalone with TLS

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  tls:
    enabled: true
    existingSecret: my-redis-tls
---
apiVersion: v1
kind: Secret
metadata:
  name: my-redis-tls
type: kubernetes.io/tls
data:
  tls.crt: <base64>
  tls.key: <base64>
```

### 4.7 Full Standalone

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
  namespace: production
spec:
  mode: standalone

  image:
    registry: docker.io   # Company mirror
    path: library/redis            # Use Redis instead of Valkey
    tag: "8.0"
    pullPolicy: IfNotPresent

  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      memory: "1Gi"     # memory limit == request; no CPU limit (Burstable QoS)

  config:
    maxmemory: "900Mi"
    maxmemoryPolicy: noeviction
    timeout: 0
    tcpKeepalive: 300
    raw: |
      hz 10
      dynamic-hz yes

  auth:
    enabled: true
    existingSecret: redis-password

  tls:
    enabled: true
    existingSecret: redis-tls

  metrics:
    enabled: true
    # Exporter inherits registry: docker.io/oliver006/redis_exporter:v1.89.0
    serviceMonitor:
      enabled: true
      labels:
        release: prometheus
      interval: "15s"

  updateStrategy:
    type: RollingUpdate

  service:
    type: ClusterIP
    annotations:
      prometheus.io/scrape: "true"

  podTemplate:
    annotations:
      sidecar.istio.io/inject: "false"
    nodeSelector:
      workload-type: cache
    tolerations:
      - key: "cache-only"
        operator: "Exists"
        effect: "NoSchedule"
    priorityClassName: high-priority
```

### 4.8 Minimal Sentinel

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
  namespace: apps
spec:
  mode: sentinel
  sentinel:
    masterName: apps.store    # required; unique per pod network (§2.11)
```

Deploys: 1 master + 2 replicas + 3 sentinels with defaults (`docker.io/library/redis:8.4.2`).

`sentinel.masterName` is the one field a sentinel instance cannot omit. It is not cosmetic —
see the warning in §2.11.

### 4.9 Sentinel with Production Settings

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
  namespace: production
spec:
  mode: sentinel

  image:
    registry: docker.io
    # Uses default path (library/redis) and tag (8.4.2)

  podDisruptionBudget:
    create: true
    maxUnavailable: 1

  resources:
    requests:
      cpu: "1"
      memory: "4Gi"
    limits:
      memory: "4Gi"     # memory limit == request; no CPU limit (Burstable QoS)

  config:
    maxmemory: "3500Mi"
    maxmemoryPolicy: noeviction

  auth:
    enabled: true
    existingSecret: redis-password

  sentinel:
    masterName: production.store   # required; unique per pod network (§2.11)
    # quorum defaults to 2 — only set it if you need a different value
    downAfterMilliseconds: 5000
    failoverTimeout: 60000
    resources:
      requests:
        cpu: "100m"
        memory: "128Mi"
      limits:
        memory: "128Mi"   # memory limit == request; no CPU limit

  metrics:
    enabled: true
    # Exporter inherits registry from image.registry
    serviceMonitor:
      enabled: true
      labels:
        release: prometheus

  podTemplate:
    affinity:
      podAntiAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchLabels:
                app.kubernetes.io/name: littlered
                app.kubernetes.io/instance: store
            topologyKey: kubernetes.io/hostname
```

### 4.10 Minimal Failover (Experimental)

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store-failover
spec:
  mode: failover   # experimental: operator-managed HA without Sentinel
```

Creates 3 data pods (1 master + 2 replicas by default), no Sentinel pods. See
§2.12 for the knobs and `config/samples/littlered_v1alpha1_littlered_failover.yaml`.

### 4.11 Minimal Cluster

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: cluster
```

Deploys: 3 masters + 3 replicas (6 pods total) with default settings.

### 4.12 Cluster with Custom Shards

```yaml
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: store
spec:
  mode: cluster
  cluster:
    shards: 6               # 6 masters
    replicasPerShard: 1     # 6 replicas
    clusterNodeTimeout: 10000

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
            app.kubernetes.io/instance: store
```

---

## 5. Labels and Annotations

### 5.1 Standard Labels (Applied by Operator)

```yaml
labels:
  app.kubernetes.io/name: littlered
  app.kubernetes.io/instance: store
  app.kubernetes.io/component: redis | sentinel | exporter
  app.kubernetes.io/managed-by: littlered-operator
  app.kubernetes.io/version: "8.0"
  redis.chuck-chuck-chuck.net/mode: standalone | sentinel | cluster | failover
```

### 5.2 Sentinel Mode Labels

```yaml
# On Redis pods (dynamic, updated on failover)
labels:
  redis.chuck-chuck-chuck.net/role: master | replica
```

### 5.3 Failover Mode Labels and Annotations

The dynamic `role` label works exactly as in sentinel mode (the `{name}` Service
routes on it). In addition, the operator stamps its **assignment channel** onto
each data pod as annotations (ADR-011); the pod reads them back through a
downward-API volume. These are operator-owned — do not set them by hand:

```yaml
# On Redis pods (operator-stamped; the intent record)
annotations:
  redis.chuck-chuck-chuck.net/assigned-role: master | replica
  redis.chuck-chuck-chuck.net/assigned-master-ip: "10.0.0.5"   # empty on the master's stamp
  redis.chuck-chuck-chuck.net/assignment-epoch: "3"            # monotonic per instance
```

---

## 6. Go Types (Reference)

```go
// LittleRedSpec defines the desired state of LittleRed
type LittleRedSpec struct {
    // Mode is the deployment mode: standalone, sentinel, cluster, or failover (experimental)
    // +kubebuilder:validation:Enum=standalone;sentinel;cluster;failover
    // +kubebuilder:default=standalone
    Mode string `json:"mode,omitempty"`

    // Image defines the container image to use
    Image ImageSpec `json:"image,omitempty"`

    // Resources defines CPU/memory for Redis container
    Resources corev1.ResourceRequirements `json:"resources,omitempty"`

    // Config defines Redis configuration
    Config ConfigSpec `json:"config,omitempty"`

    // Auth defines authentication settings
    Auth AuthSpec `json:"auth,omitempty"`

    // TLS defines TLS/SSL settings
    TLS TLSSpec `json:"tls,omitempty"`

    // Metrics defines Prometheus metrics settings
    Metrics MetricsSpec `json:"metrics,omitempty"`

    // UpdateStrategy defines how updates are rolled out
    UpdateStrategy UpdateStrategySpec `json:"updateStrategy,omitempty"`

    // Service defines Service configuration
    Service ServiceSpec `json:"service,omitempty"`

    // PodTemplate defines pod customizations
    PodTemplate PodTemplateSpec `json:"podTemplate,omitempty"`

    // Sentinel defines sentinel-specific settings (sentinel mode only)
    Sentinel *SentinelSpec `json:"sentinel,omitempty"`

    // Cluster defines cluster-specific settings (cluster mode only)
    Cluster *ClusterSpec `json:"cluster,omitempty"`

    // Failover defines failover-specific settings (failover mode only, experimental)
    Failover *FailoverSpec `json:"failover,omitempty"`

    // RequeueIntervals for tuning reconciliation frequency
    RequeueIntervals *RequeueIntervals `json:"requeueIntervals,omitempty"`
}

// FailoverSpec defines failover-specific settings. Mode failover is
// experimental: operator-managed HA without Sentinel.
type FailoverSpec struct {
    // Replicas is the number of Redis replicas; total data pods = 1 + replicas.
    // +kubebuilder:validation:Minimum=1
    // +kubebuilder:default=2
    Replicas *int32 `json:"replicas,omitempty"`

    // DownAfterMilliseconds is the sustained-failure window before the operator
    // declares the master down on probe evidence and initiates a failover.
    // +kubebuilder:default=5000
    DownAfterMilliseconds int `json:"downAfterMilliseconds,omitempty"`

    // MinReplicasToWrite is rendered into redis.conf as min-replicas-to-write
    // (0 = disabled).
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:default=0
    MinReplicasToWrite int `json:"minReplicasToWrite,omitempty"`

    // AllowUnsafeRebootstrapOnDeadlock permits breaking a no-master deadlock
    // when surviving data holders have DIVERGED replication lineages.
    // +kubebuilder:default=false
    AllowUnsafeRebootstrapOnDeadlock bool `json:"allowUnsafeRebootstrapOnDeadlock,omitempty"`
}

type ImageSpec struct {
    // Registry is the container registry hostname
    // +kubebuilder:default=docker.io
    Registry string `json:"registry,omitempty"`

    // Path is the image path (without registry or tag)
    // +kubebuilder:default=library/redis
    Path string `json:"path,omitempty"`

    // Tag is the image version tag
    // +kubebuilder:default="8.4.2"
    Tag string `json:"tag,omitempty"`

    // PullPolicy is the image pull policy
    // +kubebuilder:validation:Enum=Always;IfNotPresent;Never
    // +kubebuilder:default=IfNotPresent
    PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`

    // PullSecrets are references to secrets for pulling the image
    PullSecrets []corev1.LocalObjectReference `json:"pullSecrets,omitempty"`
}

// FullImage returns the complete image reference: {registry}/{path}:{tag}
func (i *ImageSpec) FullImage() string {
    return fmt.Sprintf("%s/%s:%s", i.Registry, i.Path, i.Tag)
}

type ConfigSpec struct {
    // Maxmemory sets Redis maxmemory (e.g., "1Gi")
    Maxmemory string `json:"maxmemory,omitempty"`

    // MaxmemoryPolicy sets the eviction policy
    // +kubebuilder:validation:Enum=noeviction;allkeys-lru;allkeys-lfu;allkeys-random;volatile-lru;volatile-lfu;volatile-random;volatile-ttl
    // +kubebuilder:default=noeviction
    MaxmemoryPolicy string `json:"maxmemoryPolicy,omitempty"`

    // Timeout is client idle timeout in seconds (0 = disabled)
    // +kubebuilder:default=0
    Timeout int `json:"timeout,omitempty"`

    // TCPKeepalive interval in seconds
    // +kubebuilder:default=300
    TCPKeepalive int `json:"tcpKeepalive,omitempty"`

    // Raw is raw redis.conf content (expert mode)
    Raw string `json:"raw,omitempty"`
}

// ... additional types follow same pattern
```

---

## 7. Validation Rules

### 7.1 Controller-Level Validation

| Rule | Error Condition |
|------|-----------------|
| `mode` must be `standalone`, `sentinel`, `cluster`, or `failover` | Invalid mode value |
| If `auth.enabled`, must have `existingSecret` | Missing authentication secret |
| If `tls.enabled`, must have `existingSecret` | Missing TLS certificate |
| If `tls.clientAuth`, must have `caCertSecret` | Missing CA certificate |
| `sentinel` config ignored if `mode=standalone` | Warning in status |
| `spec.failover` only allowed with `mode: failover` (CEL rule on the CRD; `spec.sentinel`/`spec.cluster` are gated to their modes the same way) | Rejected at admission |
| `failover.replicas` must be ≥ 1; `failover.minReplicasToWrite` must be ≥ 0 | Enforced by the CRD schema, mirrored in controller validation |
| `spec.placement.shardAntiAffinity` rejected unless `mode: cluster` (failover included) | Validation failure |
| `cluster.shards` must be ≥ `3` | Enforced by the CRD schema (minimum 3, default 3) and mirrored in controller validation (`cluster mode requires at least 3 shards`) |
| `cluster.replicasPerShard` must be `0` or `1` | Currently only 0 or 1 replica per shard supported |
| `maxmemory` must parse as quantity | Invalid memory format |

### 7.2 Status Condition on Validation Failure

```yaml
status:
  phase: Failed
  conditions:
    - type: ConfigValid
      status: "False"
      reason: ValidationFailed
      message: "auth.enabled is true but no secret configured"
```
