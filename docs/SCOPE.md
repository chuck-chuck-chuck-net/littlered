# LittleRed - Scope Definition

> Quick reference for what's in and out of scope.

**Last Updated**: 2026-08-01

---

## Project Identity

| Attribute | Value |
|-----------|-------|
| Product Name | LittleRed |
| Technical Name | `littlered` |
| API Group | `redis.chuck-chuck-chuck.net` |
| API Version | `v1alpha1` (current) |
| Default Image | `docker.io/library/redis:8.4.2` |
| Image Format | `{registry}/{path}:{tag}` (each component overridable) |

---

## In Scope

### Pre-MVP (Standalone) ✅

- [x] Single Redis/Valkey instance (StatefulSet, 1 replica)
- [x] Redis 7.2+ / Valkey 7.2+ support
- [x] In-memory only (no persistence)
- [x] Opinionated defaults (performance-optimized, noeviction)
- [x] Full redis.conf override capability
- [x] Prometheus metrics (redis_exporter sidecar)
- [x] Configurable resources (CPU/memory)
- [x] Burstable QoS by default (memory limit = request, no CPU limit)
- [x] CR status reporting

### MVP (Sentinel) ✅

- [x] Sentinel mode: 1 master + 2 replicas + 3 sentinels
- [x] Automatic failover via Sentinel
- [x] Rolling update strategy
- [x] Optional ServiceMonitor creation
- [x] Master/replica/sentinel services
- [x] Safe bootstrap (operator-authorized master registration)
- [x] Strict IP-only identity (ADR-001)

### Phase 2 (Cluster) ✅

- [x] Cluster mode: configurable shards with replicas per shard
- [x] Automatic cluster bootstrapping (MEET + ADDSLOTS + REPLICATE)
- [x] Operator-managed recovery (topology in CR status)
- [x] No PersistentVolumes required
- [x] Data durability through replication
- [x] Strict positional shard mapping (Pod N = Shard N)

### Phase 3 (Auth & TLS) ✅

- [x] Password authentication (all modes)
- [x] TLS encryption (all modes)
- [x] User-provided secrets for auth and TLS
- [x] Separate CA cert secret support
- [x] Client certificate auth support

### Phase 4 (Resilience Hardening) ✅

- [x] Kill-9 / OOM crash protection (sentinel: run-id detection, cluster: nodes.conf detection)
- [x] Ghost node pruning (SENTINEL RESET, CLUSTER FORGET)
- [x] Divergent sentinel correction (SENTINEL REMOVE + MONITOR)
- [x] Cluster quorum recovery (CLUSTER FAILOVER TAKEOVER)
- [x] Cluster partition healing (CLUSTER MEET + orphan tracking)
- [x] Replica rescue (SLAVEOF for rogue masters)
- [x] Graceful pre-stop hooks (failover before shutdown)
- [x] Operator passivity during active transitions (Rule A guardrails)

### Failover Mode (Experimental) 🚧

Operator-managed HA without Sentinel (`mode: failover`, ADR-011): 1 master +
N replicas (configurable, default 2), the operator as the sole failure detector
and failover decider. Coexists with sentinel mode — sentinel stays fully
supported; failover is offered as an alternative with an explicit trade-off
(HA coupled to operator liveness).

- [x] `mode: failover` + `spec.failover` API (replicas, downAfterMilliseconds, minReplicasToWrite, allowUnsafeRebootstrapOnDeadlock)
- [x] Operator-assignment startup protocol (downward-API annotations + epoch-fenced kill-9 yield)
- [x] Pure decision seams (`planMasterDeath`, `planFailover`) with unit tables
- [x] Reconcile flow: bootstrap, assignment engine, straggler repoint, re-authorization, status
- [x] Fast-detection master watcher (background goroutine, reconcile-accelerating only)
- [x] Full HA e2e suite — **16/16 green on a real 3-node cluster (2026-08-01)**: graceful, crash, hybrid double-failover (the graduation scenario), kill-9, chaos (corruptions 0, write availability 96.5%), deadlock tiers, rolling update
- [ ] **Open: the graduation gate remainder** (`FAILOVER_MODE_DESIGN.md` §4) — chaos/soak run and dogfooding evidence from a managed cloud. Only after that gate is the drop/coexist/replace-sentinel decision due.

### Tooling ✅

- [x] lrctl CLI tool (kubectl plugin, JSON output, shell completion)
- [x] Chaos test client (littlered-chaos-client)
- [x] E2E test suite (kind-based)
- [x] Helm chart distribution
- [x] Structured JSON operator logs with categories (recon, state, audit)

### Infrastructure ✅

- [x] Go 1.24+ implementation (Kubebuilder v4)
- [x] Kubernetes 1.28+ support
- [x] Server-Side Apply for resource management
- [x] Unit tests + integration tests (envtest) + E2E tests (kind)
- [x] Architecture Decision Records (ADR-001 through ADR-004)

---

## Out of Scope (Current)

### Explicitly Excluded

| Feature | Reason | Future? |
|---------|--------|---------|
| Persistence (RDB/AOF with PVC) | Not needed for pure in-memory use case | Maybe |
| Redis < 7.2 | Legacy, not required | No |
| Cluster slot migration | Needed for dynamic shard scaling | Yes |
| ACL-based auth | Complexity, password sufficient | Maybe |
| Auto-sizing | Sizing is app-driven | No |
| Prometheus alerts / PrometheusRules | Low priority | If trivial |
| OLM distribution | Helm sufficient | Maybe |
| Multi-cluster replication | Out of scope | Unlikely |
| Admission webhooks | Validate in controller instead | Maybe later |
| Configurable sentinel/replica counts | Fixed 1+2+3 topology in sentinel mode (failover mode already has a configurable replica count) | Yes |
| TLS certificate verification (operator→pod) | Identity via K8s API, not PKI (ADR-004) | Service mesh recommended |

### Will Not Do

- Auto-detection of Prometheus Operator (explicit enable only)
- Backup/restore (no persistence)
- Cross-namespace replication
- Custom Redis modules
- Managing PersistentVolumeClaims

---

## Architecture Constraints

Design decisions that support extensibility:

1. **Sentinel topology**: Fixed 1+2+3, architected for configurable counts in future
2. **Cluster topology**: Strict positional shard mapping (Pod N = Shard N), operator-managed
3. **Persistence**: Actively disabled by default; expert override via `spec.config.raw`
4. **Identity model**: IP-only for in-memory mode (ADR-001); hostname-based would be needed for persistence mode
5. **Operator philosophy**: Enablement over intervention (ADR-003) — trust Redis/Sentinel internal mechanisms, intervene only on permanent stalls

---

## Success Criteria

### Pre-MVP Complete ✅

1. ✅ Deploy standalone Redis/Valkey via CR
2. ✅ SET/GET operations work
3. ✅ Prometheus metrics accessible
4. ✅ Pod deletion triggers recreation

### MVP Complete ✅

1. ✅ Sentinel mode deploys all 6 pods
2. ✅ Automatic failover (kill master → new master elected)
3. ✅ Rolling updates maintain availability
4. ✅ E2E tests pass on kind
5. ✅ Helm chart installable

### Phase 2 Complete ✅

1. ✅ Cluster mode deploys configurable shards with replicas
2. ✅ Automatic slot assignment (16384 slots)
3. ✅ Cluster recovery from node failures
4. ✅ Topology stored in CR status (no PVC dependency)

### Phase 3 Complete ✅

1. ✅ Auth works in standalone, sentinel, and cluster modes
2. ✅ TLS works in standalone, sentinel, and cluster modes
3. ✅ E2E tests for auth and TLS

### Phase 4 Complete ✅

1. ✅ Sentinel mode survives `kubectl delete pod --force --grace-period=0`
2. ✅ Sentinel mode survives in-pod kill -9 (crash protection)
3. ✅ Cluster mode survives `kubectl delete pod --force --grace-period=0`
4. ✅ Cluster mode survives in-pod kill -9 (crash protection)
5. ✅ Ghost nodes pruned automatically
6. ✅ Divergent sentinels corrected automatically
7. ✅ Reconciliation algorithm changelog tracks all fixes (LR-001 through LR-010)

---

## Milestones

```
Pre-MVP ✅
├── Project setup (kubebuilder init)
├── CR definition
├── Standalone controller
├── Metrics sidecar
└── Basic tests

MVP ✅
├── Sentinel controller
├── Safe bootstrap
├── Failover handling
├── Update strategies
├── Helm chart
└── E2E tests

Phase 2 ✅
├── Cluster controller
├── Slot management
├── Quorum recovery
├── Partition healing
├── Ghost node removal
└── Chaos testing

Phase 3 ✅
├── Password auth (all modes)
├── TLS encryption (all modes)
└── Auth/TLS E2E tests

Phase 4 ✅
├── Kill-9 protection (sentinel + cluster)
├── Ghost master correction (REMOVE + MONITOR)
├── Replica rescue (Rule R)
├── Surgical pod relabeling
├── GenerationChangedPredicate (reduce reconcile churn)
├── Structured JSON logging with categories
├── lrctl CLI tool
└── Reconciliation algorithm changelog

Failover Mode (experimental) 🚧
├── ADR-011 + design note (FAILOVER_MODE_DESIGN.md) ✅
├── API surface (mode enum, FailoverSpec, status, CEL) ✅
├── Pure decision seams (planMasterDeath, planFailover) ✅
├── Resource builders + assignment startup protocol ✅
├── Reconcile flow (bootstrap, engine, status) ✅
├── Fast-detection master watcher ✅
├── e2e suite ✅ (16/16 on a real 3-node cluster)
└── Graduation gate: chaos/soak + dogfooding → decide drop/coexist/replace (open)
```
