# ADR-007: Per-Shard StatefulSets & Stable Shard Identity (Cluster Mode)

## Status
Accepted (0.3.0). Milestone 1 (the structural split) and Milestone 2 (the first-class
`spec.placement.shardAntiAffinity` placement knob — LR-022) are both implemented. Direction B
(topology-aware master balancing) remains deferred (below).

## Context

Before 0.3.0, cluster mode was a **single** StatefulSet `{name}-cluster` sized
`replicas = shards × (1+replicasPerShard)`, with a **striped pod-index→shard model**: pods
`0..shards-1` were the shard masters (pod N = shard N), pods `shards..total-1` were replicas
mapped back to master `(i-shards) % shards`. Every pod carried identical labels
(`component=cluster`); there was no per-shard or per-role label anywhere in cluster mode.

The **must-have driver** is resilience against data loss when a single failure domain
(node/zone) is lost. LittleRed is pure in-memory (EmptyDir, pillar 3.1), so a shard's
durability *is* master/replica **domain diversity**: if a shard's master and its only replica
sit on the same node and that node dies, the shard's data and its slots are gone. That
requirement was **not expressible** — you cannot scope a `topologySpreadConstraint` to
"a shard's pods" without a stable per-shard label, and a single StatefulSet cannot carry one.

### Why this is *mandatory*, not merely cleaner

The requirement forces a **stable, schedule-time, per-shard grouping key**: a
`topologySpreadConstraint` only isolates pods it can *select*, and the scheduler evaluates it
at **bind time** and never again (`...IgnoredDuringExecution`). So the shard key must be
per-pod, present *before* scheduling, and stable. A single StatefulSet cannot provide one:

- **One template ⇒ no native shard key.** A StatefulSet has exactly one `spec.template`; every
  pod is stamped from it identically. The only per-pod metadata Kubernetes injects on its own —
  `statefulset.kubernetes.io/pod-name`, `apps.kubernetes.io/pod-index` (the raw ordinal), and
  `controller-revision-hash` — is ordinal/revision identity, **none shard-semantic**. Shard is
  a *function* of the ordinal (`ordinal mod shards`), which K8s neither computes nor accepts;
  `matchLabelKeys` groups by label *equality* only (raw ordinal → wrong grain, not same-shard).
- **Operator-patched labels are too late.** The operator can label a pod only *after* it exists
  — after scheduling. `IgnoredDuringExecution` means the constraint never re-runs, so such a
  label cannot influence placement.
- **The only functioning single-STS variant is a mutating admission webhook** that stamps the
  shard label at admission (before scheduling) by parsing the ordinal — an entire extra
  control-plane component (TLS certs, failure modes, can block all pod creation) whose sole job
  is to reconstruct what per-shard StatefulSets carry declaratively for free.
- **Every no-webhook single-STS alternative fails the requirement, not just does it worse:**
  role-based anti-affinity (role unknown at schedule time + flaps, one-shot), a global even
  spread (a cluster-wide `maxSkew:1` is satisfied by 2 pods/node even when *every* shard is
  co-located), per-pod anti-affinity (forces `#nodes ≥ #pods`, kills bin-packing, still
  one-shot).

So the honest framing is **mandatory**: single-domain-loss survivability *via native
Kubernetes scheduling* is not expressible with one StatefulSet without inventing a bespoke
webhook; the clean alternatives don't achieve it at all.

The choice is **over-determined** by two independent structural wins that don't rest on the
label argument: (1) **shard = the workload unit** — per-shard rolling updates/partition,
per-shard PDB, per-shard scaling, none expressible under one shared StatefulSet; (2) it
**deletes the fragile pod-index→shard decode** — the striped `(i-shards)%shards` assumption is
exactly what produced the LR-018 consolidated-shard bug.

## Decision

### 1. One StatefulSet per shard, stable shard label
Cluster mode builds **N StatefulSets**, `{name}-shard-K` for K in `0..shards-1`, each sized
`1 + replicasPerShard`. Each stamps a static per-shard identity label
`redis.chuck-chuck-chuck.net/shard: "<K>"` on the STS selector, the pod template, and the STS
metadata. The label is set at STS creation, present at scheduling time, and never flaps.

### 2. Master identity is positional-within-shard, not striped
Shard K's intended master is `{name}-shard-K-0` (ordinal 0 within its shard STS); `-1..R` are
its replicas. The pure helper `ClusterPodRefs(name, shards, replicasPerShard)` is the single
source of truth for pod enumeration and master identity, replacing five ad-hoc
`{name}-cluster-N` ordinal loops and the `(i-shards)%shards` formula.

### 3. Shared Services stay shard-agnostic
The one headless Service `{name}-cluster` (selector `component=cluster`) and the client Service
`{name}` are **unchanged**: every shard StatefulSet uses `{name}-cluster` as its governing
`serviceName`, so peer discovery (`getent hosts {name}-cluster`) and pod DNS
`{pod}.{name}-cluster.ns.svc` keep resolving across all shards. Only the shard STS **selectors**
carry the shard label; the Services must not.

### 4. One PodDisruptionBudget per shard
`{name}-shard-K-pdb`, scoped to the shard's pods, created only when `replicasPerShard > 0`
(unchanged redundancy rule). This binds the disruption budget to the shard's failure domain: a
voluntary drain can take at most `maxUnavailable` (default 1) pod of any one shard.

### 5. Never delete data on migration (breaking change)
Renaming one StatefulSet into N rebuilds the workloads; with EmptyDir that is a clean slate.
The operator **does not** auto-delete the old `{name}-cluster` StatefulSet — LittleRed never
deletes data by default. If a pre-0.3.0 single-STS is present, the operator refuses to stand up
the per-shard StatefulSets beside it, surfaces a `LegacyClusterTopology` condition + event, and
waits for an operator to migrate/remove it. Reducing `shards` is likewise refused
(`ShardScaleDownRefused`) — it would orphan shard StatefulSets and drop their slots, and there
is no reshard-away path.

### 6. The operator is the sole topology authority (shard↔STS pinning)
A stable *pod* identity is necessary but **not sufficient** — the per-shard-scoped spread only
delivers the goal if each Redis shard (its master + replicas) actually stays inside one shard
StatefulSet. Nothing in Redis maintains that: OSS Redis/Valkey Cluster has **no failure-domain
awareness** (rack-zone awareness is a Redis Enterprise-only feature; Valkey's AZ support is
client-side read routing, not placement/failover), and two topology-blind mechanisms re-pair a
shard's master and replica across StatefulSets — (a) the operator's own empty-master reattach
(Step 4) and (b) Redis's autonomous *replica migration*. We close both:
- **Shard-aware reattach** (`chooseReattachTarget`): a restarted empty pod reattaches to the
  under-replicated slot-master **in its own shard STS**, falling back cross-shard only if none
  needs a replica (logged). This keeps a Redis shard inside one STS across failover churn.
- **`cluster-allow-replica-migration no`** in the cluster Redis config, so Redis never
  autonomously moves a replica to a foreign shard ("a more stable topology when managed
  externally"). At `replicasPerShard ≥ 2` this is load-bearing; at 1 it is defense-in-depth.

Without this the shard label pins only *scheduling*; role assignment drifts and a shard's
master/replica land in different STSs, defeating Goal 1 — observed in the very first e2e run
(the operator's Step 4 scrambled the pairing at bootstrap). This is a thin slice of Direction B
(operator role assignment) that A turns out to require; the A/B split is cleaner on paper.

**Guards** (the invariant is now checkable, not just asserted in prose): the pure
`ClusterGroundTruth.CheckShardColocation` reports any replica whose master is in a different
shard STS. `lrctl verify` runs it and **fails** on a violation (previously it green-lit a
scrambled cluster, since Redis itself was healthy), and additionally reports a `[DEGRADED]`
(warn, not fail) state when a replica's replication link is down — a healthy cluster is not
"consistent" while a shard runs at reduced redundancy. The e2e `verifyClusterTopologySync`
asserts the same invariant, so a future regression goes red in CI.

### 7. Serialize rollouts across shards (LR-021)
Splitting one StatefulSet into N dropped the global one-pod-at-a-time restart serialization the
single StatefulSet gave for free. So `reconcileClusterStatefulSet` applies template **updates**
one shard at a time — create-missing stays parallel (bootstrap), but an operator-driven template
change rolls a single shard and defers the rest until it settles (`clusterShardRolloutSettled` —
since **renamed `statefulSetRolloutSettled`** by LR-050, which reused the predicate for sentinel
mode because it was never mode-specific; the decision below is recorded as it was taken;
change detected cache-safely via an `AnnotationPodSpecHash` on the pod template). Without it a
config change restarts every shard's master in one wave — a cluster-wide availability dip (not
data loss) measured in the first e2e run. Governs operator-triggered rollouts only; a manual
`kubectl rollout restart` bypasses it.

### 8. First-class placement knob (Milestone 2, LR-022)
The per-shard-scoped `topologySpreadConstraint` that makes Goal 1 real can't be hand-written by
users (it must select on the operator-owned shard label), so `spec.placement.shardAntiAffinity
{ topologyKey, whenUnsatisfiable }` is a first-class knob. The operator injects a per-shard
constraint (`maxSkew: 1`, selector = that shard's pods) into each shard StatefulSet, appended to
`spec.podTemplate.topologySpreadConstraints`. Defaults: `kubernetes.io/hostname` +
**`ScheduleAnyway` (soft)** — matching CloudNativePG/Strimzi conventions and pillar 3.5; hard
(`DoNotSchedule`) is opt-in. The under-provisioning status condition (domains < a shard's pods)
is deferred — it needs cluster-wide `nodes` RBAC and the soft default plus existing readiness
status make it a diagnostic nicety, not a correctness need.

## Consequences
- Single-domain-loss survivability for a shard becomes expressible via a per-shard-scoped
  `topologySpreadConstraint` over the shard's StatefulSet. It survives failover **because the
  operator keeps each Redis shard inside one shard StatefulSet** (Decision 6) — *not* for free:
  the spread is over a stable pod set, and shard-aware reattach + disabled replica migration are
  what keep role flaps *within* that set. (Earlier drafts claimed "for free"; the first e2e run
  falsified that — see changelog LR-020.)
- Cluster mode is now N StatefulSets. Status still reports a flat `Nodes` list, now keyed by the
  `{name}-shard-K-N` pod names.
- The reshard/state layers (`PlanReshard`, gatherer, `cluster_state`) are unchanged — they are
  keyed by IP/NodeID/role, never by pod ordinal or STS name.
- Upgrading a pre-0.3.0 cluster in place is not supported; it is a documented clean-slate
  migration (see changelog LR-020 and USAGE upgrade notes).

## Alternatives considered
- **Single STS + mutating webhook to stamp the shard label** — works, but adds a whole
  control-plane component to emulate what per-shard STSs express declaratively. Rejected.
- **Single STS + operator-patched labels / role anti-affinity / global spread** — each fails to
  achieve single-domain-loss survivability (see Context). Rejected.

## Deferred — Direction B (topology-aware role assignment)
Balancing *masters* across domains ("no node holds ≥2 masters") is role-keyed and one-shot
scheduling cannot re-enforce it after a failover; only the operator can (re-choose which
already-placed pod is master). B rides on top of A and is justified only if master
blast-radius distribution becomes a requirement. Revisit after A is dogfooded. See
`docs/PER_SHARD_STATEFULSET_DESIGN.md` §5.
