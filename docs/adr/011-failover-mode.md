# ADR-011: `failover` Mode — Operator-Managed HA without Sentinel

## Status
Accepted (mode ships as **experimental**; see Lifecycle).
Amended 2026-08-17 (LR-038): §7 gains the outgoing-master **fence**, and §8's graduation
gate gains a **durability** bar — an availability bar alone graduated a mode that
silently lost acknowledged writes on every graceful handover.

## Context

`docs/FAILOVER_MODE_DESIGN.md` records the motivation and the framing decisions
(2026-06-24 session): the recurring source of fragile sentinel-mode failures is that the
operator and Sentinel are **two independent failure-detectors fighting over the same
state** (the LR-007/LR-008 saga, the LR-024 ghost-master deadlock self-inflicted by Rule
D's `SENTINEL RESET`, and the whole ADR-010 ghost-replica problem class). LittleRed
already routes clients via the operator-managed `redis.chuck-chuck-chuck.net/role: master`
label — Sentinel is a second, sometimes-conflicting authority, not the routing source of
truth. The design note fixed: the name (`failover`), that `sentinel` stays
(feature-completeness), that the mode ships experimental with a written graduation gate,
and the accepted trade-off (HA becomes coupled to operator liveness — the critical
dependency *moves* from the Sentinel pods to the operator, which sentinel mode's hard
failures already depend on anyway).

This ADR records the concrete design: API, topology, startup protocol, failure
detection, the failover decision, and its safety gates. Decision drivers are the
established ones: data safety over availability (ADR-001/005/008), pure unit-testable
decision seams with deterministic tie-breaks (ADR-006/008), kubelet-local readiness as
the authoritative data-presence signal — never the operator's remote dial alone
(ADR-008, LR-017), topology decisions only from the operator's global view (LR-016),
recompute from live state / persist nothing load-bearing (ADR-006), and refuse-over-lose
with an explicit opt-in only where no non-lossy path exists (ADR-005 vs ADR-006).

## Decision

### 1. API

- `spec.mode` gains `failover`. A new **`spec.failover *FailoverSpec`** section
  (CEL-gated to the mode, like `spec.sentinel`/`spec.cluster`):
  - `replicas` (int32, default **2**, min **1**) — replica count; total data pods =
    `1 + replicas`. First mode with a configurable replica count (SCOPE.md already
    planned this); nothing in the operator-led design depends on a fixed count (no
    quorum math).
  - `downAfterMilliseconds` (default **5000**) — the sustained-failure window before the
    operator declares the master dead on probe evidence (mirrors the sentinel knob it
    replaces).
  - `minReplicasToWrite` (default **0** = off) — rendered into `redis.conf`
    (`min-replicas-to-write`). Off by default: parity with sentinel mode keeps the
    graduation-gate comparison honest; bounded write loss is an explicit user choice.
  - `allowUnsafeRebootstrapOnDeadlock` (default false) — same contract as the sentinel
    field (ADR-005): guards exactly the case where electing a master discards data on
    diverged holders.
- Sentinel-only concepts (`quorum`, `failoverTimeout`, sentinel container resources) do
  **not** appear. Rejected alternative: reusing `spec.sentinel` with a relaxed CEL rule —
  less API surface, but leaks meaningless Sentinel concepts into a Sentinel-free mode;
  pre-1.0 we prefer the correct shape.
- **Status**: reuses `bootstrapRequired`, `master{podName,ip}`, `replicas`, `redis`,
  and the `LeaderlessRecovery`-style condition machinery. New monitoring surfaces
  (nothing load-bearing is persisted — every value is re-derivable from live state):
  `status.failover.masterDownSince` (detection window / recovery cooldown marker, the
  `...Since` pattern of ADR-005/008), `status.failover.assignmentEpoch` (mirror of
  the epoch stamped on pods, see §3), and `status.failover.transitionSince` (stamped on
  every epoch bump; anchors the §6 post-transition cooldown — the one value a live
  re-derivation cannot reconstruct, and losing it merely skips one cooldown window, so
  still nothing load-bearing). `ConditionSentinelReady` is not used; phase
  computation is derived from operator ground truth (readiness + gathered
  `master_link_status`), not from any Sentinel view.

### 2. Topology and resources

One Redis StatefulSet (`1 + replicas` pods, parallel pod management), the label-routed
master Service **unchanged** (this label being the sole writer-selector authority is the
point of the mode), the replicas headless Service, a PDB over the data pods (≥2 pods
always, so the PDB redundancy rule holds). **No Sentinel StatefulSet, no sentinel.conf,
no sentinel Service, no sentinel PDB.** TLS/auth plumbing, exporter sidecar, and
strict IP-only identity (ADR-001) carry over unchanged.

### 3. Startup protocol — operator assignment via downward API

Pillar 3.6 is kept: **a Redis pod does not start `redis-server` until the operator has
assigned it a role.** The Sentinel query loop is replaced by an operator-stamped
assignment channel:

- The operator patches each data pod with annotations:
  `redis.chuck-chuck-chuck.net/assigned-role` (`master`|`replica`),
  `.../assigned-master-ip` (empty for the master), and `.../assignment-epoch`
  (monotonic per instance).
- Pods mount their own annotations via a **downward-API volume**; the startup script
  polls the file until an assignment is present and *fresh* (see epoch gate), then
  `exec`s `redis-server` — bare for `master`, `--replicaof <ip> <port>` for `replica`.
  **No reachability PING before starting as replica** (ADR-002's deadlock constraint):
  Redis's own retry logic handles a temporarily unreachable master; the operator
  repoints later if the target is truly dead.
- **Epoch gate — the ADR-001 same-IP kill-9 hazard, re-owned.** Before `exec`, the
  script writes the assignment epoch to a run-marker on the EmptyDir
  (`/data/littlered-run-epoch`). The marker survives a container restart (same pod,
  same IP, wiped dataset) but not a pod replacement. On start, an assignment is honored
  only if there is no marker **or** the annotation epoch is **greater** than the marker
  epoch. A kill-9'd ex-master therefore cannot reclaim mastership from its stale
  `assigned-role: master` annotation — it parks until the operator, seeing the restart
  with its global view, either fails over to a data-holding replica (normal case, epoch
  bumped, this pod re-assigned as replica) or re-authorizes it as master (no data
  anywhere). This is Sentinel-mode's run-id yield with the operator as the authority.
- The epoch is **derived from live state** when bumped (max over current pod annotation
  epochs + 1), never read back from status; status only mirrors it.

Rejected assignment channels: **K8s API self-query** (fastest propagation, but puts the
API server in the pod boot path and hands data pods API credentials); **ConfigMap-published
master** (kubelet propagation just as bounded, plus entangles assignment churn with
config-hash rollout detection); **boot-bare + operator shaping** (weakest pillar-3.6 fit —
an empty restarted master would serve clients while still labeled, and transient
multi-master windows become normal).

### 4. Failure detection — reconcile decides, a watcher accelerates

- The **reconcile loop is the sole decider** (LR-016: global view). A per-instance
  background goroutine (`failover_monitor.go`, same ensure/stop/channel scaffolding as
  today's `sentinel_monitor.go`) probes the current master IP on a ~1s cadence with
  `ProbeTimeout`-bounded `INFO` and pushes a `GenericEvent` when a failure streak
  crosses `downAfterMilliseconds` — replacing the `+switch-master` subscription as the
  fast path. Pod add/delete/readiness events already trigger reconcile via ownership.
- The master is declared dead iff:
  - **(a) Kubernetes-authoritative:** the master pod is deleted/replaced or its redis
    container is not-Ready per kubelet. Acted on immediately — the kubelet's local
    probe is blackhole-proof (ADR-008), and in a pure in-memory instance a not-Ready
    redis holds no data.
  - **(b) Probe-evidenced, corroborated:** the master has been unreachable to the
    operator for ≥ `downAfterMilliseconds` (tracked in `masterDownSince`) **and** every
    reachable replica reports `master_link_status:down`. The corroboration requirement
    is the LR-017 lesson: the operator's own dial can be blackholed while the master is
    alive and serving; replicas provide independent viewpoints. Operator-unreachable
    but replica-links-up ⇒ operator-side network issue ⇒ **no action**.
- Slow-vs-dead discrimination and flap suppression (ADR-003a's "free hardening" we now
  own): probes use `INFO` (not bare PING), the sustained window filters blips, and a
  completed transition starts a cooldown (§6) so cascading flips are serialized.

### 5. The failover decision — one pure seam

A single pure function, **`planFailover`**, owns every "who should be master" decision —
bootstrap seeding, normal failover, and the deadlock matrix that sentinel mode needed
three separate rules for (Rule L, LR-024, bootstrap). Sentinel mode's split existed
because Sentinel was a second authority to wait on; with one decider the cases collapse
into one decision table, keyed on the gathered `RedisNodeState`s (reusing `DataHolders`,
`BestDataHolder`, and the LR-024 union-find lineage predicate `holdersDiverged` over
`{master_replid, master_replid2}`):

- **No live master, 0 data holders** → seed `redis-0` (deterministic; `pickBootstrapMasterIP`).
- **No live master, ≥1 holders, all one lineage** → promote `BestDataHolder` (highest
  offset, tie-break keys then IP — offsets are comparable within a lineage). **No
  opt-in**: a normal post-failover promotion chain is one lineage (LR-024 lesson —
  never key lineage on `master_replid` alone).
- **No live master, holders in ≥2 lineages** → **refuse** (condition
  `RefusedDataPresent`, loud event) unless `allowUnsafeRebootstrapOnDeadlock`; then
  elect the best holder and log what is discarded.
- **Live master exists** → no promotion; stragglers are repointed (Rule R reused
  verbatim: `SLAVEOF <master>` to any reachable node that is `role:master`-but-unlabeled
  or following the wrong IP).

Execution order on promotion: bump epoch + stamp assignments (annotations), `REPLICAOF
NO ONE` on the target (reusing `electMaster`/`needsPromotion`), flip the master label
(traffic moves), repoint the rest. Every step is idempotent and the sequence is
**resumable from live state** (ADR-006 discipline — a half-applied transition is
re-derived, not tracked in a persisted cursor).

### 6. Guards (the Rule A analog, redefined)

Sentinel-mode Rule A ("no terminating pods, no `FailoverActive`") gated the operator's
*nudges to Sentinel*. In failover mode the operator **is** the failover mechanism, so
the guard set differs:

- **Promotion is never blocked by the dead master's own termination** (a crash failover
  is exactly the moment a pod is terminating). It is gated on: the decision inputs
  being a completed gather, and **no unsettled prior transition**, plus a short
  post-transition cooldown keyed on `status.failover.transitionSince`, serializing
  cascades. "Unsettled" is deliberately narrower than "not settled": a transition
  blocks a NEW mastership decision only while its target — the intended master — is
  still **alive and converging** (reachable but not yet `role:master`+labeled). A
  dead/unreachable intended master never blocks its own replacement — gating on bare
  unsettledness would deadlock exactly the crash and graceful-handover recoveries this
  mode exists for, since a dead target can never again converge (red-first proven,
  `failoverPromotionUnsettled`). Secondary healing (below) still gates on full
  settledness.
- **Secondary healing** (straggler repoint, status-label corrections) keeps the
  conservative gate: a verified live consensus master and no terminating pods.
- Probes make no topology decisions (LR-016): liveness is a plain local health check;
  readiness requires `link:up` on replicas so a masterless/mis-pointed replica is
  pulled from traffic but never killed.

### 7. Graceful handover

On a master pod carrying a `deletionTimestamp` (rolling update, drain, deliberate
delete), the operator performs the §5 promotion *proactively* during the grace period
(the sentinel-mode preStop's `SENTINEL failover` becomes operator-led switchover); the
preStop hook only sleeps to hold the grace window. Rolling updates keep the existing
one-pod-at-a-time StatefulSet semantics.

**Promotion alone is not a handover — the outgoing master must also be fenced**
(amended 2026-08-17, LR-038). Promoting a replica says who the *new* master is; it does
nothing about the *old* one, which on a graceful delete is still alive and still
mastering for the rest of its preStop window. Two things then conspire: the operator
never speaks to it (the straggler repoint that would is blocked by its own
`!anyTerminating` gate, §6), and an established client connection through the master
Service is **not** re-routed by the operator's label flip. So the client keeps writing
into a doomed pod for the whole grace window — *however fast the operator promotes* —
and those writes die with it. Measured on t3e: **202 of 1171 acknowledged writes lost**,
with `DataCorruptions: 0` and write availability 97.66%. The keys were gone, not wrong,
so nothing caught it.

So the promotion carries a fence: demote the outgoing master (`REPLICAOF <new-master>`,
pure `planFailoverFence`) so it answers `-READONLY`. The loss becomes **visible write
failures instead of silent data loss** — pillar 3.2's principle, applied to failover
rather than to memory pressure. It is the §6 straggler repoint applied to the one pod
that gate excludes, at the one moment it matters, so it is best-effort and idempotent;
it is skipped when the outgoing master is unreachable (the crash path leaves nothing to
fence), already demoted, or is itself the pod being promoted.

This is a **data-safety** property, and it cut the other way from the availability
numbers: failover mode *beat* sentinel mode on write availability in both variants while
being the only one of the two that lost acknowledged writes. Sentinel's pod-led preStop
(`SENTINEL failover mymaster`, then wait for the address to change) converts handover
into visible write failures; failover-graceful converted it into silent loss. Bounding
that loss is part of the graduation gate (§8) — an availability bar alone would have
graduated the mode with this hole open.

### 8. Lifecycle

Ships **experimental**: setting `mode: failover` is the opt-in; the operator emits the
neutral warning event from the design note on first reconcile; docs label it
experimental. The graduation gate is written in `FAILOVER_MODE_DESIGN.md` §4 — the full
HA e2e suite (graceful, crash, hybrid double-failover), chaos/soak, and managed-cloud
dogfooding evidence, matching sentinel-mode's bars (data corruptions 0, write
availability > 0.40 over the 120s chaos window, recovery ≤ 90s with the watcher path
< 15s).

**Plus a durability bar (added 2026-08-17, LR-038):** on the *graceful* path, at most a
handful of acknowledged writes may be lost — the bound is the replication lag at the
promotion instant (~1 per failover at the chaos client's 10 writes/s), asserted as ≤ 5
over the two-failover tier. This bar exists because the availability bars above are
blind to it: the mode passed every one of them while losing 17% of acknowledged writes
(§7). It is measured by the chaos client's exact post-traffic sweep over every
acknowledged write, not by the sampled read counters, which cannot distinguish one lost
key read five times from five lost keys. The *crash* path is deliberately not bounded —
a kill -9 loses the unreplicated tail by construction.

## Consequences

- **Removed problem classes**: no Sentinel tables ⇒ no ghost replicas, no `SENTINEL
  RESET`/`REMOVE`+`MONITOR` healing, no two-cooks races (ADR-010's entire subject),
  no bare-sentinel bootstrap deadlock (Rule L's trigger). The deadlock matrix survives
  only as branches of `planFailover`.
- **HA is coupled to operator liveness** (accepted, design note §2.3): a master death
  during operator downtime waits for the operator. Mitigations: leader election, fast
  restart, the watcher's seconds-range detection.
- **We own Sentinel's hardening** (detection windows, flap suppression, epoch fencing) —
  encoded as pure, red-first-tested decision functions rather than Sentinel timers.
- New test surface: failover-mode analogs of the sentinel functional/failover/kill-9/
  chaos/deadlock e2e suites, a `verifyFailoverTopologySync` helper reading `INFO
  replication` (not `SENTINEL master`), and dedicated unit tables for `planFailover`
  and the master-death predicate (also closing the inherited gap: `DetermineRealMaster`
  had no dedicated table test).
- `test/e2e/failover_test.go` (which tests *sentinel* label mechanics) is renamed to
  avoid claiming the failover-mode name.

## References
- `docs/FAILOVER_MODE_DESIGN.md` (motivation, naming, lifecycle, graduation gate)
- ADR-001 (IP identity, kill-9 hazard), ADR-002 (startup deadlock constraints),
  ADR-003/003a (interference lessons, deferred hardening), ADR-005 (data-aware
  recovery matrix), ADR-006 (resumable-from-live-state), ADR-008 (kubelet readiness as
  data gate), ADR-009 (anti-churn: no proactive failback/balancing), ADR-010 (draft —
  ghost-replica class this mode eliminates)
- CLAUDE.md §3.4–3.10, §7 (test discipline)
