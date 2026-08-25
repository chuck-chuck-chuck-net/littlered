# ADR-017: State-Gated Intra-Shard Rolling Updates (Cluster Mode)

## Status

**Proposed.** Nothing in this document has been built or observed; every statement about how
the fix behaves is intent, not measurement, and is marked as such. The defect it responds to
*is* measured — see Context.

Extends **ADR-007** (per-shard StatefulSets) and its LR-021 cross-shard rollout serialization.
Prospective changelog entry **LR-047**. Does not supersede anything.

> ADR number: 017, after 016. 010 is still unallocated (the deferred ghost-replica prune policy)
> and 012 lives on the multi-site branch; checked across every branch, per LR-039's rule that an
> ID is claimed globally, not on the line you happen to be working next to.

## Context

A full-suite run on 2026-08-23 lost a shard's data in a rolling update **that reported complete
success**: all three shard StatefulSets at `observedGeneration == generation` and
`currentRevision == updateRevision`, all six pods replaced, `cluster_state:ok`, 16384 slots
assigned. CRC16 pins the loss exactly — shard 0 loaded 2 keys, shard 2 loaded 1, **shard 1
loaded 0**. The fresh `roll-cluster-shard-1-1` had no cluster contact from 21:06:14 to 21:08:16
while the StatefulSet deleted shard 1's master at 21:06:50: **96 seconds with zero copies of
`5462-10922`.** Pre-existing since 0.3.0.

**Nothing gates the handover on the replacement actually being a copy.** `buildClusterShardStatefulSet`
(`internal/controller/resources.go`) sets `UpdateStrategy: {Type: RollingUpdate}` with no
`rollingUpdate` block at all, so the intra-shard sequence belongs entirely to the StatefulSet
controller: delete the highest ordinal, wait until it is Running, Ready and available for
`minReadySeconds`, then delete the next — for a shard, `-1` (replica) and then `-0` (master).
Both of the controller's gates are blind to redundancy:

- **Readiness** (`buildClusterReadinessProbe`) is `[ ! -f /data/bootstrap-in-progress ]` plus a
  local `redis-cli ping`. It asserts that a process answers on a socket. It says nothing about
  cluster membership, slot ownership, or a replication link.
- **`minReadySeconds: 30`** is a wall-clock timer whose own comment lists three justifications,
  the third of which is *"Buffer for operator reconciliation"*.

So the invariant actually enforced before a shard's master is killed is *"the replacement pod
answers PING and has done so for 30 seconds"*, where the invariant data safety requires is
*"the replacement is a link-`up` replica of this shard's slot owner"*. Those are unrelated
propositions, and the second is not cheap: a replaced pod comes back on a wiped EmptyDir
(pillar 3.1), hence with a **new node ID**, hence needing the old ID `FORGET`-ed, itself
`MEET`-ed, `CLUSTER REPLICATE`-d and full-synced — all of which only the operator does, and
only in `reconcileCluster`'s `allPodsReady` branch. **The operator's entire window to restore
redundancy is therefore exactly `minReadySeconds` after the fresh pod passes PING.** Shards 0
and 2 used 15s and 19s of that 36s budget. This has been passing on margin, not on an
invariant, and the e2e will keep flaking until it does not.

The failure is then erased. Step 3's `SafeMissingShardTarget` assigns the orphaned range to a
reachable empty master — correctly, by its own contract — so the operator heals an
already-dead shard into a healthy-looking empty one. Combined with the OR-aggregated cluster
health (tracked separately), every signal the CR and the suite expose reads green over a shard
with no data. **This is LR-038's class exactly** — silent loss under a successful operation with
every assertion green — in the mode LR-038 did not cover. §7 rule 11 again.

**The framing that should have caught this.** LR-025 already named the unsafe state and removed
it from the migration path: *"the unsafe 'owns slots, no synced replica' state never exists"*,
enforced by the pure predicate `isLinkUpReplicaOf` (`internal/redis/migration_plan.go`). The
rolling update is the one remaining path that still transits that state — and it transits it on
the most routine operation the product has.

LR-046 closed the *latency* half of this incident (a blackholing dead pod IP starving the
reconcile ~100s) and says explicitly that it does not close this: a reconcile that is no longer
starved observes and acts sooner; it does not make a time-gated rollout wait for the right
state.

## Decision

Gate the intra-shard handover on redundancy, in the operator, and make every path that cannot
be gated loud.

1. **State-gate the rollout with `spec.updateStrategy.rollingUpdate.partition`.** The operator
   sets the partition to the shard's highest ordinal when a template change is first applied,
   and lowers it by one only once **every** pod at or above the current partition is
   simultaneously (a) at `UpdateRevision`, (b) Ready per the kubelet, and (c) a link-`up`
   replica of that shard's slot owner. The decision is a pure seam,
   `planShardRolloutPartition`, beside `clusterShardRolloutSettled` in
   `internal/controller/cluster_rollout.go`.

2. **When the gate cannot be satisfied, stall — forever, and loudly.** The partition holds, the
   old pods keep serving, and a `ClusterRolloutBlocked` condition plus one Warning event per
   transition names the shard and the failing clause. **There is no timer fallback.** Manual
   release is raising the partition by hand.

3. **`replicasPerShard: 0` warns and proceeds.** With no redundancy no rollout can be made safe,
   so gating is vacuous; the operator skips it and emits a Warning stating that the update will
   lose each shard's data.

4. **The preStop stops giving up silently.** On the last-copy branch — `resources.go` cluster
   preStop, *"No healthy replica found to take over. Proceeding with restart."* then `exit 0` —
   the pod first self-fences with `CONFIG SET min-replicas-to-write 99`, then exits as before.
   This is a mitigation, not a closure, and it ships **after** the gate in the same image.

## Rationale

### Why `partition`, and why the shape is forced rather than chosen

`partition` is the only mechanism Kubernetes offers for holding a StatefulSet's rollout
*mid-set*. Two constraints then determine the implementation, and neither is a preference.

**(a) The partition must be authoritative at build time.** Every StatefulSet is written through
`LittleRedReconciler.apply`, a server-side apply carrying `client.FieldOwner(fieldManager)` **and
`client.ForceOwnership`**. So whatever the build function computes wins on every pass, and any
out-of-band write — a patch from a healing step, a `Scale` write — is force-overwritten by the
next reconcile. That is the flap LR-044's wiring half predicted and measured the shape of, and
here it would be worse than useless: a partition that oscillates back to 0 each pass releases the
master while the replica is still unsynced, which is precisely today's defect on a 2s cycle. So
`buildClusterShardStatefulSet` takes the partition as a **parameter**, exactly as it already
takes `shardIdx` — a builder renders a decision, it does not make one (LR-044).

**(b) The cursor is the StatefulSet's own field, so nothing new is persisted.** The gate needs
Redis-level state, which exists only after the gather, while the apply runs at step 1 — the same
pre/post-gather split LR-044 faced. But where LR-044 needed `status.quarantinedSince` to hold the
state (its verdict provably self-clears), here **the live StatefulSet's `partition` value *is* the
cursor**: the pre-gather apply reads it off the existing object and re-applies it unchanged; the
post-gather step lowers it when the clauses pass. No status field, no annotation, nothing to
reconcile against ADR-006.

The value is therefore **monotone non-increasing except on a new template change**, which is what
makes it flap-proof, and both edges are single-direction in the LR-044 sense.

### LR-021 composes for free, and is not touched

With `partition > 0` the StatefulSet controller does not advance `CurrentRevision`, so
`UpdateRevision != CurrentRevision`, so `clusterShardRolloutSettled` stays false and
`reconcileClusterStatefulSet` keeps deferring every later shard. The cross-shard serialization
LR-021 built therefore continues to hold at shard granularity with **no change to that function**,
and the two gates nest cleanly: at most one shard rolls, and within it at most one pod, and only
when the shard is redundant.

The requeue cadence needs no change either, and this is worth stating because it is not obvious.
A freshly replaced pod that has not yet been reattached is an **empty master** (`role:master`,
no slots), so `HasEmptyMasters()` is true, so `IsHealthy` is false, so the instance reports
`Initializing` and stays on the **fast (2s)** interval for the whole rollout — LR-014's clause,
doing exactly the job it was added for. The repair loop that makes the replica sync is thus
already running at the cadence the gate depends on.

### What the gate waits for, and what it merely reports

*Amended after the seam was built (M2); the original Decision 1 named one redundancy clause and
left the holding-versus-blocked boundary open.*

Clause (c) is **two questions, and only one of them is the gate.** `SyncedWithOwner` — the LR-025
predicate, a link-`up` replica of the shard's slot owner — is the gate, unchanged.
`AttachedToOwner` — a replica of the owner at all, link state aside — is **reporting only**.

The split exists because a flat "Ready for longer than T while failing clause (c)" would fire on a
legitimate large-dataset **full sync**, which this ADR's own Consequences say can run for minutes
per pod. The one alarm that must not cry wolf would then cry wolf on exactly the topology we tell
users to expect, and no choice of T fixes it, because the sync is genuinely unbounded. The two
failure shapes are distinguishable from live state, so they are distinguished:

- **not attached at all** — what has to happen is the *operator's* reattach (FORGET the old node
  ID, MEET, `CLUSTER REPLICATE`), all of it on the **fast 2s cadence** for the whole rollout,
  because a not-yet-reattached pod is an empty master ⇒ `HasEmptyMasters()` ⇒ `IsHealthy` false
  (LR-014's clause, doing the job it was added for), with gossip converging ~1-2s after the MEET.
  That has a real, dataset-independent bound.
- **attached, link down** — a full sync in flight. Dataset-dependent, unbounded, and genuine
  progress. **Never reported blocked, however long it takes.**

So: blocked ⟺ at `UpdateRevision`, kubelet-Ready for ≥ `clusterRolloutReattachBudget` (**120s**,
matching `status.cluster.wipeDeadlockSince`, LR-023), **and** not attached to the owner at all.
`ReadySince` comes from the pod's own `Ready` condition `LastTransitionTime`; a zero value is never
blocked, because unknown is not evidence. Nothing is persisted, and the emitted partition is
byte-identical whether a hold is blocked or not — **the distinction changes what the operator says,
never what it does.**

**Decision order, which turned out to be load-bearing.** `Complete` is checked *before* the clauses:
a settled shard's own master **owns** the slots and is therefore nobody's replica, so evaluating
clause (c) against it would report a healthy steady-state shard as holding, and after 120s as
`ClusterRolloutBlocked` — a permanent false alarm on every healthy cluster. And the template-change
check precedes `Complete`, because at first sight of a change the shard is still settled on the
*old* template, so the other order would emit `Complete` and never gate at all.

**Two edges resolved toward "never raise":** a partition above the highest ordinal is clamped
**down** (a `replicasPerShard` decrease), and an absent partition reads as 0 — Kubernetes' own
default — rather than as "no gate". The `replicasPerShard: 0` verdict emits **no partition field at
all** rather than 0, so that StatefulSet stays byte-identical to today's.

### Resolving "the shard's slot owner", and the reads the gate may trust

*Amended after the wiring was built and verified live (M3).*

**Ownership is resolved by slot containment, not by exact range equality.** The seam's comment
named `ownerOfRange`, which requires an exact match and therefore returns *no owner* on a
fragmented or mid-reshard range (LR-018) — and no owner means clause (c) can never be satisfied,
i.e. a permanent stall on a cluster that is merely resharding. `shardSlotOwner` keys on the START
of the shard's expected aligned range and accepts any master whose ranges contain that slot;
containment degrades to the exact case and covers the rest. It iterates `ClusterPodRefs` rather
than the `gt.Nodes` map, so that when two nodes transiently claim a slot mid-failover the verdict
does not depend on Go's map hash seeding.

**When no owner can be resolved, nobody is synced and the gate holds.** That is the safe direction
and it is explicit in the wiring (`ownerID != ""`), not an accident of the predicate.

**Which reads the gate may trust.** The cursor is read **uncached** — from the API server, both
pre-gather and post-gather — because a stale-*low* partition would release the shard's master while
its replacement is still unsynced: the defect itself, arriving through a cached read. The shard's
*pods* are read uncached for the same reason, since clauses (a) and (b) come off them and a cached
pod that has not caught up with a deletion could satisfy both on behalf of a pod that no longer
exists. Both are confined to the in-flight window, so the steady loop keeps its cached read and
LR-043's cost argument for not making the steady path uncached is untouched.

The *in-flight test itself* is taken from the cached object, and that is sound rather than
circular: `clusterShardRolloutSettled` requires `ObservedGeneration == Generation`, and every write
that starts or steps a rollout bumps `Generation`, so a cached object that is behind either still
carries the old template hash or carries the new spec with a status that has not observed it.
"Settled **and** at the desired hash" is not a state a mid-rollout object can present — and an
informer cache only ever lags *backwards* in time, so it cannot show a settled future. The single
exception is a rollout started at an **unchanged** operator template hash, which is what a manual
`kubectl rollout restart` is; that path is already outside this gate by LR-021's scope, and falls
back to today's ungoverned behaviour rather than to something worse.

**`Complete` emits `partition: 0` rather than no partition at all,** so a shard that has rolled once
carries the field thereafter. It costs one extra `Generation` bump the first time and no rollout,
since the partition is outside the hashed pod template.

### Stall forever, loudly — and why a fallback would be worse than the defect

LR-043's correction is the governing precedent, and its lesson generalizes directly: **a guard
that can deny a legitimate own state is more dangerous than one that admits a bad state in a
narrow window, because the deny is a permanent stall and the admit needs a coincidence.** Here
the asymmetry runs the *other* way and settles the question in the same breath: a stalled rollout
is availability-safe — the old pods are serving, the data is intact, the instance is simply not
upgraded — while a time-released one is exactly the lossy path this ADR removes. A fallback that
lowers the partition after N minutes is not a compromise between the two; it is the current
defect with a longer timer.

What the stall must be is **loud**, since the failure mode of the whole change is over-suppression
and the operational tell has to exist before anyone needs it. `ClusterRolloutBlocked` is a
dedicated condition rather than a `Ready=False`, deliberately: a stalled rollout is a rollout that
has not finished, not an unhealthy instance, and conflating them would train an operator to ignore
the one signal that matters. (`Ready` will nonetheless read false for the ordinary LR-014 reason
above while an empty master is present; the new condition is what distinguishes "converging" from
"stuck".)

### The preStop fence is a mitigation, and it is not optional

A naive preStop refusal is not a fix — the kubelet SIGKILLs after
`terminationGracePeriodSeconds` (unset on cluster pods, so the Kubernetes default 30s; the hook's
own role-swap wait is 10s), so refusing only delays the loss. That is not what this is. The fence
follows LR-038 Addendum 2: *"I am being terminated" is local knowledge that cannot be wrong*, and
the pod is the only party holding it instantly. `CONFIG SET min-replicas-to-write 99` is
**target-free on purpose** — needing to know a successor would reintroduce the race the fence
exists to remove — and it converts acknowledged-then-lost writes into visible failures. Pillar
3.2's "errors rather than silent data loss", applied to a rollout instead of to memory pressure.

It is not optional because `partition` governs **operator-triggered rollouts only** — LR-021's
documented limitation, inherited verbatim: *"a manual `kubectl rollout restart` of the shard
StatefulSets bypasses the operator and is not serialized."* Node drains, evictions and manual
restarts are covered by nothing else. For those the fence is the only defence there is.

**The ordering hazard, stated because getting it wrong turns the fix into an incident.** The
preStop lives in the pod template, so changing it changes `AnnotationPodSpecHash` and **triggers
one rolling update of every cluster instance on upgrade** — the very operation that is unsafe
today. It must therefore ship in the same image as the gate and never before it, so the rollout
it triggers is already governed.

### `replicasPerShard: 0` — say it out loud

With one copy per shard, any rollout takes that copy down; this is pillar 3.1 working exactly as
designed and no operator-side gate can change it. Refusing the rollout would make a documented
topology un-upgradable — a larger behaviour change than the defect — and proceeding silently
leaves a data-losing operation entirely unannounced. So the operator warns and proceeds, which is
the first time the product says this anywhere.

### `bootstrapCluster`'s revision gate is incompatible with a partitioned rollout

Not in the problem report; found while grounding this ADR, and it must be fixed in the same
change.

`bootstrapCluster` (`internal/controller/cluster_reconcile.go`, the per-pod loop around :753)
requires `podRevision == shardSTSs[ref.ShardIdx].Status.CurrentRevision` and requeues at 1s
otherwise. **With `partition > 0`, `CurrentRevision` does not advance**, so every pod already
updated to `UpdateRevision` fails that gate and bootstrap requeues forever.

Unreachable on the normal path — bootstrap runs only on `TotalSlots == 0` with no replicas, and a
populated cluster mid-rollout has slots. But it is reachable in **exactly this defect's
aftermath**: a rollout that has already dropped all of a cluster's slots. Introducing a permanent
stall on the recovery path for the failure we are fixing is not acceptable.

**Recommended fix: accept `UpdateRevision` as well** (`podRevision == CurrentRevision ||
podRevision == UpdateRevision`). It is minimal and it preserves the gate's stated purpose intact —
the comment says the gate exists because *"terminating pods from the old deployment may still
exist with stale IPs and the same names"*, and both revisions are **this** StatefulSet's own, so
neither is the stale foreign deployment the gate is aimed at. The alternative — dropping the gate
in favour of the LR-043 uncached read plus the inline `deletionTimestamp` refusal, which are what
actually do the freshness work (LR-043 says so: *"the revision gate bounds which deployment a pod
object belongs to, not how fresh its cached `Status.PodIP` is"*) — is cleaner and strictly larger,
touching a bootstrap path LR-043 hardened weeks ago and which no unit test can reach. Deferred as
its own decision if ever wanted.

## Consequences

- **Rollouts get slower, and the shape of the bound changes.** From
  `shards × pods × (ready + minReadySeconds)` to
  `shards × pods × (schedule + FORGET/MEET/REPLICATE + full sync)`. For a large dataset the full
  sync dominates and can be minutes per pod. This is the correct trade and it must be documented
  in USAGE, because a user watching a 3-shard cluster take twenty minutes to roll will otherwise
  file it as a hang.
- **`minReadySeconds: 30` is retained as pure defence in depth**, and its comment must stop
  presenting itself as the safety mechanism. Whether to lower it once the state gate is real is a
  later question; the number must never again be mistaken for a guarantee.
- **Upgrading to this build triggers no rollout of its own from the partition change**, because
  `partition` lives in `spec.updateStrategy`, outside the pod template hashed by
  `AnnotationPodSpecHash` — the LR-044 "byte-identical" property, and it should be pinned by a
  test rather than asserted here. The **preStop** change does trigger one rollout, by design and
  under the new gate (above).
- **New surfaces:** a `ClusterRolloutBlocked` condition and its Warning events; a Warning for the
  `replicasPerShard: 0` case. No new status fields, no new RBAC — the reconciler already owns the
  StatefulSets it applies, and the cursor is one of their own fields.
- **An operator outage stalls a rollout instead of losing a shard**, which is the failure
  direction we want and is also the deterministic reproduction (below).
- **The residual we accept:** `partition` governs operator-triggered rollouts only. Node drains,
  evictions and `kubectl rollout restart` remain outside it and are covered only by the preStop
  fence, which converts loss into visible failure rather than preventing it. Closing that properly
  is the deferred readiness-probe alternative below.
- **`lrctl verify` reports `[OK] Cluster is healthy and consistent.` on a cluster whose shard has
  been destroyed** — captured on the M1 repro. This is *not* the OR-aggregation blindness tracked
  separately in `BACKLOG.md`: here every node agrees and every node is right about the topology.
  The project's designated ground-truth tool simply has no data-survival predicate. Recorded
  because it is why nothing caught the field incident either.
- **A residual of the gate's formulation, found in review and not closed:** clause (c) asks each pod
  at or above the partition to be a *replica* of the shard's owner, which is unsatisfiable for a pod
  that IS the owner. In the normal flow this never bites — such a pod is the one the StatefulSet is
  about to replace, and after replacement it is a replica — but if a repair path promotes a
  freshly-replaced pod back into ownership (a Step 3 `SafeMissingShardTarget` assignment onto it),
  the gate holds forever. That degrades to the chosen behaviour, a loud stall, rather than to loss;
  it is named here as a live-verification target rather than pre-emptively special-cased, since
  every extra clause on this predicate is what LR-043 warns about.

  *Amended after the M5 live sweep (2026-08-25, LR-047 Addendum 2).* The bound above is wrong in one
  direction and the reporting consequence was real. The owner-in-the-survey case does not need a
  repair path to promote anything: when the partition reaches 0 the StatefulSet deletes the shard's
  master, its preStop hands mastership to the replica, and that **promoted replica is the owner while
  still at an ordinal at-or-above the partition** — so it bites *at* partition 0, on every rollout, as
  a matter of course. The **gate** consequence is exactly as predicted and benign (a hold that
  self-clears when the shard reaches `Complete`, never a loss). The **report** consequence was not
  benign and is fixed: such a pod was counted as a stalled pod and produced a false
  `ClusterRolloutBlocked` seconds before a normal rollout finished. `podStalled` now excludes the
  shard's own owner (`shardRolloutPod.IsOwner`). The gate's clauses are deliberately unchanged.
- **A cross-mode note (§7 rule 11):** sentinel mode has the same "rollout gated by readiness plus
  `minReadySeconds`" shape and is safe only because **its readiness probe happens to be
  redundancy-aware** — it requires `role:master` or `link:up`, which LR-016 explicitly *kept* while
  stripping the liveness probe. That asymmetry is why cluster mode has this hole and sentinel does
  not; it is not a second defect to fix.

## Verification plan

Nothing below has been run. The order is the project's test discipline, tiers 1 and 3.

1. **The deterministic reproduction, observed RED first.** Scale the operator to 0 for ~40s
   immediately after a shard's replica pod is recreated, then assert byte-exact survival of that
   shard's keys. It isolates this defect from LR-046's latency half by construction — with the
   operator down, no amount of probe-bounding helps — and it must go red for the defect's actual
   reason (that shard's keys gone, topology reporting healthy), not for a timeout.
2. **The pure seam, red-first.** `planShardRolloutPartition` as a table, authored against a stub,
   with a mutation check in the other direction (an "always lower" mutant must fail exactly the
   rows that must hold), since a hold-everything stub passes the negative rows vacuously.
3. **Live on t3e, full.** The repro tier green after the fix, then `Cluster Mode Rolling Update`,
   `Cluster Mode Chaos Testing` and `Cluster Total-Wipe Re-Bootstrap` re-run green — the last
   because the wipe tiers are the ones that exercise bootstrap and the fresh-pod paths this change
   touches, and LR-043's regression was found by exactly that tier.

An honest caveat recorded in advance: the failure mode of this change is **over-suppression** — a
rollout that stalls when it should have proceeded — and, as with LR-043, unit tests cannot prove
its absence. The `ClusterRolloutBlocked` condition firing outside a genuine sync failure is the
operational tell.

*After M5 (2026-08-25):* the tell worked, and it was the condition itself that was wrong rather than
the gate — see LR-047 Addendum 2. The **gate** has not been observed over-suppressing: the full
cluster sweep (rolling update at `replicasPerShard: 1`, both wipe flavors plus partial wipe, all
three chaos contexts, functional, reshard, the repro tier, and an operator upgrade across the preStop
template change) completed with no stalled rollout and no lost key. The condition is now reachable on
demand by holding the operator down past `clusterRolloutReattachBudget` mid-rollout, which is how it
was first exercised.

## Alternatives considered

**Make the cluster readiness probe redundancy-aware** (Ready ⟺ owns slots, or is a link-`up`
replica). Genuinely attractive, and the strongest alternative here: it is local knowledge, so it
respects LR-016; sentinel mode already does exactly this; it needs no operator machinery at all;
and unlike `partition` it would govern manual rollouts, drains and evictions too, and would make
the per-shard PDBs meaningful, since PDB availability *is* readiness. **Rejected as written,** for
one reason that is fatal: at bootstrap every pod starts isolated with no slots and no master, so
every pod is not-Ready, so `allPodsReady` never holds, so the operator never gathers and never
bootstraps — the LR-018/LR-023 "repair step that can never fire" trap, load-bearing on the first
pass of every new cluster. Making it work requires decoupling the operator's `allPodsReady` gate
from kubelet readiness, and kubelet readiness is precisely LR-023's blackhole-proof data-safety
signal. That is its own decision, not a rider on this one. **Deferred, with that reason.**

**Raise `minReadySeconds`.** Rejected: more margin on a mechanism that is margin. The 30s was
already three-quarters spent on the two shards that survived.

**Rely on the per-shard PDBs.** They do not cover this, and the point of recording it is that they
look as if they might: a PDB governs the **eviction API** — drains — not StatefulSet rolling
updates and not direct deletes. Neither today's defect nor a `kubectl delete pod` consults one.

**A naive preStop refusal** (block until a replica exists). Rejected: the kubelet SIGKILLs at the
grace period, so it delays the loss rather than preventing it, and it would make every rollout
pay the full grace window. The fence keeps the `exit 0`.

**A timer fallback on the stall.** Rejected — see Rationale. It is the current defect with a
longer timer.

## References

- `CLAUDE.md` pillars 3.1 (EmptyDir, why a lost copy is lost), 3.2 (errors over silent loss),
  3.12 (per-shard StatefulSets, LR-021 serialization), 3.13; §7 rules 6, 7, 11
- ADR-007 (per-shard StatefulSets, the rollout serialization this extends), ADR-008 (kubelet
  readiness as the blackhole-proof data signal), ADR-013 (the migration whose LR-025 redesign
  established the invariant), ADR-016 (the SSA/`ForceOwnership` build-time constraint, and the
  pre/post-gather split)
- `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` — prospective **LR-047**; LR-021 (cross-shard
  serialization), LR-025 (`isLinkUpReplicaOf`, "owns slots with no synced replica"), LR-038
  (silent loss under a green operation; the pod-local self-fence), LR-043 (over-suppression as a
  permanent stall; the bootstrap revision gate's true scope), LR-046 (the latency half, explicitly
  not this), LR-014 (empty master ⇒ unhealthy ⇒ fast cadence), LR-016 (what a probe may decide)
- `BACKLOG.md` — the `(B)` entry, which is the problem statement of record
