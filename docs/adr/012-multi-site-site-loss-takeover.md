# ADR-012: Multi-Site Site-Loss Recovery (Quorum-Gated Takeover + Master Balancing)

## Status
Accepted (design). Multi-site cluster mode ships **experimental** (pillar 3.15).
The mode's non-recovery machinery — stretched-cluster bringup, cross-site
replication, round-robin placement, union gather, per-site readiness/wipe gating —
is implemented and live-verified on a real 4-cluster lab (hub + 3 sites). The
**site-loss recovery** this ADR specifies is **phased**, and its status is called
out per part:

- **Phase 1** — the quorum-arithmetic takeover (D1), the master-balance invariant +
  drift rebalancer (D2), the 2-site even-`K` auto-takeover and the ≥3-site native
  path (D3). *Design accepted; implementation pending.*
- **Phase 2** — an out-of-band STONITH fence provider + a site-quorum leg (D4),
  which lift the drifted / majority-loss / multi-site-loss cases from fail-closed to
  automatable. *Scoped, not started.*

This ADR is written partly as a **challenge surface** (like ADR-009): its core
claims are falsifiable arithmetic. "How to challenge this" says what reopens it.

## Context

Multi-site cluster mode runs one stretched Redis Cluster across sites (workload
Kubernetes clusters), every shard hosted in `sitesPerShard ≥ 2` sites, managed by a
single hub operator. Redis handles pod- and node-level failover natively. The one
failure the operator must own is **the loss of a whole site whose masters the
survivors cannot fail over natively.**

Why that can happen (verified against Redis 8.4 `cluster_legacy.c`): a
`clusterUpdateState()` **unconditional** minority check — independent of
`cluster-require-full-coverage` — sets `cluster_state:fail` and stops a node serving
whenever it reaches `< (size/2)+1` slot-serving masters (`size` counts only masters
serving ≥1 slot; PFAIL counts as unreachable). FAIL-marking and authorizing an
election likewise need a majority of slot-serving masters. So a site loss that drops
the survivors below that threshold fail-stops the *whole* cluster — total outage,
not partial. The recovery primitive is `CLUSTER FAILOVER TAKEOVER` on a surviving
replica, which claims its master's slots unilaterally at a new config epoch.

**This is the safe-leader-election-under-partition problem**, and its shape is
governed by impossibility results that tell us what no implementation can do:

- **FLP (Fischer–Lynch–Paterson, 1985):** in an asynchronous system you cannot have
  both safety and liveness from a purely asynchronous failure detector; every timeout
  smuggles in a synchrony assumption that can be wrong.
- **Chandra–Toueg (1996):** consensus needs at best an *eventually strong* (◇S)
  failure detector, which is accurate only *after* an unbounded period of confusion —
  i.e. not during the partition, which is exactly when we must decide.
- **CAP / quorum intersection (Gilbert–Lynch; Gifford/Thomas):** under a partition at
  most one side may choose availability, and it must *prove* it is the privileged
  side. With an even number of votes there is no majority, so a symmetric 2-way split
  has no quorum anywhere — the universal reason a 2-node cluster needs a third voting
  entity.

The load-bearing consequence: **the operator's decision must rest on a quorum the
partitioned party itself participates in — so an isolated party removes itself — never
on the hub observing (probing, watching, guessing) whether that party is alive.**
Redis already provides that quorum: the minority `cluster_state:fail` *is* a
self-fence. The wrong instinct — a Kubernetes/dial "liveness witness" on the hub — is
a failure detector, and by the results above cannot underwrite safety; it is
recorded as a rejected alternative below.

### The two quorum domains (the organizing model)

The subtlety unique to multi-site is that there are **two** quorum domains, and they
must not be conflated:

- **Domain-M — slot-bearing masters.** Redis's *native* quorum. `K` = shards =
  slot-serving masters; majority `q = ⌊K/2⌋+1`. Enforced **unconditionally** by Redis
  (minority fail-stop = self-fence; config epochs = fencing token). **The sole safety
  authority.** The operator cannot override it safely.
- **Domain-S — sites.** An *externally imposed* failure domain. Redis has zero
  site-awareness. Losing a site removes whatever Domain-M votes happen to sit on it.

> **Domain-M is the sole safety authority. Domain-S is not a second quorum to run —
> it is a constraint satisfied by controlling the *projection* of sites onto masters
> (placement).** Run one quorum (Domain-M) and keep the site→master projection such
> that no losable set of sites carries a Domain-M majority.

## Decision

### D1 — The takeover decision is pure Domain-M arithmetic (no failure detection)

A pure, unit-tested planner (`planSiteTakeover`, sibling of `planLeaderlessRecovery`
/ `planClusterWipeRecovery` / `planGhostMasterRecovery`) fires `CLUSTER FAILOVER
TAKEOVER` on the best surviving replica of every orphaned shard, then FORGETs the
lost site's nodes — behind a gate built **only** from first-hand Domain-M evidence:

0. **Cluster formed.** Some node in the gathered view — a reachable survivor or a
   gossip-known dead master — owns ≥1 slot. A never-bootstrapped cluster presents the
   same surface as "all masters lost" (fresh nodes = `role:master`, 0 slots, own
   `cluster_state:fail`); without this precondition, bringup would arm a takeover
   across pods that never joined a cluster and preempt bootstrap. `!clusterFormed ⇒`
   do nothing.
1. **Orphaned shards with survivors.** ≥1 shard has no reachable master and a
   reachable, promotable replica in a surviving site. (None anywhere ⇒ total loss ⇒
   descoped, manual re-bootstrap.)
2. **Native path exhausted, from the survivors' own vantage.** A surviving pod
   reports its **own** `cluster_state:fail` (the minority fail-stop, observed
   first-hand — never the hub's dial; the LR-017 vantage rule), corroborated by the
   FAIL/PFAIL flags in its `CLUSTER NODES`. If survivors hold `q`, Redis elects on its
   own — wait (pillar 3.5).
3. **Fence-safe.** The unreachable slot-serving masters are `< q`. An
   alive-but-isolated site can reach only its own masters, so by the same
   unconditional minority check it has itself fail-stopped within
   `cluster-node-timeout` — *the nuisance and the safety mechanism are the same
   check*. If the unreachable masters *could* hold a majority (drift), the lost side
   may still be serving: **refuse** (surface a condition; require
   `multiSite.allowUnsafeSiteTakeover` or, in Phase 2, a fence).
4. **Cooldown.** First observation arms `status.multiSite.takeoverPendingSince`
   (mirrors `LeaderlessSince` / `wipeDeadlockSince`); fire only past `max(60s,
   2×cluster-node-timeout)` — comfortably after the condition-3 fence has engaged and
   long enough to absorb flaps. Clears when the state clears.
5. **Quiet cluster** (Rule-A analog): no terminating survivor pods, no in-flight
   failover / reshard / serialized rollout; decided on a fresh gather.

**Arithmetic identity (the crux).** Conditions 2 (`reachable < q`) and 3
(`unreachable < q`) can hold together *only when the slot-serving masters split
exactly in half* — `K` even and `reachable = unreachable = K/2`. This is the
**fence-safe automated takeover**, and it is the *only* configuration where a
takeover is both necessary (both sides fail-stopped) and provably safe (the other
side is a guaranteed minority, hence self-fenced — whether dead or alive-partitioned).
Any odd `K`, or any drifted split, makes a site loss either native (condition 2
fails) or a refusal (condition 3 fails). Safety therefore never depends on knowing
*why* a master is unreachable — only on counting.

Actions, in order (unchanged from the primitive, which is sound): per orphaned shard
select the most-complete surviving replica (highest replication offset, within one
replication lineage — the LR-024 union-find over `{replid, replid2}`) and
`CLUSTER FAILOVER TAKEOVER` it; then `CLUSTER FORGET` the lost site's node IDs on
every reachable node — but **only while every hosting site is observable**
(`allSitesObservable`): a gossip-known node with no backing pod is indistinguishable
by NodeID from a node on a merely-unreachable site, so FORGET trails idempotently
until the site is observable again (pillar 3.4). `TAKEOVER→FORGET` ordering is
required (a sibling replica still points at the dead master until it follows the slot
claim; FORGET refuses "Can't forget my master" until then — a partial pass is
normal). Rejoining fresh pods take the ordinary empty-pod MEET + shard-aware reattach
path (new node IDs, no epoch conflict), staggered per LR-021; a fenced-alive site
that heals has its never-restarted returnee pods **recycled** (ADR-008 machinery)
once the survivors hold a majority and no shard is orphaned, so they rejoin fresh
rather than re-MEET stale ex-masters.

### D2 — Master-balance invariant + drift rebalancer

Site-loss survivability is a **joint** property of data placement (replicas in ≥2
sites — already true) and **master distribution**. With `mᵢ` = slot-serving masters
in site `i`, a site loss removes `k = Σ_{i∈L} mᵢ`; native survival needs
`K − k ≥ q`, i.e. `k ≤ ⌈K/2⌉ − 1`. To survive the loss of **any single** site,
enforce the **balance invariant**:

```
max_i mᵢ ≤ ⌈K/2⌉ − 1
```

Redis roles are fluid — masters drift on failover and can concentrate. The operator
maintains the invariant with a slow, convergent, **drift-triggered** reconciler
(`planMasterRebalance` over `mᵢ` vs the invariant) that issues graceful
`CLUSTER FAILOVER` on a replica in an under-mastered site (the no-data-loss primitive
LR-025 uses). It runs only on violation, so churn is bounded to real drift events —
not a hot-path balancer.

Execution (steady-state maintenance, so gated conservatively): it runs **only on the
healthy path** of the reconcile (never during repair/takeover/not-ready) and **only
when quiet** (no terminating survivor pods), issues **at most one graceful
`CLUSTER FAILOVER` per reconcile** (LR-021-style serialization — a graceful failover is
a per-shard write pause), and honours a post-move settle **cooldown**
(`masterRebalanceCooldown` = 30s, anchored on `status.multiSite.rebalancePendingSince`,
mirroring `takeoverPendingSince`) so a failover in flight is never restarted — a
multi-move convergence advances one failover per cooldown. A graceful-failover **target
must be an in-sync replica** (`role:replica` + `link:up`): `CLUSTER FAILOVER`
coordinates the handoff with the replica, so a lagging/disconnected one is not a valid
target. Because `planMasterRebalance` only ever selects a shard mastered at an
over-target donor, a shard already at its target is never re-selected — no thrash. A
worst-case bug here is at most an unnecessary graceful failover (an availability blip),
never data loss or split-brain.

Scope note: ADR-009 declines master balancing in **single-site** mode for *load*
reasons (reads go to replicas; `replicasPerShard:1` has no freedom; churn). Those
hold for load. Multi-site requires balancing for a **different** reason —
quorum-on-site-loss (availability) — which is a distinct concern; ADR-009 stands for
single-site.

### D3 — Regimes and the 2-site recommendation

**Two sites never survive a site loss natively, for any `K`** (the heavier side
holds `⌈K/2⌉ > ⌈K/2⌉−1`). So 2-site is Tier A/B, never Tier C. The arithmetic
auto-takeover ("0b", the fence-safe path of D1) fires iff `K` even and the split is
exactly `K/2 : K/2`:

| `K` | balanced 2-site split | lose a site | outcome |
|---|---|---|---|
| 3 (odd) | 2 + 1 | minority-loss: native · **majority-loss: refuse** | asymmetric; 0b never fires |
| **4 (even)** | 2 + 2 | either side: `reachable=2<3` ∧ `unreachable=2<3` | **0b fires on *either* loss — symmetric auto-survival** |
| 5 (odd) | 3 + 2 | minority-loss: native · majority-loss: refuse | asymmetric; 0b never fires |
| 6 (even) | 3 + 3 | either side | 0b fires on either loss |

Odd `K` in 2-site is structurally an availability half-measure (an odd count over two
sites is always lopsided, and you don't choose which site you lose). **Recommend even
`K ≥ 4` for 2-site**; then 0b (coupled to D2's balance) gives complete,
detection-free auto-survival of either site loss.

| Tier | Precondition | Automatic safe recovery envelope | Phase |
|---|---|---|---|
| **A / 0a** | 2 sites, any `K` | none — human-confirmed fence (operator does not guess) | 1 |
| **A / 0b** | 2 sites, **even `K≥4`**, balanced | either site loss (arithmetic + self-fence) | 1 |
| **C** | ≥3 sites, balance invariant satisfiable | native Redis survival; `planSiteTakeover` rarely reached | 1 |
| **B** | out-of-band STONITH provider | any single-/multi-site loss incl. drift (fence→takeover); degrades to A when the fence plane is down | 2 |

### D4 — Phase 2: STONITH + site-quorum leg

The case D1 must **refuse** — an unreachable side that could hold a master majority
(drift, or an odd-`K` majority-site loss) — is unresolvable by any observer (FLP). It
*is* resolvable by a **fence**: fencing does not detect state, it *enforces* it
(power-off / network-isolate) and takes a **positive** acknowledgement, converting
"dead-or-partitioned?" into "confirmed dead." Then fence→takeover is safe in any
configuration. Phase 2 adds a **pluggable out-of-band fence provider** (a separate
failure domain from the data path — libvirt in the lab, cloud instance-stop / BMC in
prod) and a **site-quorum leg** (a consensus membership over `{sites…, hub}`, one
vote per *site*, gating the hub's authority to fence). A mistaken fence stays *safe*
(the fenced side cannot serve; its unreplicated tail is discarded — the accepted
pillar-3.1 RPO>0 bet) but costs availability; the site-quorum leg limits needless
fences and is **not** required for safety. When the fence plane is unreachable, Tier B
degrades to Tier A (human), never to an unsafe guess.

## Rationale
- **Why safety is arithmetic, not detection.** Detection is provably unreliable
  (FLP); the minority `cluster_state:fail` self-fence is the one *mechanical*
  guarantee — the isolated side is rejecting writes before the cooldown elapses — and
  it is exactly the behavior that causes the outage, repurposed as the guard.
- **Why the operator at all.** Redis's own quorum fail-stops a symmetric split
  (correctly, for safety); breaking it to restore availability is the operator's job,
  and is safe *only* in the fence-safe even split (D1 identity) or behind a fence (D4).
- **Why refuse on drift.** A wrong takeover into a serving majority is a *live* dual
  writer — the one outcome worse than an outage. The refusal is an expected dual-site
  outcome (odd `K`, or drift), with `allowUnsafeSiteTakeover` as the explicit opt-in
  and Phase-2 STONITH as the proper resolution.
- **Why balancing is mandatory here (not ADR-009's decline).** Without it, drift
  silently removes both native survival (Tier C) and the 0b even-split, converting a
  survivable loss into an outage — an availability property ADR-009 never weighed.

## Consequences
- **`recoverSiteLoss` is the sole quorum-loss takeover AND ghost-FORGET authority in
  multi-site.** The generic `repairCluster` is reused, but its cooldown-free,
  Step-0 quorum-recovery and Step-1 stuck-orphan promotion are gated off
  (`repairMayForceTakeover(lr) == !isMultiSite(lr)`), and both repair's Step-2 FORGET
  and the takeover's FORGET are gated on `allSitesObservable` — otherwise repair
  could fire an unguarded takeover ahead of the fence/cooldown. Step-1's
  partition-heal `CLUSTER MEET` stays active (benign, non-takeover).
- **Multi-site keeps the namespaced per-site Role — no cluster-scoped `nodes` RBAC.**
  Because safety is arithmetic (D1) and not a Kubernetes liveness witness, the
  operator needs no per-Node `Ready` read; the M1 namespace-scoped workload/pod Role
  is the whole per-site credential. (This is the direct payoff of rejecting the
  witness — see Alternatives.)
- A dual-site even-split loss recovers in ~cooldown + one reconcile with zero loss of
  replicated data (Tier A/0b). A majority-master-site loss refuses (drift-refusal) and
  waits for the opt-in, a human (Phase 1), or the fence (Phase 2). ≥3-site losses stay
  native (Tier C) and `planSiteTakeover` is rarely reached.
- A dual-site inter-site **link cut** remains a full outage until the link heals — by
  design (the descoped link-partition case); the fence-safe arithmetic keeps it
  split-brain-free (both minorities self-fence).
- **Drift is a first-class, surfaced state.** Master-per-site distribution and the
  balance invariant's health are surfaced (`lrctl verify`, a condition); a violated
  invariant is "reduced site-loss resilience," not silent drift.
- New status: `status.multiSite.takeoverPendingSince`, a refusal condition. New spec
  knob: `multiSite.allowUnsafeSiteTakeover` (default false).
- **Reduced-resilience warning** (a surfaced condition, *not* an admission refusal):
  when the invariant is structurally unsatisfiable — `S·(⌈K/2⌉−1) < K` for `S` sites
  and `K` shards — the topology is still valid and functional, it simply cannot
  auto-survive an arbitrary single-site loss (the heavier-master site's loss refuses
  or needs the opt-in). Such a topology is a legitimate deployment (with manual /
  `allowUnsafeSiteTakeover` recovery), so LittleRed **surfaces a
  `ReducedSiteLossResilience` condition + event** rather than blocking it — consistent
  with the opt-in-not-block norm. The headline case is the `K=4`-over-3-sites trap
  (`⌈4/2⌉−1 = 1`, but 4 shards over 3 sites force a `2+1+1` split by pigeonhole, so
  losing the 2-master site fails — 3 sites is not automatically enough; the *ratio*
  matters). `K=2` needs no special handling — Redis Cluster's minimum of 3 masters
  already rejects it. The warning is a status surface (grouped with the master-per-site
  distribution view), not a validation gate.
- Automated dual-site recovery presumes the hub survives the lost site: the hub must
  be a third failure domain holding no data and no Domain-M vote (design doc §6).
- A multi-shard takeover issues several no-consensus epoch bumps close together;
  convergence relies on Redis's config-epoch collision resolution (asserted in the
  multi-orphaned-shard e2e).
- Test plan (red-first): planner table tests for every branch — the fence-safe even
  split (fire), drift-majority (refuse without opt-in), native-still-possible (wait),
  `!clusterFormed` (do nothing), cooldown arm/clear, quiet-gating; `planMasterRebalance`
  invariant restore/no-op; validation refusals (`K=2`, `K=4`/3-site). e2e: dual-site
  even-split kill → takeover + serve, data intact; ≥3-site single loss → native, no
  takeover; drift → refuse (or Phase-2 fence→takeover); site rejoin (fresh pods) →
  staggered resync, no epoch conflict. Also verify e2e that `CLUSTER FAILOVER
  TAKEOVER` executes on a replica whose local `cluster_state` is `fail` (it performs
  no agreement — load-bearing, so proven not assumed).

## Alternatives considered
- **A Kubernetes / dial liveness witness on the hub** (grade each affected site
  ConfirmedDown / Unobservable / Contradiction and gate the takeover on it).
  **Rejected on theory:** it is a failure detector (◇P at best), and by FLP /
  Chandra–Toueg cannot underwrite a safety decision under partition; a stale or
  partitioned Kubernetes view (a synced-then-dead informer cache reads dead pods as
  still-`Ready`; a node-lifecycle inference is not a kubelet verdict) grades a dead
  site "alive" and holds recovery, or vice-versa. It is also *redundant* — the
  arithmetic (D1) already refuses every unsafe case — and it costs a cluster-scoped
  `nodes` RBAC grant the arithmetic design avoids. Safety belongs in Domain-M; a
  liveness view belongs at most in Phase-2 fencing (a *positive* fence, not a
  detector).
- **Witness shard / arbiter** (a voting node in a third failure domain to make the
  master count odd and restore native elections in 2-site). Would work
  information-theoretically, but OSS Redis Cluster only lets **slot-serving masters
  vote** (cluster-spec), so an arbiter must hold data — it is just a third data site
  (Tier C), not a cheap witness. A voting *empty* primary is an upstream proposal
  (Valkey #1600, discussion-phase); forking Redis's vote for it violates pillar 3.5
  and the OSS-only stance. Revisit as a no-fork Tier-D option only if it lands
  upstream.
- **Asymmetric master placement** (pin a majority site). Rejected as primary
  mechanism: protects only one direction, fights fluid roles, and permanently
  degrades the other site's loss to the drift-refusal.
- **Do nothing / manual runbook only.** The takeover *is* the runbook action,
  automated behind a gate strictly stronger than a human under pager pressure would
  check — but retained as Tier A/0a for the cases arithmetic must refuse, until
  Phase-2 fencing.
- **Trust the hub's Redis-level reachability alone.** Rejected: a blackholed-but-alive
  site is wrongly judged dead (LR-017); the survivors'-vantage rule (condition 2) is
  the fix.

## How to challenge this
- A partition where D1's arithmetic authorises an *unsafe* takeover — an unreachable
  side with `< q` masters that is nonetheless *serving* despite the unconditional
  minority self-fence. That would contradict the Redis 8.4 source and invalidate the
  foundation.
- A churn-free way to keep masters balanced (D2) across failovers without
  operator-triggered `CLUSTER FAILOVER`.
- A deployment where the 2-site odd-`K` asymmetry is acceptable and even-`K` is not
  (cost), changing the D3 recommendation.

## References
- `docs/MULTI_SITE_DESIGN.md` §6 (failure model)
- CLAUDE.md pillars 3.1 (no persistence / RPO>0), 3.4 (K8s as source of truth), 3.5
  (minimal interference / no forking Redis), 3.12 (per-shard STSs), 3.15 (multi-site)
- ADR-009 (single-site master-balancing decline — distinct concern, see D2), ADR-005
  / ADR-008 (cooldown + pure-planner precedents), LR-017 (survivors'-vantage /
  blackhole trap), LR-021 (serialized rollout), LR-024/LR-025 (lineage + graceful
  `CLUSTER FAILOVER` primitives)
- Fischer–Lynch–Paterson (1985); Chandra & Toueg (1996); Gilbert & Lynch (2002);
  Gray & Cheriton, *Leases* (1989)
- Redis Cluster specification (failover authorization / config epochs):
  <https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/>;
  Valkey #1600 (voting-empty-primary, upstream):
  <https://github.com/valkey-io/valkey/issues/1600>
- Redis 8.4 `cluster_legacy.c`: `clusterUpdateState()` (unconditional minority check),
  `clusterDelNode()` (FORGET unassigns slots)
