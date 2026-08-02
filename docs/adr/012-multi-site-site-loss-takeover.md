# ADR-012: Multi-Site Site-Loss Takeover (Operator-Led, Fence-Gated)

## Status
Proposed (design-stage; multi-site cluster mode not yet implemented — see
`docs/MULTI_SITE_DESIGN.md`)

## Context
Multi-site cluster mode runs one stretched Redis Cluster across several sites
(workload Kubernetes clusters), every shard hosted in ≥2 sites
(`sitesPerShard ≥ 2`), managed by a single hub operator. Redis handles pod- and
node-level failover natively; the design descopes multi-site failures and inter-site
link cuts. That leaves exactly one failure the operator must own: **the loss of a
whole site whose masters the survivors cannot fail over natively.**

Why native failover can be unavailable (verified against Redis 8.4
`cluster_legacy.c`):

- `clusterUpdateState()` contains an **unconditional** minority check, independent of
  `cluster-require-full-coverage`: a node that can reach fewer than
  `(size/2)+1` slot-serving masters (PFAIL counts as unreachable) sets
  `cluster_state:fail` and stops serving. `size` counts **only masters serving at
  least one slot**.
- FAIL-marking a master and authorizing a replica election likewise require a
  majority of slot-serving masters.

So in a dual-site instance with masters spread evenly, a site loss leaves the
survivors with exactly half the masters — below quorum. No election runs, and the
survivors fail-stop too: **total outage, not partial**, until someone intervenes.
The same applies with ≥3 sites whenever master drift has concentrated a majority of
masters in the lost site.

The intervention primitive exists: `CLUSTER FAILOVER TAKEOVER` on a surviving
replica claims its master's slots unilaterally (new config epoch, no agreement) —
the same quorum-loss tool the single-site repair loop already uses. Once the lost
masters' slots have been claimed, they serve zero slots, drop out of `size`, and the
survivors regain quorum arithmetic on their own.

The danger is firing it at the wrong moment: if the "lost" site is actually alive
and merely partitioned from the hub, a takeover creates two serving masters per
shard — a live split-brain, with one lineage discarded on heal.

## Decision
Add an operator-owned recovery, decided by a pure, unit-tested planner
(`planSiteTakeover`, sibling of `planLeaderlessRecovery` / `planClusterWipeRecovery`
/ `planGhostMasterRecovery`), that issues `CLUSTER FAILOVER TAKEOVER` on the best
surviving replica of every orphaned shard, then FORGETs the lost site's nodes.

The gate — **all** conditions must hold:

1. **Orphaned shards with survivors.** At least one shard has no reachable master,
   and a reachable replica of it exists in a surviving site. (No survivors anywhere
   ⇒ total loss ⇒ descoped, manual re-bootstrap.)
2. **Native path exhausted.** Reachable slot-serving masters `< (size/2)+1`. If the
   survivors hold a majority, Redis will elect on its own — the operator waits
   (pillar 3.5). This condition also makes the rule mode-general: it fires for
   dual-site symmetric loss *and* for drift-created minority states, and stays
   silent whenever Redis can self-heal.
3. **Site witness is not contradictory.** Per affected site, the hub's Kubernetes
   view yields one of three states:
   - `ConfirmedDown` — site API reachable, the site's redis pods gone or not-Ready.
     Kubelet readiness is the authority (the ADR-008 argument: in a pure in-memory
     instance a not-Ready redis serves nothing and holds nothing; and unlike the
     hub's remote dial it cannot be fooled by a blackhole — LR-017).
   - `Unobservable` — site API unreachable. Ambiguous: true site death also kills
     the API; so does a mere hub↔site management-path cut.
   - `Contradiction` — site API reachable and pods Ready. The site is alive; the
     data path is partitioned. **Never take over here**: this is the descoped
     link-cut scenario (runbook, link redundancy), not a site loss. (Kubernetes
     node-lifecycle lag can make a genuinely dead site *look* alive for tens of
     seconds; the witness is re-evaluated throughout the cooldown, and the node
     controller flips pods not-Ready well inside it.)
4. **Fence safety** (required in `Unobservable`; `ConfirmedDown` suffices alone).
   The takeover is provably split-brain-free iff the unreachable slot-serving
   masters are themselves **short of a majority**: an alive-but-isolated site can
   only reach its own masters, so by the same unconditional minority check it has
   fail-stopped — fenced itself — within `cluster-node-timeout` of the cut. The
   nuisance and the safety mechanism are the same check. If the unreachable masters
   *could* hold a majority (drift), an isolated site may still be serving:
   **refuse**, surface a condition, and require either
   `multiSite.allowUnsafeSiteTakeover` or manual action.
5. **Cooldown.** First observation arms `status.multiSite.takeoverPendingSince`
   (mirrors `LeaderlessSince` / `wipeDeadlockSince`); the plan fires only if the
   state persists past `max(60s, 2 × cluster-node-timeout)` — comfortably after the
   fence in condition 4 has engaged, and long enough to absorb flaps and stale
   Kubernetes views. The marker clears when the state clears.
6. **Quiet cluster** (Rule-A analog): no terminating pods among survivors, no
   in-flight failover, reshard, or serialized rollout; decision taken on a fresh
   gather.

Actions, in order:

1. Per orphaned shard, select the most-complete surviving replica (highest
   replication offset; lineage primitives from LR-024) and issue
   `CLUSTER FAILOVER TAKEOVER` on it. Slot ownership transfers with a bumped epoch;
   the dead masters drop to zero slots, `size` shrinks, survivors leave the
   minority fail-stop, service resumes.
2. `CLUSTER FORGET` the lost site's node IDs on every reachable node (ghost
   hygiene; pillar 3.4 extended: a node is forgotten only when *its own* site's
   witness supports it — never merely because the hub cannot reach it).
3. Nothing else. Site rejoin is the ordinary empty-pod path: fresh pods (EmptyDir,
   new node IDs — no epoch conflict by construction) are MEET-ed and reattached
   shard-aware, staggered per the LR-021 serialization pattern to bound the WAN
   full-sync burst.

## Rationale
- **Why the operator, not the pods or a script.** Same conclusion as LR-016 and
  ADR-008: distinguishing "site dead" from "site unreachable" requires the global
  view — two independent witnesses (per-site Kubernetes APIs and the surviving
  pods' data-path reachability) that only the hub holds.
- **Why the fence argument carries the safety case.** All other evidence is
  circumstantial: APIs can be partitioned, kubelet views can be stale. The minority
  fail-stop is the one *mechanical* guarantee — the isolated side is rejecting
  writes before the cooldown can elapse — and it is exactly the behavior that
  causes the outage, repurposed as the guard. Writes accepted by the lost masters
  inside the `[cut, fence]` window (≤ `cluster-node-timeout`) are discarded on
  heal; that is standard asynchronous-replication failover semantics, not a new
  loss class.
- **Why refuse on drift instead of trusting the witness.** When the unreachable
  side could hold a serving majority, a wrong takeover creates a *live* dual
  writer — the one outcome worse than an outage. The refusal is expected to be
  rare (the master-rebalance follow-up keeps spreads even) and has an explicit
  opt-in escape hatch, following the `allowUnsafeRebootstrapOnDeadlock` precedent:
  the operator never takes a data-losing gamble silently.
- **Why not act on `Contradiction`.** Evidence of an alive site makes this a link
  partition, which the design descopes deliberately: taking over would trade the
  isolated site's fence-window writes and a full post-heal resync for availability
  the runbook/link-redundancy story already owns. Revisitable later as an explicit
  knob if operational reality demands it.
- **Why TAKEOVER before FORGET.** TAKEOVER alone restores both service and quorum
  arithmetic (slot transfer empties the dead masters out of `size`); FORGET is
  cleanup and can safely trail.

## Consequences
- A dual-site site loss recovers in roughly cooldown + one reconcile, with zero
  loss of replicated data; ≥3-site losses stay fully native and this rule stays
  silent.
- A dual-site inter-site **link cut** remains a full outage until the link heals —
  by design, inherited from the design doc's descope. Link redundancy is the
  mitigation; the fence keeps it split-brain-free.
- New status surface: `status.multiSite.takeoverPendingSince`, per-site witness
  conditions, a refusal condition naming the reason. New spec knob:
  `multiSite.allowUnsafeSiteTakeover` (default false).
- The gate depends on per-site Kubernetes API access — already a multi-site
  prerequisite — and on bounded probes (LR-017 `ProbeTimeout`) so a dead site
  cannot stall the reconcile that must repair it.
- Test plan (red-first): planner table tests for every branch — safe takeover
  (`ConfirmedDown`, `Unobservable`+fence-safe), refusal on drift-majority without
  opt-in, hold on `Contradiction`, cooldown arming/clearing, Rule-A quieting.
  e2e: dual-site site kill → takeover + serve; hub↔site management-path cut with
  the site alive → must **not** fire; site rejoin → staggered resync, no epoch
  conflict. Also verify e2e that `CLUSTER FAILOVER TAKEOVER` executes on a replica
  whose local `cluster_state` is `fail` (expected — it performs no agreement — but
  load-bearing, so proven, not assumed).

## Alternatives considered
- **Witness shard** (a 1-slot master in a third failure domain to make the master
  count odd and restore native elections). Deferred, not rejected: elegant, but
  partial-mesh partitions (sites cut from each other, both reaching the witness)
  can authorize mutual cross-site failovers with messy convergence — needs its own
  analysis and e2e before it can be trusted. The takeover path has no such mode
  and reuses existing machinery.
- **Asymmetric master placement** (pin a majority site so one side always survives
  partitions serving). Rejected as the primary mechanism: it only protects one
  direction, fights the fluid-roles decision, and degrades the wrong site's loss
  to the drift-refusal case permanently.
- **Do nothing / manual runbook.** The takeover *is* the runbook action, automated
  behind a gate strictly stronger than what a human under pager pressure would
  check. Precedent: LR-015, LR-023, LR-024 all began as "a human had to intervene".
- **Trust the hub's Redis-level reachability alone** (no Kubernetes witness).
  Rejected: a blackholed-but-alive site would be wrongly judged dead (the LR-017
  trap, again).

## References
- `docs/MULTI_SITE_DESIGN.md` §6 (failure model), §6.1 (witness shard)
- CLAUDE.md §3.4 (Kubernetes as source of truth), §3.5 (minimal interference)
- ADR-005 / ADR-008 (cooldown + pure-planner gating precedents), ADR-003 Rule A
- `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md`: LR-016 (probes make no topology
  decisions), LR-017 (bounded probes / blackhole trap), LR-021 (serialized
  rollout), LR-024 (lineage primitives)
- Redis 8.4 `cluster_legacy.c`: `clusterUpdateState()` (unconditional minority
  check; `size` counts slot-serving masters), `clusterDelNode()` (FORGET unassigns
  slots)
