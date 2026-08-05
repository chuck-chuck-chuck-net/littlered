# ADR-012: Multi-Site Site-Loss Takeover (Operator-Led, Fence-Gated)

## Status
Accepted — implemented, unit/envtest-verified (Milestone 3 of multi-site cluster
mode, `feat/multi-site`; the pure `planSiteTakeover` + witness + gather + executor
land here). e2e on a real two-site substrate is Milestone 4 and still pending, so
the executor's dial paths and the end-to-end takeover are not yet field-verified —
see `docs/MULTI_SITE_DESIGN.md` and changelog LR-025.

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
   silent whenever Redis can self-heal. **Vantage point:** evaluated from the
   *survivors' own view*, never the hub's dial (LR-017; ADR-011's corroboration
   principle). The direct signal is the surviving pods' `CLUSTER INFO` reporting
   `cluster_state:fail` — the minority fail-stop observed first-hand — corroborated
   by the FAIL/PFAIL flags in their `CLUSTER NODES` tables. The hub's own
   reachability is transport, not evidence; the same vantage applies to
   conditions 1 and 4.
3. **Site witness is not contradictory.** Per affected site, the hub's Kubernetes
   view yields one of three states:
   - `ConfirmedDown` — site API reachable, and per redis pod either the pod is
     **gone**, or it is not-Ready while its **Node is Ready** (kubelet-attested).
     Kubelet readiness is the authority (the ADR-008 argument: in a pure in-memory
     instance a not-Ready redis serves nothing and holds nothing; and unlike the
     hub's remote dial it cannot be fooled by a blackhole — LR-017). A pod shown
     not-Ready while its Node is NotReady/Unknown does **not** qualify: that flag
     is set by the node controller (kube-controller-manager's node-lifecycle
     controller) inferring from missed heartbeats — a remote inference, not the
     kubelet's verdict — and the pod may in fact still be running, holding data,
     serving intra-site clients (the LR-017 trap in Kubernetes clothing). Such
     pods downgrade the witness to `Unobservable` grade: fence required
     (condition 4).
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
   **Arithmetic identity:** condition 2 (reachable ≤ size/2) and this fence
   (unreachable ≤ size/2) can hold together *only when the slot-serving masters
   split exactly in half* — the fence-safe automated path IS the even-split case.
   Any odd master count makes a site loss either native (condition 2 fails) or
   this refusal (the lost side could hold the majority). Since `shards: 3` over
   two sites is the standard deployment shape, and drift recreates uneven spreads
   in even-shard instances, this refusal is an **expected, common dual-site
   outcome** — a first-class documented state with the opt-in as its escape
   hatch, not a rare corner.
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
3. Site rejoin. Fresh pods take the ordinary empty-pod path: (EmptyDir, new node
   IDs — no epoch conflict by construction) MEET-ed and reattached shard-aware,
   staggered per the LR-021 serialization pattern to bound the WAN full-sync
   burst. Pods that return **without a restart** — the fence-safe branch
   explicitly admits an alive, fail-stopped site that later heals — carry
   forgotten node IDs and stale epochs and cannot gossip themselves back in
   (survivors drop packets from unknown senders; re-admission needs MEET). The
   operator **recycles** them (pod delete, the ADR-008 machinery) so they too
   return through the fresh-pod path, rather than re-MEETing stale ex-masters
   mid-recovery.

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
  writer — the one outcome worse than an outage. The refusal is **not** rare in
  dual-site: an odd shard count (3 is the standard shape) gives one site the
  master majority by construction, and drift recreates uneven spreads in
  even-shard instances — the master-rebalance follow-up narrows only the drift
  half. It is therefore documented as an expected outcome with an explicit
  opt-in escape hatch (`allowUnsafeSiteTakeover`, following the
  `allowUnsafeRebootstrapOnDeadlock` precedent) and its own runbook entry: the
  operator never takes a data-losing gamble silently.
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
  loss of replicated data — **when the gate passes** (Kubernetes-confirmed death,
  or an even master split under an unobservable witness). The loss of a
  majority-master site under an unobservable witness refuses (drift-refusal) and
  waits for the opt-in or a human — an expected, documented dual-site outcome
  (see condition 4). ≥3-site losses stay fully native and this rule stays silent.
- A dual-site inter-site **link cut** remains a full outage until the link heals —
  by design, inherited from the design doc's descope. Link redundancy is the
  mitigation; the fence keeps it split-brain-free.
- New status surface: `status.multiSite.takeoverPendingSince`, per-site witness
  conditions, a refusal condition naming the reason. New spec knob:
  `multiSite.allowUnsafeSiteTakeover` (default false).
- The gate depends on per-site Kubernetes API access — already a multi-site
  prerequisite — and on bounded probes (LR-017 `ProbeTimeout`) so a dead site
  cannot stall the reconcile that must repair it.
- **RBAC expansion (a real deviation from the M1 namespaced-Role prerequisite).**
  The witness's condition-3 grading must distinguish a kubelet-attested pod death
  (Node Ready) from a node-controller inference (Node NotReady/Unknown), which
  requires reading each pod's **Node** `Ready` condition — a **cluster-scoped**
  resource the M1 namespace-scoped per-site cache does not serve. The `SiteClient`
  therefore gains one narrow method (`NodeReady`) implemented as a bounded,
  **uncached** direct API read, and **each site's credentials now need
  cluster-scoped `nodes` get/list** on top of the namespaced workload/pod Role of
  the design doc (§7/§8). This is the one place multi-site reaches outside a single
  namespace; the site-kubeconfig helper (`lrctl`, M5) must grant it, and it is
  called out prominently as a deviation because it widens the per-site blast
  radius beyond "namespaced Role". A failed or absent node read is treated
  conservatively (as the node-controller-inferred downgrade → fence required), so
  it can only make the witness more cautious, never authorise an unfenced takeover.
- **Fenced-alive rejoin recycle is gated on a serving majority.** The recycle
  (action 3) fires only once the survivors already hold a master majority and no
  shard is orphaned, and only for pods isolated from that majority that never
  restarted — so it never churns a pod mid-bootstrap or mid-takeover. Recycling an
  isolated, data-empty (EmptyDir) pod is data-safe either way (a genuine returnee
  gets its clean rejoin; a briefly-isolated fresh pod is only re-created). Robustly
  distinguishing the two without gossip-visibility/epoch tracking is left to the M4
  substrate.
- Automated dual-site recovery presumes the hub survives the lost site: a hub
  co-located in one of the two sites cannot recover that site's loss. The
  effective deployment requirement is the hub in a third failure domain (or
  stretched) — one holding no data and no quorum participant (design doc §6).
- **TAKEOVER→FORGET ordering is required, not just preferred**: a shard's other
  surviving replica still points at the dead master until it follows the slot
  claim, and `CLUSTER FORGET` refuses ("Can't forget my master") on such a node.
  FORGET therefore trails idempotently across reconciles; a partial FORGET pass
  is normal, not an error.
- A multi-shard takeover issues several no-consensus epoch bumps close together;
  convergence relies on Redis's config-epoch collision resolution — named here
  so the reliance is explicit, and asserted in the multi-orphaned-shard e2e.
- The ≥3-site link-cut *rejoin* is a resync storm the LR-021 staggering does not
  bound (the isolated site's pods never restarted, so there is no StatefulSet
  recreation to stagger) — accepted for now; revisit if it bites.
- Test plan (red-first): planner table tests for every branch — safe takeover
  (`ConfirmedDown`, `Unobservable`+fence-safe), refusal on drift-majority without
  opt-in, hold on `Contradiction`, downgrade of node-controller-inferred
  not-Ready (Node NotReady/Unknown) to `Unobservable` grade, cooldown
  arming/clearing, Rule-A quieting. e2e: dual-site site kill → takeover + serve;
  hub↔site management-path cut with the site alive → must **not** fire; site
  rejoin (fresh pods) → staggered resync, no epoch conflict; fenced-alive site
  heals → pre-takeover pods are recycled, never re-MEET-ed stale;
  multi-orphaned-shard takeover → concurrent epoch bumps converge (collision
  resolution). Also verify e2e that `CLUSTER FAILOVER TAKEOVER` executes on a
  replica whose local `cluster_state` is `fail` (expected — it performs no
  agreement — but load-bearing, so proven, not assumed).

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
