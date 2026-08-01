# Design Note: `failover` Mode (Operator-Managed HA without Sentinel)

> **Status:** **Implemented (experimental)** on `feat/failover-mode` — the concrete design
> is recorded in [ADR-011](adr/011-failover-mode.md), the algorithm in
> [RECONCILIATION_LOOP_FAILOVER.md](RECONCILIATION_LOOP_FAILOVER.md). The HA e2e suite is
> **green 16/16 on a real 3-node cluster (2026-08-01)** — including the §4 hybrid
> double-failover scenario, which sentinel mode kept deadlocking on (LR-007/LR-008/LR-024).
> The **§4 graduation-gate remainder is pending** (chaos/soak run, dogfooding evidence),
> and the §3.4 drop/coexist/replace decision stays deferred until that gate.
> **Created:** 2026-06-24 (design discussion) as an exploratory note. The sections below are
> kept as written — they are the historical reasoning; per-item resolution notes are inline.
> **Decision owners:** the littlered authors (spare-time OSS); also dogfooded on the
> managed-cloud hosted service, which is the intended proving ground.

---

## 1. What we are considering

A new deployment mode, **`failover`**: one master + N replicas (default 1 master + 2
replicas, mirroring sentinel's topology) **with no Sentinel pods**. The operator itself
performs all of bootstrapping, failure detection, failover, and reconciliation.

It is the same logical topology as `sentinel` mode, minus the Sentinel processes. The
difference is *who runs HA*: the operator, not Sentinel.

This sits alongside the existing modes:

| Intent | Mode | HA implementation |
|--------|------|-------------------|
| 1 node, no failover | `standalone` | — |
| HA, non-sharded | `sentinel` | Redis Sentinel (standard) |
| HA, non-sharded | **`failover`** (new) | the operator |
| Sharded | `cluster` | Redis Cluster gossip |

## 2. Motivation

### 2.1 The "two cooks" problem
The recurring source of fragile e2e failures in `sentinel` mode is that **the operator and
Sentinel are two independent failure-detectors fighting over the same state.** Sentinel has
its own election, timers, and state machine; the operator nudges it (`SENTINEL RESET`,
`REMOVE` + `MONITOR`). A nudge at the wrong moment, or inside a race, can deadlock the whole
instance into a state neither party self-heals from. The LR-007 / LR-008 saga (see
`docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` and ADR-003) is exactly this: a dual-failover
race leaves a sentinel permanently monitoring a ghost master.

The hypothesis: **managing a plain replication setup directly from the operator is less work,
less fragile, and has fewer race conditions than carefully steering Sentinel** — because
there is only one decider.

### 2.2 Why this fits LittleRed specifically (not a generic idea)
- **LittleRed already doesn't use Sentinel for client routing.** Clients reach the master via
  the `{name}` Service whose selector is the operator-managed
  `redis.chuck-chuck-chuck.net/role: master` label. So the operator's label is *already* the
  effective source of truth for traffic; Sentinel is a redundant, sometimes-conflicting second
  authority. Removing it removes the conflict, not a load-bearing component.
- Aligns with existing pillars: **3.4 K8s as source of truth**, **3.6 operator-led bootstrap**
  (bootstrap is *already* operator-led), **3.7 strict IP identity** (unchanged, arguably cleaner).
- Most machinery already exists: `internal/controller/gatherer.go` (ground truth), INFO
  replication offsets, master-label flipping (`updateMasterLabel`), the bootstrap wait-loop
  (`status.bootstrapRequired`).

### 2.3 The one real trade-off (accepted)
`failover` couples HA orchestration to **operator liveness**: if the operator is down when a
master dies, failover waits for the operator to come back. Decision from the discussion: **this
is acceptable** — that critical dependency simply *moves* from the Sentinel pods to the operator.
Mitigations: leader election + fast operator restart, and a background health-watcher goroutine
(same shape as today's `internal/controller/sentinel_monitor.go` `+switch-master` subscriber) so
detection stays in the seconds range rather than waiting on resync cadence.

Note the supporting observation: `sentinel` mode **already** depends on the operator for its hard
failures (LR-008 only resolves via operator intervention). So we are not giving up
operator-independent HA that actually worked — we are removing the fragile half.

### 2.4 Split-brain: arguably *better*, not worse
With operator-only control there is a single writer-selector authority (the master label) instead
of two (Sentinel quorum + label). Even if a partition briefly yields two Redis masters, clients
only reach the one the operator labels; the loser is re-synced on heal. Same async-replication
write-loss semantics Sentinel already has — with one decider instead of two arguing.

## 3. Decisions taken in this session

1. **Name: `failover`.** Chosen over `ha`, `primary-replica`, `mirrored`, `replication`/`replicated`.
   Rationale: it names the *guarantee* Sentinel users came for, forms a coherent ladder
   `standalone → failover → cluster`, matches Redis vocabulary, and is honest (implies no sentinel
   pods). `ha` was rejected as misleading (cluster is also HA); `replication`/`replicated` rejected
   (overlaps `cluster`, which also replicates) and disliked.

2. **`sentinel` stays — on feature-completeness grounds.** A feature-complete Redis operator within
   our scope (pure in-memory, no persistence) *must* speak the standard Redis HA dialect. Sentinel
   is table-stakes, expected to exist. It is **not** kept as a "safe default for users who won't read
   docs" — that framing was explicitly rejected as patronizing. We don't put a thumb on the scale.

3. **`failover` is offered as honest added value, validated by our own use first.** Position: "here is
   a second HA implementation, here is why we think it sidesteps the two-cooks race class, here is its
   trade-off (HA coupled to operator liveness) — decide for yourself." We are our own first users; the
   managed-cloud hosted service is the proving ground. On-prem customers get a recommendation backed by our
   production evidence, not a whitepaper.

4. **Lifecycle: ship `failover` as experimental; defer the drop/coexist/replace decision.** Introduce
   it experimental and coexisting with `sentinel`. Later — once it has earned trust against evidence —
   make a deliberate "drop it / coexist / replace sentinel" decision. Do not decide that now.

   - **How "experimental" is surfaced (proposed):** the `mode` enum gains `failover`; docs label it
     experimental; the operator emits a warning event/log on first reconcile of a `failover` instance.
     Honest, neutral wording (no steering): *"mode `failover` is experimental: operator-managed HA
     without Sentinel, under active validation — see docs for current status and trade-offs vs
     `sentinel`."* Setting the mode *is* the opt-in; no feature gate needed initially. A
     `--enable-experimental-modes` operator flag is a clean later add if a harder gate is wanted.

5. **Low-regret path.** Building `failover` does not touch `sentinel`, so the parallel work fixing the
   current sentinel deadlock is neither blocked nor wasted. If `failover` proves out, the whole class of
   operator-vs-Sentinel race bugs stops being something we maintain.

## 4. Graduation gate (experimental → decide its fate)

Write the trigger down so the deferred decision has a criterion instead of drifting:

- The full HA e2e suite passes on `failover` **as reliably or better than** `sentinel` — specifically
  the scenarios that torment sentinel: graceful failover, crash failover, and the **hybrid
  double-failover** that spawned LR-007/LR-008 (graceful immediately followed by crash on the same
  instance).
- Plus a chaos / soak run.
- Plus accumulated dogfooding evidence from the managed-cloud hosted service.

If `failover` clears the exact scenarios where `sentinel` keeps deadlocking, that is the evidence to
consider replacing. If it can't, we learned that cheaply. Use the current failing sentinel e2e (being
fixed in a parallel session as of this writing) as a candidate graduation scenario.

## 5. Design sketch (historical TODO — all pieces now decided in ADR-011)

Kept as written for the reasoning trail; each item carries its resolution.

- ✅ **Failure detection loop.** Background goroutine doing fast health probes + K8s readiness/pod events
  → signal reconcile. Needs flapping suppression and "slow vs dead" discrimination — the hardening
  Sentinel encodes for free and that we'd now own. Consider `min-replicas-to-write` to bound write loss.
  *Resolved (ADR-011 §4): the reconcile loop is the sole decider (pure `planMasterDeath`:
  kubelet-authoritative immediate + corroborated probe evidence over `downAfterMilliseconds`);
  a per-instance watcher (`failover_monitor.go`) only accelerates reconcile. `min-replicas-to-write`
  became `spec.failover.minReplicasToWrite`, off by default (§1).*
- ✅ **Failover state machine.** On master loss: pick the replica with the highest `master_repl_offset`,
  `REPLICAOF NO ONE` on it, repoint the others, flip the master label. Define the states/guards
  explicitly (analogous to ADR-003's Rule A guards: no terminating pods, no in-flight transition).
  *Resolved (ADR-011 §5/§6): one pure decision table `planFailover` (seed / promote-one-lineage /
  refuse-on-divergence), lineage-gated via `holdersDiverged`. The guards deliberately DIFFER from the
  Rule A sketch here: there is no terminating-pods gate on promotion — the dead master's own
  termination must never block its replacement; serialization is the promotion-unsettled gate + a
  post-transition cooldown.*
- ✅ **Bootstrap.** Reuse `status.bootstrapRequired` + operator-led registration; the pods' start-up
  wait-loop currently queries Sentinel — needs a Sentinel-free equivalent (wait for the operator to
  assign a master). See ADR-002 (removed startup PING check) for the deadlock-avoidance constraints.
  *Resolved (ADR-011 §3): operator-stamped assignment annotations read back through a downward-API
  volume, epoch-fenced by an EmptyDir run-marker (the ADR-001 kill-9 yield, re-owned). ADR-002's
  no-PING constraint is kept.*
- ✅ **Reuse inventory.** `gatherer.go`, `internal/redis/replication_state.go`'s offset logic (the
  offset-based promotion removed from sentinel mode per ADR-003 *is* the right primitive here, since
  there is no Sentinel consensus to wait for), `updateMasterLabel`, `resources.go` STS/SVC/CM builders
  (drop the Sentinel StatefulSet + sentinel.conf; reuse the master-label Service unchanged).
  *Resolved as sketched: the gather, `BestDataHolder`/`holdersDiverged`, the label mechanics
  (`applyRoleLabels`), master/replicas Services, and the PDB/probe builders are reused; failover-specific
  builders live in `resources_failover.go`.*
- ✅ **What gets deleted vs sentinel mode:** the Sentinel StatefulSet/config, `sentinel_monitor.go`'s
  subscriber, and the `SENTINEL RESET` / `REMOVE` + `MONITOR` healing — replaced by direct
  `REPLICAOF` orchestration the operator fully owns.
  *Resolved as sketched (ADR-011 §2 and Consequences): no Sentinel resources of any kind; the
  subscriber's role is taken by the failover master watcher.*

## 6. Next-session entry points (historical — both done)

Pick one:
1. **Sketch the `failover` design** (Section 5) — detection loop, failover state machine, reuse map.
   *Done: ADR-011.*
2. **Read the failing sentinel e2e** (parallel session) to confirm it is the same "two cooks" race
   class and lock it in as a graduation scenario.
   *Done: confirmed as LR-024 (the ghost-replica RESET → crash deadlock); the graceful+crash sequence
   is a graduation scenario per §4.*

## 7. How it started vs. how it's going (implementation day 0, 2026-08-01)

§2.1's hypothesis, restated: *managing a plain replication setup directly from the operator is
less work, less fragile, and has fewer race conditions than carefully steering Sentinel — because
there is only one decider.* Day-0 scorecard, axis by axis. (LOC figures are approximate:
mode-specific production code, whole files where cleanly attributable, function spans in shared
files otherwise; tests excluded.)

**"Less work" — no, roughly a wash (~2,850 vs ~2,275 LOC).** Removing Sentinel did not remove
the work; the operator now owns detection and promotion itself, and that costs lines
(`failover_reconcile.go` alone is ~1,000). What sentinel mode spends on steering an external
authority (`SentinelClient`, ~390 LOC / 13 methods; the sentinel-process StatefulSet/config
builders, ~340) failover mode spends on owning the mechanism (watcher, assignment engine,
startup protocol). LOC is the footnote, not the slide.

**"Fewer race conditions" — yes, structurally.** Sentinel mode's mastership logic is **seven
interacting rules** (Rule 0, A, D, R, L, LR-008 REMOVE+MONITOR, LR-024 recovery), each born from
an incident, each with ordering/gating interactions against Sentinel's own state machine — Rule D
was hardened five times and still self-inflicted LR-024. Failover mode's entire mastership logic
is **two pure functions** (`planFailover`, 6 rows; `planMasterDeath`, 6 outcomes) plus two
mechanical loops (repoint, re-auth). Every topology decision is table-tested; nothing waits on or
races a second consensus.

**"Less fragile" — the changelog is the quantitative statement.** 13 of 23 LR entries are
sentinel-mode (LR-001/004/005/007/008/009/010/011/013/015/016/017/024), and at least six of those
(001, 007, 008, 011, 013, 024) are one recurring class: operator nudges racing Sentinel's tables.
That class is **impossible by construction** here — no Sentinel tables, no ghost-replica list, no
bare-sentinel state, no RESET to mistime; ADR-010's entire subject does not exist in this mode.

**Day-0 evidence.** The hybrid double-failover — the scenario sentinel mode deadlocked on twice
(LR-007/008, then LR-024) — went green on the first e2e run, 16/16 on a real 3-node cluster, with
zero operator-code fixes needed. Client contract identical to sentinel mode (same label-routed
Services, no cluster-aware client), at half the pods (3 vs 6), plus a configurable replica count
sentinel mode never had.

**What this scorecard cannot claim.** It compares day 0 against six-months-hardened: sentinel's
13 entries are *discovered* complexity, and failover's equivalent bill has not arrived. Expected
first collectors: detection under real network weirdness — blackholing IPs, informer lag, kubelet
annotation-propagation latency under load (the LR-012/LR-017 class that unit tables cannot catch
and one clean e2e run did not stress) — concentrated in the corroboration matrix and the epoch
fence. The operator-liveness coupling (§2.3) is untested in anger: MTTR with the operator down
during a master death has not been measured. So the claim we are entitled to make today:
**failover mode eliminates the bug class that dominated sentinel's changelog; whether it has
fewer bugs overall is what the §4 gate remainder (chaos/soak, dogfooding) gets to decide.**

## 8. Pointers

- `CLAUDE.md` §2 (terminology — note "cluster" is reserved for Redis Cluster), §3.4–3.9 (pillars), §4 (modes).
- `docs/RECONCILIATION_LOOP_SENTINEL.md`, `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` (LR-007/008).
- `docs/adr/001-strict-ip-identity.md`, `002-remove-startup-ping-check.md`,
  `003-low-interference-sentinel-reconciliation.md`.
- `docs/SCOPE.md`, `docs/REQUIREMENTS.md`.
- Code: `internal/controller/{gatherer,sentinel_monitor,resources,littlered_controller}.go`,
  `internal/redis/{replication_state,gather}.go`.
