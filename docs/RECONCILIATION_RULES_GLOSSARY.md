# Reconciliation Rules Glossary

Every named reconciliation rule and repair step in the operator, in one place: what it is called,
what it actually does, where it lives, what gates it, and which incident put each gate there.

**Scope and authority.** This file is an *index*, not a specification. The behavioural authority is,
in order: the **code**, then `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` (project rule §7.7 — read
it in full before changing any rule), then the per-mode loop documents
(`RECONCILIATION_LOOP_SENTINEL.md`, `_CLUSTER.md`, `_FAILOVER.md`) and the ADRs. Where this file
knows prose and code to disagree, it says so inline (⚠ **doc drift**).

**Reading the entries.** Every `file.go:func` below was checked against the tree at
`feat/declared-operations` @ `ca3b3fe`. Rules that live inline in a large function are cited as
`file.go:func` plus the comment banner that labels them (`// Rule 0:` etc.) rather than a line
number, because line numbers move. Guards are listed with the LR entry that *added* each one — a
rule's current shape is the accumulation of its entries, never its first one.

---

## 1. Sentinel mode

The sentinel healing chain is one function: `internal/controller/littlered_controller.go` →
`reconcileSentinelCluster`. The rules below are labelled in its comments (`// Rule 0:`, `// Rule N:`,
`// Rule A:`, `// Rule D (continued):`, `// Rule R:`). Two recoveries and the capture verdict live in
their own files.

### Rule 0 — bare-sentinel re-registration

| | |
|---|---|
| **Aliases** | "re-register bare sentinels", "bare-sentinel re-registration"; historically **Rule B** (ADR-003 Decision 2 renamed it: *"This targeted form is called Rule 0 in the code"*) |
| **Purpose** | A Sentinel that is up but monitoring nothing cannot rejoin the quorum by itself — gossip needs an existing master config to find the pub/sub channel. Rule 0 hands it one. |
| **Action** | `SENTINEL MONITOR <masterName> <RealMasterIP> <port> <quorum>` issued **to that pod's IP** (never via the Service, which load-balances to one backend), then `SENTINEL SET auth-pass` and `applySentinelSettings` (down-after, failover-timeout, parallel-syncs) |
| **Lives** | `littlered_controller.go:reconcileSentinelCluster` (`// Rule 0:` banner) |
| **Mode** | sentinel |

**Guards**

- `state.RealMasterIP != ""` — the whole loop is inside that branch; with no consensus master there
  is nothing to register (LR-001/LR-004's leaderless passivity). ⚠ **doc drift**: neither
  `RECONCILIATION_LOOP_SENTINEL.md` §"Rule 0" nor ADR-003 states this precondition; the code does.
- per Sentinel: `sn.Reachable && !sn.Monitoring` (ADR-003 Decision 2 — narrowed from Rule B, which
  re-issued `MONITOR` to Sentinels that already had a master and raced Sentinel's own failover).
- **Deliberately not gated on Rule A.** Rule 0 runs *before* the guardrail because adding a monitor
  to an unconfigured Sentinel is non-disruptive. LR-040 showed the cost of that placement — Rule 0
  issues writes during exactly the churn Rule A sits out — and fixed it by *bounding* the writes
  (`newBoundedClient`, all three of Dial/Read/WriteTimeout at `ProbeTimeout`, **plus** a per-call
  context; a context alone is inert against go-redis), not by moving the rule.

**Sources**: ADR-003 (Decision 2) · LR-001 · LR-004 · LR-024 (`seedSentinelsWithMaster` no longer
skips a Sentinel that "already knows *a* master", only one that knows the *target*) · LR-040 ·
LR-041 (with the empty-name gather bug, Rule 0 re-registered all three Sentinels every ~2 s — 181
times in 4 minutes) · LR-048 (a `MONITOR` under a *different* name is accepted, which is how a
rename produced two monitored names; Rule N is the counterpart).

### Rule N — stale master-name pruning

| | |
|---|---|
| **Aliases** | "the rename prune", "stale master-name pruning", `planStaleMasterNames`, condition `StaleMasterName` |
| **Purpose** | Reconcile the **scope** of what our Sentinels monitor: every Sentinel monitors exactly `spec.sentinel.masterName` and nothing else. Not a migration — nothing is remembered. |
| **Action** | `SENTINEL REMOVE <staleName>`, per Sentinel, per stale name, discovered from `SENTINEL MASTERS` (`GetMonitoredMasters` → `SentinelNodeState.MonitoredMasters`) |
| **Lives** | pure: `internal/controller/stale_master_name_plan.go:planStaleMasterNames`; driver: `littlered_controller.go:reconcileStaleMasterNames`; wired at the `// Rule N:` banner in `reconcileSentinelCluster` |
| **Mode** | sentinel |

**Guards** (all from LR-048 unless noted; the value is in the refusals — `REMOVE` is destructive)

| # | Gate | Origin |
|---|---|---|
| G0 | a capture is in evidence ⇒ stand down entirely (`Foreign`) | the rename-to-escape-a-capture trap; fed `planForsaken`'s **`Captured`**, not `Forsaken` (a settled `Forsaken` returns ~90 lines earlier, so `Forsaken` would be a structurally dead gate) |
| G1 | `desired != ""` | LR-041 — with an empty desired name *every* name reads stale, so the failure mode is "prune everything" |
| G2 | a living, reachable master of ours: `RealMasterIP` set, in `LiveTopologyIPs`, its own Redis view reporting a reachable master | LR-008's gate reused; LR-053 pins it to the *live-topology* set, deliberately not `OwnedIPs` |
| G3 | no monitored master, under **any** name, reports an in-flight failover | LR-048; the predicate is `MonitoredMaster.FailoverInProgress()`, which LR-052 made the single shared definition |
| G4 | reachable Sentinels ≥ quorum | LR-048 |
| G5 | every stale entry's address is one of our pods **or** is flagged down — else `Foreign`; **unless our own Redis StatefulSet is mid-rollout, then `Deferred`** | byte-identical to `planForsaken` clause 3; the rollout clause is **LR-050** |
| G6 | per Sentinel, the desired name is present on **that** Sentinel, else skip and name it in the condition | LR-024's `electMaster` lesson; the caller re-confirms with a bounded `IsMonitoring` immediately before each `REMOVE` |

Deliberately **not** gates: `!anyTerminating` and `Phase == Running` (the phase lags a pass —
LR-044 M4b). G5 is evaluated before G2/G3/G4 despite its number, so a capture reports `Foreign`
rather than the generic "no living master of ours".

**Position** — after Rule 0, before Rule A, and both halves are reasoned: after Rule 0 so the desired
name is registered in the *same* pass (the two-name window stays intra-pass, and G6 passes on the
first attempt); before Rule A because a rename rewrites the Redis pod template, so a pod is
terminating from the moment of the edit — the churn Rule A sits out *is* the rename.

⚠ **naming friction**: the planner's parameter is `forsaken bool` and its doc comment says "G0
forsaken", but the call site passes `forsakenPlan.Captured` and the loop doc/changelog both insist on
`Captured`. Behaviourally correct, misleading to read.

**Sources**: LR-048 · LR-050 · LR-049 (`IsMonitoring` bound) · ADR-018 ·
`docs/SENTINEL_MASTER_NAME_RENAME_DESIGN.md`.

### Rule A — guardrails

| | |
|---|---|
| **Aliases** | "Rule A (Guardrails)", "the guardrail". Historic: **"Rule A+"** meant ghost pruning *during* leaderless periods (LR-004, and a stale comment still in the code — see drift note) |
| **Purpose** | **Not a healing rule.** It is the suppressor: while Kubernetes or Sentinel is already performing a transition, every rule below it stands down. |
| **Action** | `return nil` — skip all remaining healing for this pass |
| **Lives** | `littlered_controller.go:reconcileSentinelCluster` (`// Rule A: Guardrails` banner) |
| **Mode** | sentinel |

**Condition**: `anyTerminating || state.FailoverActive`.

> **⚠ Rule A's guard is INESCAPABLE, and that is a property of the evaluation order documented in
> §6 rather than of the rule itself (LR-055).** Rule A returns at `littlered_controller.go:1217`;
> everything that could clear a stuck `FailoverActive` sits *below* it — Rule D's `SENTINEL RESET`
> at `:1416` above all. So a Sentinel whose failover state persists blocks **all** healing, on every
> pass, with no operator path out. Before LR-052 the field was permanently false and the guard never
> fired at all; LR-052 made it real — correctly — and it is unbounded. Measured 2026-08-30:
> `failoverActive` true for 84-86 consecutive passes, ~178s, during an ordinary rename of a healthy
> instance, against LR-052's estimated ~1.84s window. Rule D's *early* prune is the only lever, and
> only as prevention: once the state exists, Rule D can no longer run. **Recorded, not fixed.**

- `anyTerminating` — any Redis or Sentinel pod carries a `DeletionTimestamp` (ADR-003 Decision 5's
  reasoning, generalised).
- `state.FailoverActive` — **had never fired in the product's history until LR-052**: it was parsed
  from a `failover-status` key neither Redis nor Valkey emits, so it was permanently `false`. LR-052
  repointed every reader at `failoverInProgress(flags, failover-state)`. It is an **OR** over
  Sentinels (only the failover *leader* carries the flag), which is required, not merely
  conservative.
- LR-001/LR-007 originally extended Rule A to also skip healing when `RealMasterIP == ""`. That is
  **no longer** the shape: the leaderless branch now hosts Rule L and the LR-024 recovery, and the
  code says so at the `// Note: state.RealMasterIP == "" (leaderless) used to be a hard blocker`
  comment.

⚠ **doc drift / stale comment**: the comment immediately below Rule A still calls ghost pruning
"Rule A+" (`littlered_controller.go`, `// We now allow reconciliation to proceed so that Rule A+
(ghost pruning) can clear stuck sentinels`). "Rule A+" is a retired name for **Rule D**; the sentence
also predates LR-001/LR-007's hard leaderless gate on RESET, which still holds via
`GhostReplicaResetSafe` and the `RealMasterIP != ""` branch.

**Sources**: ADR-003 · LR-001 · LR-004 · LR-007 · LR-013 · LR-040 (Rule 0 and Rule N precede it,
deliberately) · LR-052.

### Ghost-master / divergent-master correction (LR-005, LR-008) — *unnamed by letter*

| | |
|---|---|
| **Aliases** | "ghost master correction", "REMOVE+MONITOR", "divergent sentinel correction", "LR-008 correction". No letter was ever assigned. |
| **Purpose** | A single Sentinel that missed a `+switch-master` is pinned to the wrong master — either a **ghost** (dead IP, LR-008) or a **living but wrong** pod (LR-005). It cannot self-correct: it reaches `s_down` alone but never `o_down`. |
| **Action** | per Sentinel pod: `SENTINEL REMOVE <masterName>` then `SENTINEL MONITOR <masterName> <RealMasterIP>` + auth-pass + settings. **Never `SENTINEL RESET`** — RESET does not change the monitored master IP (LR-008's central finding, which superseded LR-007's attempt). |
| **Lives** | `littlered_controller.go:reconcileSentinelCluster`, the loop over `state.SentinelNodes` after Rule A (the two `Sentinel monitoring ghost master` / `Sentinel monitoring wrong master IP` branches) |
| **Mode** | sentinel |

**Guards**: `RealMasterIP != ""`, `!state.IsGhost(RealMasterIP)`, and `state.RedisNodes[RealMasterIP]`
present and `Reachable` (LR-008); runs after Rule A, so not during churn or a live failover; sets
`ghostMasterFound` and **returns** so the next pass verifies convergence. Classified **CONVERGENCE**
under ADR-020, so it is *not* stood down by a declared operation.

**Known limit** (LR-041's e2e observation): against an *established* capture at a high config epoch
this correction is issued every pass and never takes — a per-pod `MONITOR` starts at
`config_epoch = 0` and loses to the captor's next hello. That is why ADR-015 §9.2 declines recovery
and ADR-016 quarantines instead.

**Sources**: LR-005 · LR-007 (superseded) · LR-008 · LR-024 (`electMaster` reuses the sequence) ·
LR-041 · LR-042.

### Rule D — ghost-replica pruning

| | |
|---|---|
| **Aliases** | **"ghost pruning"** (ADR-003 Decision 4: *"Keep ghost pruning (Rule D)"*), "the RESET", historically "Rule A+" and "Rule D Extension" (LR-001) |
| **Purpose** | Remove dead pod IPs from Sentinel's *replica* list, which otherwise never age out and permanently dirty the monitoring signal. |
| **Action** | `SENTINEL RESET <masterName>`, **broadcast** to every Sentinel address (`getSentinelAddresses`) |
| **Lives** | detection: the `for _, replica := range sn.Replicas` scan inside the post-Rule-A Sentinel loop, setting `ghostFound`. Action: `littlered_controller.go:reconcileSentinelCluster`, `// Rule D (continued): Prune ghost replicas`. Predicate: `internal/redis/replication_state.go:(*ReplicationState).GhostReplicaResetSafe` |
| **Mode** | sentinel |

**Guards** — the longest gate chain in the product, and every link is an incident:

| Guard | Added by |
|---|---|
| a ghost replica actually detected, flagged `s_down` (or `o_down`) | LR-001/LR-011 |
| the operator is **not** leaderless — `RealMasterIP != ""`, consensus master living **and reachable** | LR-001, LR-007, LR-008 |
| **≥ 1 healthy known replica** (non-ghost, non-`s_down`) known to Sentinel | LR-011 — RESET wipes the whole replica list, rebuildable only from the master's `INFO` |
| **cluster whole** — `reachableRedis == SentinelRedisReplicas` (K8s-grounded, zero extra requests) | LR-013 — a force-deleted master *vanishes* rather than terminates, so Rule A's `anyTerminating` was false |
| runs **after Rule A** | LR-001/LR-007/LR-013 |
| `!operationRunning` — stood down while a declared heavy operation is in flight (classified **RESCUE**) | ADR-020, this branch |

**Three things about Rule D that are easy to miss**

1. **It is the self-inflicted trigger of the LR-024 deadlock.** A legitimate RESET between two
   failovers empties the replica list; a crash before Sentinel re-learns the replicas gives
   `-failover-abort-no-good-slave` forever. Recovery is `recoverGhostMasterDeadlock`; *prevention*
   (retiring or rate-limiting Rule D) is still deferred — prospective ADR-010,
   `docs/GHOST_MASTER_FAILOVER_DEADLOCK_DESIGN.md`.
2. **It heals the *captor* side of a cross-instance capture.** ADR-016's quarantine takes the
   victim's pods away precisely so the captor's departed entries become ordinary `s_down` ghosts and
   Rule D's gate chain passes against the captor's *own* expected pod count. Verified live twice
   (LR-044 M4a) from the captor's own log lines, 2-4 s after the victim's pods left.
3. **Its RESET also clears a latched Sentinel `RECONF_SLAVES` failover state** — measured live
   **2026-08-30**, and **not yet recorded in any LR entry**. LR-052's new residual is that a Sentinel
   whose promoted replica dies inside the reconfiguration window latches
   `SRI_FAILOVER_IN_PROGRESS` with no timer to end it (`sentinelAbortFailover` asserts
   `failover_state <= WAIT_PROMOTION`, so it cannot be reached from `RECONF_SLAVES`), and LR-052
   names a manual `SENTINEL RESET` as the escape. **Read the ordering carefully**: a latched
   `FailoverActive` makes Rule A return *before* Rule D, so the operator's own RESET cannot clear the
   latch that suppresses it. The clearing effect is real; the operator is not currently a path to it.

**Sources**: ADR-003 (Decision 4) · LR-001 · LR-007 · LR-008 · LR-011 · LR-013 · LR-024 · LR-041
(Rule D was silently *inert* while the gather queried an empty master name) · LR-044 · ADR-020.

### Rule R — "Replica Rescue"

| | |
|---|---|
| **Aliases** | "Replica Rescue" (its name in code), "straggler repoint", "zombie redirect", "the SLAVEOF rule" |
| **Purpose** | **The name misleads.** It rescues nothing: it points a pod that is not following the consensus master at the consensus master. It is *convergence*, and it is classified as such under ADR-020 with the comment `THE NAME LIES`. |
| **Action** | `SLAVEOF <RealMasterIP> 6379` (`redisclient.SlaveOf`, bounded per LR-049) |
| **Lives** | `littlered_controller.go:reconcileSentinelCluster`, `// Rule R: Replica Rescue.` |
| **Mode** | sentinel (the failover-mode analogue is step 7a, `planFailoverRepoints`) |

**Guards**

- reachable, and not the consensus master itself.
- fires only on a definitively wrong `Role == master` **or** `MasterHost != RealMasterIP` —
  **never on `LinkStatus == "down"` alone** (LR-010: re-issuing `SLAVEOF` would interrupt a
  legitimate handshake).
- after Rule A; requires `RealMasterIP != ""`.
- **not** gated on a declared operation (ADR-020). Gating it cost a measured **+180 s** on the
  first-replaced pod during a rename: a replaced pod returns following the old master with
  `link:down`, sentinel readiness needs `role:master` or `link:up`, so the pod stays unready, the
  StatefulSet stays unsettled, the operation stays pending — *the operation suppressed the healing
  its own completion condition depends on.*

**Sources**: LR-009 (introduced) · LR-010 (narrowed) · LR-016 (Rule R took over zombie redirect from
the liveness probe, which was wiping survivors) · ADR-003 Decision 1 (partially superseded) · LR-040 /
LR-049 (bounding) · ADR-020 (classification).

### Rule L — leaderless bootstrap-deadlock recovery

| | |
|---|---|
| **Aliases** | "leaderless recovery", "Rule L (leaderless recovery)", `planLeaderlessRecovery`, condition `LeaderlessRecovery` |
| **Purpose** | An already-initialized instance whose entire Sentinel quorum lost its config (all Sentinels **bare**) has no path back to a master: `bootstrapRequired` is armed once at `Phase == ""` and never re-armed. |
| **Action** | 0 data holders → seed `redis-0` (`seedSentinelsWithMaster`); exactly 1 → promote it; ≥2 → **refuse** unless `sentinel.allowUnsafeRebootstrapOnDeadlock`, then force-elect `BestDataHolder`. Promotion of a running replica is `REPLICAOF NO ONE` + REMOVE/MONITOR via `electMaster`. |
| **Lives** | pure: `internal/controller/leaderless_recovery.go:planLeaderlessRecovery`; driver: `littlered_controller.go:recoverLeaderlessDeadlock`; execution: `electMaster`, `seedSentinelsWithMaster` (both `littlered_controller.go`) |
| **Mode** | sentinel |

**Guards**

- `RealMasterIP == ""` — Rule L and the LR-024 recovery are the only rules that run here.
- `AllSentinelsBare()` — the discriminator from a *recent master death*, where Sentinels still
  monitor the dead master and can fail over on their own (LR-015).
- a reachable Sentinel **quorum**.
- Rule A already passed.
- 30 s cooldown, `status.leaderlessSince` (`leaderlessRecoveryCooldown`).
- data-safety by **holder count** (contrast LR-024's lineage gate) — LR-015.
- `unprovablyEmptyVeto` — an `AuthFailed` pod is not provably empty, so every data-discarding branch
  refuses (LR-051). Placed *after* the detection and cooldown gates, before the act step.
- `!operationRunning` (ADR-020, classified **RESCUE**).

**Related**: LR-016 — the sentinel Redis *liveness* probe was reduced to a local `PING` because it
restarted masterless replicas and wiped the very survivors Rule L exists to preserve. Readiness still
requires `link:up`.

**Sources**: LR-015 · LR-016 · LR-017 (a 146 s reconcile stall starved it) · LR-024 (shares
`BestDataHolder`) · LR-044 (the quarantine release hands Rule L its no-data reseed signature) ·
LR-051 · ADR-005.

### Ghost-master deadlock recovery (LR-024) — *unnamed by letter*

| | |
|---|---|
| **Aliases** | "the LR-024 recovery", "ghost-master deadlock recovery", `planGhostMasterRecovery`, condition `GhostMasterRecovery` |
| **Purpose** | The gap between LR-008 (needs a *living* consensus master) and Rule L (needs *bare* Sentinels): a majority of Sentinels monitor a **ghost** master with no promotable replica, so failover aborts `no-good-slave` forever while survivors hold data. |
| **Action** | elect the most-complete survivor: `electMaster` = `SENTINEL REMOVE` + `MONITOR` + `REPLICAOF NO ONE` on `BestDataHolder` |
| **Lives** | `internal/controller/ghost_master_recovery.go:planGhostMasterRecovery` (pure) and `:recoverGhostMasterDeadlock` (driver) |
| **Mode** | sentinel |

**Guards**: `RealMasterIP == ""` · `SentinelsMonitorGhostMaster()` · **`!HasHealthyKnownReplica()`**
(the discriminator from an imminent legitimate failover) · reachable quorum · 30 s
`status.ghostMasterStuckSince` (`ghostMasterRecoveryCooldown`) · safety by replication **lineage**
(`holdersDiverged`, union-find over `master_replid` **and** `master_replid2` — keying on `replid`
alone refused an ordinary promotion chain) · `unprovablyEmptyVeto` (LR-051) · `!operationRunning`
(ADR-020, **RESCUE**).

**Sources**: LR-013 (the deferred follow-up it discharges) · LR-024 · LR-041 (its discriminator was
silently dead while the gather used an empty name) · LR-051 · prospective ADR-010.

### `Forsaken` verdict — capture diagnosis

| | |
|---|---|
| **Aliases** | "the Forsaken verdict", "capture verdict", `planForsaken`, condition `Forsaken`, `status.forsakenSince`. First drafted as `SentinelForsaken`. |
| **Purpose** | Name the one state the operator must stop working on: an instance **captured** by another Sentinel deployment sharing its master name (LR-039). Recovery is declined by design (ADR-015 §9.2). |
| **Action** | none against Redis/Sentinel. It returns **before Rule 0**, logs once per transition, requeues at the **steady** interval (`requeueAfterNotRunning`), and gates the quarantine. |
| **Lives** | `internal/controller/forsaken_plan.go:planForsaken`; wired in `reconcileSentinelCluster` step 2b |
| **Mode** | sentinel (the only mode that can set it) |

**Clauses** (all four must hold, plus a 30 s `forsakenCooldown`) — conservative in one direction on
purpose, since a false positive parks a live instance:

1. ≥1 reachable **monitoring** Sentinel (bare Sentinels are Rule L's business).
2. every reachable monitoring Sentinel agrees on ONE master address (disagreement is a transition).
3. that address is **not one of our pods** (`OwnedIPs`, LR-053 — terminating pods included) **and is
   not flagged down** (keeps LR-024's ordinary post-failover debris out).
4. no reachable Redis pod of ours is a master.

**Two amendments that are load-bearing**

- **Name-agnostic** (LR-048): the clauses range over every `(address, flags)` any reachable Sentinel
  monitors under **any** name. Scoped to the desired name, a rename of a captured instance
  evaporated the verdict and took ADR-016's quarantine with it.
- **`rolling` input** (LR-050): while our own Redis StatefulSet is not settled
  (`statefulSetRolloutSettled`, read **uncached**), the operator does not attribute addresses at all.
  It **suppresses arming and nothing else** — a rollout can neither start a verdict nor clear one;
  the ordinary clauses still clear it, which the quarantine's self-clearing lifecycle depends on.

**Known hole** (LR-054, **found, not fixed**): a capture victim still holding its own data has a pod
with `link:down`, hence not Ready, hence the StatefulSet reads unsettled forever, hence LR-050 never
arms the verdict — so the state `atRisk` exists to protect can never be diagnosed, and the captor
stays poisoned.

**Sources**: LR-039 · LR-042 · LR-044 · LR-045 · LR-048 · LR-050 · LR-053 · LR-054 · ADR-015 §9.2 ·
ADR-016.

### Quarantine — Forsaken-gated scale-to-zero

| | |
|---|---|
| **Aliases** | "the quarantine", `planQuarantine`, `Forsaken` reasons `Quarantined` / `QuarantineLatched` / `QuarantineRefusedDataPresent` / `QuarantineRefusedDataUnknown` |
| **Purpose** | The *captor* side of a capture: hold the victim at 0 Redis **and** 0 Sentinel replicas so the captor's Sentinel replica list drains and **Rule D** (which already exists) prunes it; release after the settle so **Rule L**'s no-data reseed re-bootstraps the victim empty. |
| **Action** | desired replicas 0 at **build time** (`sentinelDesiredReplicas`, pre-gather, from `status.quarantinedSince` alone — an out-of-band scale-down would flap 0→3→0 every pass against SSA `ForceOwnership`) |
| **Lives** | `internal/controller/quarantine_plan.go:planQuarantine` + `:quarantineDataRisk`; `littlered_controller.go:sentinelDesiredReplicas` (pre-gather) and step 2c (arming) |
| **Mode** | sentinel |

**Guards**: gated on the `Forsaken` verdict · `atRisk` — refuse when a reachable pod holds keys **not
explained by the capture** (keys on a link-`up` replica of the captor's master are the captor's own
data; *whose* data, never *whether there is* data) · `unverified` — a pod that cannot be **proven**
empty, keyed on **kubelet readiness** (LR-023), and on `AuthFailed` regardless of the kubelet
(LR-051) · 120 s `quarantineSettlePeriod` · bounded `quarantineAttemptLimit = 2`, or `1` when auth is
off **and** the effective name is the legacy `mymaster`, then latched; the counter clears only on
`Phase == Running`.

**Sources**: LR-042 · LR-044 · LR-045 · LR-051 (accepted residual: a permanent credential mismatch
vetoes the quarantine indefinitely) · LR-054 · ADR-016.

### Bootstrap and labelling (sentinel) — *support, not healing*

- `bootstrapSentinel` (`littlered_controller.go`) — runs only while `status.bootstrapRequired`, set
  once at `Phase == ""`; seeds every Sentinel with `redis-0` via `seedSentinelsWithMaster`. Its
  never-re-armed nature is *the* cause of LR-015.
- `updateMasterLabel` (`littlered_controller.go`) — surgical `role: master` labelling; skips label
  churn while a pod is terminating (ADR-003 Decision 5) and, when leaderless, only removes the label
  from whoever holds it rather than relabelling everything (LR-006).
- `requeueAfterNotRunning` (`littlered_controller.go`) — the shared fast/steady decision;
  `Forsaken=True` selects steady. It exists because the check was inlined in `updateStatus` and
  missing from `updateSentinelStatus`, the only path sentinel mode takes (LR-045).

---

## 2. Cluster mode

Cluster mode has no lettered rules; the repair loop is **numbered steps** inside
`internal/controller/cluster_reconcile.go:repairCluster`, entered from `reconcileCluster` whenever
the cluster is not healthy or shows partitions / ghosts / orphaned slots / empty masters. Each step
that acts **returns**, so at most one class of repair happens per pass.

| Step | Name / alias | Action | Lives | Key guards (origin) |
|---|---|---|---|---|
| **0** | Quorum Recovery | `CLUSTER FAILOVER TAKEOVER` at orphan replicas | `repairCluster`, `// 0. Quorum Recovery` | fires only when `votingMasters <= shards/2`; replica's master must be unknown/dead (LR-012 notes it is a *topology* condition, not time-based) |
| **1** | Heal Partitions | `CLUSTER MEET` from the largest partition's seed | `repairCluster`, `// 1. Heal Partitions`; planning in `internal/redis/cluster_state.go:(*ClusterGroundTruth).PlanPartitionMeets` + `AttributeMeetTarget` | `HasPartitions()`; per-target **uncached** `confirmPodIP` (`cluster_reconcile.go:confirmPodIP`, denies `pod-gone` / `ip-changed` / `pod-terminating` / `confirm-failed` — LR-043); bus-side attribution (`member` / `isolated`) is **advisory** once an address is confirmed (LR-043 regression); orphan force-promotion after `clusterNodeTimeout + failoverGracePeriod` |
| **2** | Forget Ghost Nodes | `CLUSTER FORGET <ghostID>` at every reachable node | `repairCluster`, `// 2. Forget Ghost Nodes` | never forget a ghost that is still a live replica's master (promote first, Step 0/1); skip unreachable nodes (LR-012); refuse entirely while a legacy `{name}-cluster` STS exists (ADR-013 §6) |
| **3** | Recover Missing Shards | `CLUSTER ADDSLOTS` | `repairCluster`, `// 3. Recover Missing Shards` | refuses on fragmented / non-aligned ranges; target must be a reachable **empty** master (`internal/redis/reshard_plan.go:SafeMissingShardTarget`, LR-018 — Step 3 itself created the consolidated state it could not see) |
| **3b** | Consolidated-Shard Reshard | relocate a surplus range, keys preserved: native `CLUSTER MIGRATION IMPORT` (8.4+) or the incremental dance | `internal/controller/cluster_reshard.go:reshardConsolidated`, `:reshardViaDance`; pure `internal/redis/reshard_plan.go:PlanReshard` | one reachable master owning >1 expected range **and** a reachable empty master; defers on fragmented ranges / no empty master; mechanism chosen by a free gather-time capability probe; **no drop-keys opt-in** (a non-lossy reshard always exists) — LR-018, ADR-006 |
| **4** | Replication Repair (empty-master reattach) | `CLUSTER REPLICATE` | `repairCluster`, `// 4. Replication Repair`; choice in `internal/controller/cluster_topology.go:chooseReattachTarget` | skipped in `replicasPerShard: 0`; **shard-aware** — attach inside the pod's own shard STS, cross-shard only as a logged fallback (LR-020); defer until `gt.NodeKnows(empty, target)` rather than issue a doomed `ERR Unknown node` (LR-014) |
| **5** | Bootstrap | full cluster bootstrap | `cluster_reconcile.go:bootstrapCluster`, MEET round in `:bootstrapMeetRound` | only when `gt.TotalSlots == 0` **and** no replicas exist; per-pod uncached read + terminating refusal (LR-043); revision gate accepts `CurrentRevision` **or** `UpdateRevision`, since a partition freezes `CurrentRevision` (LR-047) |

⚠ **doc drift**: `docs/RECONCILIATION_LOOP_CLUSTER.md` lists Steps 0-5 and never mentions **Step
3b**, which sits between Steps 3 and 4 in the code and is documented in CLAUDE.md pillar 3.11,
ADR-006 and `docs/CLUSTER_CONSOLIDATED_SHARD_RECOVERY.md`.

### Cluster rules that live outside `repairCluster`

**Total-/partial-wipe recovery** — aliases "the wipe recovery", "pod recycle",
`planClusterWipeRecovery`.
Deletes exactly the pods stuck not-Ready so their StatefulSets reschedule them fresh.
`internal/controller/cluster_wipe_recovery.go:planClusterWipeRecovery` (pure) and
`:recoverClusterWipeDeadlock` (driver), called from `reconcileCluster`'s **not-all-Ready** branch.
Guards: redis container **not-Ready + restarted (crash-looping) + not OOMKilled**, judged by the
**kubelet's** probe (blackhole-proof; never the operator's dial — LR-017/LR-023), plus a 120 s
`clusterWipeRecoveryCooldown` (`status.cluster.wipeDeadlockSince`). A **Ready** pod is never
recycled. LR-023, ADR-008.

**Intra-shard rollout gate** — aliases "the partition gate", "state-gated rolling update",
`planShardRolloutPartition`, condition `ClusterRolloutBlocked`, warning `ClusterRolloutUngated`.
Holds `spec.updateStrategy.rollingUpdate.partition` and lowers it one ordinal at a time.
Pure: `internal/controller/cluster_rollout.go:planShardRolloutPartition`; post-gather driver:
`cluster_reconcile.go:advanceClusterRollout` (the **only** place the partition comes down); the
pre-gather apply may only hold or raise. Guards: every pod at or above the partition must be
simultaneously (a) at `UpdateRevision`, (b) Ready per the kubelet, (c) a link-`up` replica of the
shard's slot owner (`redisclient.IsLinkUpReplicaOf`, shared with the migration planner). Cursor is
the StatefulSet's own `partition`, read **uncached** while in flight. **No timer fallback** — the
chosen failure direction is a loud stall; `ClusterRolloutBlocked` is advisory only and is never
raised for an attached-but-link-down pod (an unbounded full sync is progress). `replicasPerShard: 0`
is ungated and warns. LR-047, ADR-017; the pod-local preStop fence (`CONFIG SET
min-replicas-to-write 99` on the last-copy branch) is the mitigation for drains/evictions/manual
restarts.

**Cross-shard rollout serialization** — alias "LR-021 serialization".
`cluster_reconcile.go:reconcileClusterStatefulSet` applies template *updates* one shard at a time
(create-missing stays parallel), gating the next shard on `statefulSetRolloutSettled`
(`cluster_rollout.go`) and detecting change via `AnnotationPodSpecHash`. Composes with the partition
gate for free: with `partition > 0`, `CurrentRevision` never advances, so later shards keep
deferring. Governs **operator-triggered rollouts only**. LR-021, LR-047.

**Naming**: LR-021's predicate was renamed `clusterShardRolloutSettled` →
**`statefulSetRolloutSettled`** when LR-050 reused it for sentinel mode. ADR-007, ADR-017 and
`CLAUDE.md` pillar 3.12 now carry that rename as a note beside the name they were decided under.
The **changelog entries are left alone deliberately** — LR-021 and LR-047 record the symbol as it
was at the time, and rewriting a dated record to look current destroys the audit trail.

**Legacy → per-shard migration** — phases `Standup → Meet → Replicate → Failover → Decommission →
Complete` (the pre-LR-025 phases `Draining` / `ReplicasAttached` are **retired**).
`internal/controller/cluster_migration.go:migrateLegacyCluster` (driver) + pure
`internal/redis/migration_plan.go:PlanClusterMigration`. While a migration is in flight the driver
owns the reconcile and the steady repair loop is fully suspended (ADR-013 §6). Invariants: `Failover`
only for a link-`up`-synced `{name}-shard-K-0`; legacy `FORGET` only at full new-side redundancy;
`planReplicates` never emits `REPLICATE <self>` (LR-025 addendum). LR-025, ADR-013.

**Refusals (not repairs)**: `LegacyClusterTopology` (a legacy single STS beside per-shard ones) and
`ShardScaleDownRefused` (`detectOrphanedShardStatefulSets` / `reportShardScaleDown`) — the operator
waits rather than deleting data. LR-020, ADR-007.

---

## 3. Failover mode (experimental)

No lettered rules. The engine is `internal/controller/failover_reconcile.go`
→ `reconcileFailoverAssignments`, whose comment banners number the steps. One decision table
(`planFailover`) replaces sentinel mode's bootstrap + Rule L + LR-024 family.

| Step | Name | Action | Pure seam |
|---|---|---|---|
| 1-2 | gather K8s + Redis view (no Sentinels), auth report | — | — |
| 3 | re-derive intent and live master | reads the `assigned-role` / `assigned-master-ip` / `assignment-epoch` annotations back off the pods | `failover_intent.go:resolveFailoverIntent`, `determineFailoverLiveMaster` |
| 4 | resume a half-applied transition | re-issue `REPLICAOF NO ONE` when the intended master is reachable but still a replica (ADR-006: resumable from live state, no cursor) | `needsPromotion` |
| 5 | failure detection | marker `status.failover.masterDownSince`; verdicts `ClearMarker / StartWindow / Wait / Hold / DeclareK8s / DeclareProbe` | `failover_plan.go:planMasterDeath` |
| 6 | the failover decision | seed / promote / refuse; then stamp intent → `REPLICAOF NO ONE` → **fence** → label flip → repoint | `failover_plan.go:planFailover`, `:planFailoverFence`, `:masterStartAuthorizedFor`; executed by `executeFailoverPlan` |
| 7a | **straggler repoint (the "Rule R analog")** | `SLAVEOF <liveMaster>` at every straggler — **ungated** since LR-038 | `failover_intent.go:planFailoverRepoints` |
| 7b | re-authorization | stamp fresh assignments to release parked/new pods; keeps the `settled` gate | `failover_intent.go:planFailoverReauth` |

**Guards worth knowing by name**

- `planMasterDeath` — **K8s-authoritative** death (pod gone/replaced, redis container not-Ready per
  kubelet, or terminating) declares immediately; **probe-evidenced** death additionally requires
  every reachable replica to report `link:down` (LR-017: the operator's own dial is never sufficient
  evidence). A replica still `link:up`, or no replica to corroborate ⇒ HOLD, marker kept.
- `planFailover` — 0 holders ⇒ seed `redis-0`; ≥1 holders in **one lineage** (`holdersDiverged` over
  `{replid, replid2}`) ⇒ promote `BestDataHolder`, **no opt-in**; ≥2 lineages ⇒ refuse unless
  `failover.allowUnsafeRebootstrapOnDeadlock` (`FailoverRecovery` condition,
  `RefusedDataPresent` event). Carries `unprovablyEmptyVeto` inline (LR-051).
- `failoverPromotionUnsettled` — blocks a **new** mastership decision only while the intended master
  is alive and converging; a dead target never blocks its own replacement. Cascades serialize on a
  10 s `failoverTransitionCooldown` keyed on `status.failover.transitionSince`.
- `planFailoverFence` — demote the **outgoing** master (`REPLICAOF <new>`) so it answers `-READONLY`.
  Keyed on the **pod list, not the gather** (a terminating pod must never enter the ground truth —
  it would read as a live master and as an election candidate). Skipped when the outgoing master is
  unreachable, already demoted, or is the pod being promoted. LR-038, measured 202 of 1171 lost → 0.
- `masterStartAuthorizedFor` — stamps `master-start-authorized-epoch` on **seed and bootstrap only,
  never a promotion**. The start-gate that closes the kill-9 empty-master hole (352 of 1145 writes
  destroyed → 0). The pod-side half is the EmptyDir marker
  `/data/littlered-started-under-epoch`. LR-038 Addendum 3.
- **Watcher**: `failover_monitor.go` INFO-probes the master ~1 s and pushes one `GenericEvent` per
  failure streak — it accelerates a reconcile, it never declares death.

**Sources**: ADR-011 · LR-038 (+ Addenda 1-5) · LR-017 · LR-040/LR-049 (`slaveOfBounded`) · LR-051 ·
`docs/RECONCILIATION_LOOP_FAILOVER.md`.

---

## 4. Standalone mode

**No named rules and no healing chain.** `littlered_controller.go:reconcileStandalone` reconciles
ConfigMap → StatefulSet → Service → PDB → (optional) ServiceMonitor and updates status. Anything
citing a Rule letter for standalone is a mistake. Its liveness probe is a plain local `PING` — the
shape LR-016 reduced sentinel mode's probe *to*.

---

## 5. Cross-cutting: declared heavy operations (ADR-020, in flight)

Not a rule, but it now sits **between** the rules and decides which ones may run, so a reader
tracing an ordering needs it.

- `internal/controller/operation_plan.go:planOperation` (+ `operation_wiring.go:reconcileOperation`,
  `operation_registry.go`). Reasons: `Converged | Running | Blocked | Stalled | Quarantined | Seeded`.
  Registry v1 has exactly one operation, `SentinelMasterNameRename`, whose **driver is Rule 0 + Rule
  N** — no new healing logic exists in the mechanism.
- The fork is **convergence vs rescue, not operation vs healing**, and the classification is written
  at each rule's site rather than in a central list, because *the names lie*:
  - **CONVERGENCE, never stood down** — Rule 0, the LR-005/LR-008 ghost-master correction, Rule R.
  - **RESCUE, stood down while an operation runs** — Rule D, Rule L, the LR-024 recovery.
- Getting this wrong cost a measured 180 s (see Rule R above).

---

## 6. Evaluation order (order is load-bearing)

### Sentinel — `reconcileSentinelCluster`

| # | Stage | Note |
|---|---|---|
| 0 | `bootstrapRequired` ⇒ return | bootstrap owns the pass |
| 1 | pre-gather: `sentinelDesiredReplicas` decides an **armed** quarantine from `status` alone | must be desired state at build time (SSA `ForceOwnership`) — LR-044 |
| 2 | list pods → `LiveTopologyIPs` / `OwnedIPs` (LR-053), `anyTerminating`, kubelet readiness, uncached `rolling` (LR-050) | |
| 3 | gather ground truth (`GatherReplicationState`, concurrent, `ProbeTimeout`-bounded) | LR-012/LR-017/LR-040 |
| 4 | 2a auth report (`OperatorCannotAuthenticate`) — LR-051 | reporting only |
| 5 | 2b `planForsaken` → 2c `planQuarantine` → 2d build operation input | a quarantined instance **returns here** |
| 6 | **Rule 0** | before Rule A, deliberately |
| 7 | **Rule N** | after Rule 0, before Rule A, deliberately |
| 8 | operation decision (`reconcileOperation`) → `operationRunning` | Rule 0 + Rule N are its driver |
| 9 | **Rule A** — `anyTerminating \|\| FailoverActive` ⇒ return | everything below is suppressed |
| 10 | ghost-master / divergent-master correction (LR-005/LR-008) → returns if it acted | convergence |
| 11 | if `RealMasterIP == ""`: **Rule L** and the **LR-024 recovery** (both `!operationRunning`), then return | the only leaderless rules |
| 12 | clear `leaderlessSince` / `ghostMasterStuckSince` markers | |
| 13 | **Rule D** (`!operationRunning && GhostReplicaResetSafe`) | rescue |
| 14 | **Rule R** | convergence, ungated by the operation |

⚠ **doc drift**: the mermaid flow in `RECONCILIATION_LOOP_SENTINEL.md` shows
`Gather → DetermineRealMaster → Rule 0 → Rule N → Rule A → …` and omits stages 4, 5 and 8 (the auth
report, the Forsaken/quarantine switch — which returns *before* Rule 0 — and the operation
decision). Its ground-truth table also still calls the Sentinel field **`FailoverStatus`**, renamed
to `FailoverState` / `MasterFailoverState` by LR-052.

### Cluster — `reconcileCluster` → `repairCluster`

1. legacy migration driver (owns the pass while in flight) → 2. shard scale-down refusal →
3. `ensureClusterResources` (incl. LR-021 one-shard-at-a-time template updates) → 4. readiness
aggregation; **not all Ready ⇒ wipe recovery** and return → 5. gather → 6. auth report →
7. **`advanceClusterRollout`** (before the repair branch, deliberately: a not-yet-reattached
replacement is an empty master, so `repairCluster` would return and skip the gate) → 8. health
verdict ⇒ `repairCluster` **Steps 0 → 1 → 2 → 3 → 3b → 4 → 5** (each acting step returns) →
9. `updateClusterStatus`.

### Failover — `reconcileFailover` → `reconcileFailoverAssignments`

resources → `bootstrapFailover` (while `bootstrapRequired`) → steps 1-2 gather → 3 intent →
4 resume → 5 detection → 6 decision (`planFailover` + execute + fence) → 7 healthy-path: 7a
repoint, 7b re-auth → label flip → `updateFailoverStatus`.

---

## 7. Symptom → start here

For *how to get ground truth*, use the `lrctl-debug` skill's symptom→verb playbook
(`.claude/skills/lrctl-debug/SKILL.md`; `verify` is the workhorse). This table maps the symptom to
the **rule** that owns it once you have the ground truth.

| Symptom | Rules to read | Entry points |
|---|---|---|
| A Sentinel monitors a **dead** master IP; `Authority Master: GHOST(...)` | ghost-master correction (LR-005/LR-008); if leaderless and no promotable replica, the **LR-024 recovery** | LR-005, LR-008, LR-024 |
| A Sentinel monitors a **living but wrong** master | ghost-master/divergent correction | LR-005, LR-008 |
| **Split brain** / two opinions of who is master | Rule A (`FailoverActive`, live only since LR-052), `DetermineRealMaster` (LR-004), Rule N if two *names* are involved | LR-004, LR-048, LR-052 |
| **Leaderless**: `RealMasterIP == ""`, all Sentinels bare, pods in the wait loop | **Rule L** | LR-015, ADR-005 |
| Failover aborts `-failover-abort-no-good-slave` forever | Rule D caused it; the **LR-024 recovery** fixes it | LR-011, LR-013, LR-024 |
| Ghost replicas linger in `SENTINEL replicas` | **Rule D** and its four gates (most likely deferring, by design) | LR-011, LR-013, LR-041 |
| A pod is `role:master` or follows the wrong master | **Rule R** | LR-009, LR-010, LR-016 |
| **Stale master name** / two `sentinel monitor` lines / rename did not take | **Rule N** (check the `StaleMasterName` condition reason: `Converged` / `Pruning` / `Deferred` / `Foreign`) | LR-048, ADR-018 |
| **Capture / Forsaken**: our Sentinels serve someone else's master | `planForsaken`, then the **quarantine**; the captor heals via **Rule D** | LR-039, LR-042, LR-044, ADR-015 §9.2, ADR-016 |
| Instance quarantined but should not have been | LR-050's `rolling` gate; LR-054's un-diagnosable data-holding victim | LR-050, LR-054 |
| Operator "does nothing", every rule silently inert | LR-041 (empty master name in the gather), LR-051 (`AuthFailed` read as unreachable), LR-052 (`FailoverActive` latched true ⇒ Rule A returns) | LR-041, LR-051, LR-052 |
| Reconcile stalls tens of seconds; no log lines | the bounded-probe family: LR-012 (cluster reads), LR-017 (sentinel reads), LR-040 (sentinel writes + inert-ctx), LR-046 (cluster control commands), LR-049 (`SlaveOf`/`IsMonitoring`) | LR-017, LR-040, LR-046 |
| **Slot loss** / a shard reports 0 keys after an update | the **rollout partition gate** (LR-047) and Step 3's `SafeMissingShardTarget`, which *erases* the evidence by healing a dead shard into an empty one | LR-047, ADR-017 |
| **Stuck rollout**, `ClusterRolloutBlocked` | the partition gate — it stalls on purpose and never releases on a timer; check clause (c), the link-`up` replica | LR-047, ADR-017 |
| Cluster stuck `Initializing`, one master owns two ranges | **Step 3b** reshard | LR-018, ADR-006 |
| Every cluster pod `CrashLoopBackOff` after a mass crash | **wipe recovery** (120 s cooldown, then recycle) | LR-023, ADR-008 |
| Cluster partition never heals; `Skipping CLUSTER MEET … (unattributed)` | Step 1 attribution + `confirmPodIP` — read the LR-043 regression section first | LR-043 |
| Replica welded to another shard's master | Step 4 `chooseReattachTarget`, `cluster-allow-replica-migration no` | LR-020 |
| Failover mode: master not replaced / promotion not taking | `planMasterDeath` (HOLD verdicts), `failoverPromotionUnsettled`, step 4 resume | LR-038, ADR-011 |
| Failover mode: acknowledged writes lost on a graceful delete | `planFailoverFence`, and the pod-side preStop | LR-038 |
| Failover mode: master returns **empty** after kill-9 | `masterStartAuthorizedFor` + the `started-under-epoch` marker | LR-038 Addendum 3 |

---

## 8. Retired, renamed and easily-confused names

| Name | Status | Resolves to |
|---|---|---|
| **Rule B** | retired | narrowed into **Rule 0** (ADR-003 Decision 2) |
| **Rule C** | retired | "remove SLAVEOF healing" (ADR-003 Decision 1); *partially superseded* by **Rule R** (LR-009/LR-010) |
| **Rule A+** | retired alias | **Rule D** during leaderless periods (LR-004). Still present as a stale comment in `littlered_controller.go` |
| **Rule D Extension** | retired alias | **Rule D**'s ghost-master half — which LR-008 then moved out of Rule D entirely into REMOVE+MONITOR |
| **`SENTINEL RESET` for a ghost *master*** | superseded | LR-007 tried it, LR-008 replaced it with `REMOVE` + `MONITOR`: RESET does not change the monitored master IP |
| **Topology-aware sentinel liveness probe** | superseded | LR-016 — reduced to a local `PING`; zombie redirect is Rule R, leaderless survival is Rule L |
| **`clusterShardRolloutSettled`** | renamed | **`statefulSetRolloutSettled`** (`cluster_rollout.go`), when LR-050 reused it for sentinel mode. ADR-007, ADR-017 and CLAUDE.md pillar 3.12 keep the old name with a rename note beside it; the changelog entries keep it as the dated record it is |
| **`GatherClusterState`** | renamed | `GatherReplicationState` (mode-neutral) |
| **`ClusterProbeTimeout`** | renamed | `ProbeTimeout` (LR-017) |
| **`MasterInfo.FailoverStatus` / `failover-status`** | deleted | `FailoverState` / `MasterFailoverState`, read from `failover-state`; the old wire key never existed (LR-052) |
| **`/data/littlered-run-epoch`, `runMarkerPath`** | renamed | `/data/littlered-started-under-epoch`, `startMarkerPath` (LR-038 — the old name hid the bug) |
| **`ValidIPs`** | split | `LiveTopologyIPs` (is anything of ours *alive* there) and `OwnedIPs` (is this address *ours*, terminating included) — LR-053 |
| **Migration phases `Draining` / `ReplicasAttached`** | retired | `Standup → Meet → Replicate → Failover → Decommission → Complete` (LR-025) |
| **Reshard as the migration mechanism** | superseded | replicate-then-failover (LR-025); the LR-018 reshard executor still serves Step 3b |
| **`SentinelForsaken` condition** | renamed pre-release | `Forsaken` (LR-042) |
| **Rule 1 / Rule E / …** | do not exist | no such rules; check the changelog before using a letter. "rule 11" in prose is CLAUDE.md **§7 rule 11** (cross-mode parity), not a reconciliation rule |

---

## 9. Unnamed but frequently referenced

Logic that carries no project rule name yet appears constantly in incident write-ups:

- `DetermineRealMaster` (`internal/redis/replication_state.go`) — the majority vote plus the
  ghost-majority guard (LR-004) and the Redis-only fallback; step 5 (offset-based promotion) was
  removed by ADR-003.
- `GhostReplicaResetSafe`, `AllSentinelsBare`, `SentinelsMonitorGhostMaster`,
  `HasHealthyKnownReplica`, `DataHolders`, `BestDataHolder`, `holdersDiverged` — the sentinel
  predicates every recovery rule is built from.
- `AttributeMeetTarget` / `PlanPartitionMeets` / `confirmPodIP` — cluster-mode MEET attribution.
- `unprovablyEmptyVeto` + `ClassifyProbeError` (LR-051) — the veto shared by both sentinel planners
  and inlined in `planFailover`.
- `IsLinkUpReplicaOf` — the single shared definition of "synced", used by both the rollout gate and
  the migration planner so they cannot disagree (LR-025, LR-047).
- The bounded-client family — `newBoundedClient`, `newBoundedRedisClient`, `getBoundedClient`,
  `boundedCtx`, `longBudgetCtx`: not rules, but the reason a rule is or is not allowed to run
  during churn (LR-012/017/040/046/049).

---

## References

- `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` — **authoritative**; read in full (rule §7.7)
- `docs/RECONCILIATION_LOOP.md`, `_SENTINEL.md`, `_CLUSTER.md`, `_FAILOVER.md`
- ADR-003 (rule letters), ADR-005 (Rule L), ADR-006 (Step 3b), ADR-007/ADR-017 (cluster rollouts),
  ADR-008 (wipe recovery), ADR-011 (failover mode), ADR-013 (migration), ADR-015/ADR-016 (capture,
  Forsaken, quarantine), ADR-018 (Rule N), ADR-020 (declared operations)
- `.claude/skills/lrctl-debug/SKILL.md` — symptom → `lrctl` verb playbook
