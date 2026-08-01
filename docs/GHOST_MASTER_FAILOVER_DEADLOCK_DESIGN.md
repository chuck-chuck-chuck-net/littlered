# Design: Ghost-Master Failover Deadlock (Sentinel) — RCA + Fix Options

**Status:** Proposed (design-only; no code yet). Prospective changelog entry **LR-024**,
prospective **ADR-010**, amendment to pillar 3.9/3.10.

**Author's note:** This captures a definitive RCA of the `release/0.3.0` e2e failure in
`Sentinel Failover > should elect new master after master pod deletion (crash)` (SEN-011),
the source-grounded research it triggered, and the fix options — for review **before** any
implementation.

---

## 1. TL;DR

- The crash-failover e2e deadlocks: after the master is force-deleted, every Sentinel is
  stuck monitoring a **ghost master** with an **empty replica list**, so failover aborts
  `-failover-abort-no-good-slave` forever. The CR reports no master indefinitely. Data is
  **safe** (survivor replicas keep their keys); the instance simply never serves.
- **The permanent deadlock is self-inflicted.** Sentinel's own failure modes here are
  transient and self-heal. What makes it permanent is that *our own* ghost-replica
  `SENTINEL RESET` (Rule D) emptied Sentinel's replica list ~4 s before the crash — and an
  empty list can only be rebuilt from the (now-dead) master.
- **Research (source-grounded, Redis + Valkey) is conclusive:**
  1. There is **no surgical single-replica prune** in Sentinel — the only removers are the
     whole-list `SENTINEL RESET` and the whole-master `SENTINEL REMOVE`+`MONITOR`. The
     official docs prescribe `RESET` as *the* remedy for ghost replicas.
  2. A dead replica **never ages out** — no TTL, no reaper, no staleness deletion. It
     persists until an operator acts.
- So "prune the ghost safely" is **impossible by construction**: any prune is a whole-list
  wipe, and you cannot foresee a master death seconds later.
- **Rule D (ghost-replica RESET) is not worth its risk.** It has been hardened five times
  (LR-001/007/008/011/013) on the same hazard and still deadlocks; it treats a **benign**
  condition (a lingering `s_down` ghost replica has no correctness impact and does not even
  affect CR status).
- **Recommended plan (two steps, sequenced):**
  - **Step 1 — Recovery (do first; brings e2e green):** implement the deferred LR-013
    follow-up. When Sentinel is stuck on a ghost master with no promotable replica but living
    survivors exist, the operator elects the best survivor via `REMOVE`+`MONITOR`
    (+`REPLICAOF NO ONE`). Guarantees self-heal regardless of how the
    empty-list-vs-dead-master state is reached. **Independent of any Rule D change.**
  - **Step 2 — Ghost-replica prune policy (discuss, incl. with Michael):** decide how (if at
    all) to prune benign ghost replicas. Ghosts **never age out** (§3.2), so "do nothing"
    means carrying them forever — safe for *correctness* but a permanently degraded
    monitoring signal (alert fatigue). Options: never-prune / low-frequency GC / on-request
    `lrctl` verb. The ghost-*replica* RESET (Rule D) is the *only* self-inflicted cause of
    the deadlock; Step 1 makes any residual prune race self-healing.

---

## 2. The incident & definitive RCA

### 2.1 Symptom

`lrctl verify` on the stuck instance:

```
Sentinel Status:
  - sentinel-0/1/2: monitoring 10.233.65.191      # a ghost IP — no pod has it
Redis Status:
  - redis-0/1/2: role:slave, following:10.233.65.191, link:down, keys:1
  [FAIL] Authority Master: NONE (Split Brain or Cluster not initialized)
```

All three Redis pods `1/2` (redis not-Ready: masterless replica pulled from traffic by the
`link:up` readiness gate — correct per LR-016). `master: {}`, `phase: Initializing`.
Survivors `redis-0` and `redis-2` both hold the data (`keys:1`).

### 2.2 The trigger is a **compound `graceful → crash` sequence** on a shared instance

The `Sentinel Failover` context is `Ordered`: `(graceful)` runs first and **passes**, then
`(crash)` runs on the same instance. Timeline (UTC; `down-after-milliseconds = 3000`,
`quorum = 2`):

| Time | Actor | Event |
|---|---|---|
| 09:51:25 | operator | bootstrap: sentinels monitor initial master `.64.223` |
| 09:51:46 | sentinel | **graceful** failover completes: `+switch-master .64.223 → .65.191` |
| 09:51:49.4 | operator | `Ghost node detected … .64.223` → **`SENTINEL RESET`**. Cluster is **whole** (the graceful-deleted pod returned as a fresh replica), so LR-013's gate *correctly* permits it. RESET empties the replica list. |
| ~09:51:53 | e2e (crash) | force-deletes the *current* master `.65.191` |
| 09:51:56 | sentinel | `+sdown`/`+odown master .65.191` → `try-failover` → **`-failover-abort-no-good-slave`**, every 20 s forever |

The RESET fired for the **prior (graceful) failover's** stale IP `.64.223`, in the ~4 s gap
before the **next (crash)** failover. It wiped the replica list; the crash then killed the
master before Sentinel could re-learn the replicas from its `INFO`.

### 2.3 Why it deadlocks permanently

`SENTINEL RESET` clears the master's slave table; Sentinel rebuilds it **only** from the
live master's `INFO replication` (replicas never self-announce; sentinel discovery also
flows through the master). With the master dead, the surviving replicas can never be
re-learned → no promotion candidate → `no-good-slave` in perpetuity.

### 2.4 Why the operator can't heal it — a gap between two existing rules

- **LR-008 ghost-master correction** (`REMOVE`+`MONITOR`) needs a **living consensus master
  IP** to point Sentinel at. Here every pod is `slave-of` the ghost → `RealMasterIP == ""`.
  Cannot act.
- **Rule L leaderless recovery** (LR-015) requires **all sentinels bare** (`AllSentinelsBare`).
  Here they are **not** bare — they monitor the ghost `.65.191`. Its precondition is false;
  it never fires.

`DetermineRealMaster` deliberately keeps `RealMasterIP == ""` here (majority monitor a ghost
master → it refuses the Redis-only fallback, precisely to avoid RESETing away failover
state; the ghost-majority guard in `DetermineRealMaster`, `replication_state.go`). So the operator loops
`ghost master → no master in status → requeue`, forever.

### 2.5 Regression classification

- **Not a new test:** SEN-011 is original core coverage (`f06bb02`).
- **Not a fresh single-commit break:** the ghost-replica RESET (Rule D) ships in **v0.2.1**
  (origin `c56f7c0`, 2026-02-14). LR-013 (`068ad59`, post-v0.2.1) fenced *one* trigger
  ordering (RESET-after-force-delete) via a K8s wholeness gate; this is a **second,
  uncovered ordering** (ghost-from-a-prior-failover → RESET → next crash) where the gate is
  legitimately satisfied.
- **Why it surfaces now / only on the real cluster (scm-s2):** the RESET must land in the
  ~4 s window between the two failovers. This is timing/environment-sensitive (like LR-017's
  blackhole, which only reproduced on a real cloud). Whether a later timing change (LR-017
  concurrent gather + 3 s `ProbeTimeout`) newly opened the window vs. it was always reachable
  is **not established** and does not affect the fix. A `git bisect` of
  `068ad59..HEAD` running this Ordered context on scm-s2 would attribute it if desired.

---

## 3. Research findings (source-grounded: Redis + Valkey `sentinel.c`)

### 3.1 No surgical single-ghost prune exists

- Official Redis Sentinel guide, *Removing the old master or unreachable replicas*: to
  remove a replica **forever** "you need to send a `SENTINEL RESET mastername` command to
  all the Sentinels" — it refreshes the whole list from the current master's `INFO`.
- The full `sentinelCommand` dispatch (Redis 8.x and Valkey 8.x) has **no per-replica
  removal verb**. `SENTINEL REMOVE` targets a whole *master*. The replica dict is only ever
  cleared wholesale in `sentinelResetMaster` (`dictRelease(ri->slaves)`); a whole-file grep
  finds **zero** `dictDelete(…->slaves)`.
- **Conclusion:** every ghost-removal tool Redis offers is a whole-list wipe. You cannot
  prune one ghost without emptying the replica list.

### 3.2 A dead replica never ages out

- The master-`INFO` parse (`sentinelRefreshInstanceInfo`) is **add-only** — it adds slaves
  it sees, never removes ones absent from the INFO.
- `+switch-master` (`sentinelResetMasterAndChangeAddress`) **creates** the ghost on purpose:
  it re-adds the deposed old master as a slave "so that we'll be able to sense / reconfigure
  the old master." Under our pure-in-memory + IP-identity model (pillar 3.7) the old pod
  returns with a *new* IP, so the old IP is a permanent orphan. It also carries forward any
  existing slave entries, so ghosts can **accumulate** across failovers.
- There is **no** periodic reaper; the `10 × down-after` constant in `sentinelSelectSlave`
  only *excludes* a stale slave from *promotion candidacy*, it does not delete it.
- **Conclusion:** the ghost persists indefinitely until an operator `RESET`/`REMOVE`. Redis
  and Valkey are functionally identical here.

---

## 4. The reframe: the deadlock is self-inflicted, and Rule D is the cause

### 4.1 Sentinel's native modes self-heal; only the wipe makes it permanent

A momentarily all-`s_down` replica set at the instant of master death yields a *transient*
`no-good-slave`: once a replica is reachable again, the next `try-failover` promotes it. What
makes it **permanent** is an **empty** list against a **dead** master — and the only thing
that empties the list is our own `SENTINEL RESET`.

In this incident the *graceful* failover moments earlier **proved** Sentinel had promotable
replicas and used them. Absent the RESET, the crash of `.65.191` would have promoted
`redis-2` (`keys:1`) identically. **Rule D's RESET is the sole cause of the permanent
deadlock in this scenario.**

### 4.2 Rule D's track record and (lack of) value

- **Value:** hygiene only. Origin commit `c56f7c0` "forget ghosts" — no rationale beyond
  tidiness.
- **Harm of a lingering ghost replica:** **not a correctness problem** — `s_down` ghosts are
  skipped for promotion; `HasHealthyKnownReplica()` excludes them; `status.replicas` derives
  from the StatefulSet, **not** Sentinel's list, so a ghost does not corrupt CR status or the
  operator's happy/not-happy verdict. But the cost is **not** merely cosmetic: because ghosts
  never age out (§3.2) and can accumulate, they leave `lrctl verify` **permanently** dirty —
  a real operational cost (monitoring false positives, alert fatigue, ops learning to silence
  "those stupid ghosts"). So the value on the *prune* side of the ledger is real; it is a
  monitoring-signal-quality value, not a correctness one.
- **Cost:** a whole-list `SENTINEL RESET` that has been hardened **five times**
  (LR-001/007/008/011/013) on the same hazard and **still** deadlocks (this incident).

A recurring hard-deadlock risk paid for cosmetic tidiness is a bad trade.

---

## 5. Options

### Rejected

- **R1 — surgical prune.** Impossible; no such command exists (§3.1).
- **R2 — timing gate ("don't RESET when a master is about to die").** Unimplementable: the
  death is a future external event the operator cannot foresee. LR-013's *backward*-looking
  wholeness gate is the best such gate and it is legitimately satisfied here.

### Considered

**Option A — Change the ghost-*replica* prune policy (removes the self-inflicted cause).**
Rule D's RESET is the *only* self-inflicted cause of the deadlock. Because a ghost never ages
out on its own (§3.2), "stop pruning" means carrying it forever, so a policy sub-decision is
needed. All three align with pillar 3.5 (do less when something is fragile):
- **A1 — Never prune.** Carry ghosts forever. Simplest and safest; the monitoring signal
  stays permanently degraded (alert fatigue).
- **A2 — Low-frequency GC.** RESET only when the cluster has been whole *and* no failover has
  occurred for a long, configurable window — shrinking the RESET-racing-a-death window to
  near-zero. Restores a clean signal at a small residual risk (which Step 1 recovery covers).
- **A3 — On-request `lrctl` verb.** Never prune autonomously; expose e.g.
  `lrctl … prune-ghosts` (issues the RESET) for an operator to run when they know the topology
  is stable. Safest human-in-the-loop cleanup; recovery covers any residual race.
Mitigation common to all: `lrctl verify` should present benign ghost replicas as
informational, not `[FAIL]`. *Not self-sufficient alone:* none of A1–A3 self-heals an
empty-list-vs-dead-master reached another way (e.g. LR-008 REMOVE+MONITOR racing a death) —
hence Option B is still wanted.

**Option B — Recovery: implement the deferred LR-013 follow-up.**
When Sentinel is stuck on a ghost master with no promotable replica but living survivors
exist, the operator breaks the deadlock (pillar 3.5, *External Knowledge*): elect the best
survivor via `REMOVE`+`MONITOR`(+`REPLICAOF NO ONE`).
- *Pros:* guarantees self-heal however the state is reached; a genuine safety net for
  unknown future paths.
- *Cons:* each occurrence incurs a ~cooldown outage before self-heal; more code than A.

**Option C — Both (recommended).** Prevention removes the cause so the common path never
deadlocks; recovery guarantees correctness if prevention's assumptions ever break. Defense in
depth.

---

## 6. Recommended design (Option C, sequenced)

Do the recovery **first** (§6.2, Step 1) — it is self-contained and brings the e2e green
without any Rule D change. Decide the prune policy **second** (§6.1, Step 2), separately and
including Michael. The two are independent.

### 6.1 Ghost-replica prune policy (Step 2 — deferred; discuss incl. with Michael)

Pick A1/A2/A3 (§5). The change is **well-contained** — it does *not* ripple through the
operator's health/status logic:
- The only code to touch is `GhostReplicaResetSafe` + its single caller (the Rule D RESET
  block, `littlered_controller.go:900-934`) and the `ghostFound` detection loop that feeds
  only it, plus `lrctl verify` presentation.
- **Untouched and still correct:** `DetermineRealMaster`'s ghost-master fallback suppression,
  `HasHealthyKnownReplica`, and LR-008 ghost-*master* `REMOVE`+`MONITOR`.
- **Operator status is StatefulSet-driven, not ghost-driven:** the `Ready` condition and the
  `Redis`/`Replicas`/`Sentinels` counts read STS readiness (`littlered_controller.go:487`
  ff.), so a lingering ghost does not affect the operator's happy/not-happy verdict at all.
- **The one human-facing ghost surface is `lrctl verify`** (`GetHealActions` + ghost
  reporting) — the place to soften from `[FAIL]` to informational.
- Cluster-mode ghosts (`GhostNodes` / `CLUSTER FORGET`) are a *separate* mechanism, out of
  scope.

### 6.2 Recovery (Step 1 — do first; brings e2e green) — new "ghost-master stuck" rule (sibling of Rule L)

The stuck state enters the **same** `if state.RealMasterIP == ""` branch
(`littlered_controller.go:887`) that today calls only `recoverLeaderlessDeadlock`. Add a
sibling that fires when the sentinels are **not** bare but are monitoring a **ghost** master
with **no promotable replica**.

**Detection gate (all must hold):**
- `state.RealMasterIP == ""` (no consensus living master), and
- `!AllSentinelsBare()` (some sentinel is monitoring — distinguishes from Rule L), and
- a majority of reachable sentinels monitor a **ghost** master (`IsGhost(sn.MasterIP)`), and
- `!HasHealthyKnownReplica()` (Sentinel has *no* promotable replica → it is genuinely stuck,
  not about to fail over on its own — this is the key discriminator from a *recent* master
  death where a legitimate failover is imminent), and
- at least one living Redis survivor exists (`len(DataHolders()) >= 1`, or a reachable pod).

**Cooldown:** a new `status.ghostMasterStuckSince` marker (mirroring `LeaderlessSince` /
`cluster.wipeDeadlockSince`), e.g. 30 s, so a recent master death gets its full
`down-after` + election window before the operator intervenes.

**Action:** `electMaster(bestSurvivorIP)` — already does `REMOVE` + `MONITOR` +
(`REPLICAOF NO ONE` via `needsPromotion`). No new primitive.

**Safety gate — lineage, not count.** This is the substantive difference from Rule L. Rule L
refuses at ≥2 holders (bootstrap holders may have *divergent* data). Here the survivors are
replicas of the **same** dead master — same `master_replid`, identical data — so
`BestDataHolder().diverged == false` and electing the highest-offset one is exactly what
Sentinel itself would have done. Therefore:
- `diverged == false` → elect highest-offset survivor, **no opt-in** (safe).
- `diverged == true` → refuse unless `sentinel.allowUnsafeRebootstrapOnDeadlock`, then
  force-elect `BestDataHolder` (mirrors Rule L's unsafe tier).

| | Rule L — leaderless (exists) | New — ghost-master stuck |
|---|---|---|
| Sentinel state | all bare (`AllSentinelsBare`) | monitoring a **ghost** master |
| `RealMasterIP` | `""` | `""` |
| Discriminator | `AllSentinelsBare()` | `!AllSentinelsBare` + ghost master + `!HasHealthyKnownReplica` |
| Action | `electMaster(BestDataHolder)` | **same** |
| Safety gate | holder **count** (≥2 → opt-in) | **lineage** (`diverged`) — same-master survivors elect freely |
| Cooldown | `LeaderlessSince` | `ghostMasterStuckSince` (new) |

**Pure planner:** `planGhostMasterRecovery(...)` returning a `leaderlessPlan`-style action,
so the full gate/tier matrix is unit-tested with no I/O — same pattern as
`planLeaderlessRecovery`.

### 6.3 No new RBAC

The recovery uses `SENTINEL REMOVE/MONITOR` + `REPLICAOF` on existing addresses. No new
Kubernetes verbs (contrast LR-023's `delete pods`).

---

## 7. Test plan (red-first, per CLAUDE.md §7)

1. **Unit (tier 2), assertion-first:** `planGhostMasterRecovery` table — ghost-master +
   no-healthy-replica + 1 survivor → elect; + 2 same-lineage survivors → elect
   highest-offset, no opt-in; + 2 diverged survivors → refuse unless opt-in; recent master
   death with a healthy known replica → **wait** (do not steal a legitimate failover);
   within cooldown → wait. Also a unit test asserting Rule D no longer issues RESET for a
   benign ghost replica (guards the prevention change).
2. **e2e (tier 3), target-assertion-first:** *this reproduction* — the `graceful → crash`
   Ordered sequence on one instance — asserting the operator self-heals (a living master
   elected, `keys:1` preserved) instead of deadlocking. Confirm it is **red** on current
   code first (it is — reproduced 2/2 on scm-s2), then green after. Because the deadlock is
   timing/environment-sensitive, keep the *repeatable* guard in the tier-2 planner test; the
   e2e exercises it opportunistically (cf. LR-017).

---

## 8. Open questions / decisions

1. **Ghost-replica prune policy** (Step 2): choose A1 (never) / A2 (low-freq GC) / A3
   (on-request `lrctl` verb). Investigation **done** — a lingering ghost is correctness-benign
   but never ages out, so the trade is a *permanently* degraded monitoring signal (alert
   fatigue) vs. RESET risk; the blast radius is contained (§6.1) and Step 1 recovery de-risks
   any prune. Decision deferred; to discuss incl. with Michael.
2. **Cooldown duration** for `ghostMasterStuckSince` — 30 s (match Rule L) vs. shorter (the
   outage is user-visible). Must exceed `down-after` + a Sentinel election attempt so we
   never pre-empt a legitimate failover.
3. **`lrctl verify` treatment** of benign ghost replicas once Rule D is gone —
   informational vs. silent.
4. **Bisect** to attribute the timing shift (§2.5) — nice-to-have, not required.

---

## 9. Impact

- **Docs:** new **LR-024** changelog entry; **ADR-010** (decision: retire the ghost-replica
  RESET; add operator-led ghost-master recovery). Amend pillar 3.9 (ghost healing) and
  pillar 3.10 (extend "no-living-master recovery" to the ghost-monitored variant).
- **Cross-mode parity (§7 / §10 of CLAUDE.md):** cluster mode's analog is LR-023
  (total-/partial-wipe recovery) — already covers "no living master, recycle/re-elect."
  Sentinel gains its matching guarantee here. Standalone has no failover surface.
- **Behavior:** the common failover paths stop deadlocking (prevention); any residual
  empty-list-vs-dead-master self-heals within the cooldown (recovery). Data safety is
  preserved by the lineage gate.
