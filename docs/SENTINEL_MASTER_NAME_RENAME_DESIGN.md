# In-Place Sentinel Master-Name Rename — Design & Implementation Brief

**Status: IMPLEMENTED and e2e-verified** (branch `feat/sentinel-master-name-rename`). This document
is now a **history, not a spec**: it was amended roughly ten times as milestones landed, so it
records the design *and* how measurement corrected it. **Where sections contradict each other across
amendments, the LATER section wins** — the two known cases are flagged in place (§9.2, superseded by
§16.1; §12, superseded by the shipped runbook in `docs/USAGE.md`). The permanent record is **ADR-018**
and changelog **LR-048**; read those first and this one only for the reasoning behind them.
**ADR:** **ADR-018**, not 017. 017 was reserved here when `docs/adr/` ended at 016 and was taken in
the meantime by *State-Gated Intra-Shard Rolling Updates* (LR-047), already merged into `e2e-0821`.
**Changelog ID:** **LR-048** — allocated, written. LR-049 (bounded primitives) and **LR-050 (the
rollout attribution gate, a PREREQUISITE — see §16.1)** landed on the same branch.
**Naming note:** this operation is a **rename**, never a "migration" — in this project a
migration moves data (ADR-013), and this moves none. The word is avoided throughout, including
in the condition name (§8).
**Mode:** `sentinel` only. Standalone/cluster/failover are untouched by every change below.
**Companion, deliberately out of scope:** enabling auth / rotating the password (§13).

---

## 1. What this feature is

Today `spec.sentinel.masterName` can be edited, and the operator has no notion of a rename.
This document specifies **a supported, observable, in-place rename** of an existing sentinel
instance's Sentinel master name — the operation every runbook in the LR-039 → LR-042 →
LR-044 chain tells an owner to perform ("give it a unique `masterName`"), and which the
product currently does not actually implement.

The user-visible contract we are building:

> Edit `spec.sentinel.masterName` on a healthy, quiesced sentinel instance. The operator
> re-points its Sentinels at the new name, removes the old one, and rolls the Redis pods so
> their scripts carry the new name. **The dataset is preserved.** Sentinel-aware clients must
> be reconfigured and restarted in the same window; there is no period in which both names
> resolve.

### 1.1 Requirements (frugal, on purpose)

| # | Requirement |
|---|---|
| R1 | A rename of a **healthy** (`Phase: Running`, `Ready=True`, all pods ready) sentinel instance converges without human intervention on the operator side. |
| R2 | **The dataset survives.** No `FLUSHALL`, no full-resync-from-empty, no path where the only copy of the data is on a pod that is about to be wiped. |
| R3 | At the end, **every** Sentinel monitors **exactly one** master name — the desired one — with our own master's address. No leftover entry, ever. |
| R4 | The operation is **resumable**: the operator may die at any point and the next pass continues from live state. No persisted cursor, no remembered "previous name". |
| R5 | The state is **observable**: a condition + events + log lines that say what is happening and, when it defers, why. |
| R6 | It is **safe to do nothing**: if the preconditions do not hold, the operator defers loudly rather than acting. |
| R7 | Robust on the **happy path**: platform stable, no node loss, no concurrent pod disruption, quorum intact throughout. |

### 1.2 Explicit non-requirements (write these into the runbook verbatim)

| # | Non-requirement | Why we can afford it |
|---|---|---|
| N1 | **Zero or minimal downtime.** | Sentinel-aware clients must be reconfigured with the new name and restarted anyway. A maintenance window is a precondition, not a cost. |
| N2 | **No dual-name overlap for clients.** We will *never* monitor one master under two names concurrently as a feature. | LR-039: two names = two independent failover state machines that can promote different replicas. This is the one thing we must not build. |
| N3 | **Avoiding pod restarts.** The three Redis pods roll. | The name is baked into their startup script and preStop hook (`resources.go:1243`, `:1311`); a restart is how they pick up the new one. Under EmptyDir a rolling restart is a resync, not a data loss (R2 is about *simultaneous* loss, and the roll is serialized by readiness). |
| N4 | **Robustness against concurrent disruption** — sub-quorum node/pod loss, a node drain, a second failover *during* the migration. | Maintenance window. Documented as unsupported; the operator's ordinary healing rules still apply, but the migration makes no guarantee there. |
| N5 | **Renaming a degraded instance.** | Precondition: `Running`/`Ready=True`. §7.2 shows what wedges if you ignore it, and §9 makes the operator refuse and say so. |
| N6 | **Renaming a captured (`Forsaken`) instance.** | §7.3 — a real trap. The design refuses the prune *and* keeps the capture verdict alive (WP4b), so ADR-016's quarantine still heals both sides. Refusing was the minimum; keeping the verdict is the repair. |
| N7 | Doing the auth change at the same time. | §13. One variable per window. |

---

## 2. Required reading before writing any code

Every sub-agent gets this list. Not optional — most of the design is a consequence of these.

1. `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` **end to end** (CLAUDE.md §7 rule 7). The
   load-bearing entries for this work: **LR-004** (when the Redis-only master fallback is
   allowed), **LR-005 / LR-008** (`REMOVE`+`MONITOR` is the divergent-master primitive),
   **LR-013** (why a whole-list `RESET` is a hazard), **LR-015** (Rule L, the leaderless
   deadlock we must not manufacture), **LR-024** (`electMaster`'s "skip only when already
   correct" fix — the same shape as our prune's pre-check), **LR-039** (the master name *is*
   the isolation boundary; the "no rolling cutover" decision), **LR-040 / LR-046** (every
   single-shot Sentinel/Redis call must be bounded on **both** the ctx *and* the client's own
   `Dial`/`Read`/`WriteTimeout`; a ctx alone is inert), **LR-041** (a required value belongs
   in the signature, not in construction state), **LR-042 / LR-044 / LR-045** (`Forsaken`,
   the quarantine, the pre-gather decision pattern).
2. `docs/adr/015-per-instance-sentinel-master-name.md` and
   `docs/adr/016-forsaken-gated-quarantine.md`.
3. `docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md` §9.2 and §9.4.
4. `CLAUDE.md` pillars 3.6, 3.7, 3.9, 3.10, 3.15 and the Test Discipline section.
5. `docs/RECONCILIATION_LOOP_SENTINEL.md` for the rule inventory and ordering.

---

## 3. Ground truth: where the master name lives today

All verified against the working tree on `e2e-0821`. An implementer must re-verify the line
numbers, not the facts.

### 3.1 The API surface

- `api/v1alpha1/littlered_types.go:409-436` — `SentinelSpec.MasterName`: `Required`,
  `MinLength=1`, `MaxLength=128`, pattern `^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$`.
  **No CEL transition rule — the field is mutable today.**
- `api/v1alpha1/sentinel_master_name.go:46` — `SentinelMasterName()` returns the spec value
  or falls back to `LegacySentinelMasterName` (`"mymaster"`); `:61`
  `SentinelMasterNameUnscoped()` reports only that the field is *unset*.

### 3.2 Where the effective name is consumed

| Consumer | Location | Baked into the pod? |
|---|---|---|
| Redis pod startup script (`sentinel master`, `get-master-addr-by-name`) | `resources.go:1150`, `:1152`, `:1171`, rendered at `:1243` | **Yes** — it is in `Container.Command` |
| Redis pod preStop hook (`SENTINEL SLAVES`, `SENTINEL failover`, `get-master-addr-by-name`) | `resources.go:1274`, `:1286`, `:1289`, rendered at `:1311` | **Yes** — `Lifecycle.PreStop` |
| Sentinel pod | `buildSentinelConfig` (`resources.go:888`) writes **no `sentinel monitor` line at all**; the container starts bare (`:1550` copies the CM to `/data/sentinel.conf`, `:1558` `exec redis-sentinel`) | **No** |
| Operator commands (`MONITOR`, `SET auth-pass`, settings, `REMOVE`, `RESET`, reads) | `littlered_controller.go:833` resolves it once per pass and passes it as a parameter everywhere (LR-041) | n/a |
| Gather | `gatherer.go:58` `GetSentinelState(ctx, podName, ip, masterName)`; `cmd/lrctl/cmd/gatherer.go:79` for the CLI | n/a |
| Background `+switch-master` subscriber | `sentinel_monitor.go:126` subscribes to the channel and does **not** filter by name — it only accelerates a reconcile | **No** — nothing to do here |

### 3.3 The persistence facts that make this tractable

- **Sentinel's own state is the only place the old name persists.** Sentinel rewrites
  `/data/sentinel.conf` with its `sentinel monitor <name> …` lines. `/data` is an
  **EmptyDir** (`resources.go:1436-1441`), so a Sentinel pod restart loses it entirely — the
  documented basis of Rule L (LR-015).
- **We cannot edit that file**, and we do not need to: `SENTINEL REMOVE <name>` /
  `SENTINEL MONITOR <name> …` are the API, and Sentinel persists the result itself.
- **A rename changes the Redis pod template** (the scripts are in the container spec) ⇒ the
  Redis StatefulSet rolls. It does **not** change the Sentinel pod template ⇒ the Sentinel
  StatefulSet does **not** roll. This asymmetry is the whole reason a stale entry survives
  today.
- The Redis STS pod template carries `AnnotationConfigHash` over `redis.conf` only
  (`resources.go:1049-1052`); `AnnotationPodSpecHash` is stamped **only** by the cluster shard
  builder (`resources.go:2047`). So ConfigMap content changes alone never restart sentinel-mode
  pods — relevant to Alternative D (§6.4).

### 3.4 The rollout interlock (a load-bearing property, not a coincidence)

- Sentinel-mode Redis **readiness** = `role:master` **or** `master_link_status:up`
  (`resources.go:728-740`). A restarted replica cannot become Ready until it has found a
  master **under the new name**.
- The Redis STS sets `minReadySeconds: 35` by default (`resources.go:1064-1069`) and rolls in
  reverse-ordinal order.
- Therefore: **the Redis rollout physically cannot outrun the Sentinel-side rename.** The
  first pod to roll is a replica; it parks in the startup wait-loop until the new name
  resolves; the StatefulSet will not touch the next pod. The master (`redis-0`) is rolled
  **last**, minutes after the Sentinel side has converged. §6.3 relies on this instead of
  building a rollout gate.
- **Scope of that guarantee — read it precisely.** It orders pods `redis-1` and `redis-0`
  *behind* the rename. It does **not** order `redis-2`: the STS apply happens at
  `littlered_controller.go:674`, **before** the gather, so the highest-ordinal pod is deleted in
  the same pass in which Rule N prunes. That is intended (it is a replica, and §7.1 accounts for
  it), but the interlock must not be quoted as if it covered all three.
- **VERIFIED LIVE (WP0, t3e, 2026-08-26). The interlock holds.** Measured from a 1s sampler,
  against an operator image whose production code is byte-identical to `e2e-0821`:
  `redis-2` Ready under the new name → `redis-1` deleted = **34.76s**; `redis-1` Ready →
  `redis-0` deleted = **33.63s**. Both land on `minReadySeconds: 35`, i.e. the StatefulSet
  waited for readiness *plus* the full availability window before advancing, and `redis-0`
  rolled last. The `redis-2` caveat above reproduced exactly: deleted at **t0+0.6s**, in the
  same pass, before the gather. **§6.3 stays rejected.** The paragraph below records why this
  needed verifying at all; it is kept rather than deleted, because the reasoning is the reusable
  part.
- **This property was traced, not measured, and it was the single load-bearing assumption of the
  whole design** — it is what lets §6.3 reject a persisted migration phase. It has the exact
  shape of ADR-016's "the captor heals via Rule D": an inference from three independently
  documented mechanisms, correct as it turned out, but only *known* after M4a observed it live —
  and M4a falsified the companion 120s derivation in the same run. So it was verified **first**,
  as **WP0** (§10), before any wiring was built on it — and, as in M4a, the same run falsified a
  companion claim (§7.1's Forsaken assertion, below).
- Sentinel **container** probes are a bare `PING` (`resources.go:1591-1618`), so if we ever
  *did* roll the Sentinel STS the three pods would cycle fast, with a window where the whole
  quorum is bare. One more reason not to (§6.2).

---

## 4. What happens today if you just edit the field (the defect)

Traced, not run. An implementer should confirm the first two steps live before building —
it is a five-minute observation and it is the "red" for the e2e (§11.4).

1. **Pass 1** applies the new Redis STS template (`littlered_controller.go:674` — before the
   gather, LR-044 wiring) → K8s begins rolling `redis-2`.
2. The gather asks every Sentinel for the **new** name. Sentinel answers an unknown name with
   `ERR No such master with that name`, which `gatherer.go:80-87` maps to
   `Monitoring:false, Reachable:true` — **the whole quorum reads bare** (this is exactly the
   plausible-looking lie of LR-041, arriving here legitimately).
3. `DetermineRealMaster` (`replication_state.go:92`): no majority monitors anything, no ghost
   majority, no active failover ⇒ **step 4 fallback** picks the one reachable `role:master`
   pod. `RealMasterIP` is correct.
4. **Rule 0** (`littlered_controller.go:980-1013`) sees `Reachable && !Monitoring` on all three
   and issues `SENTINEL MONITOR <new> <masterIP>` + `auth-pass` + settings. A `MONITOR` under a
   *different* name is accepted, so now **each Sentinel monitors both names**.
5. **Nothing ever removes the old entry.** Every `Remove` call site
   (`littlered_controller.go:1058`, `:1078`, `:1895`) passes the *current* name; the operator
   has no list-all call (`internal/redis/client.go` has none) and no memory of the previous
   value.

Net: the rename *converges* and the data survives, but it lands in exactly the state LR-039
names as the hazard — one master, two names, two independent failover state machines — and
leaves it there **permanently**. Then:

- The `redis-0` roll runs a preStop **baked with the old name**: `SENTINEL SLAVES <old>`
  returns nothing (10×1s of "Waiting for Sentinel to discover replicas"), `CLIENT PAUSE 30000
  WRITE` fires, `SENTINEL failover <old>` either **errors** (if the old entry were gone) or —
  today — **triggers a real failover under the stale name**, concurrently with the new name's
  state machine having its own opinion of who the master is.
- Any future master death is adjudicated twice, by two quorums, over the same three pods.

**This is the bug LR-048 records.** The feature and the bug fix are the same change.

### 4.1 Confirmed live, with the numbers (WP0, t3e, 2026-08-26)

Every step above reproduced. Both names were present on all three Sentinels **0.8s after the
patch** and **still present 12m39s later** on a `Running`/`Ready=True` instance. The sharpest
single artefact is Sentinel's own persisted state, `/data/sentinel.conf`:

```
sentinel monitor wp0-rename-mn.rn 10.233.192.95 6379 2
sentinel config-epoch wp0-rename-mn.rn 2
sentinel known-replica wp0-rename-mn.rn 10.233.192.148 6379
sentinel known-replica wp0-rename-mn.rn 10.233.192.250 6379
sentinel monitor mymaster 10.233.192.95 6379 2
sentinel config-epoch mymaster 3
sentinel known-replica mymaster 10.233.192.236 6379   <- dead IP from the roll
sentinel known-replica mymaster 10.233.192.224 6379   <- dead IP from the roll
sentinel known-replica mymaster 10.233.192.110 6379   <- dead IP from the roll
sentinel known-replica mymaster 10.233.192.250 6379
sentinel known-replica mymaster 10.233.192.148 6379
```

Two `monitor` lines. The **stale** one carries five known-replicas where two were deployed —
three of them dead IPs that never age out (LR-024) — and a **higher config-epoch** (3 vs 2).

**And the preStop fired a real failover under the stale name, as §4 predicted.** From
`rn-sentinel-1` at t0+89s:

```
* Executing user requested FAILOVER of 'mymaster'
# +new-epoch 1
# +try-failover master mymaster 10.233.192.110 6379
# +elected-leader master mymaster 10.233.192.110 6379
# +promoted-slave slave 10.233.192.250:6379 ... @ mymaster 10.233.192.110 6379
* +slave-reconf-sent slave 10.233.192.236:6379 ...    <- a DEAD IP
```

`Executing user requested FAILOVER` is Sentinel's line for an operator-issued
`SENTINEL failover` — i.e. the baked preStop hook. **The measured consequence is the headline
evidence for LR-048:**

| window | duration |
|---|---|
| the two names name **different addresses** | **88.5s** |
| the two names name **two different, live, running pods** as master | **56.6s** |

Two quorums, two config epochs, two failover state machines, over the same three pods — LR-039's
named hazard, produced by a **supported field edit on a healthy instance**. Data survived
(4000/4000 keys) only because writes were quiesced per the runbook.

---

## 5. Decision

> **The operator reconciles the *scope* of what its Sentinels monitor, not a migration.**
> Desired state: *every Sentinel monitors exactly the desired name, and nothing else.* A
> stale name is discovered from Sentinel's own answers (`SENTINEL masters`), pruned with
> `SENTINEL REMOVE`, and only ever after the desired name has been confirmed present on that
> same Sentinel. Nothing is remembered; no "from" name, no phase, no cursor.

Five properties follow, and they are why this shape was chosen over the alternatives in §6:

1. **Evidence-driven, so no persisted state (R4).** The previous name is not needed — anything
   monitored that is not the desired name is by definition stale. This also repairs an
   instance a *previous* botched rename already broke (§4), and any out-of-band `MONITOR`
   someone added by hand. LR-041's lesson applied at the level of state rather than signatures:
   don't store what you can read.
2. **One decision, one existing primitive.** `REMOVE` is already in the client
   (`client.go:447`), already bounded (LR-040), and `REMOVE`+`MONITOR` is the project's
   established divergent-master primitive (LR-005/LR-008). We add one *read* (`SENTINEL
   masters`) and one *rule*.
3. **Register-then-prune, and the prune verifies its own precondition.** The two-name window
   is intra-pass (milliseconds), never across passes, and a Sentinel is never left bare on
   purpose. The pre-check (`IsMonitoring(desired)`, `client.go:471`) is the LR-024 shape:
   skip only when already correct, otherwise act.
4. **No dual-name serving, ever (N2).** We are not reversing LR-039's decision; we are
   implementing the *cutover* it said had to be a window.
5. **No rollout gate (§3.4).** Readiness already serializes the Redis roll behind the rename,
   so we need neither a persisted migration phase nor a hoisted gather — the two things
   LR-044's wiring half warns are expensive.

---

## 6. Alternatives considered

### 6.1 Make `masterName` immutable (CEL transition rule) — REJECTED for this branch, keep as the fallback

Honest and free: a rename becomes delete-and-recreate, which is what ADR-015 §9.2 and ADR-016
already lean on for the capture case. **Rejected because it forecloses the operation the
product's own runbooks demand** — every capture remedy is "give it a unique name", and telling
an owner to destroy a working instance's dataset to escape a *theoretical* capture is a worse
trade than the migration we can actually build. Keep it in the ADR's Alternatives with a
trigger: **if the prune rule cannot be made safe, ship immutability instead of shipping the
current silent two-name state.** Doing nothing is not an option — §4 is a defect either way.

### 6.2 Roll the Sentinel StatefulSet and let EmptyDir do the pruning — REJECTED as the mechanism, noted as a manual escape hatch

Stamp the effective name (or a hash) into the Sentinel pod template so a rename rolls those
pods; they come back bare (§3.3) and Rule 0 registers only the new name. Attractive because it
needs *no* new Redis call and no new rule.

Rejected as the primary mechanism because:
- Sentinel readiness is a bare `PING` (§3.4), so all three pods cycle in seconds and the
  quorum can be entirely bare — trading a two-name window for a **no-monitoring** window, and
  giving up failover protection for the duration.
- It only fixes the state on pods that restart. It cannot repair an instance already in the
  §4 state (no template change is pending there), so we would *still* need the prune.
- It manufactures pod churn as a side effect of an API field's value, which is the kind of
  coupling LR-021 had to serialize away in cluster mode.
- Cycling Sentinel pods releases and re-acquires pod IPs — the §9.4 warm-IP window that
  ADR-016 accepts only because it has no choice. Here we do.

Keep it in the runbook as the **manual escape hatch**: `kubectl rollout restart
statefulset/<name>-sentinel` provably clears every entry, at the cost of a bare window.

### 6.3 A staged migration with a persisted phase — REJECTED (unnecessary)

Hold the Redis STS template update until the Sentinel side has converged, so a Redis pod is
never running a script whose name disagrees with what its Sentinels monitor. It needs the
desired name *at build time*, i.e. before the gather (`littlered_controller.go:660-680`), which
means persisting the migration phase in status — the LR-044 pre-gather pattern, or ADR-013's
`status.migration` phases.

Rejected because §3.4's readiness interlock delivers the ordering for free: the roll cannot
advance past the first (replica) pod until the new name resolves, and the master is rolled
last. We would be paying persisted load-bearing state (an ADR-006 tension every time) for an
ordering physics already gives us. **Recorded as the fallback with an explicit trigger: if WP0 (§10) shows the roll racing the
rename, build this instead of Rule N's ungated form.** The symptom would be `redis-0`
terminating while a stale entry still exists.

### 6.4 Make the pods name-agnostic (read the name from a mounted file) — DEFERRED, and it is the right long-term shape

Move the name out of the container spec into a file (a key in the existing ConfigMap, mounted),
re-read by the startup wait-loop on each iteration and by the preStop at exec time. Then:

- a rename triggers **no rollout at all** (the running processes never use the name except at
  start and stop);
- the preStop of a pod that is being terminated *during* a rename uses the **current** name, so
  the graceful-handover hole in §4 closes rather than being documented around;
- and the whole ordering question disappears.

Deferred, not rejected: it edits the sentinel startup script, which is the highest-consequence
file in the repo (LR-003, LR-016, LR-023 all turn on its behaviour), it needs one adoption
rollout anyway, and ConfigMap propagation to a kubelet is minutes-scale and unsynchronised
(§3.3 also shows nothing hashes that CM, so the operator gets no restart and no signal). It is
worth its own ADR later. **The prune rule is a prerequisite for it either way** (it is what
converges Sentinel's own persisted state), so building the prune first is not wasted work.

### 6.5 Dual-name overlap for a graceful client cutover — REJECTED, hard

Monitor the master under both names for a grace period so clients can migrate one at a time.
This is LR-039's named hazard and N2. Two entries = two failover state machines over the same
three pods; they can promote different replicas, and the loser's writes are discarded on
resync. **No sub-agent may implement this, even as an opt-in.** If someone asks for
client-side overlap, the answer is the auth work in §13 (where overlap *is* achievable) plus a
maintenance window.

---

## 7. Mechanics, in order, with the interactions that matter

### 7.1 The happy path, pass by pass

Preconditions (enforced by §9's gates and stated in the runbook): `Phase: Running`,
`Ready=True`, 3/3 Redis and 3/3 Sentinel pods ready, no failover in flight, **writes quiesced**
(clients stopped for their own reconfiguration).

| t | What happens |
|---|---|
| `t0` | Owner edits `spec.sentinel.masterName` (and, per the runbook, has already stopped the clients). |
| `t0+ε` **pass 1** | Redis STS applied with the new scripts → K8s deletes `redis-2` (highest ordinal, a replica). Sentinel STS unchanged. |
| same pass | Gather: all three Sentinels read `Monitoring:false, Reachable:true` (§4 step 2). `planForsaken` clause 1 needs a reachable **monitoring** Sentinel ⇒ no capture verdict **in this pass**. But see §7.1b — the claim that "a rename never reads as a capture" is FALSE later in the roll. `DetermineRealMaster` step 4 ⇒ `RealMasterIP` = the live master. |
| same pass | **Rule 0** registers the desired name on all three Sentinels (+`auth-pass`, +settings). |
| same pass | **Rule N (new)**: for each reachable Sentinel, `MonitoredMasters` still lists the old name ⇒ confirm `IsMonitoring(desired)` ⇒ `SENTINEL REMOVE <old>`. **R3 satisfied at the end of pass 1**, seconds after the edit. |
| same pass | Rule A skips the remaining healing (`anyTerminating` is true because `redis-2` is going). Rule N sits **before** Rule A, deliberately — §7.5. |
| `+~40s` | `redis-2` returns on an empty EmptyDir, resolves the master under the **new** name, syncs, goes Ready (`link:up`), waits out `minReadySeconds: 35`. |
| `+~2min` | `redis-1` rolls identically. |
| `+~3min` | `redis-0` (the master) rolls. Its **baked** preStop uses the **old** name: `SENTINEL SLAVES <old>` → nothing (10s of waiting), `CLIENT PAUSE 30000 WRITE`, `SENTINEL failover <old>` → `ERR No such master with that name` (the entry is gone) → no proactive handover. The pod dies; the new name's quorum reaches SDOWN after `down-after-milliseconds` (30s default) and promotes a replica. **Expected, documented, and harmless with writes quiesced** (N1). |
| `+~4min` | `redis-0` returns as a replica; `updateMasterLabel` moves `role: master` so the `{name}` Service follows; `SentinelMasterNameUnscoped` clears if it was set; phase `Running`. |
| after | Owner reconfigures and restarts the Sentinel-aware clients with the new name, verifies with `lrctl verify`. |

### 7.1a Measured, not estimated (WP0, t3e, 2026-08-26)

The table above is the *design's* trace. These are the numbers, from a 1s sampler over a
3-pod instance holding 4000 keys, `t0` = the patch. **They replace the ~4-6 minute estimate.**

| edge | Δt0 |
|---|---|
| gather reads the whole quorum bare; master label pulled | +0.03s |
| Rule 0 registers the desired name on all 3 Sentinels | +0.1s |
| `redis-2` terminating (the un-interlocked pod) | +0.6s |
| **both names present on all three Sentinels** | +0.8s |
| `redis-2` 2/2 Ready under the new name | +12.9s |
| `redis-1` deleted / Ready | +47.6s / +55.5s |
| `redis-0` deleted; preStop fires `SENTINEL failover <old>` | +89.1s |
| forced `+switch-master` under the **stale** name | +90.1s |
| desired name repointed by the operator's LR-008 correction | +122.1s |
| **final sustained `Running`/`Ready=True`** | **+176.8s (2m57s)** |

**~3 minutes, and the estimate was pessimistic — but read the caveat, because it inverts.**
This run was *faster because of the bug*: the stale entry still existed, so the master's
baked-old-name preStop `SENTINEL failover <old>` **succeeded** and handed over instantly. Once
Rule N removes that entry, the same call errors (`ERR No such master with that name`) and the
desired name's quorum must wait out `down-after-milliseconds` before electing — exactly as the
table above predicts. **Post-fix budget: ~3 min + ~30s at the `redis-0` edge.** The structure
of §7.1 was right; only its magnitude was wrong. Each replica roll additionally costs ~12s of
`Ready=False`, the master's ~53s, and the CR flaps `Running → Initializing → Running` twice
more before settling.

### 7.1b A rename DOES transiently look like a capture — corrected by WP0

§7.1 originally asserted flatly that a rename "must never read as a capture", reasoning from
`planForsaken` clause 1 (which needs a reachable *monitoring* Sentinel, and mid-rename they all
read bare). **WP0 falsified that.** At **t0+89.1s** the operator logged
`Forsaken=False/CaptureSuspected` — 1 of ~110 one-second samples. All four clauses held:

1. the quorum was monitoring again (Rule 0 had re-registered ~89s earlier);
2. all three agreed on **one** address, `10.233.192.110`;
3. that address was the **just-replaced `redis-0`** — no longer in `ValidIPs`, so `IsGhost` is
   true, and **not yet flagged down**, because `s_down` needs `down-after-milliseconds` (30s);
4. no reachable pod of ours was `role:master` in that instant.

**What saved it was the 30s `forsakenCooldown`, not the clauses** — the signature cleared in
~35s when the LR-008 ghost-master correction repointed the desired name. That is thin margin on
a claim the design made flatly, and clause 1 was never the thing protecting us.

**Consequence, and it is a hard requirement on WP4b:** making `planForsaken` name-agnostic
widens what it evaluates, and the rename's transient state is exactly where that widening could
bite. **WP4b MUST carry this measured shape as a regression row: the rename window must not
produce a capture verdict.** Use the real fixture — quorum unanimous on a just-replaced pod's
address, ghost, not-yet-down, no master of ours — not a synthetic one. If WP4b's change makes
this latch, the feature has traded a two-name bug for a spurious quarantine, which is strictly
worse.

Bound on the observation: 1s sampling, so the suspicion held for at least one sample and less
than 30s. Also observed unprompted in the same run: Rule D's ghost-replica `SENTINEL RESET`
fired **twice** against the *desired* name (`+reset-master`), triggered by the departed
`redis-2`'s IP going `s_down`. LR-024's self-inflicted-deadlock ingredient is live on this path;
it did not deadlock here.

### 7.2 If the instance is NOT healthy when renamed (why N5 is a precondition)

If no reachable pod is `role:master` at pass 1 (mid-failover, leaderless, a pod already down),
then `RealMasterIP == ""`: Rule 0 cannot register, Rule N defers (§9 gate 2), and the Redis
pods roll into a wait-loop on a name nobody monitors. The instance then presents Rule L's
signature (all Sentinels bare under the desired name, `RealMasterIP == ""`) and **Rule L is the
safety net**: 0 holders → reseed; exactly 1 → promote; **≥2 holders → REFUSE** without
`allowUnsafeRebootstrapOnDeadlock` (LR-015). That last case is a genuine wedge requiring a
human decision.

So: refuse-and-say-so is the behaviour (§9), the precondition goes in the runbook, and the
risk register carries it (§14). Note the silver lining worth stating in the ADR: **Rule L's
recovery lands on the desired name**, because `electMaster` issues `REMOVE`+`MONITOR` with the
name the operator currently wants — the rename completes even out of the wreckage.

### 7.3 The trap: renaming to escape a capture (N6) — must be detected

An owner reading the LR-039/LR-042 runbook will try exactly this: *"we were captured, so let's
give the instance a unique name."* On a captured victim, at pass 1:

- The Sentinels monitor the **old (shared)** name pointing at the **captor's** master.
- Asked about the new name they read bare ⇒ `planForsaken` clause 1 fails ⇒ **the capture
  verdict disappears**, and with it the quarantine that would have healed both sides in ~4
  minutes (ADR-016, measured 3m41s-3m58s).
- No pod of ours is `role:master` (they are all replicas of the foreign master) ⇒
  `RealMasterIP == ""` ⇒ Rule 0 and Rule N both stand down.
- The victim's pods hold **the captor's** keyspace, so Rule L sees ≥2 data holders and
  **REFUSES**.

Net: **a rename converts a diagnosed, self-healing capture into an undiagnosed leaderless
refusal.** The rename must therefore refuse when a *stale* entry points at an address that is
not one of our pods and is not flagged down — the `planForsaken` clause-3 test, applied to the
stale name. `SENTINEL masters` returns the full field map per master (`flags`,
`failover-status`, `num-slaves`, `num-other-sentinels`), so this costs nothing extra.

- **In scope (WP3/WP4):** Rule N returns a distinct `Foreign` verdict → no prune, a `Warning`
  event and a condition message that names the foreign address and says *"do not rename to
  escape a capture; let the quarantine complete first"*.
- **Also in scope, and it is the actual fix (WP4b):** make `planForsaken` **name-agnostic** by
  feeding it `MonitoredMasters`, so a capture under a *stale* name is still a capture. Refusing
  is a diagnostic; keeping the verdict alive is a repair — the quarantine then still runs and
  still heals both sides in ~4 minutes, which is strictly the better outcome for an owner who
  renamed in a panic.

**Why this could not stay deferred.** Rule N's per-entry discriminator (G5: prune only if the
stale entry's address is one of our pods *or* is flagged down) catches a capture only while the
**captor's master is alive**. If that master is momentarily `s_down` — a captor mid-failover, at
precisely the moment the owner renames — G5 passes, the prune fires, the stale entry is gone,
and the capture is now both undiagnosed *and* unrecoverable, with no `Foreign` warning ever
emitted.

**⚠ CORRECTED BY M4 — the sentence that used to follow was wrong.** This document originally
concluded *"only a verdict that does not depend on the name can close that"*, and treated G0 as
the **structural closure** of the `s_down`-captor hole. It is not. Name-agnosticism closes the
case where the captor's master is **alive** — the common case, and the one where a prune actually
does damage. The `s_down` case is refused by `planForsaken` **clause 3 on principle**, and
correctly so: from Sentinel's vantage an address that is not ours and is not answering is
**indistinguishable from our own dead ex-master** (LR-024's entire subject), so calling it a
capture would park live instances after every ordinary failover — exactly the false positive the
verdict's one-way conservatism exists to prevent. The existing table pins this
(`"ordinary dead ghost master is NOT capture (flagged down)"`, a *foreign* address flagged
`s_down,o_down,master` asserting `Captured=false`), so the alternative was not implementable
without editing a row — i.e. without silently changing LR-042's verdict.

**So the `s_down`-captor hole is NARROWED, not closed**, and G0 is worth having for the alive
case regardless. The residual is recorded as K2d. If it is to be closed, **the lever is Rule N's
side, not the verdict's** — G5 would have to require a not-ours address to have been flagged down
for a settle before it counts as prunable debris.

`planForsaken` going name-agnostic remains what makes Rule N's refusal path sound for the alive
captor, and it earns gate **G0** in §9.

**The LR-038 caution still applies and is discharged deliberately, not ignored:** widening what
the ground truth contains changes every rule that reads it at once. `MonitoredMasters` is a new
field consumed by exactly two planners (`planStaleMasterNames`, `planForsaken`), both pure, both
table-tested, and the change to `planForsaken` is *additive* — every clause that fires today on
the desired name must still fire identically, with new rows only for the stale-name case. WP4b
carries a regression obligation to prove that: the whole existing `planForsaken` table must pass
unchanged.

### 7.4 Interaction with the quarantine (LR-044)

A quarantined instance has **zero** Sentinel pods, so there is nothing to prune and Rule N is
unreachable (`reconcileSentinelCluster` returns early on `ScaleToZero`). After release, Rule L
re-bootstraps it **empty** and the Sentinels come back **bare** — so an owner who renames a
quarantined instance gets the new name for free on release, with no old entry anywhere. That is
the recommended remedy order and it belongs in the runbook: **capture → let the quarantine
finish → then rename.**

### 7.5 Rule ordering, and why Rule N is not behind Rule A

Placement inside `reconcileSentinelCluster`: **after Rule 0, before Rule A**
(`littlered_controller.go` — Rule 0 at `:980-1013`, Rule A at `:1015`).

- **After Rule 0** so the desired name is already registered on a bare Sentinel in the same
  pass — that is what makes the two-name window intra-pass and makes the prune's pre-check pass
  on the first attempt.
- **Before Rule A**, i.e. it runs while `anyTerminating` is true. This is deliberate and it is
  the opposite of the LR-040 defect: the churn Rule A sits out (`redis-2` terminating) is
  *exactly* when the rename is in flight, and gating on `!anyTerminating` would leave the
  two-name window open for the whole multi-minute roll — the one window in which
  `redis-0`'s stale-name preStop could fire a real failover under the old name.
- **LR-040's actual lesson applies in full:** an action that runs during churn **must be
  bounded**. Every call Rule N makes (`Masters`, `IsMonitoring`, `Remove`) goes through
  `newBoundedClient` (`client.go:93`) — all three of `Dial`/`Read`/`WriteTimeout` at
  `ProbeTimeout` **and** a per-call ctx deadline. A ctx alone is inert (LR-040's measured
  5.02s → 5.00s).

### 7.6 What the operator must NOT do here

- **No `SENTINEL RESET`.** RESET does not remove a master entry (LR-007/LR-008) and wiping the
  replica list is the LR-013/LR-024 hazard. `REMOVE` of the *stale* name is surgical and
  touches nothing about the desired name's state.
- **No `REMOVE` of the desired name** in this rule. Re-pointing the desired name at a
  different address is LR-005/LR-008's job and stays there.
- **No prune while a failover is in flight under any monitored name** (§9 gate 3).
- **No memory of the previous name** in status, annotations, or an env var.

---

## 8. Naming — decided

Condition **`StaleMasterName`**, planner `planStaleMasterNames`, plan type
`StaleMasterNamePlan`, file `internal/controller/stale_master_name_plan.go`, rule **N**. Use
these names everywhere; the ADR records the decision, not the alternatives.

**Why not the obvious candidates.** *"Migration"* is wrong on the merits: in this project a
migration moves **data** (ADR-013's legacy→per-shard `status.migration` phases), and this
operation moves none — §5's entire decision is that there is no migration to model.
*"MasterNameScope"* names the implementer's predicate, not the operator's problem; a human
reading it mid-incident learns nothing. `StaleMasterName` names the defect, in the words the
runbook and the changelog already use.

**Polarity: `True` is bad**, matching `Forsaken` and `LegacyClusterTopology`. A healthy CR
carries `False`/`Converged`, so the condition stays quiet until something is actually wrong —
the alternative (`True` in steady state, like `Ready`) would light up on every healthy instance
forever and push all four interesting states onto the `False` side. The condition is a *transient progress/diagnostic* surface, not a terminal
verdict.

| Reason | Status | Meaning |
|---|---|---|
| `Converged` | `False` | Every reachable Sentinel monitors exactly the desired name. The steady state; nothing to report. |
| `Pruning` | `True` | Stale names observed and being removed this pass. Message names the stale names, and any Sentinel **skipped** by G6. |
| `Deferred` | `True` | Stale names observed but a gate refuses. Message names **which** gate. |
| `Foreign` | `True` | §7.3 — the stale name points at someone else's master, **on a settled instance** (LR-050: mid-rollout the same reading is `Deferred`, because a pod we have just replaced looks identical). `Warning` event; do not rename to escape a capture. |

---

## 9. The pure seam (this is the spec sub-agents implement against)

```go
// internal/redis/client.go  — AMENDED (M1): the type lives in internal/redis, not the
// controller package. It is a wire-shaped value the client produces and the gather carries
// on SentinelNodeState.MonitoredMasters; the planner consumes redisclient.MonitoredMaster.
type MonitoredMaster struct {
    Name          string
    IP            string
    Flags         string // e.g. "master", "s_down,master", "failover_in_progress,master"
    FailoverState string // NOT "FailoverStatus" — see the G3 note below
}

// MonitoredMaster.FailoverInProgress() is the correct test — string equality is not.

// internal/controller/stale_master_name_plan.go

type staleEntry struct {
    SentinelIP  string
    SentinelPod string
    Names       []string
}

type StaleMasterNamePlan struct {
    Prune   []staleEntry // per Sentinel, the names to REMOVE (desired name never included)
    Skipped []string     // Sentinel pods carrying a stale name but not yet the desired one (G6)
    Reason  string       // Converged | Pruning | Deferred | Foreign
    Message string       // human-readable, goes in the condition message
}

func planStaleMasterNames(
    state *redisclient.ReplicationState,
    desired string,
    quorum int,
    forsaken bool, // the (now name-agnostic) planForsaken verdict for this pass — G0
) StaleMasterNamePlan
```

**Gates — every one must hold before any `Prune` entry is emitted:**

| # | Gate | Why (which incident) |
|---|---|---|
| G0 | **A capture is in evidence.** The name-agnostic verdict (WP4b) holds when the capture is under a *stale* name, and stands Rule N down entirely: no prune, reason `Foreign`. Fed `forsakenPlan.Captured`, not `Forsaken` — see §9.1 item 6. | §7.3. It closes the **alive**-captor case that G5 alone cannot see. It does **not** close the `s_down`-captor case (§7.3's correction, K2d). Ordering: computed before Rule N and passed in, never re-derived. |
| G1 | `desired != ""` | LR-041 defence in depth; an empty name is a plausible-looking lie, not an error. |
| G2 | A living, reachable master of **ours** to (re)register onto: `state.RealMasterIP != ""` **and** `state.ValidIPs[RealMasterIP]` **and** the `RedisNodes[RealMasterIP]` entry is `Reachable && Role == master`. | LR-008's gate, reused. Pruning without this manufactures the LR-015 leaderless deadlock. |
| G3 | No monitored master — **under any name**, including stale ones — reports an in-flight failover, and `!state.FailoverActive`. | A failover under the stale name is still a real state machine reconfiguring our pods. |
| G4 | The number of reachable Sentinels is `>= quorum`. | Do not operate on a minority. |
| G5 | Every stale entry's master address is either **one of our pods** (`state.ValidIPs`) or **flagged down** (`s_down`/`o_down`). Otherwise ⇒ `Foreign`, prune nothing. | §7.3. Same discriminator as `planForsaken` clause 3. **Necessary but not sufficient on its own** — it is blind to a captor whose master is transiently down, which is why G0 exists. |
| G6 | For each Sentinel, the desired name is present on **that** Sentinel. The planner uses the gathered view; the **caller re-confirms with a bounded `IsMonitoring`** immediately before each `REMOVE`. | LR-024's `electMaster` lesson: enforce the invariant, don't assume it. A Sentinel not yet carrying the desired name is skipped, not pruned — Rule 0 gets it next pass. |

**G6 must not fail silently (R3 says "no leftover entry, *ever*").** A skipped Sentinel is
listed in `Skipped` and **named in the condition message**, so "pruning, one Sentinel lagging by
a pass" is distinguishable from "one Sentinel permanently stuck and never converging". The
planner does not bound how long that may last, and deliberately so: a Sentinel that never
accepts the desired name is a **Rule 0** failure, not a Rule N failure, and it is a pre-existing
condition of this codebase — Rule 0 has no convergence bound either. The ADR states that
plainly rather than inventing a timeout here; the observability is the deliverable.

Deliberately **not** a gate: `!anyTerminating` (§7.5), and `Phase == Running` (the phase is
written at the tail of the pass and lags by one — LR-044's M4b finding; gate on the state, not
on the phase).

**G3's field values — SETTLED by M1, source-confirmed, and the placeholder was wrong.** Read
first-hand from `redis/redis` 8.0 `src/sentinel.c` and `valkey-io/valkey` 8.1 `src/sentinel.c`,
which agree exactly:

- The key is **`failover-state`**. There is **no `failover-status`** in either project.
- It is emitted **only** inside `if (ri->flags & SRI_FAILOVER_IN_PROGRESS)` (redis `:3435`,
  valkey `:3317`) — so in steady state the field is **absent**, not `"none"`.
- Values (`sentinelFailoverStateStr`, redis `:3366` / valkey `:3249`): `none`, `wait_start`,
  `select_slave`, `send_slaveof_noone`, `wait_promotion`, `reconf_slaves`, `update_config`,
  `unknown`.
- The `flags` field independently carries a `failover_in_progress` token from the same reply —
  a second free signal.

So this document's placeholder `{"", "none", "no-failover"}` was wrong twice over: `no-failover`
exists nowhere, and absence (not `"none"`) is the steady state. The planner must call
`MonitoredMaster.FailoverInProgress()` — idle iff the state is `""` or `"none"`, OR-ed with the
`flags` token — never compare strings itself.

**⚠ G3's second clause `!state.FailoverActive` is VACUOUS TODAY, and must not be relied on.**
The pre-existing read paths (`internal/redis/client.go:198`, `:224`,
`cmd/lrctl/cmd/gatherer.go:116`) all look up `failover-status` — a key Sentinel never emits — so
`MasterInfo.FailoverStatus` is permanently `""`, `SentinelNodeState.FailoverStatus` is
permanently `""`, and `ReplicationState.FailoverActive` is **permanently false in sentinel
mode**. This is LR-041's exact shape: a plausible-looking lie rather than an error. Consequences
reach past this feature — `littlered_controller.go:1017` is **Rule A**, whose failover half has
therefore never fired, and `replication_state.go:154`'s ghost-master guard is likewise
one-sided. **Out of scope here, tracked separately** (§16). For Rule N it means only this: the
**per-entry** `FailoverInProgress()` check is the load-bearing half of G3 and must be
implemented as if `!state.FailoverActive` were not there — which it effectively is not.

**Purity:** no I/O, no clock, no `Now()`. Everything comes from the four parameters.

### 9.1 Accepted deviations from §9's letter (M2, implemented and reviewed)

1. **G5 is evaluated BEFORE G2/G3/G4**, not in numeric position. The §7.3 trap fails G2 as well
   (no pod of ours is master ⇒ `RealMasterIP == ""`), so numeric order would report the generic
   *"Deferred: no living master of ours"* and **never** the `Foreign` warning — in precisely the
   case the warning exists for. Both outcomes prune nothing, so only the sentence the owner reads
   changes. §9 was ambiguous; this resolves it. **Accepted.**
2. **The planner does NOT gate on `sn.Monitoring`** — deliberately. At pass 1 every Sentinel
   reads `Monitoring:false, Reachable:true` (the single-name probe asks about the *new* name)
   while still carrying the old entry, as WP0 measured. Gating on `Monitoring` would make Rule N
   inert on exactly the pass it must act. **Accepted, and load-bearing — do not "tidy" it away.**
3. **Stale names present but no Sentinel carries the desired name yet** ⇒ `Deferred` naming G6,
   not `Pruning` with an empty plan (which would be a lie). **Accepted.**
4. **A stale entry with no address** is treated as foreign — it cannot be attributed to one of
   our pods, and refusing is the safe direction. **Accepted.**
5. **Determinism is part of the contract**: `Prune` sorted by pod, names sorted, findings
   deduped and sorted. The gather is a map, so without this an unchanged topology renders a
   different message every pass and an operator cannot tell a new event from a re-render.
   **Accepted, and it should have been in §9 from the start.**

6. **G0 is fed `forsakenPlan.Captured`, not `Forsaken`** (M3). §9 says "the instance is not
   `Forsaken`", but at Rule N's call site `Forsaken` is **structurally always false**: a settled
   Forsaken returns from the switch ~90 lines above (`littlered_controller.go:968`), as does a
   quarantined instance (`:954`, which is what keeps §7.4 true). Passing `Forsaken` would
   therefore have been a **dead gate** — the §15b failure mode, caught before it shipped rather
   than years later. `Captured` is the reachable and strictly more conservative reading: while a
   capture is merely *in evidence*, Rule N stands down and ADR-016 owns the instance.
   **Accepted, and better than what §9 specified.**
7. ~~**A fifth condition reason, `ForeignSuspected`** (M3), for §9.2's settle~~ — **reverted by M8/LR-050** together with `staleMasterNameForeignCooldown` and `status.staleMasterNameForeignSince`. The rollout gate closes §9.2's window at its source, so the settle had nothing left to settle. See §16.1.
8. ~~**The settle covers the G0 path as well as G5's**~~ (M3, reverted with item 7). It has to: `planForsaken`'s own
   `CaptureSuspected` window is reachable mid-rename (§7.1b), so an unsettled G0 would emit the
   same wrong Warning through a second door.
9. **The condition write is suppressed when nothing changed** (M3) — otherwise every healthy
   sentinel instance takes a status write every 2s.

### 9.2 ⚠ SUPERSEDED BY §16.1 — G5 fires a FALSE `Foreign` during a normal rename

> **This section is kept for its diagnosis, which is correct and load-bearing, and its REMEDY is
> obsolete.** M3 implemented the settling period below (`ForeignSuspected`,
> `staleMasterNameForeignCooldown`, `status.staleMasterNameForeignSince`); **LR-050 deleted all
> three** and closed the window at its source with the rollout attribution gate. Mid-rollout the
> reading is now `Deferred` naming that gate — no accusation, no `Warning`, and, unchanged, no
> prune. See §16.1 and §9.1 items 7/8. Read the rest of this section as the analysis that led there.


Found in review by reading M2's G5 against WP0's measurements. **The planner is correct as a
pure function; the defect is that §8 makes `Foreign` emit a `Warning`, and §9 gave the caller no
settling period.**

G5's discriminator is `!ValidIPs[ip] && !flaggedDown(flags)` — byte-identical to `planForsaken`
clause 3. WP0 measured that predicate going true **during an ordinary rename**: at t0+89.1s the
just-replaced `redis-0`'s address was no longer in `ValidIPs` and had not yet reached `s_down`
(30s of `down-after-milliseconds` still to run), which is exactly what produced the
`CaptureSuspected` sample in §7.1b. The stale name pointed at that same address in that window.

So on a healthy, legitimate rename the operator would emit a `Warning` reading *"this instance
may be captured — do not rename to escape a capture"* **at the very moment the owner is
performing the rename the runbook told them to perform.** It self-clears in ≤30s once the
address reaches `s_down`, and it prunes nothing meanwhile, so **R2/R3 are not at risk — this is
an observability defect, not a safety one.** But an alarming, wrong warning during the documented
happy path is not shippable.

**A just-replaced pod of ours and a captor's master are structurally indistinguishable from
Sentinel's view** — both are absent from `ValidIPs`, both unflagged. That is the same ambiguity
`planForsaken` faces, and it is already solved the same way: a **settling period**. The planner
is pure and has no clock, so this belongs in the caller.

**Required of WP4 (M3):** treat `Foreign` as a *suspicion* until it has persisted, mirroring
`forsakenSince`/`forsakenCooldown` — a `status` timestamp, and no `Warning` event and no scary
condition message until it has held past the cooldown. Below it, the condition may say
*"Deferred"* or a neutral suspicion reason; it must not accuse. Reuse `forsakenCooldown`'s 30s
unless there is a reason not to, and note that ≥30s is exactly the window WP0 measured, so a
shorter one would not close it.

---

## 10. Work packages

Dependency order: **WP0 gates everything** — it is an observation, not a build, and no wiring is
committed until it reports. Then **WP1 → WP2 → WP4 → WP4b**; **WP3** in parallel from the start;
**WP5-WP8** after WP4b. WP3, WP4b and WP6 are where the red-first discipline actually bites —
brief those sub-agents on CLAUDE.md's Test Discipline section explicitly, and require them to
paste the observed RED output into their report.

### WP0 — verify the rollout interlock and the §4 defect (do this first, build nothing)

The design's rejection of a persisted migration phase (§6.3) rests entirely on §3.4's claim that
readiness serializes the Redis roll behind the rename. That claim is **traced, not measured**.
Confirm it live before anyone writes code that depends on it.

- **Environment:** t3e or s1, a healthy 3-pod sentinel instance with `masterName: mymaster` and a
  few thousand distinguishable keys.
- **Do:** patch `spec.sentinel.masterName`, then observe — with 1s sampling, as LR-044's M4a did
  — the pod phases, the per-pod readiness transitions, and `SENTINEL masters` on all three
  Sentinels throughout.
- **Record, as measurements that replace §7.1's estimates:**
  1. **The interlock.** Does `redis-1` stay untouched until `redis-2` is Ready under the new
     name? Is `redis-0` genuinely last? Capture the timestamps.
  2. **The §4 defect, confirmed.** Both names present on all three Sentinels, and **still present
     after convergence** — this is LR-048's evidence and the e2e's red (§11).
  3. **`redis-0`'s stale-name preStop**, since today the old entry still exists: does
     `SENTINEL failover <old>` fire a **real failover under the stale name** (§4)? That is the
     sharpest available demonstration of why the feature is a bug fix.
  4. Wall-clock for the whole roll, per edge of §7.1's table.
- **Exit criteria — this is a decision point, not a formality:**
  - Interlock **holds** ⇒ proceed as designed.
  - Interlock **does not hold** (e.g. `redis-0` terminates while a stale entry still exists)
    ⇒ **stop and build §6.3 instead** (staged migration with a persisted phase). Do not attempt
    to patch Rule N around it.
- **Deliverable:** a short observation report with the raw samples, pasted into the ADR's
  Consequences and the changelog entry. No code.

### WP1 — client: read the monitored master list, bounded

- **Files:** `internal/redis/client.go`; test `internal/redis/client_masters_test.go`.
- **Add:** `func (c *SentinelClient) GetMonitoredMasters(ctx context.Context, sentinelAddr string) ([]MonitoredMaster, error)`
  — single address (not the loop-over-`c.addresses` shape; the callers want per-pod answers),
  built on `newBoundedClient(addr)` (`client.go:93`) **plus** a per-call
  `context.WithTimeout(ctx, ProbeTimeout)`. Underlying call: go-redis
  `SentinelClient.Masters(ctx) *SliceCmd` (v9.22.0, `sentinel.go:853`) — a slice of field
  maps; parse `name`, `ip`, `flags`, `failover-status` defensively (unknown/extra fields
  ignored, a malformed entry skipped, never a panic).
- **Red-first obligations:** (a) a parse table over real `SENTINEL masters` output including
  two masters, missing fields, and an empty reply; (b) **a bound test** reusing LR-040's
  `blackholeListener` (accept, never reply) asserting the call returns within
  `ProbeTimeout + 1s` — it must be observed RED at ~5.0s against an unbounded client first,
  exactly as LR-040/LR-046 did, because a budget that does not discriminate 3s from
  `DefaultTimeout` proves nothing.
- **Done when:** both tests green, `make lint` clean, and no existing call site changed.

### WP2 — gather: carry the list into ground truth

- **Files:** `internal/redis/replication_state.go` (add
  `SentinelNodeState.MonitoredMasters []MonitoredMaster`), `internal/controller/gatherer.go:58`,
  `cmd/lrctl/cmd/gatherer.go:79`, plus the `Gatherer` interface and any fakes.
- **Cost decision (record it in the ADR):** one extra bounded round-trip per Sentinel per pass
  (3 per 2s pass at steady state). Pay it **unconditionally** rather than only when a Sentinel
  reads bare — a Sentinel carrying **both** names reads `Monitoring:true`, so a lazy probe
  would never see the state a previous botched rename left behind (§4), which is the state
  most instances in the field will actually be in. A failed `Masters` call must degrade to an
  empty list, never to `Reachable:false`.
- **cliGatherer parity:** `redis-cli -p 26379 sentinel masters` via the existing exec path, so
  `lrctl` and the operator cannot disagree (LR-041's parity rule).
- **Red-first:** an envtest/unit assertion that a Sentinel monitoring two names is gathered
  with both — RED before the field exists (undefined symbol counts, per LR-044's precedent).

### WP3 — the pure planner (start immediately, parallel to WP1/WP2)

- **Files:** `internal/controller/stale_master_name_plan.go`,
  `internal/controller/stale_master_name_plan_test.go`.
- **Implement §9 exactly.**
- **Red-first table, minimum rows:** converged (no prune); one stale name, healthy ⇒ prune;
  **two** stale names on one Sentinel; stale on one of three Sentinels only; `RealMasterIP == ""`
  ⇒ Deferred; master IP not in `ValidIPs` ⇒ Deferred; stale entry reports `failover-status:
  in-progress` ⇒ Deferred; `state.FailoverActive` ⇒ Deferred; below quorum ⇒ Deferred; stale
  entry pointing at a **foreign live** master ⇒ `Foreign`, empty `Prune` (the §7.3 row — build
  its fixture from the LR-044 M4a capture shape); stale entry pointing at a **down** address
  (ordinary post-failover debris) ⇒ prune allowed; a Sentinel that does **not** yet carry the
  desired name ⇒ that Sentinel skipped, **named in `Skipped`**, while the others prune;
  `desired == ""` ⇒ Deferred; **`forsaken == true` ⇒ `Foreign`, empty `Prune`, whatever every
  other input says** (the G0 row — assert it beats an otherwise-perfect prune case, which is the
  `s_down`-captor hole).
- **Mutation check, mandatory** (LR-043/LR-044 precedent): a *prune-everything* mutant must
  fail every Deferred/Foreign row, and a *prune-nothing* mutant must fail every prune row.
  Report both.
- **Done when:** table green, mutants fail as stated, planner has zero I/O and no clock.

### WP4 — wiring: Rule N in `reconcileSentinelCluster`

- **Files:** `internal/controller/littlered_controller.go` (insert between Rule 0 at `:1013`
  and Rule A at `:1015`), `api/v1alpha1/littlered_types.go` (the new condition constant),
  `docs/API_SPEC.md`.
- **Per Sentinel in `plan.Prune`:** bounded `IsMonitoring(addr, desired)` (G6) → if false,
  log-and-skip → else `Remove(ctx, staleName)` per stale name, each logged on the **audit**
  logger with pod, address, name, and the desired name.
- **Condition + events:** set `StaleMasterName` per §8; **one** event per transition, never per
  reconcile (LR-042's log-once discipline); `Foreign` is a `Warning`. The `Pruning` message names
  the stale names **and** any `Skipped` Sentinel. Mirror the condition into the in-memory status after a successful update — the
  LR-044 "found in the way of observing this" bug, do not reintroduce it.
- **No cadence change.** A rename converges in one pass; do not touch
  `requeueAfterNotRunning` (LR-045).
- **Done when:** envtest covers "CR renamed ⇒ stale name removed, desired name kept", the unit
  suite and `make lint` are clean, and a **grep proves no other `Remove` call site changed**.

### WP4b — make `planForsaken` name-agnostic (in scope, §7.3)

- **Files:** `internal/controller/forsaken_plan.go` (or wherever `planForsaken` lives — locate
  it, do not assume), its test file, and the Rule N call site for the G0 wiring.
- **Change:** feed `planForsaken` the gathered `MonitoredMasters` so its clauses evaluate over
  **every** name a Sentinel monitors, not only the desired one. A capture under a stale name is
  a capture. Then pass the verdict into `planStaleMasterNames` as G0 — computed once per pass,
  never re-derived inside Rule N.
- **Additive only, and prove it.** Every clause that fires today on the desired name must fire
  identically afterwards. **The entire existing `planForsaken` table must pass unchanged, with
  no row edited** — if a row has to be adjusted, stop and report it, because that is a behaviour
  change to the LR-042 verdict and it needs an owner decision, not a patch. New rows only for the
  stale-name case.
- **Red-first:** the load-bearing new row is *capture under a stale name while the desired name
  reads bare* ⇒ `Forsaken` — RED today (the verdict currently evaporates, which is the §7.3
  trap). Add the `s_down`-captor row too: captor's master flagged down, stale entry otherwise
  prunable ⇒ still `Forsaken`, so G0 stands Rule N down where G5 would have let it through.
- **Mutation check:** a mutant that ignores the stale names must fail both new rows and pass
  every old one — that is the precise statement of "additive".
- **Done when:** old table green untouched, new rows green, mutant fails as stated, and the
  quarantine e2e tiers (`sentinel_quarantine_test.go`) still pass unchanged.

### WP5 — `lrctl`

- **Files:** `internal/redis/cross_instance.go` (or a sibling), `cmd/lrctl/cmd/*`, `docs/LRCTL.md`.
- `verify` and `inspect` print, per Sentinel, **every** monitored name with its master address
  and flags; `verify` **FAILs** on a name other than the CR's, and distinguishes
  "stale (ours)" from "foreign (someone else's master)". This is the tool the runbook's
  verification step uses, and the thing that would have made §4 visible from day one.
- **WP0 proved the gap is total, not partial.** On the post-rename instance — carrying two
  `sentinel monitor` lines and five stale known-replicas — `lrctl verify` reported it
  **entirely healthy** and never mentioned the stale entry:

  ```
  Sentinel Status:
    - Sentinel rn-sentinel-1: monitoring 10.233.192.95
    - Sentinel rn-sentinel-2: monitoring 10.233.192.95
    - Sentinel rn-sentinel-0: monitoring 10.233.192.95
  Sentinel Identity:
    Master name: wp0-rename-mn.rn
    [OK] No foreign Sentinel contact observed (3 sentinels, 2 replicas expected).
  [OK] Cluster configuration is consistent.
  ```

  It queries only the desired name, so the second monitor line is invisible to the project's own
  ground-truth tool. **Consequence for §12: runbook step 5 is not implementable until WP5
  lands** — there is no way today to ask "does every Sentinel monitor *only* this name". Say so
  in the runbook rather than shipping a step that cannot be followed.
- `lrctl debug-dump` records the same.

### WP6 — e2e (the honest red is available here — do not squander it)

- **File:** extend `test/e2e/sentinel_master_name_test.go` (LR-039's home) with
  `Describe("Sentinel Master Name Migration", Label("sentinel"))`.
- **Tier 1 — full rename, data preserved (the regression guard).** Deploy with
  `masterName: mymaster`; write N distinguishable keys; assert replicated (`DBSIZE` on both
  replicas, per LR-016's precondition lesson); patch `masterName` to `<ns>.<name>`; then
  assert, in order: every Sentinel monitors the new name; **no Sentinel monitors `mymaster`**
  (`SENTINEL masters` output length is 1); all N keys readable through the `{name}` Service and
  present byte-for-byte on the master; `Phase: Running`, `Ready=True`;
  `SentinelMasterNameUnscoped` cleared; and finally **a failover still works under the new
  name** (delete the master pod, assert a new one is elected and the keys survive) — that last
  step is what proves we left behind one healthy state machine rather than a broken one.
  **This tier goes RED against current code** on the "no Sentinel monitors `mymaster`"
  assertion (§4). Capture that RED output verbatim; it is the entry's evidence and it is the
  thing LR-044's tiers could not obtain.
- **Tier 2 — the refusal.** Stage a §7.3 shape (reuse the LR-044 / LR-039 capture staging
  helpers in `sentinel_quarantine_test.go` and `sentinel_master_name_test.go`, including the
  `PUBLISH`-reply-`1` positive control and the "assert the precondition over all three
  Sentinels before injecting" finding), rename, and assert: **`Forsaken` still holds** (WP4b —
  the verdict survives the rename, which is the actual repair), `StaleMasterName=True/Foreign`,
  **no** `REMOVE` issued, the foreign entry still there, `Consistently` for ~60s. Then let it
  run: the quarantine should still fire and still heal both sides, i.e. **a panicked rename no
  longer defeats ADR-016**. That last assertion is the one worth the e2e budget.
- **Tier 3 (optional, cheap) — idempotence.** Rename twice in quick succession; assert exactly
  one name at the end and no thrash.
- **Explicitly not covered, say so in the entry:** renaming a degraded instance (§7.2) and
  anything under concurrent disruption (N4).

### WP7 — documentation (one sub-agent, last, with everything else merged)

- `docs/adr/017-sentinel-master-name-rename.md` — Context (§4 is the defect), Decision (§5),
  Alternatives (§6, all five, with the reasons **kept**), Consequences (the ~4-6 min window,
  the stale-name preStop no-op, the deferred name-agnostic-pods idea, and the **naming decision
  of §8 with its reasoning**). Carry WP0's measurements, not this document's estimates.
- `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` — **LR-048**, in the house format
  (Problem / Root cause / Fix / Gates / Tests with the observed reds / Regresses / Impacts),
  including the §7.3 trap, the name-agnostic `planForsaken` change as part of the same entry
  (it amends LR-042's verdict — say so explicitly there), and the parity note that cluster and
  failover modes have no equivalent (their cross-instance analogue is LR-043 and needs no
  rename).
- `docs/USAGE.md` — replace the current "there is no rolling cutover" paragraph
  (`:375-390`, `:466`) with the runbook in §12; keep the sentence about clients, delete the
  implication that an edit is unsupported.
- `docs/API_SPEC.md` — the `StaleMasterName` condition and its four reasons; state plainly that
  the field is mutable and what an edit triggers.
- `CLAUDE.md` — pillar 3.7 gains the migration (one or two sentences, no more); §4's sentinel
  bullet gains "renaming is supported in place, see USAGE"; §9 gets a short entry.
- `docs/RECONCILIATION_LOOP_SENTINEL.md` — Rule N in the rule inventory, with its position
  relative to Rule 0 and Rule A.

### WP8 — release hygiene

`make manifests generate` (new condition constant only — no CRD schema change is expected;
if a CEL rule is added instead per §6.1, that changes), `make lint`, `make test`, and
`make licenses` only if dependencies moved (they should not).

---

## 11. Verification plan

| Tier | What | Where the red comes from |
|---|---|---|
| Observation | **WP0: the §3.4 interlock and the §4 defect, live** | n/a — it *is* the red; and a decision gate for §6.3 |
| Unit (pure) | `planStaleMasterNames` table + both mutants | authored against a stub, RED on every row (WP3) |
| Unit (pure) | `planForsaken` name-agnostic rows + old table untouched | RED: the verdict evaporates under a stale name today (WP4b) |
| Unit (client) | parse table; **bound** test at `ProbeTimeout + 1s` | RED at ~5.0s unbounded (WP1) |
| Unit (gather) | two-name Sentinel is gathered with both names | RED as undefined symbol / missing field (WP2) |
| envtest | renamed CR ⇒ stale removed, desired kept, condition set | RED before WP4 |
| e2e | §WP6 tiers 1-3, t3e | **Tier 1's pre-fix RED is BANKED** (against operator `9c2dd35`): `monitors [mymaster lr048-red...], want exactly [...]`, with Sentinel's two `monitor` lines as the artefact. HEAD-green `3 Passed \| 0 Failed` in 967s; prune lands **1.4s** after the patch. **Now red again on the K9 guard — §16.** |
| Manual | `lrctl verify` before/after | — |

**Environment:** t3e or s1 (both have carried the sentinel tiers). Record the timings actually
measured for each edge of §7.1's table — the ADR should carry measurements, not the estimates
in this document, and the estimates should be replaced rather than left standing next to the
real numbers (LR-044's M4a precedent).

---

## 12. Runbook (SHIPPED — the authoritative copy is now `docs/USAGE.md`)

> **Superseded in two places by what was measured.** (a) Step 6's *"~4-6 min"* is wrong: §7.1a
> measured the settled `Running`/`Ready=True` at **+176.8s (2m57s)**, plus **~30s** at the `redis-0`
> edge post-fix. (b) Step 5's *"requires WP5"* caveat is discharged — `lrctl verify` reports every
> monitored name and fails on a stale one as of `f6cae73`; see `docs/LRCTL.md`. The shipped runbook
> in `docs/USAGE.md` (*"Renaming the Sentinel master name in place"*) carries the corrected numbers
> and is the copy to maintain. This one is left as the draft it was.


**Preconditions.** `Phase: Running`, `Ready=True`, all pods ready; no failover in flight; the
instance is **not** `Forsaken` (if it is, let the quarantine complete first — it returns the
instance empty and healthy, after which the rename is trivial); a maintenance window with
**clients stopped**; the platform stable (no drains, no node maintenance).

1. Note the current name: `kubectl get littlered <n> -o jsonpath='{.spec.sentinel.masterName}'`.
2. Confirm health: `lrctl status <n>` and `lrctl verify <n>` — the latter must report exactly
   one monitored name and no foreign contact.
3. **Stop the Sentinel-aware clients.** (Clients that use the `{name}` Service are unaffected
   by the name itself, but the window includes a master failover, so they will see a gap.)
4. Patch the field:
   `kubectl patch littlered <n> --type=merge -p '{"spec":{"sentinel":{"masterName":"<ns>.<n>"}}}'`
5. Within seconds: `lrctl verify <n>` must show every Sentinel monitoring **only** the new
   name. (This check requires WP5 — before it, `verify` queries only the desired name and
   reports a two-name instance as entirely healthy.) If it shows the old one still present, read the
   `StaleMasterName` condition — its message names the gate that refused, and any Sentinel it
   skipped.
6. Wait for the Redis rollout (~4-6 min; it includes one master failover with no proactive
   handover — expected, see below). Watch `kubectl get pods -w` and
   `kubectl get littlered <n> -w` to `Running`/`Ready=True`.
7. Verify the data: your own key check, plus `lrctl verify <n>`.
8. Reconfigure the clients with the new master name and start them.

**What you will see, and it is expected:** the master's preStop hook still carries the *old*
name while it is being replaced, so it cannot hand over proactively; the master's replacement
therefore waits out `down-after-milliseconds` (30s by default) before Sentinel elects a
successor. With writes quiesced this costs availability only.

**Escape hatch** if a stale entry survives (e.g. the operator was down for the whole window):
`kubectl rollout restart statefulset/<n>-sentinel` — Sentinel state is EmptyDir, so the pods
come back with nothing and the operator registers only the desired name. Expect a short window
with no monitoring.

**Do not:** rename a degraded instance; rename to escape an active capture; rename and change
the password in the same window.

---

## 13. Opportunistic notes on the auth question (deferred to its own session)

Recorded so the next session starts ahead, not to be resolved here.

- **Where the password lives now:** `spec.auth.enabled` + `existingSecret` inject
  `REDIS_PASSWORD` (secretKeyRef) into **both** the Redis and Sentinel containers, consumed as
  `--requirepass` / `--masterauth` (`resources.go:472-486`) and
  `--requirepass … --sentinel sentinel-pass …` (`buildSentinelContainer`, `resources.go:1550-1558`).
  It is read **only at process start**; there is no live-reconfiguration path today. The
  operator additionally sets `auth-pass` per master at registration
  (`littlered_controller.go:1007`).
- **So enabling or rotating auth rolls both StatefulSets**, with a mixed-credential window in
  which: a not-yet-rolled replica cannot authenticate to a rolled master (`masterauth`
  mismatch); `sentinel-pass` mismatch breaks Sentinel↔Sentinel links, so the **quorum splits**
  for the duration; and the operator's own probes carry the new password immediately, so an
  un-rolled pod answers `ERR Client sent AUTH, but no password is set` and reads as
  **unreachable** in the gather. None of this is handled or covered by a test today.
- **The multi-password idea is real, but only for the client edge.** Redis ≥6 lets the *default
  user* hold several valid passwords (`ACL SETUSER default >old >new`), which genuinely allows
  an overlapping client cutover: add the new password, migrate clients, drop the old. But
  `masterauth`, `sentinel-pass` and Sentinel's `auth-pass` are each **single-valued**, so the
  internal links get no overlap from it — they need one coordinated flip, which the operator
  can do because it owns all three. That split — *overlap on the client edge, atomic switch on
  the internal edges* — looks like the right shape for a design.
- **Two pitfalls to carry into that session.** (1) `CONFIG SET requirepass` collapses the
  default user's password list back to a single value, and every pod restart re-applies
  `--requirepass` from its args — so a multi-password window is **fragile under any restart**
  unless the operator re-establishes it, which argues for driving it from the ConfigMap/args
  (i.e. a rollout) rather than from a live `ACL SETUSER`. (2) Both edges of the window need the
  operator to be able to talk to pods on **either** credential; it currently resolves exactly
  one password per pass (`getRedisPassword`), so "try both" is a real change to the gather.
- **Relationship to this document:** independent. Do them in separate windows (N7). The one
  interaction worth remembering is that enabling auth *does* change the Sentinel pod template,
  so that rollout wipes Sentinel's EmptyDir state — which is why the current `USAGE.md` advice
  to do the rename and the auth change together happens to work. Once Rule N exists, that
  coincidence is no longer load-bearing, and the two operations should be **separated** rather
  than combined.

---

## 14. Risk register

| # | Risk | Severity | Mitigation / decision |
|---|---|---|---|
| K1 | Renaming a degraded instance wedges on Rule L's ≥2-holder REFUSE (§7.2). | High if it happens | Runbook precondition; `Deferred` condition naming the gate; Rule L is still the safety net and lands on the new name. **Not** prevented in code (no webhooks) — accepted, documented. |
| K2 | Renaming a captured instance hides the capture (§7.3). | High | Closed in this change, at two levels: name-agnostic `planForsaken` (WP4b) keeps the verdict alive so the quarantine still heals both sides, and G0 stands Rule N down on it. G5's per-entry test is the second line, not the first — on its own it is blind to a captor whose master is transiently `s_down`. `Foreign` condition + `Warning` + runbook. |
| K2b | WP4b widens what `planForsaken` reads, and LR-038 warns that widening ground truth changes every rule at once. | Medium | The field feeds exactly two pure planners; the change is additive and pinned by the requirement that the **entire existing `planForsaken` table pass unchanged**, plus a mutant that must fail only the new rows. A row that has to be edited stops the WP. |
| K2d | **Residual, accepted:** a captor whose master is `s_down` at the instant of the rename is caught by neither G5 nor G0, so the prune fires and the capture evidence is lost. | Low (narrow window; needs the captor to be mid-failover at that instant) | Not closed. Closing it means a G5 settle (a not-ours address must have been flagged down for a settle before counting as prunable), which delays legitimate pruning of ordinary post-failover debris — WP0 measured that debris as the common case. The runbook's remedy order (**capture → let the quarantine finish → then rename**, §7.4) already avoids it. |
| K2c | §3.4's interlock is assumed, and §6.3 was rejected on it. | Medium | WP0 verifies it live **before** anything is built, with an explicit exit criterion that reopens §6.3 if it does not hold. |
| K3 | Rule N runs during churn (before Rule A), so an unbounded call could stall a reconcile. | Medium | Every call bounded on ctx **and** client timeouts (LR-040/LR-046); WP1's bound test is mandatory and must discriminate 3s from 5s. |
| K4 | The prune is a destructive primitive aimed by a predicate. A wrong `REMOVE` of the desired name would wipe a live entry. | Medium | The planner never emits the desired name (unit-pinned by the prune-everything mutant); the caller re-confirms `IsMonitoring(desired)` per Sentinel before each `REMOVE`. |
| K5 | The extra `SENTINEL masters` probe per Sentinel per pass adds steady-state traffic. | Low | 3 bounded single round-trips per 2s pass; the gather already makes several per Sentinel. Decision recorded in the ADR with its reason (a lazy probe cannot see the both-names state). |
| K6 | The master's stale-name preStop is a no-op, so the handover is not proactive. | Low (with N1) | Documented as expected. Closed for good only by §6.4 (name-agnostic pods) — deferred. |
| K7 | Renaming to a name another instance on the pod network already uses causes a *new* capture. | Low, user error | `lrctl verify`'s cross-instance diagnostic; the runbook recommends `<namespace>.<name>`. Validation cannot see the network. |
| K8 | An owner does rename + auth in one window and cannot tell which change broke what. | Low | N7 + §13. |
| K9 | ✅ **CLOSED by M8/LR-050 — see §16.1.** Was: ⛔ **REALISED — see §16. No longer a risk; a measured defect that blocks the feature.** The rename transiently presents the capture signature (§7.1b, measured: `CaptureSuspected` at t0+89s, all four `planForsaken` clauses held). Only the 30s `forsakenCooldown` prevented it latching, and WP4b widens what the verdict reads. | **High if WP4b regresses it** | Mandatory WP4b regression row built from WP0's measured shape; the cooldown stays as the backstop. A spurious quarantine of a healthy instance mid-rename would be strictly worse than the bug being fixed. |
| K9b | G5's `Foreign` verdict fires spuriously during a normal rename (§9.2), emitting a "you may be captured" `Warning` on the documented happy path. | ✅ **CLOSED by M8/LR-050**, with K9 and by the same gate | Was: a caller-side settling period (M3). That settle is **deleted**; mid-rollout the reading is now `Deferred` naming the gate — no accusation, no event, and (unchanged) no prune. |
| K10 | Rule D's ghost-replica `SENTINEL RESET` fires against the **desired** name during the roll (observed twice in WP0, triggered by departed pod IPs going `s_down`). | Low here, known class | LR-024's self-inflicted-deadlock ingredient, live on this path; it did not deadlock in WP0. Not made worse by this change — Rule N never issues RESET (§7.6). Prevention remains ADR-010's deferred subject. |

---

## 16. ⛔ BLOCKING: K9 realised — an ordinary rename settles a false capture verdict

Found by WP6's e2e on t3e (2026-08-26), by the `expectNeverForsaken` guard the design made
**mandatory** and which no tier carried until M5b added it. **Both earlier e2e runs went green
*over* this state**, including one in which the quarantine deleted the instance's pods — because
nothing was asserting on it.

**The defect.** A supported rename of a healthy sentinel instance, with **no other Sentinel
deployment anywhere on the cluster**, drives a settled `Forsaken=True` and quarantines it:

```
18:29:27  Pointing Sentinel at master   master=10.233.192.21          <- its OWN master
18:29:43  Removed stale Sentinel master name  stale=mymaster desired=...a
18:29:53  Removed stale Sentinel master name  stale=...a     desired=...b
18:31:55  Sentinel reported a ghost master. Ensuring no pod is labeled as master.
18:31:57  Instance is forsaken and quarantined: captured by another Sentinel
          deployment sharing its master name. Halting management.
          foreign_master=10.233.192.21  quarantine=Quarantined  attempt=1
```

One address, called **its own ghost master** and, two seconds later, **a foreign captor**.

**Severity: data destruction on a supported operation, gated by luck.** The quarantine scales
both StatefulSets to 0 and storage is EmptyDir. On the data-free instance it fired and the pods
were deleted. On the instance holding 500 keys the verdict *still settled*, and only LR-044's
`atRisk` data clause vetoed the deletion. **Whether the dataset dies depends on whether the pods
happen to hold data at that instant** — the veto is not a guard against this, it is a
coincidence. `Forsaken=True` also halts healing, so a renamed instance is briefly unmanaged.

**Root cause, structurally:** `planForsaken(state, since, now)` has **no churn awareness at
all** — verified in the signature and the call site. It cannot distinguish "our own pod was just
replaced by a rolling update" from "a foreign master is alive at that address". Mid-roll, a
just-replaced pod's IP has left `ValidIPs` and has not yet reached `s_down`, so all four clauses
hold and only `forsakenCooldown` stands in the way. §9.2 already said *"what saved it was the 30s
cooldown, not the clauses"*; that margin is now measured as insufficient.

**Exonerated: WP4b.** By the time the verdict fires, Rule N has pruned the stale names and the
quorum monitors only the **desired** name — a name-scoped `planForsaken` would fire identically.
This is what K2b asked about, and the evidence answers it.

**Why the cause is under investigation rather than assumed (M7).** The leading hypothesis is
that the feature *consumes exactly the margin protecting it*: pre-fix, the master's baked-old-name
preStop `SENTINEL failover <old>` **succeeded** (the stale entry existed), and WP0 measured the
forced `+switch-master` one second after the pod was deleted — an instant handover, so the ghost
window stayed under 30s. Post-fix, Rule N has removed that entry, the call errors, and the quorum
waits out `down-after-milliseconds` — **30s by default, against a 30s cooldown.** §7.1a recorded
that +30s as a *latency* cost and never connected it to the verdict. If confirmed, the
intermittency is not luck about *whether* but about *which* rename lands on the wrong side.
Candidate amplifier: Rule D's `SENTINEL RESET`, observed firing twice mid-roll in both WP0's and
M5b's runs (K10 / LR-024's ingredient).

### 16.1 ✅ RESOLVED (M8, LR-050) — the operator does not attribute addresses while its own StatefulSet rolls

**The measurement (M7)** settled the cause and eliminated three of the four fix options in one
stroke. The signature window is **42.5s** — `preStop stall ~21s + downAfterMilliseconds 30s +
election ~1.5s` — against a 30s `forsakenCooldown`, so the verdict fires at **T+30**, **12.5s
before the instance heals itself**. Run C's dose-response control (`downAfterMilliseconds: 5000`)
produced **no verdict**. That makes it a *timer* defect, not a clause defect: `downAfterMilliseconds`
is user-settable and unbounded, so **no value of `forsakenCooldown` can be correct for every
instance**, and options (b) lengthen-the-cooldown and any `anyTerminating`-style margin are ruled
out on principle rather than on taste. Remembering departed pod IPs would work but needs cross-pass
state.

**The decision (owner):** *while the operator's own Redis StatefulSet is mid-rollout, the operator
does not attribute addresses.* Not to a captor (`planForsaken`), and not as foreign (Rule N's G5).
A rollout of our own making is precisely the window in which "is this address one of ours?" cannot
be answered from the gather, so the honest answer is **hold**, not accuse. The predicate is
LR-021's `clusterShardRolloutSettled`, renamed `statefulSetRolloutSettled` and passed into both
pure planners in-signature (LR-041). Config-independent, no new state, and it covers strictly more
than a rename — a deleted, crash-looping or not-yet-Ready pod fails the same replica clauses.

**The subtlety that decides the implementation:** the gate suppresses **arming**. It must not make
`planForsaken` return "not captured", because the call site's `default` branch calls
`clearForsaken` — so a naive "no verdict while rolling" would *retract* a capture diagnosed before
the rename, reopening §7.3 and turning tier 2 red. **A rollout cannot START a capture verdict, and
it never CLEARS one either — only the ordinary clauses do, on the ordinary evidence.**

The *stronger* reading (hold an armed verdict up against an **absent** signature while rolling) is
wrong, and only the cluster said so. The quarantine **release** scales the instance `0 → 3`, which
reads as unsettled, and the pods return with bare Sentinels and no signature at all — so the
verdict was carried over a state with zero evidence, the call site returned before `clearForsaken`,
and the victim never left quarantine (measured: `phase: Initializing` for the full 480s of the
tier-2 assertion; LR-044 is explicit that the lifecycle rests on the verdict self-clearing once the
pods are gone). The gate is therefore a **one-way suppression of arming**, and that costs §7.3
nothing: a captured instance keeps presenting the signature under the **stale** name (WP4b).

**Net removal of surface.** §9.2's settle is gone with the defect it softened: the
`ForeignSuspected` reason, `staleMasterNameForeignCooldown`, and `status.
staleMasterNameForeignSince` are **deleted** (owner: no status-field inflation for a
once-in-an-instance-lifetime operation). Mid-roll, G5's reading is `Deferred` naming the gate —
no accusation, no `Warning`, and, unchanged, no prune. K9b is closed with K9.

**Accepted hole:** a *stuck* rollout means the gate never lifts, so a genuine capture there goes
undetected. Accepted by the owner — *"we don't fix on operator level if something's broken below"*;
such an instance is already `Ready=False` and visibly broken, and the quarantine exists to heal the
**captor**, which it cannot do for an instance that cannot roll. LR-023 is the precedent if it is
ever closed: its own rule, not a timer.

**Verified live (t3e, 2026-08-27, operator `3014676`).** Both M7 recipes, e2e tier-1 fixture shape,
product-default `downAfterMilliseconds: 30000`. The capture signature reproduced at full strength —
`planForsaken SIGNATURE WINDOWS: from 92.4s to 134.4s duration=42.0s` — and the operator logged
`Sentinel reported a ghost master` in it, exactly as in the §16 excerpt above. **No `Forsaken`
condition at any 0.5s sample, no `forsakenSince`, no `quarantinedSince`, no pod deleted;
`StaleMasterName` never reached `Foreign`.** With data: **500 of 500 keys present** on the new
master, exactly one monitored name on all three Sentinels. See changelog LR-050.

**The three M5b tiers are green on t3e** (`Sentinel Master Name Rename`, operator `6f20511`):
`SUCCESS! -- 3 Passed | 0 Failed`, 871s — including tier 2, whose whole point is that the capture
verdict **survives** a panicked rename and the quarantine still fires. `expectNeverForsaken` was
not weakened; it is now green with the assertion doing work. One earlier run's tier 1 failed on its
post-rollout `Consistently(30s, phase == Running)` reading `Initializing` 5.2s in — the §7.1b CR
flap the tier's own comment anticipates, racing its `Eventually`/`Consistently` boundary; not the
K9 assertion, and unrelated to the gate (tier 1 never arms a verdict).

**Status:** the §16 blocker is cleared; the K9 e2e guard is the regression guard.

---

## 15b. Discovered en route — the dead `failover-status` key (NOT this feature)

Found by M1 while source-confirming G3; recorded here so it is not lost, and deliberately **not
fixed in this change**.

**Defect.** Three pre-existing call sites read `failover-status`, which neither Redis nor Valkey
Sentinel emits (the key is `failover-state`). Every consumer of the resulting
`ReplicationState.FailoverActive` is therefore dead code in sentinel mode:

| Consumer | Effect of the permanent `false` |
|---|---|
| `littlered_controller.go:1017` — **Rule A** | The `\|\| state.FailoverActive` half has never fired. Rule A guards on `anyTerminating` alone, so a real Sentinel failover with no terminating pod does **not** suspend healing. |
| `replication_state.go:154` — ghost-master guard | `!s.FailoverActive` is always true, making the branch strictly more permissive than intended. |
| `lrctl verify.go:309`, `json_output.go:313` | `Healthy` is never reduced by an in-flight failover; `verify` never reports one. |

**Why it is not fixed here.** Turning a dead guard live *changes reconciliation behaviour* —
exactly the "⚠ re-enables previously-dead behaviour" hazard LR-041 attached to its own fix. It
needs an owner decision, its own LR entry, and e2e watch (Rule A suddenly suspending healing
during failovers is a behaviour change with real blast radius, and it interacts with LR-024's
ghost-master path). It is **not** a rider on ADR-017.

**Trigger:** schedule it as its own change before anyone writes a new rule that gates on
`FailoverActive`. Rule N does not (see G3).

---

## 15. Definition of done

1. Renaming a healthy instance leaves **exactly one** monitored name on **every** Sentinel,
   with the dataset intact, and a subsequent failover works — proven by WP6 tier 1 on a real
   cluster, with the pre-fix RED recorded.
2. An instance already in the §4 two-name state is repaired by the operator with no human
   action.
3. Every refusal path names its gate in the condition message, and each fires at most one
   event per transition.
4. LR-048 and ADR-017 are written, `USAGE.md` carries the runbook (replacing the current
   "no rolling cutover" framing), `API_SPEC.md` and `CLAUDE.md` are updated, and
   `make lint && make test` are clean.
5. A rename of a **captured** instance no longer defeats the quarantine: `planForsaken` is
   name-agnostic, the verdict survives, ADR-016's recovery still runs — proven by WP6 tier 2,
   with the pre-fix RED recorded.
6. WP0's measurements are in the ADR, and §7.1's estimates have been **replaced** by them rather
   than left standing beside them (LR-044's M4a precedent).
7. The deferred items are recorded **as decisions with triggers**, not as loose ends:
   name-agnostic pods (§6.4), immutability as the fallback (§6.1), auth (§13).
