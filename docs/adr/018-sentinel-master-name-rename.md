# ADR-018: In-Place Sentinel Master-Name Rename (Sentinel Mode)

## Status

Accepted and implemented (branch `feat/sentinel-master-name-rename`). Changelog entry **LR-048**
(the defect and the feature — they are the same change), with **LR-050** as a **prerequisite**
rather than a follow-up (the rollout attribution gate, without which this feature quarantines
healthy instances) and **LR-049** as a bounded-primitives fix found in its review.

Continues **ADR-015** (the per-instance master name this operation changes) and **ADR-016** (the
quarantine an owner must not try to escape by renaming). Supersedes nothing; it implements the
cutover ADR-015 said had to be a maintenance window, and closes ADR-015's Consequences claim that
*"no rolling cutover exists"* — that remains true of the *client* edge, and was silently false of
the operator's own behaviour, which registered the new name and never removed the old one.

> ADR number: **018**, not 017. The design brief reserved 017 when `docs/adr/` ended at 016; 017 was
> taken in the meantime by *State-Gated Intra-Shard Rolling Updates* (LR-047) and is already merged
> into `e2e-0821`. 010 remains unallocated (the deferred ghost-replica prune policy) and 012 lives
> on the multi-site branch. Checked across every branch, per LR-039's rule that an ID is claimed
> globally and not on the line you happen to be working next to.

**This operation is a rename, never a "migration."** In this project a migration moves *data*
(ADR-013's legacy→per-shard phases), and this moves none. The word is avoided throughout, including
in the condition name.

## Context

`spec.sentinel.masterName` has been editable since ADR-015 made it required, and every runbook in
the LR-039 → LR-042 → LR-044 chain tells a captured or legacy-named owner to *"give it a unique
`masterName`."* The operator had no notion of a rename at all.

**What actually happened when you performed the operation the product's own documentation asks
for** (traced, then measured live on t3e, 2026-08-26, on a healthy 3-pod instance holding 4000
distinguishable keys; `t0` = the patch):

1. The Redis StatefulSet is applied with the new pod template **before** the gather, so a pod is
   terminating from the moment of the edit.
2. The gather asks every Sentinel about the **new** name. Sentinel answers an unknown name with
   `ERR No such master with that name`, which the gatherer maps to `Monitoring:false,
   Reachable:true` — **the whole quorum reads bare**. That is a legitimate arrival of LR-041's
   plausible-looking lie.
3. `DetermineRealMaster` falls through to its step-4 Redis-only fallback and correctly identifies
   the live master.
4. **Rule 0** re-registers the bare quorum: `SENTINEL MONITOR <new> <masterIP>`. Sentinel accepts a
   `MONITOR` under a different name, so **each Sentinel now monitors both**.
5. **Nothing ever removed the old entry.** Every `Remove` call site passes the *current* name, the
   client had no list-all call, and the operator remembers no previous value.

Both names were present on all three Sentinels **0.8s after the patch** and **still present 12m39s
later**, on an instance reporting `Running` / `Ready=True`. The sharpest artefact is Sentinel's own
persisted state, `/data/sentinel.conf`:

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

Two `monitor` lines. The **stale** one carries five known-replicas where two were deployed — three
of them dead IPs that never age out (LR-024) — and a **higher config epoch** (3 against 2).

**And it was not merely dormant debris.** The master pod's preStop hook is baked into its container
spec with the **old** name, and because the stale entry still existed, its `SENTINEL failover <old>`
**succeeded**. From `rn-sentinel-1` at t0+89s:

```
* Executing user requested FAILOVER of 'mymaster'
# +new-epoch 1
# +try-failover master mymaster 10.233.192.110 6379
# +elected-leader master mymaster 10.233.192.110 6379
# +promoted-slave slave 10.233.192.250:6379 ... @ mymaster 10.233.192.110 6379
* +slave-reconf-sent slave 10.233.192.236:6379 ...    <- a DEAD IP
```

| window | duration |
|---|---|
| the two names name **different addresses** | **88.5s** |
| the two names name **two different, live, running pods** as master | **56.6s** |

Two quorums, two config epochs, two failover state machines over the same three pods — LR-039's
named hazard, produced by a **supported field edit on a healthy instance**, and left in place
permanently. The data survived (4000/4000 keys) only because writes were quiesced per the runbook.

`lrctl verify` reported that instance as entirely healthy, because it asked each Sentinel about one
name and a Sentinel carrying a leftover entry answers that question perfectly well.

So the feature and the bug fix are the same change: there was no way to *not* decide here. Doing
nothing leaves a defect; the only question was whether to close it by making the field immutable or
by making the rename work.

## Decision

> **The operator reconciles the *scope* of what its Sentinels monitor, not a migration.**
> Desired state: *every Sentinel monitors exactly the desired name, and nothing else.* A stale name
> is discovered from Sentinel's own answers (`SENTINEL masters`), pruned with `SENTINEL REMOVE`,
> and only ever after the desired name has been confirmed present on that same Sentinel. Nothing is
> remembered — no "from" name, no phase, no cursor.

The rule is **N**, the pure decision is `planStaleMasterNames`
(`internal/controller/stale_master_name_plan.go`), and the reported state is the
`StaleMasterName` condition.

Five properties follow, and they are why this shape was chosen over the alternatives below.

1. **Evidence-driven, so no persisted state.** The previous name is not needed: anything monitored
   that is not the desired name is by definition stale. That also repairs an instance a *previous*
   botched rename already broke, and any out-of-band `MONITOR` someone added by hand. LR-041's
   lesson applied to state rather than to signatures — do not store what you can read.
2. **One decision, one existing primitive.** `REMOVE` was already in the client and already bounded
   (LR-040), and `REMOVE` + `MONITOR` is this project's established divergent-master primitive
   (LR-005 / LR-008). The change adds one *read* (`SENTINEL masters`) and one *rule*.
3. **Register-then-prune, and the prune verifies its own precondition.** Rule N sits **after Rule
   0**, so the two-name window is intra-pass (milliseconds) rather than multi-pass, and a Sentinel
   is never left bare on purpose. The caller re-confirms `IsMonitoring(desired)` with a bounded call
   immediately before each `REMOVE` — LR-024's `electMaster` shape: enforce the invariant, do not
   assume it.
4. **No dual-name serving, ever.** This does not reverse ADR-015's decision; it implements the
   cutover that decision said had to be a window.
5. **No rollout gate, and no persisted phase**, because readiness already serializes the Redis roll
   behind the rename — see the interlock below, which was verified before anything was built on it.

**Rule N runs before Rule A**, i.e. while `anyTerminating` is true, and that is deliberate. The
churn Rule A sits out is *exactly* when a rename is in flight, since the field edit rewrites the
Redis pod template. Gating on `!anyTerminating` would hold the two-name window open for the whole
multi-minute roll — the one window in which `redis-0`'s baked stale-name preStop fires a real
`SENTINEL failover <old>`, which is the 56.6s measured above. **LR-040's actual lesson applies in
full and is discharged rather than inherited:** an action that runs during churn must be bounded, on
the client's own `Dial`/`Read`/`WriteTimeout` **and** a per-call context deadline, because a context
alone is inert against go-redis.

### The gates

`REMOVE` is a destructive primitive aimed by a predicate, so nearly all of the value is in the
refusals. Every gate must hold before any prune is emitted:

| # | Gate | Which incident it comes from |
|---|---|---|
| G0 | A capture is **in evidence** (`planForsaken`'s `Captured`, computed once per pass and passed in) stands the whole rule down: reason `Foreign`, prune nothing. | The rename-to-escape-a-capture trap. Fed `Captured` rather than `Forsaken` because a *settled* `Forsaken` returns from the switch ~90 lines earlier, which would have made the gate structurally dead. |
| G1 | `desired != ""`. | LR-041. With an empty desired name **every** name reads as stale, so the failure mode is "prune everything", not "do nothing". |
| G2 | A living, reachable master of **ours** to keep monitoring: `RealMasterIP` set, in `LiveTopologyIPs` (the live-topology half of the set LR-053 split; G2 asks for a master to keep monitoring, which is liveness, not attribution), and its own Redis view reporting a reachable master. | LR-008's gate reused. Pruning without it manufactures LR-015's leaderless deadlock. |
| G3 | No monitored master — **under any name** — reports an in-flight failover. | A failover under the stale name is still a real state machine reconfiguring our pods. |
| G4 | Reachable Sentinels ≥ quorum. | Do not operate on a minority. |
| G5 | Every stale entry's address is one of our pods **or** is flagged down; otherwise `Foreign`, prune nothing — **unless our own StatefulSet is mid-rollout, in which case it is `Deferred`, unattributable rather than foreign** (LR-050). | The capture trap, second line of defence. Same discriminator as `planForsaken` clause 3, so the operator and `lrctl` cannot disagree about what counts as debris. |
| G6 | Per Sentinel, the desired name is present on **that** Sentinel; otherwise it is skipped, **named in the condition message**, and left to Rule 0. | LR-024. R3 is "no leftover entry, *ever*", so an invisible skip is a defect — "lagging by a pass" must be distinguishable from "permanently stuck". |

Deliberately **not** gates: `!anyTerminating` (above) and `Phase == Running` (the phase is written
at the tail of the pass and lags by one — LR-044's M4b finding; gate on the state, not on the
phase).

Two implementation calls beyond the design's letter, both accepted on review and recorded here
because they change which sentence an owner reads:

- **G5 is evaluated before G2/G3/G4.** The capture trap fails G2 as well (no pod of ours is a
  master, so `RealMasterIP` is empty), so numeric order would report the generic *"Deferred: no
  living master of ours"* and never the `Foreign` warning — in precisely the case the warning exists
  for. Both outcomes prune nothing; only the diagnosis changes.
- **The planner does not gate on `sn.Monitoring`.** At pass 1 every Sentinel reads
  `Monitoring:false, Reachable:true` (the single-name probe asks about the *new* name) while still
  carrying the old entry. Gating on `Monitoring` would make Rule N inert on exactly the pass it must
  act.

### The rollout interlock, which is what lets there be no persisted phase

The rejection of a staged, persisted migration rests entirely on one property: **the Redis rollout
physically cannot outrun the Sentinel-side rename.** Sentinel-mode Redis readiness is `role:master`
**or** `master_link_status:up`, so a restarted replica cannot become Ready until it has found a
master **under the new name**; the StatefulSet sets `minReadySeconds: 35` and rolls in
reverse-ordinal order; therefore the first pod to roll parks in the startup wait-loop until the new
name resolves, and the master (`redis-0`) is rolled last, minutes after the Sentinel side has
converged.

That was **traced, not measured**, and it had the exact shape of ADR-016's *"the captor heals via
Rule D"* — an inference from three independently-documented mechanisms, correct as it turned out,
but only *known* once observed. ADR-016's companion inference was falsified in the same run that
confirmed the main one, so this one was verified **first**, before any wiring was built on it, with
an explicit exit criterion that would have reopened the staged design.

**Verified live (t3e, 2026-08-26)**, from a 1s sampler against an operator whose production code is
byte-identical to `e2e-0821`: `redis-2` Ready under the new name → `redis-1` deleted = **34.76s**;
`redis-1` Ready → `redis-0` deleted = **33.63s**. Both land on `minReadySeconds: 35`, i.e. the
StatefulSet waited for readiness *plus* the full availability window before advancing, and
`redis-0` rolled last.

**Read the guarantee precisely.** It orders `redis-1` and `redis-0` behind the rename. It does
**not** order `redis-2`: the StatefulSet apply happens *before* the gather, so the highest-ordinal
pod is deleted in the same pass in which Rule N prunes — measured at **t0+0.6s**. That is intended
(it is a replica), but the interlock must never be quoted as if it covered all three.

### Naming

Condition **`StaleMasterName`**, planner `planStaleMasterNames`, rule **N**. The ADR records the
decision, and the two rejections are on the merits rather than on taste:

- ***"Migration"* is wrong.** In this project a migration moves data (ADR-013), and the entire
  content of this Decision is that there is no migration to model.
- ***"MasterNameScope"* names the implementer's predicate, not the operator's problem.** A human
  reading it mid-incident learns nothing. `StaleMasterName` names the defect, in the words the
  runbook and the changelog already use.

**Polarity: `True` is bad**, matching `Forsaken` and `LegacyClusterTopology`. A healthy CR carries
`False`/`Converged`, so the condition stays quiet until something is actually wrong; the
alternative (`True` in steady state, like `Ready`) would light up on every healthy instance forever
and push all four interesting states onto the `False` side. The condition is a *transient
progress/diagnostic* surface, not a terminal verdict, and it never affects `Ready`.

| Reason | Status | Meaning |
|---|---|---|
| `Converged` | `False` | Every reachable Sentinel monitors exactly the desired name. The steady state. |
| `Pruning` | `True` | Stale names observed and being removed this pass. The message names them, and any Sentinel skipped by G6. |
| `Deferred` | `True` | Stale names observed but a gate refuses. The message names **which** gate — including the LR-050 rollout gate. |
| `Foreign` | `True` | A stale entry points at somebody else's live master, **on a settled instance**. `Warning` event: do not rename to escape a capture. |

## Alternatives considered

### A. Make `masterName` immutable (a CEL transition rule) — rejected here, and kept as the standing fallback

Honest and free: a rename becomes delete-and-recreate, which is what ADR-015 §9.2 and ADR-016
already lean on for the capture case.

Rejected because **it forecloses the operation the product's own runbooks demand.** Every capture
remedy is "give it a unique name", and telling an owner to destroy a working instance's dataset to
escape a *theoretical* capture is a worse trade than the rename we can actually build.

**Kept with an explicit trigger: if the prune rule cannot be kept safe, ship immutability rather
than shipping the two-name state.** Doing nothing was never available — the Context is a defect
either way.

### B. Roll the Sentinel StatefulSet and let EmptyDir do the pruning — rejected as the mechanism, retained as the manual escape hatch

Stamp the effective name (or a hash) into the Sentinel pod template so a rename rolls those pods.
They come back bare (Sentinel's `/data` is EmptyDir, pillar 3.1) and Rule 0 registers only the new
name. Attractive because it needs no new Redis call and no new rule. Rejected because:

- Sentinel container probes are a bare `PING`, so all three pods cycle in seconds and the quorum can
  be **entirely bare** — trading a two-name window for a *no-monitoring* window, and giving up
  failover protection for the duration.
- It only fixes pods that restart. It cannot repair an instance already in the two-name state (no
  template change is pending there), so the prune would still be needed.
- It manufactures pod churn as a side effect of an API field's *value*, which is the coupling LR-021
  had to serialize away in cluster mode.
- Cycling Sentinel pods releases and re-acquires pod IPs — ADR-015 §9.4's warm-IP window, which
  ADR-016 accepts only because it has no choice. Here we do.

**Retained in the runbook as the manual escape hatch:** `kubectl rollout restart
statefulset/<name>-sentinel` provably clears every entry, at the cost of a bare window.

### C. A staged rename with a persisted phase — rejected, because the interlock was verified

Hold the Redis pod-template update until the Sentinel side has converged, so a Redis pod is never
running a script whose name disagrees with what its Sentinels monitor. It needs the desired name at
*build* time, i.e. before the gather, which means persisting the phase in status — LR-044's
pre-gather pattern, or ADR-013's `status.migration` phases.

Rejected because the readiness interlock delivers that ordering for free, and we would be paying
persisted load-bearing state (an ADR-006 tension every time) for an ordering physics already gives
us. **This was a decision gate, not an assumption**: the exit criterion was that an unverified
interlock reopens this alternative, and the verification measured **34.76s / 33.63s**, both on
`minReadySeconds: 35`, with `redis-0` genuinely last.

### D. Make the pods name-agnostic (read the name from a mounted file) — deferred, and it is the right long-term shape

Move the name out of the container spec into a file (a key in the existing ConfigMap, mounted),
re-read by the startup wait-loop on each iteration and by the preStop at exec time. Then a rename
triggers **no rollout at all**; the preStop of a pod terminating *during* a rename uses the
**current** name, so the stale-name handover hole below closes rather than being documented around;
and the whole ordering question disappears.

Deferred, not rejected: it edits the sentinel startup script, which is the highest-consequence file
in the repository (LR-003, LR-016 and LR-023 all turn on its behaviour); it needs one adoption
rollout anyway; and ConfigMap propagation to a kubelet is minutes-scale and unsynchronised, with
nothing hashing that ConfigMap today, so the operator gets neither a restart nor a signal. It is
worth its own ADR. **The prune rule is a prerequisite for it either way** — it is what converges
Sentinel's own persisted state — so building the prune first is not wasted work.

**Trigger:** revisit when the stale-name preStop no-op (below) becomes more than an availability
cost, or when a second reason to decouple the pods from the name appears.

### E. Dual-name overlap for a graceful client cutover — rejected, hard

Monitor the master under both names for a grace period so clients can migrate one at a time. This is
LR-039's named hazard, stated as a decision: two entries are two failover state machines over the
same three pods; they can promote different replicas, and the loser's writes are discarded on
resync. **This must not be built, even as an opt-in.** The Context section is what it looks like
when it happens by accident. If someone asks for client-side overlap, the answer is the auth work
(where overlap genuinely *is* achievable on the client edge) plus a maintenance window.

## Consequences

- **A rename of a healthy instance now converges with no human action on the operator side, and the
  dataset survives.** Measured (t3e, 2026-08-26, 1s sampler, 4000 keys): the prune lands **1.4s
  after the patch**, and the instance reaches sustained `Running`/`Ready=True` at **+176.8s
  (2m57s)**, replacing the design's ~4-6 minute estimate. Each replica roll costs ~12s of
  `Ready=False`, the master's ~53s, and the CR legitimately flaps `Running → Initializing →
  Running` on the way.

- **The fix removes a fast handover the bug was accidentally providing, and the budget is ~3 minutes
  + ~30s.** Pre-fix, the master's baked-old-name preStop `SENTINEL failover <old>` **succeeded**,
  because the stale entry still existed — the forced `+switch-master` landed one second after the
  pod was deleted. With the entry gone, the same call errors (`ERR No such master with that name`),
  no proactive handover happens, and the desired name's quorum must wait out
  `down-after-milliseconds` (30s by default) before electing. **This is expected, documented, and
  harmless with writes quiesced** — a maintenance window with clients stopped is a precondition of
  the operation, not a cost of it. It is closed for good only by Alternative D.

- **That removal is also what made LR-050 a prerequisite rather than a follow-up.** The instant
  handover was holding a latent verdict bug shut. With it gone the instance has no master of its own
  for `preStop stall (~21s) + downAfterMilliseconds (30s) + election (~1.5s) ≈ 42.5s`, and for that
  whole window a just-replaced pod of ours is, to `planForsaken`, byte-identical to a captor's live
  master. The verdict settled at T+30 and **quarantined a healthy instance 12.5s before it would
  have healed itself**, deleting all six pods on EmptyDir. LR-050's rollout attribution gate — while
  our own Redis StatefulSet is not settled, the operator does not *attribute* addresses, neither as
  a captor nor as foreign — is what makes this feature shippable. **This ADR is not implementable
  without it.**

- **An instance already in the two-name state is repaired with no human action**, because the rule
  is evidence-driven and remembers nothing. So is any out-of-band `MONITOR` someone added by hand.

- **One extra bounded round trip per Sentinel per pass** (three per 2s pass at steady state), paid
  **unconditionally** rather than only when a Sentinel reads bare. A Sentinel carrying *both* names
  answers `Monitoring:true`, so a lazy probe would never see the state a previous botched rename
  left behind — which is the state most affected instances in the field are actually in. A failed
  read degrades to an **empty list**, never to `Reachable:false`.

- **`lrctl verify` gains a `Sentinel Identity` block and a behaviour change**: it now reports every
  monitored name per Sentinel, classified `desired` / `stale — ours` / `FOREIGN` on the operator's
  own discriminator, and **exits non-zero** on any name other than the CR's. An instance a script
  previously read as healthy while carrying a leftover name now reports failure — which is the
  point, and the reason the runbook's verification step is only implementable from this version on.
  See `docs/LRCTL.md`.

- **New surface:** one condition (`StaleMasterName`, four reasons). **No new status field and no new
  RBAC.** The design's M3 settle — a fifth reason `ForeignSuspected`, a
  `staleMasterNameForeignCooldown`, and `status.staleMasterNameForeignSince` — was **deleted** by
  LR-050, which closes the window they softened at its source. The owner's objection is the
  rationale and is recorded as such: **no status-field inflation for a
  once-in-an-instance-lifetime operation.**

- **The `Forsaken` verdict is now name-agnostic**, which amends ADR-016's verdict without touching
  its four clauses. A capture under a *stale* name is still a capture, so an owner who renames in a
  panic no longer defeats the quarantine that heals both sides. Two further consequences of the
  wider observation set, both toward the safe direction: a Sentinel carrying only a stale name now
  counts for clause 1, and two names naming two different addresses is now a clause-2 disagreement —
  which an ordinary rename transiently produces, so the widening *removes* a suspicion the
  desired-name view raised on its own.

- **Renaming a *degraded* instance is a precondition violation and is not prevented in code.** With
  no reachable `role:master` pod at pass 1, `RealMasterIP == ""`, Rule 0 cannot register, Rule N
  defers on G2, and the Redis pods roll into a wait-loop on a name nobody monitors. The instance
  then presents Rule L's signature, and **Rule L is the safety net**: 0 holders → reseed; exactly 1
  → promote; **≥2 holders → refuse** without `allowUnsafeRebootstrapOnDeadlock`. That last case is a
  genuine wedge needing a human decision. The silver lining is worth stating: **Rule L's recovery
  lands on the desired name**, because `electMaster` issues `REMOVE` + `MONITOR` with the name the
  operator currently wants — the rename completes even out of the wreckage. Refuse-and-say-so is the
  behaviour; the precondition goes in the runbook; there are no webhooks to enforce it.

- **The remedy order for a captured instance is `capture → let the quarantine finish → then
  rename`**, and it belongs in the runbook rather than in code. A quarantined instance has zero
  Sentinel pods, so there is nothing to prune and Rule N is unreachable; after release Rule L
  re-bootstraps it empty and the Sentinels come back bare, so the owner gets the new name for free
  with no old entry anywhere.

- **Accepted residual (K2d):** a captor whose master is transiently `s_down` at the instant of the
  rename is seen by neither G5 (it reads as flagged-down debris) nor G0 (`planForsaken` clause 3
  refuses to call a down address a capture, and correctly so — from Sentinel's vantage a not-ours
  address that is not answering is indistinguishable from our own dead ex-master, which is LR-024's
  entire subject, so calling it a capture would park live instances after every ordinary failover).
  So the prune fires and the capture evidence is lost. **Narrowed, not closed.** Closing it means a
  G5 settle — a not-ours address must have been flagged down for some period before it counts as
  prunable debris — which delays legitimate pruning of ordinary post-failover debris, and that
  debris is the common case. If it is ever closed, the lever is **Rule N's side, not the verdict's**.

- **Two deferred items with triggers, recorded as decisions rather than loose ends:** immutability
  (Alternative A, trigger: the prune rule proving unsafe), name-agnostic pods (Alternative D,
  trigger above), and the auth change, which is deliberately a **separate maintenance window** — one
  variable per window. Note that enabling auth *does* change the Sentinel pod template, so that
  rollout wipes Sentinel's EmptyDir state, which is why the previous `USAGE.md` advice to do both at
  once happened to work. Once Rule N exists that coincidence is no longer load-bearing, and the two
  operations should be **separated** rather than combined.

- **`spec.sentinel.masterName` is documented as mutable.** It always was; the change is that editing
  it now does what a reader expects.

## Verification status

**The interlock and the defect were both measured before anything was built** (t3e, 2026-08-26) —
the numbers are in Context and in the Decision's interlock section, and they replaced the design's
estimates rather than standing beside them.

**Pure seams, red-first.** `planStaleMasterNames`: a 16-row table plus a determinism test, authored
against a zero-value stub and observed **red on all 17**, mutation-checked in both directions — a
prune-everything mutant fails all 11 `Deferred`/`Foreign` rows, a prune-nothing mutant fails all 5
prune rows plus determinism, and a never-`Converged` mutant fails the converged row, which neither
of the first two can reach. `planForsaken`'s name-agnostic rows: 10 rows, **red on 3**, with the
**entire pre-existing table passing unedited and the test file diff containing zero deletions** —
the precise statement of "additive", pinned by a mutant that ignores the stale names and fails
exactly the three new rows.

**e2e (`Sentinel Master Name Rename`, t3e), and the pre-fix red is banked.** Tier 1 was observed red
against operator image `9c2dd35` for exactly the right reason:

```
Timed out after 180.006s.
rn-full-1787768094-sentinel-0 monitors [mymaster lr048-red.rn-full-1787768094],
want exactly [lr048-red.rn-full-1787768094]
```

The three tiers are `SUCCESS! -- 3 Passed | 0 Failed` (871s at operator `6f20511`). Tier 2 is the
one worth the budget: it renames a *captured* victim and asserts the verdict **survives**, no
`REMOVE` is issued, the pods stay — and then that ADR-016 still runs end to end, so a panicked
rename no longer defeats the quarantine.

**The headline result is the full suite: `123 Passed | 0 Failed` on t3e (2026-08-28)** — which is
what shows that LR-050's amendment to `planForsaken` and LR-049's bounding of `SlaveOf` cost
nothing in the cluster, failover, quarantine or leaderless tiers.

**LR-050's own live proof, on the recipe that produced the defect:** the capture signature
reproduced at full strength (**42.0s** with 500 keys, **41.0s** without) against a 30s cooldown,
with **no `Forsaken` condition at any 0.5s sample, no `forsakenSince`, no `quarantinedSince`, no pod
deleted**, `StaleMasterName` never reaching `Foreign`, exactly one monitored name on all three
Sentinels, and an exact sweep of **`present=500 missing=0`** on the new master. A negative result is
only evidence when the precondition is present, which is why the window is quoted.

**What is not covered, stated plainly:**

- **Renaming a degraded instance** — the Rule L wedge above. Guarded by `planLeaderlessRecovery`'s
  unit table, not by an e2e.
- **Concurrent disruption during a rename** — sub-quorum node or pod loss, a node drain, a second
  failover mid-rollout. Out of scope by requirement (a maintenance window is a precondition); the
  ordinary healing rules still apply, but the rename makes no guarantee there.
- **A partial capture** producing no verdict, inherited from ADR-016.
- **`HoldDataUnknown`**, inherited from ADR-016: staging it needs a pod that is Ready per the
  kubelet while unreachable from the operator, i.e. traffic shaping this suite does not have.
  Deferred to `feat/e2e-harness`.
- **Rule D's ghost-replica `SENTINEL RESET` fires against the *desired* name during the roll** —
  observed twice in the 2026-08-26 run, triggered by departed pod IPs reaching `s_down`. That is
  LR-024's self-inflicted-deadlock ingredient, live on this path; it did not deadlock. Rule N never
  issues `RESET`, so this is not made worse here, and prevention remains ADR-010's deferred subject.
- **A stuck rollout never lifts LR-050's gate**, so a genuine capture arriving in that window goes
  undetected. Accepted by the owner — *"we don't fix on operator level if something's broken
  below"*: such an instance is already `Ready=False` and visibly broken, and the quarantine exists to
  heal the **captor**, which it cannot do for an instance that cannot roll. LR-023 is the precedent
  if it is ever closed — its own rule, not a timer.

## References

- `CLAUDE.md` pillar 3.7 (the master name as the isolation boundary), pillar 3.15 (the quarantine),
  §4, §9
- ADR-015 (per-instance master name — this implements the cutover it declared a window), ADR-016
  (the quarantine an owner must not rename to escape), ADR-005 (Rule L, the safety net for a
  degraded rename), ADR-003 (Rule D, live on this path), ADR-006 (the "nothing load-bearing in
  status" tension a persisted phase would have created), ADR-013 (why this is not called a
  migration), ADR-017 (the ADR number this one is *not*)
- `docs/SENTINEL_MASTER_NAME_RENAME_DESIGN.md` — the design and, by amendment, the history of how
  measurement corrected it
- `docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md` §9.2, §9.4
- `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` — **LR-048**, **LR-050** (the prerequisite),
  **LR-049**; LR-039 (the hazard), LR-041 (put mandatory values in the signature; gather/CLI
  parity), LR-040 / LR-046 (both halves of a bound), LR-042 / LR-044 / LR-045 (the verdict and the
  quarantine), LR-024 (re-confirm the invariant; why a down address is not a capture), LR-021 (the
  settledness predicate LR-050 reuses)
- `docs/LRCTL.md` — the `Sentinel Identity` block and the `verify` exit-code change
- `docs/USAGE.md` — the runbook
