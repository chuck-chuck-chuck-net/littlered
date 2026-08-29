# ADR-020: Declared Operations — Managing Interference Separately From Safety

## Status

**Proposed.** Implements `docs/RECONCILIATION_RUNLEVELS_CONCEPT.md` (agreed 2026-08-28); the build
plan is `docs/RECONCILIATION_OPERATIONS_IMPLEMENTATION_PLAN.md`. Phase 0 — the invariant sweep this
ADR depends on — is **complete and committed**: **LR-051** (unreachability carries why),
**LR-052** (`FailoverActive`'s evidence pipeline), **LR-053** (the `ValidIPs` split), plus
**LR-054**, found by the sweep's own e2e and deliberately **recorded, not fixed**.

Continues **ADR-016** and **ADR-018**, whose interaction (LR-048 → LR-050) is the defect that
motivates the whole thing. Supersedes nothing. It constrains every future ADR that adds an
*intrusion*, which is its point.

> ADR number: **020**. 019 is reserved by `docs/AUTH_ENABLEMENT_AND_ROTATION_DESIGN.md` and is
> still unwritten; 020 is taken here so a parallel landing cannot collide. 010 remains unallocated
> (the deferred ghost-replica prune policy) and 012 lives on the multi-site branch. Checked across
> every branch, per LR-039's rule that an ID is claimed globally.

**"Operation" is the settled word** — `status.operation`, `status.acknowledgedOperations`,
condition `OperationInProgress`, the term *heavy operation*. It is used everywhere: API, conditions,
events, logs, `lrctl`, docs. "Runlevel" was the working term in the concept and is **not** used in
code; "maintenance" is avoided because it implies the human-declared window this ADR rejects.

## Context

Every *intrusion* the operator gains — deadlock rescue, ghost-master correction, quarantine, the
master-name rename, auth reconfiguration next — is a stronger action taken against an instance.
Pillar 3.5 is that we do nothing where we must do nothing, because interfering at the wrong moment
is a reliable route to data loss.

The cost is not that each new intrusion needs new guards. It is that **each new intrusion requires
re-verifying every existing guard**, by reasoning, with nothing forcing the check:

> N intrusions × M guards = an O(N×M) verification surface, checked by hand.

**LR-048 → LR-050 is the proof.** Rule N removed the stale Sentinel entry. That entry was,
accidentally, what made the outgoing master's preStop `SENTINEL failover` succeed — so removing it
opened a 42.5s window in which a just-replaced pod of ours is byte-identical to a captor's live
master. `planForsaken` then quarantined a **healthy** instance 12.5s before it would have healed
itself, deleting six pods on EmptyDir. Nobody re-examined `planForsaken` when Rule N was designed.
It took a live e2e to find, and only because that milestone added a regression guard beyond its
brief. That is not a process failure; it is the O(N×M) surface being checked by a human.

**We already have an operation ladder, built by accident.** `reconcileSentinelCluster` contains four
"stop doing things" gates, at four depths, with four different scopes, each added by a different
incident: the quarantine `ScaleToZero` return; the settled `Forsaken` return; Rule A
(`anyTerminating || FailoverActive`); and LR-050's attribution gate. This ADR does not introduce a
new concept. It makes an existing accidental pattern explicit and uniform.

## Decision

**Two mechanisms, with different jobs, and neither subsumes the other.**

| | **Operation** | **Invariant guard** |
|---|---|---|
| Answers | *What declared change is in progress?* | *Is the thing I am about to act on actually true?* |
| Nature | A record of a human's spec edit | A check against observable evidence |
| Job | **Manage interference** | **Ensure the action is safe** |
| Fails when | The change is not captured | The evidence is misread |
| Cost | N + M | M |

Concretely:

1. **A bounded, documented set of *heavy* spec fields** — the **registry**. Membership is an API
   concept: adding a field changes behaviour. Admission requires the two-clause test in Rationale.
2. **Intent is captured per field, and acknowledged on COMPLETION**, in
   `status.acknowledgedOperations` as an HMAC fingerprint. "Unacknowledged" therefore means
   *unfinished work from a spec change*, which survives operator death and is idempotent across
   restarts.
3. **A heavy operation runs in an early branch** of the mode's reconcile, after the quarantine
   decision and before regular healing, with a short enumerated list of what still runs.
4. **Heavy operations serialize**, in a static precedence order asserted per pair.
5. **The acknowledgment is never an operational input.** It answers *was this asked for?* and
   nothing else reads it.
6. **No global gate and no waiver knob.** If an operation is safe enough not to need a window it
   does not need a waiver; if it is not, the fix is to make it safe.
7. **The invariant guards stay, unchanged and unreferenced by the operation set.**

**Registry v1 has exactly one member:** `SentinelMasterNameRename` (`spec.sentinel.masterName`,
sentinel mode, precedence 100, `StallAfter` 15m, citation *LR-050*). Auth joins by adding a row.

The pure decision is `planOperation`, a ten-row table. The full table, the storage shapes and the
milestone breakdown are in the implementation plan and are not repeated here.

## Rationale

### The mechanism is not a risk-acceptance signal, and that reading must not survive

The concept's first draft justified auth as the first customer because it *"needs the risk-acceptance
signal."* That reasoning belongs to a `spec.maintenanceMode` that was abandoned, and it contradicts
the decision not to ship a waiver. It was corrected in the concept on 2026-08-28 and the correction
is load-bearing here, because the stale reading is the more intuitive one.

The auth design **engineers its window away** rather than accepting risk: each stage keeps both
credential states acceptable on every edge, so at no instant is any pair of peers mutually
unauthenticable. The price is stages — two rollouts for enablement, three for rotation — not risk.

What the mechanism actually buys is **intent capture where none is derivable**, plus serialization
and exclusivity. Rotation is the proof and should carry the argument: a password is a `secretKeyRef`
whose value changes **no pod template**, so drift detection has nothing to detect *even in
principle*. The rename is merely the member we can prove today.

### Why the operation does not subsume the invariants

Three independent reasons, each sufficient.

1. **It removes interference; it does not make your own operation safe.** Run Rule N with all
   regular healing disabled: it still needs G2 (a living, reachable master of ours) before it
   prunes, because pruning without one manufactures LR-015's leaderless deadlock. That is
   self-inflicted damage, not interference.
2. **Undeclared churn.** An image bump, a chart change, a node drain, an eviction — nobody declares
   any of these, and they open the identical window. The operation mechanism structurally cannot
   cover them. LR-050's invariant covers them without being told they exist.
3. **The arithmetic.** A guard that names causes is N×M. An operation reduces each guard to one
   question, so N+M. An invariant never refers to the operation set at all, so M — and **adding a
   fifth operation leaves every invariant provably still correct**, because it never enumerated the
   operations. This holds only if heavy operations do not overlap, which is what serialization buys.

### Intent is a change EVENT, and the acknowledgment happens on completion

The change is observable exactly once. After it passes, all that remains is a discrepancy, and a
discrepancy is ambiguous by construction: someone changed the spec, or the world broke, or a capture
occurred. Deriving intent from drift is the same conflation that produced LR-050, where
`planForsaken` could not tell our own churn from a captor because both present as drift.

**The bar for telling intent from drift is 100% — no false positive, no false negative.** This is
*not* a safety bar and must not be read as one. It is what keeps the two mechanisms separate: if
intent detection is fuzzy, invariants must compensate for a bad intent reading, and one mechanism is
doing two jobs again.

Acknowledging on *observation* fails that bar: the operator dies between the write and the action and
the intent is lost silently — a false negative of exactly the forbidden kind. Acknowledging on
**completion** means the record says "the work this fingerprint stands for is finished", which is an
observation of an event and therefore not recomputable. That is the same ground on which
`LeaderlessSince`, `GhostMasterStuckSince`, `ForsakenSince`, `QuarantinedSince` and
`QuarantineAttempts` are consistent with ADR-006 while a derived capability flag was not.

### The fingerprint is an HMAC, and that is structural rather than decorative

ADR-018 refused to remember the previous master name; Rule N derives what to prune from evidence,
which is what lets it repair an instance a *previous* botched rename broke. That refusal stands.

A hash **enforces** it: no rule can recover a name from the record, so the refusal cannot be quietly
walked back by a later contributor who notices a convenient field. The key is the instance UID,
which matters the moment auth is admitted and the fingerprinted value is a **password** — a bare
digest of a short secret is a dictionary lookup.

### The admission test, and why clause B is a citation and not a judgement

The concept's working test — *"requires a window and its failure mode is undeploy and redeploy"* —
is judgement on both halves and would not have stopped the set accreting. Replaced by:

> **A — Human-initiated.** The operation exists only because a human edited a declared field on the
> CR, or on an object the CR names. Nothing the *world* does is ever a heavy operation; undeclared
> churn is the invariants' job, and admitting world-events re-creates O(N×M) inside the registry.
>
> **B — Demonstrated interference, with a citation.** There is at least one named, documented case
> where regular healing and this operation contradict each other: an LR entry, a measured window, or
> a proven planner interaction. The registry entry carries it as a string field and a unit test
> asserts every entry has one.

Clause B enforces R1 mechanically. *"It feels risky, give it a window"* cannot be admitted, because
there is nothing to cite. An operation that is safe under concurrent healing is **not heavy**.

### What survives the branch

Exclusivity runs both ways: regular healing does not run during a heavy operation, and heavy actions
do not run outside one. The short list of what still runs, enumerated deliberately rather than
discovered case by case:

- **Every resource apply.** The operation is *driven* by the pod template; suppressing the applies
  suppresses the operation.
- **The pre-gather quarantine decision and the `Forsaken` branch**, which sit above the operation
  branch and win outright. A `replicas: 0` StatefulSet reads *settled*, so an operation over a
  quarantined instance would "complete" work no pod ever executed.
- **The gather** (read-only), and **Rule 0**, which the rename driver needs in the *same pass* for
  its G6 precondition and which is non-disruptive by construction.
- **The `role: master` label.** This is writer routing; suppressing it strands writes on a dead pod.
- **Status, conditions, events, `lrctl`.** The instance must not go dark exactly when someone is
  watching it hardest.

Suppressed: Rule D, the LR-005/LR-008 ghost-master correction, Rule R, Rule L, and the LR-024
recovery — precisely Rule A's set, reached one gate earlier.

**Suppressing Rule L and LR-024 is a HOLD, not a skip.** Their markers keep accruing (LR-038: *the
timer never resets on a veto*), so the instant the operation completes the recovery fires with its
cooldown already elapsed.

**This is close to a no-op against today's behaviour, deliberately.** During a rename a pod is
terminating from the moment of the edit, so Rule A already returns before every suppressed rule. The
branch makes the existing, accidental suppression explicit, uniform and *reported*.

### Serialization, and why precedence is asserted rather than derived

With K heavy operations there are K(K−1)/2 pairs, and not one has ever been analysed. Serializing
collapses that surface to zero: a pair never occurs, so a pair never needs analysis. It is also what
makes the arithmetic above sound — "this invariant holds" is proven against one operation at a time,
and if two overlap the proof does not carry.

**Serialize, do not refuse.** Two pending intents run one after the other. Refusing and telling the
human to un-declare one is making them drive the train when they only wrote the timetable.

The order is a **static precedence list, justified per pair**, not derived and not arrival order. No
total order is derivable: the remedy order for a capture is quarantine-then-rename, while the
auth/rename interaction points the other way. The rename × auth justification is already written, as
the auth design's **N9** — *both operations roll the same StatefulSets and both interact with the
LR-050 attribution gate*. That the project has been writing D4's requirement as unenforceable runbook
prose across two designs is the case for the mechanism.

### Three traps for the implementer

1. **LR-050's `rolling` gate stays exactly as it is.** Do not replace it with "an operation is in
   progress" and do not delete it as redundant. It names a *fact*, so it covers the image bump, the
   drain and the eviction that nobody declares. Unifying them is the exact mistake this ADR exists
   to prevent, and it is tempting because in the rename case they fire together.
2. **No planner gains a "skip during an operation" clause.** Suppression lives at the branch, never
   inside `planForsaken`, `planQuarantine`, `planLeaderlessRecovery`, `planGhostMasterRecovery` or
   `planStaleMasterNames`. Every existing table must pass with no row edited (LR-048's K2b). If a
   row must change, the branch is in the wrong place.
3. **Exactly one call site reads `acknowledgedOperations`.** Enforce it at review.

## Alternatives considered

### A. A human-declared `spec.maintenanceMode` — rejected

Two objections, the second decisive. *Usability:* the human has no intent for "maintenance mode";
their intent is "the master name should be X" or "auth should be enabled". A second declaration makes
them drive the train. *Consistency:* five consecutive ADRs say derive from live state and never
invent persistence — ADR-006 (which rejected persisting a capability as **either** status **or**
annotation), ADR-011, ADR-013, ADR-017 (*"the StatefulSet's own partition field is the cursor"*) and
ADR-018 (*"no 'from' name, no phase, no cursor"*).

### B. Pure derivation from drift — rejected

The successor position, and it fails on a sharper point: **`spec` disagreeing with observed is
*drift*, and drift has many causes.** It is the LR-050 conflation restated. It also cannot express
serialization, and for rotation there is nothing to derive from at all.

### C. `generation != observedGeneration` — rejected as insufficient, not as wrong

100% accurate for *"some spec change is unreconciled"*, and **free** — the field already exists on the
CR and is entirely unwired. But it cannot distinguish a `masterName` change from a `replicas` change,
and it cannot serialize. Worth wiring on its own merits; it is not this mechanism.

### D. Generation **plus** drift — rejected, and this is the one to record

*"Unacknowledged generation AND name drift ⇒ rename intent"* **fails the 100% bar**: edit
`spec.replicas` while a capture causes name drift and it reads as rename intent. A narrow false
positive is still a false positive, and the whole value of the separation is that intent detection is
never approximate.

### E. A second reconcile loop for maintenance — rejected

Two loops means two places that must know the same things, and they drift. The existing
quarantine/`Forsaken` shape is the model: read the state early, take a different branch.

### F. A global gate or a blanket `allowUnsafeOperation` — rejected

D7. Where a waiver is genuinely unavoidable the shape already exists and is narrow:
`sentinel.allowUnsafeRebootstrapOnDeadlock`, one specific unsafe path, not a mode. Registry v1 ships
no waiver, and the auth design concludes it needs none.

## Consequences

**Positive.** A new intrusion costs one registry row and one citation, instead of a hand-audit of
every guard. Invariants never enumerate operations, so they stay provably correct as the set grows.
The accidental four-gate ladder becomes one explicit, reported mechanism. The serialization the
project has twice written as runbook prose becomes enforceable. Rotation becomes expressible at all.

**Negative, and accepted.**

- **The exit edge is where the first bug will be.** When an operation completes the regular loop
  resumes and sees whatever state the operation left, possibly mid-anything. The loop must therefore
  be safe against *arbitrary* state — which is invariants again, not operation-knowledge. A dedicated
  test tier feeds each existing planner the states an operation can leave; it is not optional.
- **A stalled operation stalls forever, loudly.** `StallAfter` raises a condition and an event; there
  is **no auto-exit timer**, on ADR-017's lesson that a timer is the defect with a delay.
- **Head-of-line blocking, and it must be answered TWICE.** For the queue: a wedged operation A stops
  B indefinitely; loud condition, `Pending` list, no auto-skip. **And at the invariant level, with no
  queue involved at all** — LR-054 is the worked case: one unhealthy pod pins an invariant false
  *permanently*, where a rollout would do so only transiently. The invariant case is harder, because
  a queue has an obvious place to hang a condition while **a withheld invariant is silent by
  construction**. LR-054 was found only because an e2e stopped winning a race, which is not a
  detection strategy. Any withholding this mechanism performs must say that it withheld.
- **The registry is an API surface.** Adding a field changes behaviour, so it is versioned and
  documented, and the citation field is enforced by test.
- **Two plan rows carry more weight than their size suggests**, and both are upgrade-safety: seeding
  the acknowledgment for a bootstrapping instance and for an existing fleet on first observation.
  Without them, every instance in a fleet declares an operation the moment the operator is upgraded.

**Neutral.** Retrofitting the rename changes little at runtime — Rule A already returns before every
suppressed rule during a rename — so the delta is reporting, not suppression. That is deliberate: it
is what makes the first member provable rather than speculative.

### R5 is a bug-class predictor, and LR-054 is why this ADR believes it

R5 (*suspect any data structure that answers two different questions*) was stated from `ValidIPs`,
which was latent — constructible, argued reachable, never observed. Building the split surfaced a
second instance of the identical class in the same pass, and that one is live and measured:
`statefulSetRolloutSettled` answers *"is a rollout of ours in flight?"* **and** *"is every pod
healthy?"*. LR-050 consumed the first; the second is what breaks attribution for an instance that is
permanently degraded rather than transiently rolling. The consequence is that a capture victim can
never be diagnosed in exactly the case worth diagnosing — **the state `atRisk` exists to protect is
the state that makes the instance unsettled**, so the gate withholds the verdict that gates the
refusal. Stated from one latent instance, R5 immediately paid out a live one. That is the case for
the invariant sweep being O(1) leverage, and it is evidence rather than assertion.

## Verification plan

Red-first per CLAUDE.md §7 Test Discipline. Full tiers in the implementation plan; the load-bearing
ones:

| Tier | What | Where the red comes from |
|---|---|---|
| Pure | `planOperation`'s ten rows, plus three mutants: *always run*, *always converged*, and **acknowledge-on-sight** | the third is D1's central claim and needs its own named test |
| Pure | fingerprint determinism, UID sensitivity, and that no plaintext appears in the record | red before the helper exists |
| Regression | every existing planner table passes **unedited** (K2b) | a stop condition, not a red |
| envtest | a changed `masterName` renders the operation, holds while unsettled, acknowledges only after settle | red before the wiring |
| e2e | operator killed **mid-rename** resumes and completes | red against acknowledge-on-observation — D1's claim, end to end |
| e2e | a **non-heavy** field edit declares nothing and suppresses nothing | red against the rejected generation-only mechanism |
| e2e | operator upgrade over an existing fleet declares **nothing** | red against a build missing the seeding row |

## References

- `docs/RECONCILIATION_RUNLEVELS_CONCEPT.md` — the agreed concept; R1–R5, D1–D8, the open questions
- `docs/RECONCILIATION_OPERATIONS_IMPLEMENTATION_PLAN.md` — milestones, the `planOperation` table,
  storage shapes, the three traps
- `docs/AUTH_ENABLEMENT_AND_ROTATION_DESIGN.md` — prospective ADR-019; N9 is D4's first customer
- ADR-006 (nothing recomputable in status), ADR-011, ADR-013, ADR-017 (the live field as cursor; no
  timer fallback), ADR-018 (no remembered name)
- Changelog: LR-015, LR-024, LR-038, LR-041 (mandatory values in the signature), LR-043, LR-044,
  LR-048, LR-050, and the Phase 0 sweep LR-051 / LR-052 / LR-053 / LR-054
