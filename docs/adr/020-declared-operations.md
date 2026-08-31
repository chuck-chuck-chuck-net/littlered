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
4. **Heavy operations serialize.** Order comes from the operation already running; simultaneous
   CR-resident heavy changes are refused at **admission** by a CEL transition rule; and the one pair
   admission cannot see carries a declared `Requires` dependency. There is no precedence table, and
   nothing encodes "A before B because I said so".
5. **The acknowledgment is never an operational input.** It answers *was this asked for?* and
   nothing else reads it.
6. **No global gate and no waiver knob.** If an operation is safe enough not to need a window it
   does not need a waiver; if it is not, the fix is to make it safe.
7. **The invariant guards stay, unchanged and unreferenced by the operation set.**

**Registry v1 has exactly one member:** `SentinelMasterNameRename` (`spec.sentinel.masterName`,
sentinel mode, `StallAfter` 15m, citation *LR-050*). Auth joins by adding a row — and, for its
CR-resident fields, a term in the admission rule.

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

### Intent is a diff over the heavy projection of the SPEC — never over the world

Stated precisely, intent is

> `diff( heavy(spec_now), heavy(spec_baseline) )`

— a diff of the **heavy projection of the declaration**, and of nothing else. That framing is worth
more than "a change event", because it says exactly what intent is *not*: the world is not an input.
`spec` disagreeing with *observed* is **drift**, and drift has many causes — someone changed the
spec, or the world broke, or a capture occurred. Deriving intent from drift is the same conflation
that produced LR-050, where `planForsaken` could not tell our own churn from a captor because both
present as drift.

**The baseline is `spec_last_completed`, not `spec_previous_revision`**, and that single choice is
what makes the record survive operator death: the question is "is there unfinished work", not "what
changed most recently".

There is a GitOps reading of this worth stating, because it draws the two mechanisms' boundary in
storage: detecting that *something heavy changed* needs only the declaration — a `git diff` with
everything but the heavy fields filtered out, no cluster access at all. Detecting that it is *still
pending* needs the completion record, which is cluster state and **cannot** live in git, because
whether work finished is not a property of the declaration. Intent is declarative; completion is
observed. That is the ADR's whole thesis, appearing again one layer down.

**The bar for telling intent from drift is 100% — no false positive, no false negative.** This is
*not* a safety bar and must not be read as one. It is what keeps the two mechanisms separate: if
intent detection is fuzzy, invariants must compensate for a bad intent reading, and one mechanism is
doing two jobs again.

Acknowledging on *observation* fails that bar: the operator dies between the write and the action and
the intent is lost silently — a false negative of exactly the forbidden kind. Acknowledging on
**completion** means the record says "the work this fingerprint stands for is finished", which is an
observation of an event and therefore not recomputable — see Alternative A for why that is what makes
it consistent with ADR-006 rather than an exception to it.

### The fingerprint is a keyed hash, and it serves three purposes

An **HMAC** is a hash computed with a key — here the instance UID — so an attacker holding the digest
cannot recover the input by hashing candidate values. Three reasons, and the first is the one that
forced the shape:

1. **Never confuse one intent for another.** The obvious alternative is to name an intent by the
   *field* it changes. That fails: the same field changes repeatedly, so "finished" would match the
   wrong edit and an operation would be acknowledged that never ran for the value now in `spec`. The
   fingerprint identifies **which value** completed, not merely which field.
2. **Never leak the value.** The moment auth is admitted, the fingerprinted value is a **password**.
   A bare digest of a short secret is a dictionary lookup, so the key is not optional.
3. **Structurally enforce ADR-018's refusal.** Rule N derives what to prune from evidence — anything
   that is not the desired name is stale — which is what lets it repair an instance a *previous*
   botched rename broke. A keyed digest cannot be reversed into a name, so a later contributor
   cannot quietly start reading the record as "the old name" and walk that refusal back.

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

Exclusivity runs both ways, and the boundary is:

> **During a heavy operation the operator does not ASSIGN AUTHORITY.** It may *propagate* an
> authority decision already made, and it may *clean up debris*. Everything else runs under its
> normal guards.

**Assigning authority** means creating a new fact about who holds data — which pod is master, or
which node owns a slot range. **Propagating** means making reality match a decision that already
exists. What "authority" is differs per mode, and naming it per mode is what makes the rule usable:

| mode | authority is | suppressed — assigns it | still runs — propagates or cleans |
|---|---|---|---|
| **standalone** | nothing: one pod, no authority exists | *(empty by construction)* | — |
| **sentinel** | which pod the quorum monitors as master | **Rule L** (seed / promote / force-elect), **the LR-024 recovery** (elect a survivor) | Rule 0, Rule D, Rule R, the LR-005/LR-008 correction |
| **failover** | which pod the operator stamps as master | `planFailover`'s seed / promote / unsafe-elect | the straggler repoint, the outgoing-master fence |
| **cluster** | which node owns a slot range | Step 0/1 `CLUSTER FAILOVER TAKEOVER`, Step 3 missing-slot assignment, Step 3b's destination choice | `MEET`, `FORGET`, `REPLICATE`, the shard-aware reattach |

> **⚠ D6 originally said "regular healing does not run during a heavy operation", and measurement
> proved that wrong. Corrected 2026-08-30.** The first build suppressed everything Rule A skips, on
> the reasoning that a rename keeps a pod terminating so Rule A would have returned anyway. That
> holds only *while* something is terminating. Once the last pod is created and nothing is
> terminating, Rule A lets healing run and the operation branch did not — a suppression strictly
> longer than Rule A's, whose extra window is exactly when the instance needs help converging.

**Two rejected formulations, kept because each is the obvious next guess.** *"The operator does not
elect a master"* is too narrow: it misses cluster **Step 3**, which is not an election at all, and
which LR-047 caught reassigning an orphaned range to a reachable empty master — correctly by its own
contract — thereby *"healing an already-dead shard into a healthy-looking empty one"* and erasing the
failure. *"The operator does not change topology"* is too broad: `MEET`, `FORGET`, `REPLICATE` and
Rule R are all topology changes and all safe, because none of them decides anything.

**Ghost pruning is not authority assignment, and that is a ledger fact rather than a judgement
call.** Rule D issues `SENTINEL RESET`, and **LR-007 established by incident that RESET does not
change the monitored master IP** — it clears the replica and sentinel lists and nothing else. That
finding is precisely why LR-008 had to introduce `REMOVE` + `MONITOR` to repoint a stuck Sentinel.
So Rule D is *structurally incapable* of assigning authority. It reads like a topology operation and
is one, but not of the kind that decides.

The classification is a **local property of each rule, declared where the rule lives**, never a
central list to keep in sync — the same shape as `Requires` beating a precedence table. And it
**cannot be applied by reading rule names**: Rule R is literally called *"Replica Rescue"* and
assigns nothing.

**The failover and cluster rows are proposed, not verified.** Registry v1 is sentinel-only, so
nothing exercises them; they are confirmed when those modes are wired. Step 3b's key-preserving
relocation is the one row worth arguing about — it is listed as assigning because it chooses a
destination.

The short list of what still runs, enumerated deliberately rather than discovered case by case:

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

- **Rule D, Rule R and the LR-005/LR-008 correction**, which reach Rule A exactly as they always
  did, so Rule A's `!anyTerminating` / `!FailoverActive` guards still apply. Narrowing the
  suppression is deliberate and hoisting them earlier is not: those guards exist for their own
  reasons, and moving the rules ahead of Rule A would change behaviour far beyond this feature.

Suppressed: **Rule L and the LR-024 recovery** — the two sentinel-mode rules that assign authority,
a strict subset of Rule A's set.

**Suppressing Rule L and LR-024 is a HOLD, not a skip — stated precisely, because the first draft
overclaimed it.** A clock that has *already started* is **never reset** by the suppression (LR-038:
*the timer never resets on a veto*), so the instant the operation completes, a recovery whose
cooldown had begun fires with that cooldown already elapsed. A clock that would have *started*
during the operation starts when the operation ends, because `setLeaderlessSince` and
`setGhostMasterStuckSince` are called from inside the rules being suppressed. That is Rule A's
existing behaviour verbatim — a zero delta, not a regression — but "the markers keep accruing" was
the stronger claim and it is not the one delivered.

**Measured cost of getting this wrong, kept as the reason the rule exists.** With the whole of Rule
A's set suppressed, a rename took **311s** against **162s** on the build without the branch, three
runs, cluster idle. Two of the three pods were identical across builds; the entire difference was the
**first-replaced pod**, which returns following the old master's address with `link:down`, fails the
`link:up` readiness gate, and is precisely what Rule R repoints. Pod unready → StatefulSet unsettled
→ operation pending → Rule R suppressed → pod unready. It cost 180s here and has **no exit** in the
case Rule R was written for, where Sentinel never repoints at all. The generalizable form:
**an operation must never suppress the healing its own completion condition depends on** — which is
why Rule 0 was on this list from the first draft, and the reason simply was not generalized.

**This is close to a no-op against today's behaviour, deliberately.** During a rename a pod is
terminating from the moment of the edit, so Rule A already returns before every suppressed rule. The
branch makes the existing, accidental suppression explicit, uniform and *reported*.

### Serialization, without a precedence table

With K heavy operations there are K(K−1)/2 pairs, and not one has ever been analysed. Serializing
collapses that surface: a pair never runs concurrently, so a pair never needs concurrent analysis. It
is also what makes the arithmetic above sound — "this invariant holds" is proven against one
operation at a time, and if two overlap the proof does not carry.

An earlier draft answered *"in what order?"* with a static precedence list justified per pair. That
is rejected. Ordering is not purely a property of the operations' definitions — it can depend on
context unavailable when the table is written (whether the instance is currently captured, for one) —
so a table formalizes a decision it is not always in a position to make, and it grows as K².
**Three mechanisms replace it, and between them the table has nothing left to decide.**

**1. A running operation is itself the record of order.** If A is in progress,
`status.operation.name` is set and a later intent B queues behind it. That is chronological, for
free, with no timestamps and nothing persisted — and it covers every case where the operator was up
across the two edits. `metadata.managedFields` was considered as a timestamp source and rejected: a
single `apply` puts both fields in one fieldset under one timestamp, so it does not answer the case
that needs answering.

**2. Simultaneous CR-resident heavy changes are refused at ADMISSION, so the ambiguous state is
unrepresentable.** A CEL transition rule on `spec` permits at most one heavy field to change per
update. This is deliberately *not* a reconcile-time refusal, and the distinction is the whole point:
**the operation is not what changes the spec.** Editing `masterName` rewrites the pod template
immediately and the StatefulSet rolls whether or not Rule N runs, so declining to run a driver holds
nothing back — it leaves the instance half-changed, which is exactly LR-048's two-names-forever
state. Refusal has to happen before the spec changes or not at all. It also lands the error at
`kubectl apply`, which is the best place an error can land, and it makes the auth design's **N9** —
*do the rename and the auth change in separate windows* — enforceable rather than runbook prose.

Transition rules do not fire on create, so a CR that sets every field at once is unaffected. Scoped
to updates, verified live (see Verification).

**3. What admission cannot see is exactly one counterpart, and it is resolved by a DEPENDENCY rather
than by a rule.** CEL evaluates the CR, and rotation's fingerprint is the **Secret's content**, which
is not in the CR — so admission structurally cannot refuse "rotate and rename in one go". That is a
real limit, and counting what survives it is what retires the table: at most **one** CR-resident
intent can pend (clause 2), rotation contributes at most **one** more, and anything sequential is
already ordered by clause 1. The orderless residual is **a single pair, always the same pair** — one
CR intent and one Secret intent.

That pair does not need a rule invented for it, because its order is a **fact about one of the
operations**: rotation requires auth to be on. So the registry entry carries a `Requires` edge —
`PasswordRotation` requires `AuthEnablement` — and the ordering falls out of it.

**`Requires X` means "X is not pending", NOT "X has run", and the difference is the whole of it.**
The event reading deadlocks the common case: an instance created with `auth.enabled: true` never
performs an enablement, so a rotation would wait forever for something that will never happen. Under
the state reading every case is right — auth on since creation is seeded at bootstrap and therefore
not pending; auth on before the operator upgrade is seeded per candidate (table row 3) and likewise;
auth being enabled *now* is pending, so rotation waits, correctly; and with auth off, rotation's
`Applies` is false and there is no candidate at all. The seeding rows are what make the dependency
work, which is the second job they do beyond upgrade-safety.

Note the deliberate asymmetry, because it is the same distinction twice and in opposite directions:
completion is **recorded as an event** (whether work finished is not recomputable from live state),
and dependencies are **evaluated as state** (whether an operation ever ran is the wrong question).
A `Requires` that is genuinely blocked *does* hold its dependants, which is ordinary head-of-line
blocking and correct — rotating through a half-applied enablement is exactly what it should prevent.

**A dependency is a better primitive than a precedence number, for exactly the reason the table was
rejected.** A precedence integer demands knowledge the author does not have: where does this
operation sit relative to every operation anyone will ever add? A dependency demands only what the
author *does* have: what must be true for my own operation to make sense. It is local knowledge,
available at authoring time, which is precisely what the table was missing.

It also degrades correctly in every direction:

- **Rename × rotation** — no edge. They commute in the sense that matters (either order works), and
  serialization already prevents concurrency, so an arbitrary-but-deterministic tiebreak suffices.
  "Arbitrary is fine" is what commuting *means*.
- **Auth-enable × rotation** — one edge, stating the fact; inert whenever auth was already on.
- **Auth-disable × rotation** — resolves itself: rotation's `Applies` is "auth is on", so once
  disabled it is not a candidate and there is no pair to order.
- **A genuine cycle** — A requires B requires C requires A — is **detectable**, where precedence
  integers accepted it silently by being unable to express it. That is the honest place for refusal:
  not "two things changed at once", but "you have declared something unorderable."

The edge is an **explicit `Requires` field**, not an implicit precondition folded into `Applies`.
The two are equivalent in effect; they are not equivalent in maintainability. Hiding an ordering
constraint inside a predicate that reads like a mode filter is how it gets deleted in a refactor by
someone who never knew what it was holding.

**The net result is that nothing in this design encodes "A before B because I said so."** Sequential
edits are ordered by the operation already running; simultaneous CR edits are unrepresentable; the
one remaining pair carries a dependency that states a fact about itself.

**Serialize, do not refuse — with one correction.** Two *well-formed* pending intents run one after
the other; refusing those would be making the human drive the train. But a single delta changing two
heavy fields is not a well-formed timetable, it is a broken one: a train cannot be in two places at
once. Refusing that is not driving the train, it is declining to invent a schedule the author never
wrote.

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
them drive the train.

*Consistency:* ADR-006 (which rejected persisting a capability as **either** status **or**
annotation), ADR-011, ADR-013, ADR-017 (*"the StatefulSet's own partition field is the cursor"*) and
ADR-018 (*"no 'from' name, no phase, no cursor"*) each declined to persist something.

**The principle those five support is narrower than "never persist", and stating it broadly would
make this ADR self-refuting, since it introduces persistence.** What they establish is ADR-006's
actual rule: **do not persist what is recomputable.** In all five the value could be re-derived from
live state, so persisting it created a second source of truth that could disagree with the first — a
capability, a phase, a cursor. `acknowledgedOperations` is consistent with that rule rather than an
exception to it, because "this work finished" is an observation of an event at a point in time and is
**not** recomputable from live state — the same ground on which `LeaderlessSince`,
`GhostMasterStuckSince`, `ForsakenSince`, `QuarantinedSince` and `QuarantineAttempts` already sit.
Five cases where persistence was unnecessary do not establish that it is never necessary, and the
sixth case is this one.

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

### E. A static precedence table for concurrent heavy operations — rejected, and replaced

The first draft of this ADR. Rejected for three reasons, in increasing weight. It is K² documentation
for a K-entry data structure. **Ordering is not purely a property of the operations' definitions** —
it can depend on context unavailable when the table is written — so the table formalizes a decision
it is not always in a position to make. And, decisively, it is unnecessary: admission refusal caps
CR-resident intents at one, a running operation orders anything sequential, and the residual is a
single known pair.

It is **replaced rather than merely deleted**, which matters if a second Secret-driven operation ever
appears: `Requires` edges form a DAG, each declared from the authoring operation's own preconditions.
That scales in the direction precedence did not — a new entry states what it needs, never where it
ranks — and a cycle is an error rather than a silently-accepted ordering.

### F. Chronological ordering from observed arrival — rejected

The intuitive answer, and it fails twice. **Order is frequently undefined:** in a GitOps flow two
heavy fields usually change in one commit and one apply, and `managedFields` then carries a single
timestamp for both. **And where order matters most, chronology is wrong:** operations that do not
commute — enabling auth and rotating a password, say — have a safe order that is a property of the
*operations*, not of the sequence someone happened to type. Honouring the typed order would execute
the impossible one first. Chronology captures intent about *what*, never about *how*.

What survives from it is real and is adopted: when the operator *did* observe the edits sequentially,
the running operation already encodes that order at no cost.

### G. A second reconcile loop for maintenance — rejected

Two loops means two places that must know the same things, and they drift. The existing
quarantine/`Forsaken` shape is the model: read the state early, take a different branch.

### H. A global gate or a blanket `allowUnsafeOperation` — rejected

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
- **The registry and the admission rule can drift**, and this is the new coupling to watch: the
  registry lives in Go, the CEL rule in the CRD. A heavy field added to one and not the other means
  admission and reconcile silently disagree. Mitigate by generating the rule's terms from the
  registry, or failing that by a test that asserts the two agree — but record it as a maintenance
  cost rather than pretending it is free.
- **Admission cannot see anything outside the CR.** Rotation's fingerprint is the Secret's content,
  so a batched rotate-plus-rename is not refusable at apply time. This is bounded rather than open
  (see Rationale: it leaves exactly one pair), but it is a genuine asymmetry between CR-resident and
  externally-resident heavy state, and a second Secret-driven operation is the trigger to revisit.
- **Seeding carries more weight than its size suggests, and it must be PER CANDIDATE.** An
  already-initialized instance with no ack row for a candidate is *seeded*, never run. Keying this on
  "the whole ack list is empty" — the first draft — is a whole-list heuristic doing a per-row job, so
  it works for a one-entry registry and silently re-runs a completed operation as soon as there are
  two. Per-candidate seeding makes the fleet-upgrade case fall out as the special case where every
  candidate is missing, instead of being its own rule. Without it, every instance in a fleet declares
  an operation the moment the operator is upgraded.
- **The ack list is bounded by the registry, not by time** — one row per operation *name*, updated in
  place. It needs no expiry, and an age-based one would be a defect rather than hygiene: with
  per-candidate seeding an expired row is merely re-seeded, and without it the row's absence reads as
  pending and re-runs finished work. The only real accumulation is a row whose operation has been
  *de-registered*, which is a registry-driven prune and not a timer.

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
| CRD | the admission rule permits a create with every field set, permits a one-heavy-field update, and **refuses** a two-heavy-field update | red before the rule exists |

**The admission rule is verified live** (t3e, throwaway CRDs in a separate group, deleted and
confirmed). Two rounds, and the second corrected the first.

**Round 1 (2026-08-30) established the shape and one useful negative.** A synthetic create-guard
failed to compile (`undefined field`), which is the point: a transition rule needs no create-guard,
because it does not fire on create. Observed: create with everything set — **accepted**; one heavy
field changed — **accepted**; two — **refused** with the rule's message.

**⚠ Round 1's form was nevertheless BROKEN, and the ADR recorded it as verified. Corrected during
M2.3.** It guarded the optional *parents* (`has(self.sentinel)`) but not the optional **leaf**, and
`spec.sentinel.masterName` is optional — an instance predating ADR-015 legally omits it, which is
exactly what `SentinelMasterName()` falls back to `LegacySentinelMasterName` for. Against such an
object CEL raises `no such key: masterName` and the API server **rejects every update, including to
an entirely unrelated field**. Reproduced in review:

```
The AdrForm "noleaf" is invalid: spec: Invalid value: "object":
  no such key: masterName evaluating rule: ADR-020 as committed (leaf unguarded)
```

The round-1 probe never saw it because its fixture always set the leaf. **The verification was real
but its inputs were not representative, which is the more dangerous kind of green** — the shipped
rule would have frozen every legacy instance at its next edit.

**The shipped form compares the EFFECTIVE name**, defaulting an absent leaf to the legacy value so
it mirrors `SentinelMasterName()` exactly:

```
(has(self.sentinel) && has(oldSelf.sentinel) &&
 (has(self.sentinel.masterName)    ? self.sentinel.masterName    : 'mymaster') !=
 (has(oldSelf.sentinel.masterName) ? oldSelf.sentinel.masterName : 'mymaster') ? 1 : 0) <= 1
```

Re-verified on the same object shape: an unrelated-field update — **accepted**; absent leaf set
explicitly to `mymaster` — **accepted**, correctly, because the *effective* name did not change;
and the two-heavy-field refusal is intact. The project already carries three spec-level
`XValidation` rules, so the surface is established rather than new.

**Generalizable, and it is this ADR's own subject turned on itself:** a guard is only as good as the
inputs it was exercised against, and an optional field's *absence* is an input. Compare the effective
value an accessor would return, never the raw one — the rule and the accessor must agree about what
"unset" means, or they disagree in exactly the population that predates the field.

## References

- `docs/RECONCILIATION_RUNLEVELS_CONCEPT.md` — the agreed concept; R1–R5, D1–D8, the open questions
- `docs/RECONCILIATION_OPERATIONS_IMPLEMENTATION_PLAN.md` — milestones, the `planOperation` table,
  storage shapes, the three traps
- `docs/AUTH_ENABLEMENT_AND_ROTATION_DESIGN.md` — prospective ADR-019; N9 is D4's first customer
- ADR-006 (nothing recomputable in status), ADR-011, ADR-013, ADR-017 (the live field as cursor; no
  timer fallback), ADR-018 (no remembered name)
- Changelog: LR-015, LR-024, LR-038, LR-041 (mandatory values in the signature), LR-043, LR-044,
  LR-048, LR-050, and the Phase 0 sweep LR-051 / LR-052 / LR-053 / LR-054
