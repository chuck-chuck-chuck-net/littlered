# Reconciliation Runlevels and Invariant Guards — Concept

**Status:** concept, agreed in discussion (2026-08-28). Not a design, not an ADR. Written to be
planned in more detail and then built.
**Scope:** all modes. The first real customer is auth enablement / password rotation
(`docs/AUTH_ENABLEMENT_AND_ROTATION_DESIGN.md`); the sentinel master-name rename (ADR-018) is the
worked example that exposed the problem.
**Naming:** "runlevel" is the working term and is **not settled** — see §8.

---

## 1. The problem

Every new *intrusion* the operator gains — deadlock rescue, ghost-master correction, quarantine,
the master-name rename, auth reconfiguration next — is a stronger action taken against an
instance. The project's first pillar (3.5, "enablement over intervention") is that we do nothing
where we must do nothing, because interfering at the wrong moment is a reliable route to data
loss. The changelog is largely a record of learning that the hard way.

The cost is not that each new intrusion needs new guards. It is that **each new intrusion
requires re-verifying every existing guard**, by reasoning, with nothing forcing the check:

> N intrusions × M guards = an O(N×M) verification surface, checked by hand.

**LR-048 → LR-050 is the proof.** Rule N (the rename prune) removed the stale Sentinel entry.
That entry was, accidentally, what made the outgoing master's preStop `SENTINEL failover`
succeed — so removing it opened a 42.5s window in which a just-replaced pod of ours is
byte-identical to a captor's live master. `planForsaken` then quarantined a **healthy** instance
12.5s before it would have healed itself, deleting six pods on EmptyDir.

Nobody re-examined `planForsaken` when Rule N was designed. It took a live e2e to find, and only
because that milestone added a regression guard *beyond its brief*. That is not a process
failure; it is the O(N×M) surface being checked by a human.

**The generalizable lesson, already recorded in LR-048:** *a fix that removes a broken mechanism
also removes whatever that mechanism was accidentally providing.*

---

## 2. The observation: we already have runlevels, undeclared

`reconcileSentinelCluster` already contains four "stop doing things" gates, at four depths, with
four different scopes, each added by a different incident:

| Where | Gate | Scope |
|---|---|---|
| `littlered_controller.go:954` | quarantine `ScaleToZero` → `return` | stop everything; pods to 0 |
| `:968` | settled `Forsaken` → `return` | stop everything; leave pods |
| `:1046` | Rule A (`anyTerminating \|\| FailoverActive`) → `return` | stop healing; Rule 0 and Rule N still run |
| LR-050's gate | rollout unsettled → withhold attribution | stop *attributing*; everything else runs |

That is a runlevel ladder, built by accident. The proposal below does not introduce a new
concept — it makes an existing accidental pattern **explicit and uniform**.

---

## 3. Two mechanisms, two jobs

The central distinction, and the thing to hold on to:

| | **Runlevel** | **Invariant guard** |
|---|---|---|
| Answers | *What operation is in progress?* | *Is the thing I am about to act on actually true?* |
| Nature | A **claim** about intent, supplied by a human | A **check** against observable evidence |
| Job | **Manage interference** | **Ensure the action is safe** |
| Fails when | The human does not declare | The evidence is misread |
| Cost | N + M | M |

> **A runlevel carries the human's risk acceptance. Invariants carry safety.**

The operator cannot derive "a maintenance window is open, clients are quiesced, the platform is
stable, and losing this instance is acceptable" from any observation. That is information only
the human has, and today it lives in runbook prose the code cannot read. That is what a runlevel
is for.

### 3.1 Why the runlevel does not subsume the invariants

Three independent reasons, each of which alone is sufficient:

1. **The runlevel removes interference; it does not make your own operation safe.** Run Rule N in
   maintenance mode with all regular healing disabled: it still needs G2 (a living, reachable
   master of ours) before it prunes, because pruning without one manufactures LR-015's leaderless
   deadlock. That is self-inflicted damage, not interference. The maintenance loop needs its own
   invariants, and they turn out to be the same ones.
2. **Undeclared churn.** An image bump, a chart change, a node drain, an eviction — nobody
   declares maintenance for any of these, and they open the identical window. The runlevel
   structurally cannot cover them. LR-050's invariant covers them without being told they exist.
3. **The arithmetic.** A guard that names causes is N×M. A runlevel reduces each guard to one
   question, so N+M. An invariant never refers to the operation set at all, so M — and, more
   importantly, **adding a fifth operation leaves every invariant guard unchanged and provably
   still correct**, because it never enumerated the operations. That is precisely the
   re-verification burden we are trying to escape. **This holds only if heavy operations do not
   overlap** — an invariant proven against one operation says nothing about that operation running
   during another. Serialization (D4) is what makes the claim sound.

### 3.2 The invariants already in the tree

Built without being named as a category:

| Invariant | Where |
|---|---|
| An address can only be attributed to us when our own pod set is stable. | LR-050 |
| In a pure in-memory instance a not-Ready redis holds no data — kubelet readiness, never the operator's dial, is the data-safety signal. | LR-023, reused by LR-044's `unverified` |
| The operator's own dial is never sufficient evidence for a verdict. | LR-017; failover's corroboration clause |
| "Owns slots with no synced replica" never exists. | LR-025, applied to rollouts by ADR-017 |
| Before removing a name, a living reachable master of ours must exist. | Rule N G2 (LR-008 reused) |
| Whether data is at risk depends on *whose* data it is, not whether there is data. | LR-044 `atRisk` |

---

## 4. The rules that follow

These are the testable consequences. They are the point of the whole concept.

**R1 — Never add to existing healing code a guard that names a new operation.**
*"Skip this during a rename"* is accommodation, and it compounds. This is the O(N×M) trap.

**R2 — Do fix existing code when a new operation reveals the guard was already wrong.** The test:

> **Would the fix have been correct before the new operation existed?**

LR-050 passes: an image bump could always have opened that window; `planForsaken` was never able
to tell our own churn from a captor's master, and that was true the day it was written. So it is
a pre-existing defect, and its fix names a **fact** (*is our StatefulSet settled*), not an
operation. A hypothetical *"don't run Rule D during a rename"* fails the test — meaningless
before renames existed, and it needs a new clause per future operation.

R2 matters because the early branch could otherwise become an excuse to leave real defects in
place (*"regular healing does not run during maintenance, so we need not fix it"*) — while the
undeclared churn of R3 walks straight into them.

**R3 — Prefer guards that name a fact over guards that name a cause.**
- *"don't attribute addresses during a rename"* → names a cause → fragile
- *"don't attribute addresses while our own StatefulSet is unsettled"* → names a fact → robust

**R4 — When a guard's justification requires an enumeration, the enumeration is the bug.**
LR-050's fix was literally recognising that *"during a rename, or an image bump, or auth, or…"*
collapses to *"whenever the StatefulSet is unsettled"*.

**R5 — Suspect any data structure that answers two different questions.** This is how the bug
class is born. `ValidIPs` means "pods that count as live topology" (correct — LR-038 requires the
terminating-pod filter) and is *also* used to mean "pods that are ours" — and for that question a
terminating pod of ours is still ours. One structure, two concepts; the collision produced the
LR-050 defect. LR-050 fixed the symptom by adding a second input; the conflated concept is still
unsplit.

---

## 5. Design decisions

### 5.0 How this converged (the rejected positions are the valuable part)

Two earlier positions were held and abandoned; both are recorded because their reasons constrain
the final shape.

**Rejected — a human-declared `spec.maintenanceMode`.** Two objections, the second decisive.
*Usability:* the human has no intent for "maintenance mode"; their intent is "the master name
should be X" or "auth should be enabled". A second declaration makes them drive the train when
they only wanted to write the timetable. *Consistency:* five consecutive ADRs say derive from live
state and never invent persistence — ADR-006 (*"Nothing is persisted — not a status field… a
status field is a monitoring surface"*, and it rejected persisting a capability as **either**
status **or** annotation), ADR-011 (*"derived from live state when bumped… never read back from
status"*), ADR-013 (phase *"re-derived from live cluster state every pass"*), ADR-017 (*"the
StatefulSet's own partition field is the cursor, so nothing new is persisted"*), ADR-018 (*"no
'from' name, no phase, no cursor"*).

**Rejected — pure derivation from drift.** The successor position, and it fails on a sharper
point: **`spec` disagreeing with observed is *drift*, and drift has many causes.** Someone changed
the spec; or the world broke; or a capture occurred. Deriving intent from drift is the same
conflation that produced K9 — `planForsaken` could not tell our own churn from a captor because
both present as drift. It also cannot express serialization (§5.4), and for rotation-as-shipped
there is nothing to derive from at all (no template changes).

### 5.1 D1 — Intent is a change EVENT, not a state comparison

The change is observable **exactly once**. After it passes, all that remains is a discrepancy, and
a discrepancy is ambiguous by construction.

**The bar for telling intent from drift is 100% — no false positive, no false negative.** This is
*not* a safety bar and must not be confused with one. It is the bar that keeps the two mechanisms
separate: if intent detection is fuzzy, invariants must compensate for a bad intent reading, and
we are back to one mechanism doing two jobs — the thing this whole concept exists to escape.

Worked example of the separation, because it is easy to slip: on a **capture-then-rename**, the
intent question has a definite answer (*yes, the human asked for a rename*) and the safety
question has a different one (*and the instance is captured*). Both are true. The intent
mechanism is not ambiguous there; the situation is simply both asked-for and unsafe, which is
precisely why both mechanisms are needed and neither can cover for the other.

### 5.2 D2 — Per-field acknowledgment over a declared heavy set

Three candidate mechanisms; only one clears the bar.

| Mechanism | Verdict |
|---|---|
| `generation != observedGeneration` (coarse) | 100% for *"some spec change is unreconciled"*. **Free — the field already exists on the CR (`littlered_types.go:827`) and is entirely unwired.** Cannot distinguish a `masterName` change from a `replicas` change, and cannot serialize. |
| generation **+** drift ("unacknowledged generation AND name drift ⇒ rename intent") | **FAILS the bar.** Edit `spec.replicas` while a capture causes name drift and it reads as rename intent. A narrow false positive is still a false positive. |
| **Per-field acknowledgment over a bounded heavy set** | **Chosen.** 100% at the granularity needed; survives restarts; expresses serialization. |

**Acknowledge on COMPLETION, not on observation.** This is what makes it 100% rather than 99%:
"unacknowledged" then means *unfinished work from a spec change*, which survives operator death
and is idempotent across restarts. Acknowledging on sight loses the intent silently if the
operator dies between the write and the action — a false negative of exactly the kind the bar
forbids.

**The heavy set is a load-bearing API concept**, not an implementation detail: adding a field to
it changes behaviour. It must be explicit, documented and versioned.

Precedent for the storage: status already carries five load-bearing **event observations** —
`LeaderlessSince`, `GhostMasterStuckSince`, `ForsakenSince`, `QuarantinedSince`,
`QuarantineAttempts`. ADR-006 forbids persisting *recomputable* state; an observation of an event
at a point in time is by definition not recomputable, which is why those five are consistent with
it and a capability flag was not.

### 5.3 D3 — The acknowledgment is NOT an operational input

ADR-018 refused remembering the previous master name. That refusal stands and is untouched: Rule N
derives what to prune from **evidence** (anything that is not the desired name is stale), which is
what lets it repair an instance a *previous* botched rename broke.

An acknowledgment record is a different object with a different purpose. It never tells any rule
what to do. It answers one question — *was this asked for?* — and nothing else reads it.

### 5.4 D4 — Heavy operations serialize

With K heavy operations there are K(K−1)/2 pairs, and **not one has ever been analysed**. Rename ×
auth exists today and has never been reasoned about. Serializing collapses that surface to zero:
a pair never occurs, so a pair never needs analysis.

It is also what makes §3.1's arithmetic sound. "This invariant holds" is proven against one
operation at a time; if two heavy operations overlap, the proof does not carry. **Serialization is
the precondition for reasoning about invariants one operation at a time.**

The project already wants this in prose and cannot enforce it: the auth design's N7 (*"one
variable per window"*) and §13 (*the rename and the auth change "should be separated rather than
combined"* — today's advice to combine them works only by coincidence, because enabling auth rolls
the Sentinel StatefulSet and wipes the EmptyDir the rename otherwise needs cleared). Per-field
intent turns an unenforceable instruction into a mechanism.

**Serialize, do not refuse.** Two pending intents run one after the other. Refusing and telling
the human to un-declare one is train-driving again — they wrote a timetable with two entries, and
honouring both in a safe order is the operator's job.

**The order is a static precedence list**, deterministic and justified — not arrival order. Order
demonstrably matters: §7.3's remedy order is *capture → let the quarantine finish → then rename*,
and the auth/rename EmptyDir interaction above.

### 5.5 D5 — An early branch, not a second loop

Two loops means two places that must know the same things, and they drift. The existing
quarantine/`Forsaken` shape is the model: read the runlevel early, take a different branch. The
maintenance path is a small purpose-built driver, not a parallel universe.

### 5.6 D6 — Exclusivity in both directions

Regular healing does not run during a heavy operation; heavy actions do not run outside one.

### 5.7 D7 — No global gate; a narrow per-operation opt-in where a waiver is unavoidable

**If an operation is safe enough not to need a window, it does not need a waiver either; if it is
not safe enough, the fix is to make it safe, not to make the human sign for it.** ADR-018 shipped
exactly this way — preconditions in the runbook, N4 documenting non-robustness under concurrent
disruption as an explicit non-requirement — and the auth design concludes no window is needed for
either of its features.

Where a waiver genuinely is unavoidable, the shape already exists:
`sentinel.allowUnsafeRebootstrapOnDeadlock` — a narrow, per-operation opt-in in spec for one
specific unsafe path, not a global mode. One line in the timetable saying "yes, I accept *this*
risk".

### 5.8 D8 — The runlevel does not replace invariant guards

§3.1. Both, with different jobs. The auth defects in `BUGS_AUTH_PREEXISTING.md` are the worked
proof: **not one of them is fixed by any runlevel**, because the credential lives in a Secret the
operator does not own, so no CR-level mechanism ever sees the change.

---

## 6. Candidate invariants to extract

A sweep worth doing independently of the runlevel work, because each finding is O(1) leverage:

1. **`Reachable:false` conflates three facts** — "cannot route", "process dead", "wrong
   credential" — and different rules must act differently on each. Measured consequence: a
   credential mismatch is byte-identical to a dial timeout, `DataHolders()` filters on
   `Reachable`, so Rule L's ≥2-holder REFUSE — the gate that exists specifically to stop the
   operator discarding data — **can never fire**. Invariant wanting to be stated: *unreachability
   must carry why*. See the auth design §3.5/§3.5a.
2. **`FailoverActive` is a sound invariant with a broken evidence pipeline.** The guard says the
   right thing; the field reads `failover-status`, a key Sentinel has never emitted (the real key
   is `failover-state`), so it is permanently false and Rule A's second half has never fired.
   Concept is fine; the plumbing is not. Tracked in `BACKLOG.md`.
3. **`ValidIPs` serves two concepts** — R5 above. The deepest of the three, and the one whose fix
   is a conceptual split rather than a patch.

---

## 7. Worked examples from the ledger

For anyone planning this, these are the cases that motivate each part:

- **LR-048 → LR-050** — a new intrusion silently removed an accidental protection; the fix was an
  invariant that deleted three surfaces rather than adding any. Motivates §1, R2, R4.
- **LR-039 → LR-042 → LR-044** — the capture chain, where "do nothing" had to be named as a state
  (`Forsaken`) before anything could act on it. Motivates §2.
- **LR-015 / LR-016** — the leaderless deadlock, and a probe that "healed" the survivors it was
  meant to preserve. Motivates §3.1(1).
- **LR-024** — a rule's own correct action (Rule D's RESET) creating a permanent deadlock for a
  different rule. The canonical "one part of reconcile fights another".
- **LR-025 / ADR-017** — an invariant ("owns slots with no synced replica never exists") stated
  once and then reused to gate a completely different mechanism. The model for §3.2.

---

## 8. Open questions for the detailed design

1. **Naming.** "Runlevel", "mode", "state" are all taken or overloaded in this project (`spec.mode`
   is the deployment mode; `status.phase` exists). The name must say what it means to an operator
   reading a CR at 03:00. Decide before writing code, and use one name everywhere.
2. **What survives the branch.** Not literally nothing — status reporting must continue or the
   instance goes dark exactly when someone is watching it hardest, and the maintenance driver
   itself does things that look like healing (Rule 0 registering a name). Enumerate the short
   list deliberately rather than discovering it case by case.
3. **The exit edge.** When maintenance ends the regular loop resumes and sees whatever state
   maintenance left, possibly mid-anything. So the regular loop must be safe against **arbitrary**
   state — which is invariants again, not operation-knowledge. This is where I would expect the
   first bug.
4. **Transition guards.** Entering and leaving is itself a distributed operation with the same
   race class. LR-050 was precisely a transition bug. "Leaving while a rollout is in flight" needs
   the same care as everything else.
5. **The admission test.** What qualifies as a maintenance operation? Working proposal: it
   requires a window **and** its failure mode is "undeploy and redeploy". Without a sharp test,
   the mode accretes.
6. **Forever-in-maintenance.** An instance silently unmanaged because someone forgot to exit needs
   a loud condition. Per ADR-017's lesson, **not** an auto-exit timer — a timer would be the
   defect with a delay.
7. **~~Refusal mechanism for D2~~** — settled by the D2 revision: no global gate. What remains is
   per-operation: does any auth path genuinely need an `allowUnsafe…`-style opt-in? The auth
   design says no; revisit if WP0 says otherwise.
8. **Head-of-line blocking (from D4).** A serialized queue means a wedged operation A stops B
   from ever running, leaving an intent unreconciled indefinitely. Same shape as LR-050's accepted
   stuck-rollout hole, and it wants the same treatment: a loud condition, and **no auto-skip
   timer** (ADR-017's lesson — a timer would be the defect with a delay).
9. **Defining the heavy set.** Which fields, and what makes one heavy? It is an API surface (D2),
   so the answer needs to be a documented list with a stated admission test, not a judgement made
   per field as they arrive.
10. **The static precedence order (from D4).** What order, and justified how? Two interactions are
    already known (§7.3's capture-before-rename; the auth/rename EmptyDir coincidence). Whether a
    total order is derivable or must be asserted per pair is open.
11. **Narrow-first or general?** Recommendation: **narrow — build it for auth, generalise when a
   second customer appears.**

   *Corrected 2026-08-28.* This item previously justified auth as the first customer because it
   "needs the risk-acceptance signal". That reasoning is stale — it belongs to the abandoned
   spec-declared D1 (§5.0) — and it **contradicts D7**, which cites the auth design's own
   conclusion that **neither** of its features needs a window. The auth design engineers the
   window away rather than accepting risk: each stage keeps both credential states acceptable on
   every edge (`nopass` short-circuits go-redis's 3-arg `AUTH default <pw>`; `masteruser default`
   tolerates a nopass master; `auth-user`/`auth-pass` lets a Sentinel read one; the `user`
   directive in argv holds two passwords across restarts). The price is stages — two rollouts for
   enablement, three for rotation — not a window.

   The conclusion stands on three better reasons, all from the converged D1-D4:

   - **Rotation has no observable signature at all.** The password is a `secretKeyRef` that
     changes no pod template, so there is nothing to derive from even in principle. It is the case
     that makes intent capture (D1/D2) **mandatory** rather than merely tidy — and note that the
     defect and the underivability are the same problem (§5.0).
   - **Serialization already has a stated requirement here.** The auth design's N9 refuses to do
     the rename in the same window — *"both operations roll the same StatefulSets and both
     interact with the LR-050 attribution gate"* — which is D4's first real customer, written down
     before D4 existed.
   - **Its staged rollouts must not be fought by regular healing**, which is D5/D6.

   The rename shipping without any of this remains evidence that the general case is harder to
   justify than the auth case.

---

## 9. Next steps

1. Settle §8.1 (naming) and §8.5 (the admission test) — both are cheap and both block writing.
2. Write the ADR: the two mechanisms, D1-D5, R1-R5, and the accepted holes.
3. Independently, run the §6 sweep. Each extracted invariant is O(1) leverage and pays off
   whether or not the runlevel is built — item 1 in particular is a **present, data-losing
   defect** (auth design §3.5a, Path C) and wants its own LR entry and fix ahead of any feature.
4. Build it for auth as the first customer (auth design WP1 already assumes something of this
   shape).
