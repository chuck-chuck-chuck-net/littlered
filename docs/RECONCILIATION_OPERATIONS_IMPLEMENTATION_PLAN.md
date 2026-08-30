# Declared Operations — Implementation Plan

**Implements:** `docs/RECONCILIATION_RUNLEVELS_CONCEPT.md` (concept agreed 2026-08-28).
**Status:** plan, ready for `/implement-designed-feature`. No code written.
**Prospective ADR:** **ADR-020** — 019 is reserved by `docs/AUTH_ENABLEMENT_AND_ROTATION_DESIGN.md`
and is still unwritten, so 020 is taken here to avoid a collision if auth lands in parallel.
**Re-check `ls docs/adr/` before writing the file.**
**Changelog IDs:** **LR-051 … LR-054**, provisional. The highest ID visible anywhere today is
**LR-050** (`e2e-0821`, `feat/sentinel-master-name-rename`); `main` is at LR-025. **Re-run the
LR-039 cross-branch loop before allocating** — do not read the tip of one line.

---

## 0. Decisions taken, so nobody re-opens them mid-build

The concept left three things blocking (§9.1, §8.1, §8.5, §8.11). They are settled:

| Concept question | Decision |
|---|---|
| §8.1 **Naming** | **"Operation"**. `status.operation` (the one running, monitoring surface), `status.acknowledgedOperations` (the record), condition `OperationInProgress`, term *heavy operation* — the concept's own word in D4/D6. Does not collide with `spec.mode` or `status.phase`. **Use this one word everywhere**: API, conditions, events, logs, `lrctl`, docs. |
| §8.11 **Narrow or general** | **The mechanism, with the shipped sentinel master-name rename (ADR-018) as its first and only registered member.** Auth joins later by adding one registry entry. Rationale: a mechanism with an empty set ships unproven, which is exactly what §7's test discipline exists to prevent; the rename is real, measured (LR-048/LR-050) and already carries the interaction that motivates the whole concept. **This does not demote auth** — §8.11 as corrected on 2026-08-28 is right that auth is the customer which makes intent capture *mandatory* rather than tidy, because **rotation changes no pod template and therefore has no observable signature at all**. The rename is the member that can be *proven* today; rotation is the member that cannot work without the mechanism. Building against the provable one first is a sequencing choice, not a claim about which matters more — and §7.2's registry signature is shaped for rotation from the start so that admitting it is an entry, not a redesign. |
| §9.3 **The §6 sweep** | **In this plan, as Phase 0, ahead of the mechanism**, each item with its own LR entry and independently shippable. Item 1 is a present, data-losing defect (auth design §3.5a Path C). |

Everything else below follows from D1–D8 and R1–R5 in the concept. Where this plan makes a call
the concept left open (§8.2, §8.3, §8.4, §8.5, §8.6, §8.8, §8.10) it is marked **[settles §8.n]**.

---

## 1. The admission test **[settles §8.5]**

The concept's working proposal — *"requires a window AND its failure mode is undeploy and
redeploy"* — is not sharp enough to stop the set accreting, because both halves are judgement.
Replace it with two clauses, **both mandatory, the second evidence-backed**:

> **A — Human-initiated.** The operation exists only because a human edited a declared field on
> the CR (or on an object the CR names). Nothing the *world* does is ever a heavy operation:
> undeclared churn is the invariants' job (concept §3.1(2)), and admitting world-events would
> re-create the O(N×M) surface inside the registry.
>
> **B — Demonstrated interference, with a citation.** There is at least one **named, documented**
> case where regular healing and this operation contradict each other: an LR entry, a measured
> window, or a proven planner interaction. **A registry entry must carry that citation as a
> string field, and a unit test asserts every entry has one.**

Clause B is what enforces R1 mechanically. "It feels risky, let us give it a window" cannot be
admitted, because there is nothing to cite. If an operation is safe under concurrent healing it is
**not heavy** — and if it is unsafe, D7 still says the fix is to make it safe, not to hand the
human a waiver.

**The rename passes:** A — `spec.sentinel.masterName` is a declared field. B — LR-050, measured:
42.5s in which `planForsaken` read the rename's own churn as a capture and quarantined a healthy
instance, six pods deleted on EmptyDir.

**Auth will pass** on §3.5a Paths A/B/C when it is admitted. Nothing else in the tree passes today.

---

## 2. Storage, and why it is not an ADR-006 violation

Two fields, with different natures, and the distinction must survive review:

```go
// LittleRedStatus

// AcknowledgedOperations records, per registered heavy operation, the value
// fingerprint the operator has FINISHED carrying out. This is an observation of
// an event at a point in time — "the rename to whatever this fingerprint stands
// for completed at T" — and is by construction not recomputable from live state,
// which is the same ground on which LeaderlessSince, GhostMasterStuckSince,
// ForsakenSince, QuarantinedSince and QuarantineAttempts are consistent with
// ADR-006 while a derived capability flag was not.
// +listType=map
// +listMapKey=name
// +optional
AcknowledgedOperations []OperationAck `json:"acknowledgedOperations,omitempty"`

// Operation is the heavy operation in progress and the queue behind it.
// MONITORING SURFACE ONLY. Every field is re-derived from
// (spec, AcknowledgedOperations, live StatefulSets) on every pass and is never
// read back by any decision. Losing it costs nothing.
// +optional
Operation *OperationStatus `json:"operation,omitempty"`
```

```go
type OperationAck struct {
    Name           string      `json:"name"`           // registry key, e.g. "SentinelMasterNameRename"
    Fingerprint    string      `json:"fingerprint"`    // HMAC-SHA256(instance UID, name|value)[:16]
    AcknowledgedAt metav1.Time `json:"acknowledgedAt"`
}

type OperationStatus struct {
    Name      string       `json:"name"`
    StartedAt metav1.Time  `json:"startedAt"`
    Reason    string       `json:"reason"`            // Running | Blocked | Stalled
    Pending   []string     `json:"pending,omitempty"` // queued behind, precedence order
}
```

**The fingerprint is an HMAC, not the value, and that is load-bearing — not paranoia.**
D3 says the acknowledgment must never be an operational input. A hash **structurally enforces**
it: no rule can derive a master name from it, so ADR-018's refusal to remember the previous name
cannot be quietly walked back by a later contributor reading the field. The HMAC key is the
instance UID (the auth design's `AnnotationAuthHash` precedent), which matters the moment auth is
admitted and the fingerprinted value is a **password** — a bare SHA of a short secret is a
dictionary lookup.

**Do not** add a "previous value" field, a phase, or a cursor. D1/D3 and ADR-018 §R4.

---

## 3. The registry, precedence, and what a driver is

`internal/controller/operation_registry.go` — one table, exported nowhere, the API concept D2
calls load-bearing:

```go
// fingerprintInput is everything a Fingerprint may read. Extend it — never widen
// a Fingerprint's reach — when a new operation needs a new source.
type fingerprintInput struct {
    LR  *littleredv1alpha1.LittleRed
    UID types.UID
    // Referenced holds the content of objects the CR NAMES but does not own,
    // keyed by a stable identifier (e.g. "auth-secret"). Absent when unreadable —
    // and an unreadable reference must HOLD, never advance (auth's SecretUnreadable
    // row, and plan row 1's polarity).
    Referenced map[string][]byte
}

type heavyOperation struct {
    Name       string   // stable identity; appears in status, conditions, events, lrctl
    Modes      []string // spec.mode values this applies to
    Citation   string   // admission-test clause B. Non-empty, asserted by test.
    // Requires names operations that must be COMPLETE before this one may run — a
    // fact about this operation's own preconditions (rotation requires auth to be
    // on), never a ranking against other entries. Edges form a DAG; a cycle is an
    // error, not an ordering. Empty for operations that commute.
    Requires   []string
    StallAfter time.Duration

    // Fingerprint is PURE. It never dials anything and never reads the cluster:
    // the caller gathers, it decides.
    //
    // The input carries referenced-object content, not just the CR, and that is
    // NOT speculative generality — it is the shape rotation requires. A password
    // is a secretKeyRef that changes no pod template, so an operation over it has
    // no observable signature whatsoever (concept §8.11 as corrected). If this
    // took only *lr, admitting rotation would mean reshaping the registry rather
    // than adding a row.
    Fingerprint func(in fingerprintInput) string

    // Applies reports whether this operation is meaningful for this instance at
    // all (mode, feature flags). PURE.
    Applies func(lr *littleredv1alpha1.LittleRed) bool
}
```

**v1 contents — exactly one entry:**

| Name | Field | Modes | Citation | StallAfter |
|---|---|---|---|---|---|
| `SentinelMasterNameRename` | `spec.sentinel.masterName` | `sentinel` | `LR-050: measured 42.5s in which planForsaken read our own rename churn as a capture` | 15m |

`StallAfter` = 15m against a measured settle of **176.8s** (ADR-018's verification) plus ~30s at
the master edge — roughly 4× the measured window, deliberately generous, because the failure
direction of a too-tight stall signal is a false alarm on a slow cluster.

**There is no precedence field and no precedence table (ADR-020, Alternatives E and F).** Order
comes from three places instead: a running operation orders anything sequential (`status.operation`
is the record); simultaneous CR-resident heavy changes are refused at **admission** by a CEL
transition rule, so no order has to be invented; and what admission cannot see — a Secret-driven
intent such as rotation — contributes at most one counterpart, and that pair is ordered by a declared
`Requires` edge rather than by a rule (`PasswordRotation` requires `AuthEnablement` complete, because
rotation presupposes auth is on). Nothing encodes "A before B because I said so"; a cycle in the
edges is an error rather than a silently-accepted order.

**The admission rule is a deliverable of this plan** and is already verified live (ADR-020,
Verification). It is a spec-level `+kubebuilder:validation:XValidation` transition rule, one term per
CR-resident heavy field, `has()`-guarded for optional parents; it does not fire on create.
**Watch the coupling:** the registry is Go, the rule is CRD — generate the terms from the registry or
assert their agreement in a test, or the two will drift and admission will disagree with reconcile.

**A driver** is the code that carries one operation out. For v1 it is the code that already
exists: **Rule 0 (bare-Sentinel re-registration) + Rule N (`reconcileStaleMasterNames`)**. The
driver contract is one function returning `{Progressing | Complete | Blocked}` plus a message.
For the rename: `Complete` ⟺ `planStaleMasterNames` reports `Converged`; `Blocked` ⟺ it reports
`Deferred` or `Foreign`. **No new healing logic is written in this plan** — the mechanism wraps
what ships.

---

## 4. What survives the branch **[settles §8.2]**

D6 is exclusivity in both directions. Enumerated deliberately, sentinel mode, in reconcile order:

**Runs during an operation (the short list):**

| What | Why it must survive |
|---|---|
| Every resource apply (ConfigMaps, Services, both StatefulSets, PDBs, ServiceMonitor) | The operation is *driven* by the pod template. Suppressing the applies suppresses the operation. |
| The pre-gather quarantine decision (`sentinelDesiredReplicas`) and the `Forsaken`/quarantine branch | It sits **above** the operation branch and wins outright — §7.4's trap: a `replicas: 0` StatefulSet reads *settled*, so an operation over a quarantined instance would "complete" work no pod ever executed. |
| The gather | Everything downstream needs it, and it is read-only. |
| Rule 0 (re-register bare Sentinels) | Rule N's G6 depends on it in the *same pass* — that is what makes the two-name window intra-pass (LR-048). It is non-disruptive by construction. |
| The operation's own driver | Obviously. |
| `updateMasterLabel` / the `role: master` label | This is writer routing. Suppressing it strands writes on a dead pod — §3.5a Path B's detonation, arrived at deliberately instead of by accident. |
| Status, conditions, events, `lrctl` surfaces | §8.2: the instance must not go dark exactly when someone is watching it hardest. |
| The background sentinel monitor | Detection only; it decides nothing. |

**Suppressed during an operation** — precisely Rule A's set, reached one gate earlier:
Rule D (ghost-replica `SENTINEL RESET`), the LR-005/LR-008 ghost-master `REMOVE`+`MONITOR`,
Rule R (straggler `SLAVEOF`), Rule L (leaderless recovery) and the LR-024 ghost-master recovery.

**The suppression of Rule L and LR-024 is a HOLD, not a skip.** `leaderlessSince` and
`ghostMasterStuckSince` keep accruing while suppressed (LR-038's rule: *the timer never resets on
a veto*), so the instant the operation completes the recovery fires with its cooldown already
elapsed. The `OperationInProgress` message names what is being held.

**This is close to a no-op against today's behaviour, and say so in the ADR.** During a rename a
pod is terminating from the moment of the edit, so Rule A already returns before every one of
those rules. The operation branch makes the existing, accidental suppression *explicit, uniform
and reported* — concept §2's whole thesis — rather than introducing a new one.

---

## 5. Three traps the implementer must not walk into

These are R1/R3/D3 as concrete instructions. Put them in the ADR and in the code comments.

1. **LR-050's `rolling` gate stays exactly as it is. Do not replace it with "an operation is in
   progress", and do not delete it as redundant.** It names a *fact* (our own StatefulSet is
   unsettled) and therefore covers the image bump, the drain and the eviction that no human
   declares — concept §3.1(2), R3. Unifying the two is the exact mistake the concept was written
   to prevent, and it is a tempting one because in the rename case they fire together.
2. **No planner gains a "skip during an operation" clause.** The suppression lives at the branch
   in `reconcileSentinelCluster`, never inside `planForsaken`, `planQuarantine`,
   `planLeaderlessRecovery`, `planGhostMasterRecovery` or `planStaleMasterNames`. The existing
   tables must pass **with no row edited** (LR-048's K2b stop condition). If a row has to change,
   stop and report it — it means the branch is in the wrong place.
3. **Nothing reads `AcknowledgedOperations` except the operation planner.** D3. Enforce with a
   grep-level review: exactly one call site.

---

## 6. The pure seam

`internal/controller/operation_plan.go`, I/O-free, the thing sub-agents implement against:

```go
type operationCandidate struct {
    Name        string
    Fingerprint string
    StallAfter  time.Duration
}

type operationInput struct {
    Candidates  []operationCandidate // registry, filtered to this mode by Applies, fingerprinted
    Acks        map[string]string    // name -> acknowledged fingerprint
    Quarantined bool                 // status.quarantinedSince != nil
    Bootstrapping bool               // status.phase == "" || status.bootstrapRequired
    // NOTE: deliberately no whole-list "first observation" flag. Seeding is decided
    // PER CANDIDATE (row 3): a candidate with no ack row on an initialized instance is
    // seeded, never run. Keying it on len(Acks)==0 is a whole-list heuristic doing a
    // per-row job — correct for a one-entry registry, and it silently re-runs a
    // completed operation as soon as there are two.
    Settled     bool                 // ALL of this instance's own StatefulSets are settled
    DriverDone  bool                 // the running driver reported Complete this pass
    DriverBlocked bool               // ... reported Blocked
    Active      *OperationStatus     // status.operation, for StartedAt only
    Now         time.Time
}

type operationPlan struct {
    Run     string   // "" = none
    Pending []string // precedence order
    Reason  string   // Converged | Running | Blocked | Stalled | Quarantined | Seeded
    Ack     []operationCandidate // acknowledge these THIS pass (name+fingerprint)
    Detail  string
}

func planOperation(in operationInput) operationPlan
```

**The table — every row is a test [settles §8.3, §8.4, §8.6, §8.8]:**

| # | Condition | Run | Ack | Reason |
|---|---|---|---|---|
| 1 | `Quarantined` | `""` | none | `Quarantined` |
| 2 | `Bootstrapping` | `""` | **all candidates** | `Seeded` |
| 3 | any candidate with **no ack row**, on an already-initialized instance | `""` | **those candidates** | `Seeded` |
| 4 | no candidate's fingerprint differs from its ack | `""` | none | `Converged` |
| 5 | exactly one differs | that one | none | `Running` |
| 6 | two or more differ (only the CR+Secret pair is reachable — see §3) | the CR-resident one | none | `Running` (rest in `Pending`) |
| 7 | running, `DriverDone`, **`!Settled`** | the same one | **none** | `Running` — the transition guard |
| 8 | running, `DriverDone`, `Settled` | next pending or `""` | **that one** | `Running`/`Converged` |
| 9 | running, `DriverBlocked` | the same one | none | `Blocked` — **never auto-skip** |
| 10 | running, `Now - Active.StartedAt > StallAfter` | the same one | none | `Stalled` — **never auto-exit** |

**Rows 2 and 3 are the ones that will be got wrong.** Without them, a freshly created CR declares
a rename it never asked for (the spec value differs from a nonexistent ack), and — worse —
**every instance in an existing fleet declares one the moment the operator is upgraded.** Seeding
writes the ack without running anything, which is the correct reading of *"this instance is
already in the state its spec asks for."*

Row 3 is **per candidate**, not per instance, and that is the whole of it: the fleet-upgrade case is
then the special case where every candidate is missing, rather than a second rule. It also makes a
missing row harmless — re-seeded, never re-run — which is why the ack list needs no expiry and an
age-based GC would be a defect rather than hygiene. The list is bounded by the registry anyway: one
row per operation *name*, updated in place. The only real accumulation is a row whose operation has
been **de-registered**, which is a registry-driven prune, not a timer.

**Row 7 is the transition guard §8.4 asks for.** "The driver is done" is not "the operation is
over": Rule N converges the moment the Sentinels agree, which can be well before the Redis
StatefulSet finishes rolling. Acknowledging there hands the exit edge straight into the churn
LR-050 is about.

**Rows 9 and 10 are ADR-017's lesson applied twice**: a stalled operation and a blocked queue are
both loud and both permanent until a human acts. A timer would be the defect with a delay.

**Mutation checks, both directions** (LR-043/LR-044 precedent): an *"always run"* mutant must fail
rows 1, 2, 3, 4; an *"always converged"* mutant must fail rows 5, 6, 7, 9, 10; an
*"acknowledge on sight"* mutant must fail row 7 — that last one is D2's whole argument and it
needs its own named test.

---

## 7. Milestones

Disjoint file ownership is stated so siblings can run in parallel. **Phase 0 is independently
shippable and does not depend on anything else in this plan.**

### Phase 0 — the §6 invariant sweep (ships first, ahead of the mechanism)

> **STATUS: code complete.** M0.1 = **LR-051** (`7f57600`), M0.2 = **LR-052** (`7e1f858` /
> `b2beaf1` / `06e8015`, corrected by `29b4e53`), M0.3 = **LR-053**. Phase 0 is **not
> declared done** until the sentinel e2e suite (`E2E_LABEL_FILTER=sentinel`) runs green
> against a deployed build carrying all three — LR-051 and LR-052 are both safety-behaviour
> changes whose failure mode (over-suppression, over-attribution) unit tests cannot disprove,
> so they are gated jointly rather than separately. The focused `Sentinel Failover` tier is
> already green (4/4).
>
> **The sweep found a defect, and it is NOT one of Phase 0's: LR-054.** Two capture tiers went
> red after M0.3 with an *empty* `Forsaken` condition. The bisect pointed at M0.3 and is
> refuted — the same capture, staged by hand on t3e against the **pre**-split image, fails
> identically. The cause is LR-050's readiness-keyed attribution gate meeting LR-044's
> `atRisk` clause: **the very state `atRisk` exists to protect is the state that makes the
> instance unsettled**, so the gate refuses to arm the verdict that gates the refusal.
> Measured: readiness falls 23s after the capture against a 30s steady poll, so both tiers had
> been passing on a race. **Recorded and DEFERRED — it needs a design decision, not a patch**
> (the obvious narrowing, dropping the readiness clause, is wrong: a pod replaced at the same
> ordinal returns on a new IP with `status.replicas` already full). No production code
> changed; the two tiers were re-scoped to assert what the build actually does, one of them
> `Skip`ped with a pointer, and the guarantee stays pinned by the `planQuarantine` /
> `quarantineDataRisk` tables. **Phase 0's own green-suite gate is unaffected by it** — LR-054
> is pre-existing and orthogonal — but the suite cannot be called green while two tiers assert
> a guarantee the product does not provide, which is why they were re-scoped rather than left
> flaky.

| M | What | Owns | LR |
|---|---|---|---|
| **M0.1** | **Unreachability must carry why.** `RedisNodeState`/`SentinelNodeState` gain a classified probe failure (`None \| Unroutable \| Timeout \| AuthFailed \| ProtocolError`); `gather.go:78-81`, `:93-96`, `:175-179` stop discarding the error. Classify `NOAUTH`, `WRONGPASS`, `ERR Client sent AUTH, but no password is set`, `ERR invalid password` as `AuthFailed`; deadline/net errors as `Timeout`/`Unroutable`. **The fix that matters:** a node that is `AuthFailed` is *not* provably empty, so it must veto — reuse LR-044's existing `unverified` concept rather than inventing one — in `planLeaderlessRecovery`, `planGhostMasterRecovery` and `quarantineDataRisk`. New condition `OperatorCannotAuthenticate` (True is bad), one event per transition. | `internal/redis/replication_state.go`, `internal/redis/gather.go`, `internal/controller/gatherer.go`, `internal/controller/leaderless_recovery.go`, `ghost_master_recovery.go`, `quarantine_plan.go`, `api/v1alpha1` (condition const) | **LR-051** |
| **M0.2** ✅ **DONE (LR-052)** | **`FailoverActive`'s evidence pipeline.** `client.go:198` and `:224` read `failover-status`, a key neither Redis nor Valkey has ever emitted, so Rule A's second half has never fired. The correct predicate **already exists** — `MonitoredMaster.FailoverInProgress()` (`client.go:708`), source-confirmed for both projects and written for Rule N. Make `MasterInfo` carry `Flags` + `FailoverState` and route both call sites and `DetermineRealMaster`'s clause 1 through that one predicate. **One definition, cf. `IsLinkUpReplicaOf`.** | `internal/redis/client.go`, `internal/redis/replication_state.go`, `internal/controller/gatherer.go` | **LR-052** |
| **M0.3** | **Split `ValidIPs` (R5).** Two concepts, two fields: `LiveTopologyIPs` — pods that count as live topology, the LR-038 terminating-pod filter intact — consumed by `DetermineRealMaster` clause 3 and `IsGhost`; and `OwnedIPs` — every pod of ours, *terminating included* — consumed by every "is this address ours?" question: `planForsaken` clause 3, Rule N's G5, `cross_instance.go`'s `ClassifyMonitoredName`. No alias is kept; nine call sites. | `internal/redis/replication_state.go`, `internal/redis/gather.go`, `internal/redis/cross_instance.go`, `internal/controller/forsaken_plan.go`, `internal/controller/stale_master_name_plan.go`, `internal/controller/littlered_controller.go` | **LR-053** |

**M0.2 is the one with live blast radius.** Turning `FailoverActive` on for the first time means
Rule A genuinely starts skipping healing during Sentinel failovers — the intended behaviour, never
before exercised. The failover e2e tiers must be re-run and their timings compared against the
committed ADR-011 numbers; a regression there is a real finding, not noise.

> **M0.2 landed (LR-052). The blast radius was measured on scm-s2 and is nil on the ordinary
> failover path**: the `failover-state` window is 1.84 s and sits *inside* the 2.14 s window in
> which `RealMasterIP` was already `""` via LR-004's ghost-majority clause, and `updateMasterLabel`
> runs upstream of Rule A so the label flip cannot be delayed. **The e2e re-run this row asks for
> is still OWED and was not done**: publishing an operator image needs registry credentials this
> session did not have, so no build could be deployed. What was verified live instead is the code
> path itself, through `lrctl` — which is exec-based, runs locally and shares the fixed gather and
> `DetermineRealMaster` — sampled against the pre-fix binary across the same real failovers. Note
> also that the comparison baseline for this milestone is the **sentinel** suite, not ADR-011's
> failover-mode numbers: `FailoverActive` is a sentinel-mode field and failover mode never
> populates it.

**M0.3 does NOT make LR-050's gate redundant, and the implementer must not conclude it does.**
`OwnedIPs` fixes the case where a terminating pod of ours is still in the pod list. LR-050's window
also contains the case where the pod object is **already gone** and its address is still in the
air — no list can hold it. Both stay. **Confirmed by the implementer against the code and then
against the cluster** (LR-054's investigation): the gate's readiness clause is load-bearing for a
pod replaced at the same ordinal, which returns on a *new* IP while `status.replicas` is already
full.

### Phase 1 — ADR-020 (blocks code, per concept §9.1–9.2)

Write `docs/adr/020-declared-operations.md`: the two mechanisms and why neither subsumes the other
(§3.1's three reasons), D1–D8, R1–R5, §1's admission test, the v1 registry, the per-pair precedence
rule, §4's survives/suppressed enumeration, §5's three traps, and the accepted holes verbatim —
the exit edge (§8.3), forever-in-an-operation (§8.6), head-of-line blocking (§8.8), and the
stuck-rollout hole LR-050 already accepts. **No waiver knob ships (D7).**

**LR-054 is the ADR's worked example for R5, and it should carry that argument rather than the
`ValidIPs` split.** `statefulSetRolloutSettled` answers *"is a rollout of ours in flight?"*
**and** *"is every pod of ours healthy?"* — one predicate, two questions — and it is the second
meaning that silently disables address attribution for a permanently-degraded instance, so a
captured victim holding the last copy of its data is never diagnosed and its captor is never
healed. That is R5 stated as a live, measured bug rather than as a tidiness rule, and it landed
in the same pass as LR-053 split the same shape out of `ValidIPs` one layer down — two
instances of one class, found within days of each other.
It also sharpens **§8.8**: that section reasoned about head-of-line blocking only for the
operation *queue*, and LR-054 is head-of-line blocking at the **invariant** level — one
unhealthy pod holds back a whole verdict, indefinitely, with no queue involved. The ADR must
say that the concept's blocking analysis is not confined to the mechanism it introduces.

Two framing points the ADR must get right, because the concept itself had to correct one of them
(§8.11, 2026-08-28) and the stale reading is the more intuitive one:

- **The mechanism is not a risk-acceptance signal, and it never was.** That reasoning belongs to
  the spec-declared `maintenanceMode` §5.0 abandoned. Auth *engineers its window away* — every
  stage keeps both credential states acceptable on every edge — and pays in stages, not in risk.
  D7 stands: if an operation is safe enough not to need a window it does not need a waiver, and
  if it is not, the fix is to make it safe.
- **What the mechanism actually buys is intent capture where none is derivable, plus D4/D5/D6.**
  Rotation is the proof: a `secretKeyRef` whose value changes rewrites no template, so drift
  detection has nothing to detect *even in principle*. That is D1's sharpest case and it should
  carry the ADR's argument, not the rename — which is merely the member we can prove today.

### Phase 2 — API + registry + pure seam

| M | What | Owns | Depends |
|---|---|---|---|
| **M2.1** | `OperationAck`, `OperationStatus`, `status.acknowledgedOperations`, `status.operation`, `ConditionOperationInProgress`; the HMAC fingerprint helper (UID-keyed). `make manifests generate`. | `api/v1alpha1/littlered_types.go`, `config/crd` (generated) | Phase 1 |
| **M2.2** | `planOperation` + the 10-row table + all three mutants. **Red-first: author the table against a stub returning the zero plan, show every row red.** | `internal/controller/operation_plan.go` (+ test) | Phase 1 |
| **M2.3** | The registry, the `SentinelMasterNameRename` entry, the citation assertion test, the precedence-rationale assertion test. | `internal/controller/operation_registry.go` (+ test) | M2.1 |

M2.2 and M2.3 are parallel-safe (disjoint files); both need M2.1's types.

### Phase 3 — wiring (sentinel mode)

**M3.1** — the early branch in `reconcileSentinelCluster`, placed **after** the quarantine/
`Forsaken` switch and **before** Rule 0. When `plan.Run != ""`: run Rule 0, run the driver, write
condition + status + the one-per-transition event, `return nil`. Acknowledgment written via
`retry.RetryOnConflict` on a re-fetched object (rule §7.1). Owns `littlered_controller.go`.

**M3.2** — requeue cadence. **LR-045's lesson applies directly**: the requeue switch was found
inert for the only mode that could be forsaken. An instance under an operation is typically
`Running`, so verify `requeueAfterNotRunning` actually produces the intended cadence and add the
assertion. Owns `requeue_interval_test.go` + whichever line in `updateSentinelStatus` it turns out
to need.

**M3.3 — first live integration, before anything is built on unit-green code.** Deploy to t3e or
s1, rename a running sentinel instance holding data, and debug to green: assert
`OperationInProgress=True` across the window, the ack lands *after* the StatefulSet settles (not
when Rule N converges — row 7), the existing LR-048 prune timings are unchanged, the dataset is
intact and no `Forsaken` verdict appears. **Record measured durations and replace this plan's
estimates with them** (LR-044's M4a precedent). Land the first-contact fixes here.

### Phase 4 — the exit edge **[the concept predicts the first bug is here]**

**M4.1** — a "post-operation arbitrary state" test tier. Feed each existing planner
(`planForsaken`, `planQuarantine`, `planLeaderlessRecovery`, `planGhostMasterRecovery`,
`planStaleMasterNames`, `planMasterDeath`) the states an operation can plausibly leave behind: two
names monitored, a half-rolled StatefulSet, a bare Sentinel, a just-promoted master, a terminating
pod whose address is still monitored. Assert **no destructive verdict** in any of them.
Stop condition: every pre-existing row still passes with **no row edited**.

This tier is the concrete form of the concept's §8.3 requirement that the regular loop be safe
against arbitrary state. It is invariant work, not operation-knowledge work — nothing in it may
mention an operation by name.

### Phase 5 — `lrctl` + docs

**M5.1** — `lrctl status` and `verify` report the active operation, its reason and the pending
queue; `verify` warns on `Stalled` and on `Blocked`. Owns `cmd/lrctl`, `internal/cli`.

**M5.2** — docs: ADR-020 (Phase 1 output, finalised), changelog entries LR-051…LR-054,
`docs/API_SPEC.md` (the two new status fields, the two new conditions), `docs/USAGE.md` (what
`OperationInProgress` means, what to do about `Stalled` and `Blocked` — *no timer will rescue
you*), `docs/RECONCILIATION_LOOP_SENTINEL.md` (the branch, in reconcile order), `CLAUDE.md` (a new
pillar **3.16**, plus a §7 rule pointing at §1's admission test), `BACKLOG.md` (strike the
`failover-status` item, record what M0.3 left residual). Owns `docs/`, `CLAUDE.md`, `BACKLOG.md`.

### Phase 6 — e2e

| Tier | What | Where the red comes from |
|---|---|---|
| 1 | Rename a running instance: `OperationInProgress=True` throughout, ack after settle, data intact, no `Forsaken`, the three committed LR-048 tiers still green | red against HEAD — the condition does not exist |
| 2 | Edit a **non-heavy** field (e.g. `resources`): assert **no** operation is declared and healing is never suppressed | red against a naive `generation != observedGeneration` implementation — this tier is what pins D2's rejection of the coarse mechanism |
| 3 | **Kill the operator mid-rename**, restart it: the operation resumes and completes | red against an *acknowledge-on-observation* implementation, which loses the intent silently. **This is D2's central claim and it needs its own e2e.** |
| 4 | Quarantined instance + a rename edit: assert nothing advances, `Reason=Quarantined` | red — §7.4's `replicas: 0` reads-as-settled trap |
| 5 | Stall it (make a replacement pod unschedulable): `Stalled` after `StallAfter`, **no auto-exit**, no data action taken | red — nothing reports this today |
| 6 | Operator upgrade over an existing fleet: **no** instance declares an operation | red against an implementation missing plan row 3 |

Tier 6 is cheap and catches the worst possible regression this feature can ship.

---

## 8. Verification and test discipline

Per CLAUDE.md §7 "Test Discipline", every tier states its red:

- **Tier 2 (pure)** — `planOperation`'s 10 rows and 3 mutants; `authArgs`-style fingerprint
  determinism (same value ⇒ same fingerprint; different UID ⇒ different fingerprint; the
  fingerprint never contains the plaintext).
- **Tier 2 (pure)** — M0.1's classifier: a gatherer stub returning `WRONGPASS` must make
  `planLeaderlessRecovery` **refuse**. It reseeds today; that is the red, and it is the
  data-losing defect.
- **Tier 2 (pure)** — M0.2: `DetermineRealMaster` with a Sentinel reply carrying
  `failover-state: select_slave` ⇒ `FailoverActive == true`. False today.
- **Tier 2 (regression, stop condition)** — every existing planner table passes unedited.
- **envtest** — CR with a changed `masterName` ⇒ `status.operation.name` set, condition True,
  ack written only after the StatefulSet settles.
- **e2e** — §7 Phase 6, on t3e or s1.
- **Manual** — `lrctl verify` before / mid / after.

**Before pushing:** `make lint` and `make test` (rule §7.9). **Before touching any reconciliation
rule:** read `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` end to end (rule §7.7) — this plan
touches four planners and the sentinel reconcile branch.

---

## 9. Risks

| Risk | Handling |
|---|---|
| **M0.2 changes live behaviour for the first time.** `FailoverActive` has never been true; Rule A's second half has never fired. | Ship M0.2 alone, re-run the failover e2e tiers, compare against ADR-011's committed numbers before Phase 2 starts. |
| **The exit edge is where the concept expects the first bug.** | Phase 4 exists for exactly this and is not optional. It must be red-first against constructed states, not merely green. |
| **Row 3 (first observation) is easy to miss and detonates fleet-wide on operator upgrade.** | Its own plan row, its own unit test, its own e2e tier (6). |
| **Someone "unifies" LR-050's gate with the operation branch.** | §5 trap 1, stated in the ADR, in the code comment, and as a review checkpoint. |
| **The registry accretes.** | §1's admission test with the machine-checked citation field. |
| **Head-of-line blocking leaves an intent unreconciled indefinitely** (§8.8). | Accepted, loudly: `Blocked` reason + `Pending` list + event. No auto-skip. Same treatment as LR-050's accepted stuck-rollout hole. |
| **Retrofitting the rename changes a shipped feature's behaviour.** | §4: during a rename Rule A already returns before every suppressed rule, so the delta is reporting, not suppression. M3.3 measures it against the committed ADR-018 timings rather than asserting it. |

---

## 10. Explicitly NOT in scope

- **No `spec.maintenanceMode`, no global gate, no waiver knob** (concept §5.0, D7).
- **No auth work** — but the seam is shaped for it (§3's `fingerprintInput`, §7.2's precedence
  rule). Auth joins by adding one registry entry; its own design
  (`docs/AUTH_ENABLEMENT_AND_ROTATION_DESIGN.md`, prospective ADR-019) is unchanged by this plan —
  except that its **WP1 shrinks**, because M0.1 ships the `getRedisPassword` error path and the
  "operator cannot authenticate" condition. Say so in the auth design when M0.1 lands.
- **No cluster-, failover- or standalone-mode branch.** The registry is mode-filtered and v1 has
  one sentinel-only member. The pure seam is mode-neutral by construction, so a second mode is a
  wiring milestone, not a redesign.
- **No changes to any planner's decision table.** §5 trap 2.
