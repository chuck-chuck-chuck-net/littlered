# ADR-016: Forsaken-Gated Quarantine (Sentinel Mode)

## Status

Accepted and implemented (branch `e2e-0821`). Changelog entries **LR-044** (decision layer,
wiring, live verification, committed coverage) and **LR-045** (the requeue-cadence correction
LR-044's verification found). Builds on **LR-042**, which named the `Forsaken` verdict this
quarantine is gated on.

> ADR number: 016, after 015. 010 and 012 are claimed on sibling branches (010 the deferred
> ghost-replica prune policy, 012 multi-site site-loss takeover) and 010 is still prospective —
> the gaps are allocation, not availability.

## Context

A cross-instance Sentinel capture (ADR-015, LR-039) has two sides, and only one of them is loud.

LR-042 dealt with the loud one. A captured instance is unrecoverable by design — ADR-015 §9.2
declines recovery because the dataset is flushed roughly a second after the `SLAVEOF` and the
operator structurally cannot win the reclaim — but nothing told the operator that, so it kept
treating the instance as converging and re-derived the same dead end 30 times a minute forever.
The `Forsaken` verdict stopped it: while it holds, the operator does not heal the instance, logs
once per transition rather than per reconcile, and leaves it loudly `Ready=False`.

That closed the operator's own churn and left LR-042's own gap open, which LR-042 recorded as a
known gap rather than fixing: **the captor is silently healthy.** Verified on a live pair — the
victim reports `Initializing` / `Ready=False`, while the instance whose master was adopted reports
`Running` / `Ready=True` / *"All Redis and Sentinel pods are ready"* with its Sentinels holding
five replicas where two were deployed, three of them the victim's pods. Its own topology is
intact, so no healing rule fires. But its Sentinel **failover-candidate set is poisoned**, and on
its next master death Sentinel can promote a *foreign* pod as its master. `lrctl verify` flags it
(`FAIL`); the operator says nothing. So the situation LR-042 stabilised is one in which a
worthless instance is parked while it keeps damaging a healthy neighbour.

The captor must not be operated on directly. Its Sentinels are not confused — the victim's pods
are *genuinely* replicating from its master, so its master's `INFO replication` genuinely reports
five replicas. Sentinel rebuilds its replica list from the master's `INFO` (replicas never
self-announce, LR-013), so a `SENTINEL RESET` on the captor clears a list that repopulates seconds
later — and RESET is the LR-024 hazard. Surgery on the captor cannot work while the cause is
alive.

## Decision

Take the victim's pods away, let the captor heal through a rule that already exists, then give the
victim back empty.

1. **Quarantine.** While the instance is forsaken, its desired **Redis and Sentinel** replica
   counts are both 0. Sentinel is not optional: the victim's *sentinels* publish hellos on the
   captor's master's channel under the shared name, so the captor learns them as peers. That is
   the `num-other-sentinels` inflation which distorts the captor's quorum math and puts foreign
   sentinels in its elections.

2. **The captor heals itself, through Rule D.** Once the victim's pods are gone, the captor's
   master `INFO` reports the right count, the departed entries become ordinary `s_down` ghost
   replicas (a dead replica never ages out, LR-024), and Rule D's gate chain passes — living and
   reachable consensus master (LR-008), ≥1 healthy known replica (LR-011), K8s-grounded wholeness
   judged against the *captor's own* expected pod count (LR-013). Its `SENTINEL RESET` prunes
   them. The operator never speaks to the captor about the capture.

3. **Settle for 120s, then release.** The pods come back with every Sentinel bare, no master and
   zero data holders — Rule L's no-data reseed signature, which needs no opt-in (LR-015). So
   "re-bootstrap" is handing Rule L a state it already handles: no new bootstrap machinery, and
   `bootstrapRequired` is not re-armed.

4. **Bounded, then latch.** `status.quarantineAttempts` counts attempts. N = 2 normally, N = 1
   when the instance's own configuration is what makes capture reachable (auth disabled **and**
   the effective master name is the shared legacy `mymaster`). At the limit the instance stays at
   zero rather than being released again.

5. **Refuse rather than quarantine when data could be lost**, on two clauses of its own
   (`quarantineDataRisk`) on top of the verdict — see Rationale.

Default-on, with no opt-out knob: the instance is provably empty and unrecoverable while actively
damaging a neighbour, and an opt-in nobody sets protects nobody.

The decision is the pure `planQuarantine` (`internal/controller/quarantine_plan.go`), gated on
LR-042's `Forsaken` verdict (`planForsaken`).

6. **The verdict carries an evidentiary floor** (added by LR-056, 2026-09-04): a **majority of
   the Sentinels this instance deploys** must be monitoring and must agree. Clause 2 is
   documented as unanimity and this ADR's own verification relied on it — the M4a injection had
   to hit all three of the victim's Sentinels — but clause 1 asked only *"at least one Sentinel
   monitors something"*, and a Sentinel that is unreachable or bare left the **denominator**
   rather than the vote. With two peers silent, one Sentinel's word armed a verdict that takes
   both StatefulSets to zero on EmptyDir. The floor is keyed on the deployed count
   (`sentinelProcessReplicas`, fixed at three), which is LR-013's wholeness gate reused, and
   deliberately **not** on `spec.sentinel.quorum` — that field has no `Minimum`, so `quorum: 1`
   would make the floor vacuous.

## Rationale

### This is not a reversal of ADR-015's declined recovery

The most important paragraph in this document, because the two look alike and are not.

ADR-015 Alternative J declined **reclaiming mastership** — splitting LR-024's
`HasHealthyKnownReplica` veto on the down flag, electing a survivor and re-`MONITOR`-ing it — on
two grounds: nothing survives to salvage, and the operator cannot outbid the captor's config
epoch, because `SENTINEL MONITOR` creates the master entry at `config_epoch = 0` and loses to the
next hello ~2s later, while each `REMOVE` + `MONITOR` wipes that Sentinel's replica list, which is
LR-013's hazard exactly.

Quarantine fights neither ground. It **salvages nothing** — the victim comes back empty — and it
**never contests the epoch**: no `MONITOR`, no `REMOVE`, no Sentinel command about the capture at
all, on either side. §9.2 itself names the outcome quarantine produces as the only achievable one:
*"a recovery restores an empty instance, which is precisely what deleting and recreating the CR
already achieves"*. Quarantine is therefore the **automation of the accepted fallback**, not the
rejected reclaim. And the reason it is worth automating is not the victim at all — it is the
healthy neighbour the victim stops damaging.

### Data safety, and the refinement that saved the clause from being inert

The quarantine deletes pods, so it carries its own data clauses.

`atRisk` — a reachable pod holds keys that are **not** explained by the capture. Keys on a pod
that is a link-`up` replica of the captor's master are the captor's own dataset, replicated in;
the original is still on the captor, so discarding the copy loses nothing. Keys anywhere else may
be the only copy in existence. This is what makes the quarantine provably lossless rather than
lossless-by-argument, and it independently closes the one path §9.2 could only rule out on timing
("replication blocked before the sync starts") — it closes it whatever the timing.

**The clause was first specified as "all reachable pods hold 0 keys", and that literal formulation
would have been inert in the case that matters most.** A captured victim whose pods completed
their full sync holds *the captor's* keyspace, so `Keys > 0` on every one of them and a 0-keys
gate would never let the quarantine fire. The field incident only showed 0 keys because an RDB
version mismatch broke the sync, and the capture analysis is explicit that this was luck: *"The
loud failure was luck. The RDB version mismatch is the only thing separating this from a silent
one"* — where a version-compatible victim instead serves the captor's keyspace to its own clients
with no alarm at all. The silent case is the common one and it is the one with keys everywhere. So
the discriminator has to be **whose** data the keys are, not whether there are keys. The M4a live
run reproduced exactly that shape (all three victim pods link-`up` replicas of the foreign master,
their own 10 keys flushed and replaced by the captor's 100), so the inert formulation is not a
hypothetical.

`unverified` — a pod that cannot be **proven** empty. Keyed on **kubelet readiness**, not on the
operator's dial: LR-023 settled which signal to use for this judgement, since a not-Ready redis in
a pure in-memory instance holds no data and readiness is blackhole-proof where a remote dial can
be fooled (LR-017). So only an unreachable pod the kubelet still calls Ready is unverified. Keyed
on gather reachability instead, a permanently crash-looping or blackholing pod would be unverified
forever and would veto the one action that helps the neighbour — the pod that can never answer
holding the captor dirty for exactly as long. A pod the kubelet has no view of counts as Ready:
unknown readiness is not evidence of emptiness.

A **terminating** pod is outside the gather and therefore invisible to both clauses. Judged
harmless rather than overlooked: its RAM is gone whatever the planner decides, and widening the
gather to see it is strictly worse (LR-038 — a terminating pod in the gather reads as live
topology to every other rule).

"Never quarantine an instance that still has a master of its own" needs no clause: `planForsaken`
clause 4 already refuses the verdict while any reachable Redis pod of ours is a master, and the
asymmetry is structural — a merge resolves by config epoch, the winner keeps its master, the
loser's pods all become replicas of the winner, so "no master of its own" *is* the loser.

### The 120s settling period — the number stands, its original derivation does not

It was reasoned as: the captor is `Running`, so it reconciles on the steady 30s interval, so allow
~4 steady passes for Sentinel to re-read its master's `INFO`, for the departed entries to become
`s_down` ghosts, and for Rule D's gate chain to pass. 120s also matches the existing cluster-mode
precedent `status.cluster.wipeDeadlockSince` (LR-023), and it shrinks the warm-IP window: while
the captor still lists the victim's *old* addresses as `s_down` replicas, a fresh victim pod
landing on one of those recycled IPs is the very coincidence that starts a capture (LR-039).

**The M4a live run falsified the premise.** The captor briefly *leaves* `Running` when its
Sentinel-known replica count collapses (`reasons: Sentinel knows 0/2 replicas as healthy`), so it
is polled **fast** and heals in 5-12s, not in four steady passes. The real bound is the ghost
reaching `s_down` (`down-after-milliseconds`), not the requeue cadence.

So 120s is **generous rather than tight**, and no change is proposed — a settle that overshoots
costs the victim availability it does not have anyway, and the warm-IP argument prefers the longer
window. But the stated reasoning must not stand as written, and this is what is true.

### `status`, not an annotation, for the attempt counter

ADR-006's "nothing is persisted" was about an internal *engine capability* (async slot migration)
that a free gather-time probe could answer. An attempt count is a monitoring metric. The governing
split is **annotation = intent, status = monitoring** (pillar 3.14), and this is monitoring.
`status.quarantineAttempts` is also the clearest operational signal the state has: "quarantined
twice" says *your configuration is the problem* better than any condition message, which is why it
is surfaced on the `Forsaken` condition's reason (`Quarantined` / `QuarantineLatched` /
`QuarantineRefusedDataPresent` / `QuarantineRefusedDataUnknown`) and as one Warning event per
transition rather than per reconcile.

The counter clears on **success** (`Phase == Running`), never on the signature being absent. That
is forced: the verdict provably self-clears once the pods are gone — with no pods there is no
reachable *monitoring* Sentinel, so `planForsaken` clause 1 fails and `clearForsaken` runs on
every pass of the quarantine — so a counter cleared on absence would reset every cycle and never
latch. `status.quarantinedSince` is the only thing that holds the state, which is also why an
armed quarantine is decided **first and without reference to the verdict**.

Consequence, recorded as an open owner decision rather than settled: the bound is therefore
**per-episode, not per-lifetime**. An instance that genuinely re-bootstrapped, served, and is only
*then* recaptured gets a fresh budget, so the latch bites when recapture happens *before* the
instance is healthy — the intended "self-heal the lucky case, latch when it is not luck" shape —
while an instance that oscillates through healthy states between captures keeps re-rolling.

### Why zero must be the desired state at build time, not a scale-down

Both sentinel StatefulSets are applied through a server-side apply carrying `ForceOwnership`, so
whatever `.Spec.Replicas` the build function computes is authoritative on every pass. Scaling the
live object out of band — a `Scale` write, or a patch from the healing step — is force-overwritten
by the next reconcile. And the two applies run **early**, well before `reconcileSentinelCluster`
where the verdict lives, so deciding late and acting out of band yields a **0→3→0 flap every
pass**: the applies force 3 back, the healing step takes it away, and in between the pods are
genuinely scheduled, come up, and rejoin the captor's quorum — re-polluting the neighbour the
quarantine exists to protect. Strictly worse than the churn LR-042 removed.

So `sentinelDesiredReplicas` runs before either apply and computes the armed quarantine from
status alone. Arming stays after the gather, where the verdict and the data clauses live. Both
edges are consequently monotone — 3→3→0 and 0→0→3, never 0→3→0 — at a cost of one interval of
latency per edge on a 120s settle.

The rejected alternative was hoisting the gather above the StatefulSet applies. It needs no
pre-gather contract and avoids a second decision point, but it reorders the sentinel reconcile
flow — the flow whose ordering is load-bearing in LR-013, LR-015, LR-024, LR-040 (*"Rule 0 runs
before Rule A"*) and LR-041 — and LR-038's lesson applies directly: moving or widening the gather
changes what every rule sees at once. The planner already supports the pre-gather decision by
design, so the reorder buys nothing that is needed and risks something that is.

## Consequences

- A capture now resolves without a human: the captor is clean within ~5-12s of the victim's pods
  leaving, and the victim is serving again (empty) about 4 minutes after the capture. Measured
  end-to-end: 3m51s / 3m58s (M4a), 3m41s (M4b), 3m40s under full-suite load.
- **A quarantined instance looks alarming and is meant to.** Both StatefulSets read
  `.spec.replicas: 0`, `Ready=False`, `Forsaken=True/Quarantined`. Documented in the USAGE
  runbook, because an operator who finds it will otherwise read it as the operator having deleted
  their instance.
- Two new status fields (`status.quarantinedSince`, `status.quarantineAttempts`) and four new
  `Forsaken` reasons. No new RBAC — the reconciler already owns the StatefulSets it applies.
- **Manual release of a latched instance is clearing `status.quarantinedSince` and
  `status.quarantineAttempts`** — not editing the StatefulSets, which are re-applied from this
  decision every pass. Clearing only the `Forsaken` condition does *not* release it; the marker
  holds the state, by design.
- **The residual we accept: cycling through zero pods releases pod IPs**, and ADR-015 §9.4's
  address-adoption path is reopened for that window — a fresh victim pod landing on a recycled IP
  the captor still holds is the coincidence that starts a capture. Accepted, not closed. The
  alternative leaves a healthy neighbour permanently poisoned; the settling period shrinks the
  window (the captor prunes those addresses before the pods return); and the N=2 latch bounds the
  number of dice-rolls.
- **The floor's cost, and why it does not blunt this ADR** (LR-056). Making the verdict harder to
  reach trades a false-positive deletion against leaving a genuine captor poisoned for longer,
  which is the trade this whole document is about — but the trade is measured, not open. Every
  capture on record is unanimous across all three of the victim's Sentinels: the field incident
  (LR-039), M4a twice, and the e2e helper, which asserts exactly that before any spec proceeds.
  And a capture the floor refuses is **still diagnosed**: `lrctl verify`'s `DetectCrossInstance`
  has no floor and must keep none, since LR-039 built it to fire on a *partial* capture. **A
  floorless diagnostic and a floored verdict, because only one of them deletes pods.** The
  failure direction is LR-047's — withholding is bounded and non-destructive, while a false
  verdict is unrecoverable by the sole-authority invariant above.
- A quarantined instance can never report `Running`, because `allReady` requires `Redis.Ready > 0`
  — so the quarantine cannot reset the counter that is counting it, and the latch still bites.
- The release lands up to ~150s after arming rather than 120s, since a forsaken instance is polled
  at the steady 30s interval and 30s granularity on a 120s timer is immaterial. This is only true
  as of LR-045: LR-042's steady-cadence promise was inert for sentinel mode, the one mode that can
  be forsaken, so M4a measured the release at 120-122s on fast polling. The number was
  accidentally right and its reasoning was wrong; LR-045 makes the reasoning true.

## Verification status

**The load-bearing inference is verified live.** That the captor heals through Rule D was an
*inference* from three independently-documented gates (LR-008, LR-011, LR-013), and LR-044 states
plainly that it must be verified before the change could be called done. M4a verified it on t3e
(2026-08-22, operator `01e2df3`), twice, from the operator's own log lines on the captor 2-4s
after the victim's pods left:

    Ghost node detected in Sentinel topology  ip=10.233.192.74 flags=s_down,slave sentinel=captor-sentinel-0
    Issuing SENTINEL RESET to clear ghost nodes from topology  master=lr044.shared reachableRedis=3

`reachableRedis=3` is LR-013's wholeness gate passing against the captor's own expected pod count,
exactly as the inference required. Sentinel counts went `num-slaves 2 → 5` and
`num-other-sentinels 2 → 5` on capture, then back within ~12s (cycle 1) and ~5s (cycle 2) of the
pods leaving; the captor's own 100 keys were intact on all three of its pods at every check, and
`lrctl verify captor` went from `FAIL` (*"reports 5 replicas; 2 were deployed"*) to
`[OK] No foreign Sentinel contact observed`.

Also confirmed: the wiring holds with **no flap** (sampled at 1s across both edges — `61 × 3, then
104 × 0, then 79 × 3`, one transition each way at a single sample boundary); and the re-bootstrap
is genuinely Rule L, named in the CR rather than guessed (`LeaderlessRecovery=False/Reseeded`,
*"no data present, seeded victim-redis-0 as master"*). It **cannot** be intercepted by Rule 0 /
LR-008 the way LR-017 documented as a hazard for that tier, because the victim's Sentinels come
back genuinely bare and there is no master anywhere to re-register them onto.

Timings, cycle 1 → cycle 2:

| edge | cycle 1 | cycle 2 |
|---|---|---|
| capture → first `Captured` verdict | 30s | 31s |
| verdict → armed (`forsakenCooldown`) | 31s | 31s |
| armed → both `.spec.replicas: 0` | same pass | same pass |
| replicas 0 → pods gone | ~7s | ~4s |
| pods gone → captor's Sentinels clean | **~12s** | **~5s** |
| armed → release (`quarantineSettlePeriod`) | 122s | 120s |
| release → victim has a master (Rule L) | ~39s | ~38s |
| **capture → victim serving again** | **~3m51s** | **~3m58s** |

**What is not covered, stated plainly.**

- **A fourth tier was added by LR-056 and it is the only one here with an honest red**
  (`> A capture reported by ONE Sentinel while its peers are bare`): observed RED on t3e against
  the pre-fix operator — *"a capture verdict was armed on ONE Sentinel's word"*, 39.4s in, i.e.
  one `forsakenCooldown` plus a pass — and green after, with the whole Describe re-run green on
  the fixed build (4 of 4) as the over-suppression check. It also carries the LR-054
  discriminator (the victim's Redis StatefulSet must be 3/3 Ready), without which a refusal
  could be credited to LR-050's rollout gate rather than to the floor.
- The three original e2e tiers (`Sentinel Forsaken-Gated Quarantine` — full cycle,
  `HoldDataPresent` refusal, `Latched`) are **green from birth**. They assert behaviour that
  already shipped and was already verified by hand in M4a, and the honest red was not obtainable:
  building the pre-LR-042 operator and deploying it to watch tier 1 fail was attempted and blocked
  by the environment's permission policy. What they carry instead is two intermediate positive
  controls (the captor's polluted `5`/`5` asserted *before* its healed `2`/`2`; the `PUBLISH` reply
  asserted `1`) plus tier 2's precondition re-asserted inside its own `Consistently`, so a green
  cannot be earned by the staged state quietly decaying into a safe one.
- **`HoldDataUnknown` has no e2e.** Staging it needs a victim pod that is Ready per the kubelet
  while being unreachable from the operator — which is the whole content of the clause — i.e. a
  traffic-shaping capability this suite does not have. Deferred by decision to a `feat/e2e-harness`
  branch; a pointer comment sits where the tier would go and the decision matrix stays covered by
  `TestQuarantineDataRisk` / `TestPlanQuarantine`.
- A *partial* capture (1 of 3 Sentinels) producing no verdict is also uncovered.
- The pure seams are red-first: `TestPlanQuarantine` (11 rows) red on 11 of 11 against a zero-value
  stub, with an "always quarantine" mutant failing exactly the four rows that must never scale to
  zero; `TestQuarantineDataRisk` red on 3 of 5, plus one further red on the readiness rekey;
  `TestQuarantinedInstanceNeverGetsItsPodsPutBackByTheBuilders` — the flap guard, written as a
  *sequence* because the failure mode is an interleaving — red on the three passes that would have
  re-created the pods.

## Alternatives considered

**Operate on the captor** (`SENTINEL RESET` there, or prune the foreign replicas). Rejected: its
Sentinels are not confused, so a RESET clears a list that repopulates from the master's `INFO`
seconds later, and RESET is the LR-024 hazard. Surgery on the captor cannot work while the cause
is alive.

**Reclaim the victim's mastership** (ADR-015 Alternative J). Still rejected, unchanged, and
quarantine is not a back door to it — see Rationale.

**Delete the victim's CR.** Equivalent in outcome and it is what the runbook already says, but it
is not something an operator may do to a user's object. Quarantine is the reversible form: the
workloads go to zero, the CR and its spec stay, and one status edit brings it back.

**Unbounded retry.** Rejected: every recapture re-pollutes the captor, so an unbounded retry does
not merely fail to fix the victim, it repeatedly degrades a healthy neighbour.

**An opt-in knob.** Rejected: the instance is provably empty (by the data clauses) and
unrecoverable (ADR-015 §9.2) while damaging a neighbour, and an opt-in nobody sets protects
nobody.

**Weakening the `Forsaken` verdict to carry the data clauses.** Rejected: LR-042's verdict answers
"is this instance still ours to manage", and an instance holding data is still not ours to manage.
Weakening it would put the operator back to thrashing the instance. The clauses live in the
quarantine planner, which is the thing that deletes pods.

## References

- `CLAUDE.md` pillar 3.15, pillar 3.7 (the capture problem), §9
- ADR-015 (per-instance master name; Alternative J, and the Consequences addendum this ADR adds),
  ADR-005 (Rule L, the re-bootstrap), ADR-003 (Rule D, the captor's healing path), ADR-008
  (kubelet readiness as the blackhole-proof data-safety signal), ADR-006 (the status/annotation
  split this departs from, and why)
- `docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md` §9.2 (declined recovery, and its addendum),
  §9.4 (the address-adoption residual this reopens per cycle)
- `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` — **LR-044**, **LR-045**; LR-042 (the verdict),
  LR-039 (the incident), LR-008 / LR-011 / LR-013 (Rule D's gates), LR-015 (Rule L), LR-017 /
  LR-023 (dial versus kubelet readiness), LR-024 (why RESET on the captor is a hazard), LR-038
  (what the ground truth is allowed to contain)
