# Cluster Image Upgrade Lost a Whole Shard — Problem Report

**Status:** OPEN — **the root cause is NOT found**, and no operator change is proposed until a
reproduction with artefacts exists (§8). The two **test** defects in §3 are fixed; the **operator**
defect is not. Reserves **LR-065** (LR-063/064 are claimed on `feat/multi-site`; the changelog
entry is owed *with* the fix, not before it).
**Opened:** 2026-10-05, from a full-suite run on t3e. **Last revised:** 2026-10-11.
**Build:** `4133938` (`main`), operator image `sha-4133938`.
**Affected:** cluster mode, operator-triggered rolling update — i.e. pillar 3.12 / ADR-017 /
LR-047, on a build that carries the state gate those closed.
**Why it matters now:** the failing tier is the *acceptance test* for a feature queued for
release (#95, "prove Redis image upgrades keep the data"). The feature it was written to prove
is the one it caught breaking.
**Release decision (2026-10-05):** #95 is **held back from the 2026-10-06 release**, which ships
as 0.4.1 with dependency updates and little else. The tier is not shipped as evidence that image
upgrades keep the data, because on its one contact with a real cluster it showed they did not.

---

## 0. For the reader — what this is, and what it asks of you

**The tier did its job.** #95 is test-only (three files, no operator code) and on its first
contact with a real cluster it caught a **pre-existing** cluster-mode defect that every other
signal the product exposes reported as success. Nothing here is a criticism of the tier; the two
§3 corrections are about what it *said* when it failed, not about whether it should have failed.

What has happened since the first draft, all on 2026-10-05, after reading the reconciliation
ledger end to end, ADR-017's implementation and its two wiring halves, and the Kubernetes
StatefulSet controller at the version in use:

| | |
|---|---|
| **§10 is new and is the next thing to run** | the **platform-vs-application** campaign — written down *before* it runs, with its arms, its control and its stopping rule. It is the owner's framing: establish whether the loss is in Kubernetes/littlered or in redis 7.4.3→7.4.8, before anything else. §10.1 is the evidence the failing run already carries, and it points hard at the platform. |
| **§6 H2's evidence is REFUTED** | the floor arithmetic in the first draft was wrong (Kubernetes source cited). The correction **removes a diagnostic**; it does not expose a defect, and a faster roll would not have been one. |
| **§6 gains H5 and H6** | two candidate mechanisms, both argued and neither established. |
| **§3's two test defects are FIXED** | red-first, test-only, `make lint` 0 issues. The second one was not in the first draft and is what makes H3 answerable at all. |
| **§9 is new** | two standing questions the incident exposed but does not own — including one declined decision, recorded so it is not re-proposed. §9.1 has since been **closed** (`5c8053d`). |
| **§3's fixes and §9.1's closure are Michael's** | `91af5eb`, `432f3a0`, `b37a58f`, `5c8053d`. An independent implementation of §3 converged on the same design and lost on three points; §3 records which and why, because the convergence is corroboration and the differences are the useful part. |

**What it asks of you:** sanity-check §10's experiment design — in particular the stopping rule
(a green arm proves nothing, and that is easy to forget once a run comes back clean) and the
`SEED` (an arm without it is a different experiment). The suite-time cost this section previously
asked about no longer exists: the shipped read-back is per tag, not per key. Everything else is
recorded, not pending on you.

---

## 1. Symptom, as the suite reported it

```
[FAIL] Redis Image Upgrade — Cluster Mode [It] keeps every key across a patch upgrade 7.4.3 -> 7.4.8
  Timed out after 180.007s.
  seeded dataset was not preserved across migration
  value mismatch for key {t4}mig:14
  Expected <string>:  to equal <string>: v-{t4}-14
```

One key. That reading is wrong, and the test is what made it wrong — see §3.

The instance was otherwise green throughout: `expectClusterServingAllSlots` passed, every pod
had been replaced (UID), every pod ran 7.4.8, the CR reported `Running`. The rollout itself
reported success: *`rolled 6 pods in 2m24s`*.

## 2. What actually happened: shard 2's entire dataset is gone

Reconstructed from the `Eventually` loop's own `kubectl exec` trace (`fail-1.log:3733-3820`).
The fixture seeds 20 keys per shard behind one hash tag each — `{t2}`→shard 0, `{t1}`→shard 1,
`{t4}`→shard 2 (`findHashTagInSlotRange` probes t1,t2,t3,… per `GenerateSlotRanges` entry).

75 GETs were issued across the 180s window. Every attempt ends on a `{t4}` key:

```
{t2}mig:2   {t1}mig:4   {t4}mig:6     ← attempt ends
{t2}mig:1   {t2}mig:14  {t2}mig:17  {t1}mig:1  {t1}mig:14  {t4}mig:7   ← ends
{t2}mig:16  {t1}mig:11  {t4}mig:2     ← ends
{t1}mig:5   {t1}mig:13  {t4}mig:4     ← ends
…
```

17 of the 75 GETs hit a `{t4}` key and **every one of them is the last GET of its attempt** —
17 occurrences over 180s at a 10s poll is exactly one per attempt. No `{t1}` or `{t2}` key ever
failed. Nine distinct `{t4}` keys are directly proven absent (2,3,4,6,7,9,11,14,18); the other
eleven were never reached, because the assertion aborts at the first mismatch in a randomly
ordered map.

**So: at least 9 of shard 2's 20 keys are gone, and the pattern admits no reading other than
all 20.** This is LR-047's failure mode — an entire shard's dataset destroyed by a rolling
update that reported complete success — reproduced on a build that ships `planShardRolloutPartition`.

It is also LR-047's *erasure*, unchanged: the operator healed the orphaned range onto a
reachable empty master (Step 3, `SafeMissingShardTarget`, correct by its own contract), so the
post-mortem topology is a healthy cluster serving 16384/16384 slots with one shard silently empty.
**A green topology is not evidence of data survival** — the sibling tier says so in its own
`By()` string, three minutes earlier in the same log.

## 3. The test hid its own blast radius

`verifyDataset` (`test/e2e/cluster_migration_test.go:889` **as of `4133938`** — the §3 fix below
has since moved every line number in this file) iterates a `map[string]string` and
fails on the first mismatch. Map order is randomised, so the reported key is arbitrary, and the
count is always one. It reported `{t4}mig:14`; the truth is a shard.

This is not cosmetic. "One key is missing" and "a shard died" lead to entirely different
investigations — the first reads as a replication tail, the second as a redundancy-gate failure.
Anyone picking this up from the summary line alone would have drilled in the wrong direction.

**The call, kept for the record:** fix this before analysing the defect — collect every mismatch,
group by hash tag, report counts per shard; and get the red by FLUSHing one shard after the tag
patch, then assert the message names all 20 keys and the shard rather than one key.

**✅ FIXED (test-only), `91af5eb` on `fix/cluster-dataset-report-and-gate-asserts`.** The
read-back collects **every** mismatch, groups by hash tag — i.e. by shard — and leads with the
blast radius, distinguishing an **absent** key (an explicit `Absent` field, not an empty-string
convention) from one holding a **wrong value** (LR-038: lost is not corrupt). An exec error is
returned rather than recorded as an absence that was never observed, and the caller retries the
whole attempt (LR-051: unreachability is not emptiness). `describeDatasetMismatches` is pure and
pinned by `test/e2e/dataset_report_unit_test.go`; the helpers live in
`test/e2e/dataset_report_test.go` rather than in the migration tier's file. The incident's own
shape now renders as:

```
20 of 60 seeded keys did not read back. Grouped by hash tag, i.e. by shard:
  {t4}: 20 of 20 seeded — 20 absent
      {t4}mig:1 absent
      ... (and 15 more)
```

**And it costs less than the version it replaced.** Keys sharing a hash tag share a slot, so one
`EXISTS` plus one `MGET` per **tag** is legal under a `-c` client — three execs for a 150-key
dataset instead of one per key — with the per-key absent/wrong classification running only on the
failure path, where the extra execs buy the split the report exists for. An earlier, independent
implementation of this same fix read per key and would have added ~45s to each migration tier;
that cost does not exist here, and the question of whether to accept it is moot.

*(Two implementations of this section were written independently and converged on the same
design, down to the names `datasetMismatch` / `datasetTag` / `describeDatasetMismatches` /
`verifyDatasetWithin`. The one described above won on three points — the explicit `Absent` field,
the per-tag read, and the file layout — and is the one that shipped.)*

**And a second defect in the same fixture, added 2026-10-05: the seed is never verified.**
`writeDatasetSpanningShards` (`test/e2e/cluster_migration_test.go:870`) issues 60 `SET`s and
returns the map. It reads nothing back, and it does not wait for replication. So the tier
**cannot distinguish "the upgrade lost it" from "it was never there"** — which is exactly the
question H3 asks, and the patch is issued 0.7s after the last `{t4}` `SET`. Assert the full
dataset present (and, for H3's sake, replicated) *before* patching, so a failure after the patch
is attributable to the patch. (The 60 `SET`s are this tier's figure; the same helper seeds 150 for
the migration tiers.)

**✅ FIXED (test-only), `432f3a0` on the same branch.** `writeDatasetSpanningShards` asserts the
full dataset reads back before returning, and the cluster image tier additionally calls
`expectShardDatasetReplicated`, which issues `WAIT 1 5000` on each shard's master and requires an
acknowledgement before the tag is patched — the direct answer to H3, and the cluster-mode twin of
the sentinel tier's `connected_slaves` wait. It is **loud rather than vacuous** if
`{name}-shard-K-0` is not that shard's master: Redis **refuses** `WAIT` on a replica
(`waitCommand` errors on `server.masterhost`, `src/replication.c`, identical at 7.4 and 8.0), so
the exec fails instead of quietly returning a count. Skipped at `replicasPerShard: 0`, where there
is no redundancy to wait for and the roll is ungated by construction (ADR-017 Decision 3).

## 4. What is NOT known

Stated plainly, because the useful part of this report is the boundary.

- **No operator logs for the window.** The run was `SKIP_OPERATOR_DEPLOY=true` without
  `DEBUG_ON_FAILURE`, so the namespace was torn down and the operator was at `--zap-log-level=info`
  (see §7). The decisive evidence — the `partition` transitions for `img-cluster-*-shard-2`,
  whether `ClusterRolloutBlocked` ever fired, whether `-shard-2-1` was a link-`up` replica of the
  slot owner when the cursor dropped to 0 — does not exist.
  **Note (2026-10-05): it would have existed at `info`.** `advanceClusterRollout` logs
  `Cluster rollout gate  shard=K verdict=… partition=… hold=… holdPod=… blocked=…` once per pass
  through a rollout, at `Info` and not `V(1)`, and Step 3 logs
  `Assigning missing shard to master  shardIdx=… target=…` on the audit logger. Nothing had to be
  turned on; the lines were simply not kept. **Preserving the operator log is therefore the single
  highest-value change to how this is reproduced** — it answers H2's successor question, H5 and H6
  outright.
- **Whether the gate was bypassed or satisfied spuriously.** Both are open. `planShardRolloutPartition`
  could have been absent from the applied StatefulSet, or it could have lowered the cursor on a
  pod that satisfied `IsLinkUpReplicaOf` without actually holding the data.
- **Whether it is specific to an image change.** An image bump differs from the LR-047 tier's
  config bump in one way that matters: the new tag may need an image *pull*, which lengthens the
  gap between pod deletion and the replacement answering PING. Nothing yet connects that to the
  loss. **This is exactly what §10's campaign is designed to settle**, and §10.1 is the evidence
  the failing run already carries on it.
- **Frequency.** Observed once in one full-suite run.

## 5. It does not reproduce in isolation

```
make test-e2e SKIP_KIND_SETUP=true SKIP_OPERATOR_DEPLOY=true \
  FOCUS='Redis Image Upgrade — Cluster Mode' DEBUG_ON_FAILURE=true
→ 2 Passed | 0 Failed   (patch spec 160.674s)
```

Green, both steps, on the same cluster and the same build, ~5 hours later. So this is **not a
"run it more times and take the mean" question** — a shard either kept its data or it did not,
and this one did not. It is load- or ordering-dependent, and an isolated re-run will keep
passing. Any reproduction attempt must recreate suite conditions.

Note the tier was **verified on Kind (podman) only** when it landed (`4133938`). t3e is first
contact with a real cluster — the LR-024 / LR-025 pattern, where a design survives Kind and
fails on contact with a different environment.

## 6. Candidate hypotheses, with the experiment that discriminates each

Ordered by how cheaply they can be falsified. None is favoured; §4 is why. **Read §10 first** —
it cuts across all six on a single axis (platform or application) and is the cheapest thing to run.

**H1 — contamination from the immediately preceding spec.** The spec that ran before it is
`Cluster Rolling Update Redundancy Gate (LR-047) must not lose a shard when the rollout's
redundancy window is unavailable`, whose technique is to **scale the shared operator Deployment
to 0 for 90 seconds** (`cluster_rollout_gate_test.go:298`). Timeline from the log:

| time | event |
|---|---|
| 09:14:50.0 | operator scaled to 0 |
| 09:16:20.5 | gate window expires, operator restored |
| 09:16:33.3 | operator `availableReplicas=1` |
| 09:19:26.3 | gate spec's last assertion |
| 09:19:26.x | `kubectl delete littlered rollgate-b --timeout=2m` |
| **09:19:27.1** | **image-upgrade CR applied — 0.8s later** |

`scaleOperator(1)` does wait for availability, and the operator had ~3 minutes before the new CR,
so "operator still cold" is weak. What is *not* weak: `kubectl delete` on the CR returns once the
CR object is finalised, while its owned StatefulSets and six pods are garbage-collected
asynchronously. The image-upgrade instance was therefore created on a single-node cluster still
tearing down another six-pod cluster instance, with the operator concurrently processing that
deletion. **Experiment:** run `MODE=cluster` (preserves suite order) vs. the two specs alone in
that order vs. the image tier with an artificial 2-minute settle inserted before it.

**H2 — ~24s/pod. ⚠ THE EVIDENCE IS REFUTED (2026-10-05); the question behind it is not.**

The first draft read: *"`minReadySeconds: 30` + LR-021 + the ADR-017 gate naively floor a
three-shard roll well above 144s, so both runs roll below the floor — either the floor reasoning
is wrong or the gate is systematically weaker than designed."* **The floor reasoning was wrong.**
It charged `minReadySeconds` once per *pod*; the StatefulSet controller charges it once per
*shard*. Read first-hand at `kubernetes/kubernetes` release-1.36 (t3e's line), not inferred from
our own design docs:

- `pkg/controller/statefulset/stateful_set_control.go:763-792` — the rolling update counts
  `isUnavailable(pod, minReadySeconds, now)` across the set and returns while
  `unavailablePods >= maxUnavailable` (1). `minReadySeconds` gates **the next deletion**, not each
  pod's own completion.
- `pkg/controller/statefulset/stateful_set_utils.go:640-648` — `completeRollingUpdate` fires on
  `UpdatedReplicas == ReadyReplicas == Replicas == *spec.Replicas`, with no further wait. The last
  pod of a shard costs nothing once it is Ready.

So one shard at `replicasPerShard: 1` pays the 30s exactly **once**:

| t | event |
|---|---|
| +0s | new template + `partition: 1` applied; the StatefulSet deletes ordinal 1 |
| +5s | ordinal 1 Ready |
| +10..20s | the operator advances `partition` 1 → 0 (ordinal 1 is a link-`up` replica) |
| +35s | ordinal 0 deleted — **released by `minReadySeconds`, not by the gate** |
| +45s | ordinal 0 Ready → `completeRollingUpdate` → the next shard is allowed to roll |

≈45s × 3 shards ≈ **135-150s against a measured 144s.** Both runs roll at exactly the floor the
design produces. Nothing about the timing is anomalous, **and a faster roll would not have been a
defect in itself** — speed was only ever a proxy for *"the gates are not being applied"*, and the
proxy does not hold.

**What survives the correction is a statement about our instrumentation, not about the product.**
The gate's advance — the operator observing the replacement Ready, then FORGET/MEET/REPLICATE and
the link reaching `up`, measured at 8-16s in LR-047's own live runs — completes *inside* the 30s
`minReadySeconds` window. So the two guards are not both visible: the timer binds and the gate
finishes early underneath it. **A rollout with the partition applied and a rollout with no
`rollingUpdate` block at all take the same wall-clock time.** The clock is structurally incapable
of distinguishing a working gate from an absent one, which is why H2 could be neither confirmed
nor denied by it. This is the shape LR-052 named for a different guard: *a guard that never fires
looks identical, in every log and every test, to one that never needs to* — here, a guard that
never **binds** looks identical to one that is not there.

**One thing the timing does settle, and it narrows the search.** Every pod was replaced, so
ordinal 0 went down on every shard; there is no third option in which the gate held and stalled.
**Either the partition was applied and advanced to 0 on all three shards, or it was never applied
at all** — and the operator says which, once per pass, at `Info` (§4).

**Experiment (substance unchanged, instrument sharpened):** sample
`spec.updateStrategy.rollingUpdate.partition` on each shard StatefulSet at 1s through a rollout,
*and keep the operator's own `Cluster rollout gate` lines*. Worth doing whatever the loss turns
out to be — but read it as closing an observability gap, not as chasing a timing anomaly.

**H3 — recency of the write.** `{t4}` (shard 2, the shard that died) is seeded **last**, and the
tag patch is issued **0.7s** after its final `SET` (seed completes 09:19:58.31, patch 09:19:59.02).
The shard whose keys had the least time to settle is the shard that lost them. With three shards
and one loss this is 1-in-3 by chance, so it is an observation and not a hypothesis yet.
**Experiment:** insert a settle + explicit `WAIT 1 0` after seeding and see whether the loss moves,
disappears, or stays on shard 2.

**H4 — image pull lengthens the replacement window.** See §4. **Experiment:** pre-pull 7.4.8 onto
the node and re-run under suite conditions.

**H5 — the gate engaged but resolved the wrong owner (added 2026-10-05).** Clause (c) asks whether
the replacement is a link-`up` replica of *the shard's slot owner*, and `shardSlotOwner`
(`internal/controller/cluster_rollout.go`) resolves that owner by slot **containment** — any master
whose ranges contain the first slot of the shard's expected range. Containment is deliberate and
load-bearing (exact matching returns "no owner" on a fragmented or mid-reshard range, which makes
clause (c) unsatisfiable and stalls forever — ADR-017 says so), but it means a transiently
consolidated range (LR-018) resolves to a node that is not the shard's real serving owner, and
clause (c) is then evaluated against the wrong master. **Experiment:** sample `CLUSTER NODES`
through the roll alongside the cursor; the operator's gate line already carries `verdict`/`hold`
per pass.

**H6 — the trailing edge, which the gate does not cover by construction (added 2026-10-05).**
ADR-017 enforces LR-025's invariant on the **leading** edge: redundancy is proven before the
master is taken down. After it is taken down the shard holds exactly **one** copy — the promoted
replica — until the replacement is FORGET/MEET/REPLICATE-ed and has full-synced, and nothing gates
that window. At `replicasPerShard: 1` the window is unavoidable; what matters is whether anything
destroys or orphans the sole copy inside it. The candidate to rule on first is **Step 3 itself**:
while the old master is gone and the promoted replica has not yet claimed the range,
`shardOwners[2]` reads empty, and `SafeMissingShardTarget` admits a reachable **empty** master —
which is precisely what the fresh `-shard-2-0` is. That would make LR-047's *erasure* the **cause**
rather than the cover-up. **Argued reachable, NOT established:** it additionally needs the fresh
node's own view to have those slots unassigned (isolated, or gossip not yet converged), and Step 1
returns early while a partition exists, so it requires a specific interleaving. **One log line
settles it either way:** `Assigning missing shard to master  shardIdx=2 … target=…-shard-2-0`.

## 7. Collateral finding from the same run (already understood, logged here for completeness)

Two specs — `Sentinel Advanced Failover Polling-Only Recovery` and `Failover Mode Polling-Only
Recovery` — failed only on a log-substring assertion. Commit `7963b95` demoted the two lines they
grep for to `V(1)`; the suite normally compensates by deploying with `LOG_LEVEL=debug`
(`e2e_suite_test.go:299,310`), but `SKIP_OPERATOR_DEPLOY=true` skips that deploy and the operator
was left at the Makefile default `info`. Both tiers' functional assertions passed. Confirmed green
after redeploying at debug.

**The latent problem is not the log level.** Two tiers depend on the operator running at debug,
`SKIP_OPERATOR_DEPLOY=true` is a supported path, and nothing says so or fails fast. Either assert
on an info-level observable, or have the suite read the deployed operator's `--zap-log-level` when
it skips its own deploy and refuse to run those tiers otherwise.

## 8. Next steps, in order

**Ruled 2026-10-05: fix on evidence, not on a hypothesis.** No code change to the operator is
proposed until a reproduction with artefacts exists; every mechanism in §6 is argued, and this
ledger records what happens when a guard is changed on an argument (LR-043's over-suppression,
LR-050's stronger reading falsified live within minutes).

1. ~~**Make the test report the truth** (§3) — all mismatches, grouped by shard — **and verify
   the seed before patching**.~~ **DONE** — `91af5eb` + `432f3a0`, test-only, see §3.
2. **Reproduce under the failing run's own spec ORDER, as §10's campaign, not as a bare
   re-run.** `SEED=1791181154` — the failing run's Ginkgo seed, recovered from `fail-1.log:30`
   (`Random Seed: 1791181154`) and replayable since `b37a58f`. Ginkgo reshuffles top-level
   containers on every run, so "the same selection" was never "the same order", and H1 is an
   *ordering* hypothesis: a reproduction that omits the seed has removed the variable it set out
   to test. `make list-e2e MODE=cluster SEED=1791181154` previews the order with no cluster. The
   reproduction and the platform-vs-application cut are the *same run*: give it §10.3's arms
   (A1 metadata edit, A2 fake tag, B the real 7.4.8) and its stopping rule, so a red both
   reproduces the defect and tells us which half of the system it is in.
   `make test-e2e SKIP_KIND_SETUP=true SKIP_OPERATOR_DEPLOY=true MODE=cluster DEBUG_ON_FAILURE=true`
   (`DEBUG_ON_FAILURE` sets `--fail-fast` *and* skips the CR delete, so `lrctl verify` / `inspect`
   can be run against the damaged instance). Operator at `LOG_LEVEL=debug`, **and capture the
   operator log for the whole window** — per §4 the gate's own verdict and Step 3's assignment
   line are both already emitted at `Info`, and losing them is what made the first run
   undiagnosable. **Decide the stopping rule before the first run**, because a green arm is
   uninterpretable (§10.3) and that is easy to forget once a clean result is in hand.
3. **Instrument the cursor** (§6 H2) regardless of whether step 2 goes red. Note it is no longer a
   timing question: the clock cannot carry this signal, so the cursor and the gate's log line are
   the *only* way to answer "did the gate engage".
4. ~~**Assert the gate in the tier** (§9.1) — so a silently-absent gate goes red instead of going
   fast.~~ **DONE** — `5c8053d`, see §9.1.
5. Only then design a fix, and land the LR-065 entry with it.

Per rule §7, whoever picks this up reads `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` end to end
first. LR-047 is the direct ancestor and ADR-017 is the design under suspicion, but the guard
history around them is the part that will not be visible from the code.

---

## 9. Two standing questions this raised, which it does not own

### 9.1 Nothing asserts that the ADR-017 gate engaged

Follows directly from §6 H2. The gate is state-based and correct by its own tests, but at
`replicasPerShard: 1` it never **binds** — `minReadySeconds: 30` is the slower of the two
constraints, so the gate's work finishes underneath it and leaves no trace in any timing the
suite or production measures. If the partition silently stopped being applied, every e2e tier
would stay green, every rollout would take the same time, and the only thing separating a shard's
two deletions would once again be a wall-clock timer — the exact pre-LR-047 state.

**✅ CLOSED by `5c8053d`** (`test/e2e/cluster_rollout_gate_test.go`). A new `shardRolloutPartition`
helper reads the cursor straight from the API server, and the LR-047 tier now asserts four things
the suite could not previously see: the partition **is applied at all**; it sits at the shard's
highest ordinal while the replica is being replaced; it does **not move** while the operator is
scaled to zero (only `advanceClusterRollout` lowers it, so a cursor that moves there was moved by
something else); and every shard is back at 0 once the instance settles (ADR-017's `Complete`),
so a rollout the gate never released is not mistaken for a pass. The commit states this section's
own conclusion independently: *"at replicasPerShard 1 its advance finishes underneath
minReadySeconds … Without this a silently absent gate goes FAST instead of RED."*

### 9.2 `minReadySeconds: 30` — considered and DECLINED (2026-10-05)

ADR-017 retained it as "pure defence in depth" and deferred *"whether to lower it now that a real
gate exists"*. §6 H2 makes the motivation concrete — lowering it would make the gate the binding
constraint, and therefore visible. **Declined, and the reason decides it:** the ADR-017 gate
governs **operator-triggered rollouts only**. A manual `kubectl rollout restart`, a node drain or
an eviction never passes through `reconcileClusterStatefulSet`; the partition stays at whatever
was last applied (0, on a settled shard), and `minReadySeconds` is then the **only** thing
separating the two deletions, with ADR-017 Decision 4's preStop fence as the single mitigation.
Lowering it makes those paths strictly worse in order to buy observability on a path that already
has a stronger guard — and the observability is obtainable for free via §9.1. It also costs one
rolling update of every cluster instance (ADR-017 Decision 4's hazard) for no safety gain.

Recorded as a decision so it is not re-proposed: **the timer is not redundant, because the gate
does not cover the paths the timer is there for.**

---

## 10. Platform or application? — the campaign, written down before it is run

Added 2026-10-05, on the owner's framing: before anything else, establish whether the loss lives
in the **platform** (Kubernetes / littlered / the ADR-017 gate) or in the **application** (an
actual 7.4.3 → 7.4.8 incompatibility). Every mechanism in §6 assumes the former; that assumption
should be *tested*, not asserted.

### 10.1 What the failing run already says — three independent arguments, all against the version

1. **Sentinel mode performed the identical transition in the same run and lost nothing.**
   `fail-1.log:2647` / `:2746` — `Redis Image Upgrade — Sentinel Mode` passed **both** steps,
   7.4.3 → 7.4.8 (149.7s) and 7.4.8 → 8.4.2 (106.3s), 300 keys verified intact each time, on the
   same cluster, ~13 minutes before the cluster tier ran. Same binaries, same replication
   direction (a newer replica syncing from an older master).
2. **Two of three cluster shards survived the identical transition, in the run that failed.**
   Every shard performs the same replica-replace-then-master-replace with the same two versions.
   A version incompatibility is shard-symmetric; one shard of three is a race signature.
3. **It is non-deterministic** (§5). A version incompatibility is deterministic.

### 10.2 The structural argument, and the caveat that makes it interesting

If a patch upgrade genuinely broke replication, the replacement replica could not sync ⇒
`master_link_status:down` ⇒ ADR-017's clause (c) is never satisfied ⇒ **the gate holds and the
rollout stalls**, data intact and loud. That is the failure direction ADR-017 deliberately chose.
We observed the opposite: a *completed* rollout with data gone.

The caveat is the useful half: that argument holds **only if the gate was engaged**.

| | gate engaged | gate absent |
|---|---|---|
| version incompatible | stalled rollout, no loss | loss |
| version fine | no loss | loss (today's §6 candidates) |

So both surviving hypotheses route through the same unanswered question — *was the partition
applied?* — which is why the operator log (§4) is the highest-value measurement whichever arm
one believes.

### 10.3 The experiment, and the flaw it must be run around

The clean cut is to **remove the version as a variable while keeping the operation identical**.
A real 7.4.3 → 7.4.8 upgrade changes three things at once: the redis binary (V), a real image
pull (P), and the pod template / rolling replacement (T). The arms below isolate T.

| arm | what changes | cost |
|---|---|---|
| **A1 — metadata edit** | T only. Edit a CR label/annotation: pillar 3.17 puts pod labels in the pod template, so the operator rolls every shard identically. `desiredClusterShardTemplateHash` hashes the *whole* template and nothing special-cases the image field, so the roll is byte-identical in shape. | free, today, no registry |
| **A2 — fake tag** | T only, but preserving the exact shape of the failing operation (a `spec.image.tag` patch) and exercising a pull. Relabel 7.4.3 in our own registry as `7.4.4` and "upgrade" to it. | one registry push |
| **B — control** | V + P + T. The original 7.4.3 → 7.4.8. | none |

**The flaw to design around: a green arm proves nothing.** Non-reproduction is already the
baseline — n=1 red under suite order, n=1 green focused — and **there is no estimate at all of the
reproduction rate under suite conditions**. So:

- **Any red in A1 or A2 ⇒ the platform. Strong, clean, stop and read the operator log.**
- **All-green in A1/A2 ⇒ nothing**, unless B is red in the same campaign under the same conditions.

Hence the stopping rule, fixed before the first run: run **A and B under identical suite
conditions**, N repetitions each, operator at debug with its **log preserved**, and §3's two test
fixes landed first so a red names a shard instead of a key.

**"Identical suite conditions" now means something exact, which it did not when this was
written.** The failing run's Ginkgo seed is `1791181154` (`fail-1.log:30`), and since `b37a58f`
it can be replayed:

```bash
make list-e2e MODE=cluster SEED=1791181154   # preview the order, no cluster needed
make test-e2e MODE=cluster SEED=1791181154 SKIP_KIND_SETUP=true SKIP_OPERATOR_DEPLOY=true \
     DEBUG_ON_FAILURE=true                   # replay it
```

Every arm carries the same `SEED`. This matters most for **H1**, which is an *ordering*
hypothesis: the gate spec that scales the operator to zero ran immediately before the image tier,
and whether it does so again was previously left to Ginkgo's shuffle. An arm run without the seed
is a different experiment — it has removed the variable it set out to test — and must not be
counted toward the stopping rule.

### 10.4 Two things the A arms do NOT isolate

Both A arms also remove the image pull, so a green A does not separate *"redis is innocent"* from
*"the pull was the cause"* (**H4**). Keep H4's own cell — pre-pull 7.4.8 onto the node, then run
the real upgrade — in reserve for the case where A is green and B is red.

### 10.5 One correction to the premise this campaign started from

*"We know we can do a rolling restart of the cluster — we have been e2e-testing it for ages."*
The **tier** is old; the **guarantee** is six weeks old. LR-047 — a shard's entire dataset
destroyed by a rolling update that reported complete success — came out of exactly that tier
(`Cluster Mode Chaos Testing > should preserve data after rolling update`, 2026-08-23), with no
image change anywhere near it. ADR-017's partition gate is the fix and landed 2026-08-25, and per
§9.1 nothing asserts it is still engaged.

So the long green history belongs to the tier, not to the guarantee — and **the one previous
sighting of this exact symptom** (a whole shard, Step 3's `SafeMissingShardTarget` erasure, green
topology, success reported) **was a plain config-change roll.** Byte-identical shape, six weeks
apart, one with an image change and one without. The most economical reading is that the image is
incidental and this is the same defect class; 10.1 is why that reading is not merely economical
but evidenced.

---

## Appendix A — the raw evidence, preserved

The source log is `fail-1.log` in the repo root (untracked, 927 KB, run of 2026-10-05 08:00–10:19).
**It is the only copy and it is local to one checkout — a reader of this report does not have it**,
which is why every line this report leans on is reproduced here.

**The run's Ginkgo seed, which is what makes the reproduction replayable** (`fail-1.log:30`):

```
Random Seed: 1791181154
```

**The sentinel-mode result from the same run, which §10.1 rests on** (`fail-1.log:2647`, `:2746`):

```
Redis Image Upgrade — Sentinel Mode keeps every key across a patch upgrade 7.4.3 -> 7.4.8
• [149.688 seconds]
Redis Image Upgrade — Sentinel Mode keeps every key across a major upgrade 7.4.8 -> 8.4.2
• [106.251 seconds]
```

Both passed, 300 keys verified intact each time, on the same cluster ~13 minutes before the
cluster tier ran — i.e. the identical version transition, twice, with nothing lost. The cluster
tier's major step never ran: `S [SKIPPED] … because an earlier spec in an ordered container
failed` (`fail-1.log:3841`).

Full GET sequence of the failing `verifyDataset` loop (`fail-1.log:3733-3820`), in order.
`←` marks an attempt's final GET:

```
 1 {t2}mig:2    2 {t1}mig:4    3 {t4}mig:6  ←
 4 {t2}mig:1    5 {t2}mig:14   6 {t2}mig:17   7 {t1}mig:1   8 {t1}mig:14   9 {t4}mig:7  ←
10 {t2}mig:16  11 {t1}mig:11  12 {t4}mig:2  ←
13 {t1}mig:5   14 {t1}mig:13  15 {t4}mig:4  ←
16 {t2}mig:2   17 {t1}mig:4   18 {t4}mig:6  ←
19 {t2}mig:16  20 {t1}mig:11  21 {t4}mig:2  ←
22 {t2}mig:19  23 {t1}mig:10  24 {t1}mig:17  25 {t4}mig:11 ←
26 {t1}mig:2   27 {t1}mig:19  28 {t1}mig:20  29 {t1}mig:5   30 {t1}mig:13  31 {t4}mig:4  ←
32 {t2}mig:3   33 {t2}mig:6   34 {t4}mig:18 ←
35 {t2}mig:4   36 {t2}mig:10  37 {t2}mig:11  38 {t1}mig:3   39 {t1}mig:8
40 {t2}mig:8   41 {t2}mig:9   42 {t1}mig:16  43 {t4}mig:3  ←
44 {t1}mig:2   45 {t1}mig:19  46 {t1}mig:20  47 {t1}mig:5   48 {t1}mig:13  49 {t4}mig:4  ←
50 {t2}mig:16  51 {t1}mig:11  52 {t4}mig:2  ←
53 {t2}mig:1   54 {t2}mig:14  55 {t2}mig:17  56 {t1}mig:1   57 {t1}mig:14  58 {t4}mig:7  ←
59 {t2}mig:1   60 {t2}mig:14  61 {t2}mig:17  62 {t1}mig:1   63 {t1}mig:14  64 {t4}mig:7  ←
65 {t2}mig:12  66 {t2}mig:18  67 {t1}mig:12  68 {t1}mig:18  69 {t4}mig:9  ←
70 {t2}mig:18  71 {t1}mig:12  72 {t1}mig:18  73 {t4}mig:9  ←
74 {t2}mig:7   75 {t4}mig:14 ←  (assertion gives up, 180s elapsed)
```

17 `{t4}` reads, 17 attempt terminations, zero `{t4}` reads that are not a termination.
Distinct `{t4}` keys proven absent: 2, 3, 4, 6, 7, 9, 11, 14, 18.

Relevant timestamps, same log:

```
09:19:27.074  img-cluster-1791184767 applied (0.8s after rollgate-b's delete was issued)
09:19:48.506  seeding begins — {t2} shard 0, then {t1} shard 1, then {t4} shard 2
09:19:58.310  seeding complete
09:19:59.018  spec.image.tag patched to 7.4.8     (0.7s after the last {t4} SET)
09:22:12.411  every pod replaced — "rolled 6 pods in 2m24s"
09:22:22.702  every pod confirmed on 7.4.8
09:25:23.656  verifyDataset gives up after 180s
```
