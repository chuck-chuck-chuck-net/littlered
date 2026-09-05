# Maintenance Release — Core Consolidation: DRAFT PROPOSAL

**Status: DRAFT. Ideas, not decisions.** Nothing here has been ruled on, nothing is scheduled, and
several items are deliberately posed as questions with a recommendation rather than as work items.
It exists so the next release can be argued about with numbers instead of impressions.

**Scope: the core operator's reconcile paths.** Not features, not modes, not the enterprise-readiness
waves in `BACKLOG.md`. The subject is whether what we have can be maintained by someone who did not
write it.

---

## 1. Where we actually are (measurements, not impressions)

| | 2026-01-31 (sentinel only) | 2026-07-30 (per-shard cluster) | 2026-09-05 |
|---|---|---|---|
| production Go (`internal/` + `api/`, no tests/generated) | 3,031 | 10,431 | **22,335** |
| tests | 644 | 10,845 | **36,192** |
| docs + `CLAUDE.md` | 2,320 | 6,823 | **22,993** |

Production **doubled in five weeks**. Test:production is **1.62:1**; docs ≈ production. Also: 18 pure
planners, 16 status conditions, 48 LR entries on this branch (61 IDs allocated across all branches),
133 e2e specs, **2h29m** for a full suite.

### 1.1 Complexity is not evenly distributed across modes

| | sentinel | cluster | failover | standalone |
|---|---|---|---|---|
| mode-attributable production LOC | ~5,400 + most of the 2,687-line controller | **7,398** | 4,169 | — |
| **LR entries (recorded failure modes)** | **29** | 12 | 1 (+5 addenda) | 0 |
| e2e specs | **55** of 133 | 34 | 25 | 19 |
| status conditions | **7** of 16 | 2 | 1 | 6 shared |

**Cluster mode is the larger body of code; sentinel is the larger source of trouble** — on roughly a
third of the mode-attributable lines it carries ~60% of the incident ledger, 41% of the suite (and
its slowest tiers), and 44% of the status surface. Failure modes per line, sentinel runs ~3× cluster.

The mechanism is pillar 3.5's own subject: **sentinel is the only mode where the operator shares
authority with a second distributed algorithm it cannot tell to stand down.** Cluster gossip is an
algorithm too, but our interventions there are node-ID-keyed and mostly idempotent
(`MEET`/`FORGET`/`REPLICATE`); Sentinel's are **address-keyed and destructive** — `RESET` wipes a
list rebuildable only from the master's `INFO`, `REMOVE`+`MONITOR` resets a config epoch to zero.
That is LR-001/007/011/013/024 in one sentence. Separately, the whole LR-039 → LR-061 chain rests on
a property only Sentinel has: the master name is its sole isolation boundary, it is user-editable,
and it is baked into pod templates.

**Confound, stated so the table is not over-read:** sentinel is eight months old with real field
exposure; failover mode is five weeks old and has one entry partly because nothing in production has
hammered it yet. Age explains some of 29-vs-1. It does not explain the mechanism above, nor the
measured comparison: across the 10-pass matrix failover ≥ sentinel on every availability number with
zero data loss on both sides, and on kill-9 the distributions do not overlap (85-96% vs 43-74%).

---

## 2. The diagnosis: the code is not too smart, it is too flat

Most of the growth is **essential**. Every LR entry is a real failure mode of this domain: no
persistence, so durability is replication; ephemeral IPs, so identity is a hazard; three topologies
with three failure detectors. Nobody invented the ghost master or the recycled pod IP.

One number is **accidental**, and it is the debt:

> `reconcileSentinelCluster` is **672 lines**, and its ordering is load-bearing in at least eight
> documented places — Rule 0 before Rule A (LR-040), Rule N after Rule 0 but before Rule A (LR-048),
> the quarantine decision before the gather (LR-044), the operation branch immediately before Rule A
> (LR-058), the leaderless branch after it (LR-059), the `Forsaken` switch above all of them, Rule
> R's exception inside Rule A (LR-060), Rule D below it. **None of that is enforced by a type, a
> signature, or a test.** It lives in comments and in the ledger.

That is where the defects now come from. The four found in the last week are **one shape** — a guard
suppressing the healing its own exit depends on:

| | guard | what it suppressed | cost |
|---|---|---|---|
| LR-058 | the first operation branch | Rule R | 311s vs 162s on a rename |
| LR-059 | ADR-020's authority boundary | Rule L | wedged 7m56s, unbounded |
| LR-060 | Rule A's `FailoverActive` half | Rule R | 179s of an unmanaged instance |
| LR-061 | Rule N's prune vs a pod's baked name | the pod's own startup | instance cannot self-recover |

Not one was a wrong predicate. The predicates are fine; the **composition** is where it breaks.

Two further signals that this is a structural limit rather than a run of bad luck:

- Three of the four were found by **feeding planners arbitrary states** (ADR-020 Phase 4) or by a
  live cluster — not by anyone reasoning about the code.
- During the LR-061 investigation the severity bound was written **wrong twice, in opposite
  directions**, by a reader holding the entire 4,100-line ledger. That is a statement about how much
  correctness lives in ordering and timing rather than in structure.

### 2.1 What is already healthy, and must not be "simplified" away

- **18 pure decision seams with red-first tables.** They are why LR-056 went from diagnosis to fix in
  an afternoon: the decisions are testable in milliseconds and e2e is a thin shell.
- **The ledger.** Every fix this month leaned on three to six prior entries. It is the reason the
  same defect is rarely made twice.
- **The invariant / operation split (pillar 3.16).** A genuinely good abstraction — it is what let
  LR-060 *partition* Rule A rather than bolt on a fifth ad-hoc gate.

Any proposal that trades these for tidiness is a bad trade.

---

## 3. Proposal, in order

### P1 — Make the sentinel reconcile ordering explicit (the runlevels concept's unbuilt half)

`RECONCILIATION_RUNLEVELS_CONCEPT.md` §2 already observes that we have **a runlevel ladder built by
accident**: four "stop doing things" gates, at four depths, with four different scopes, each added by
a different incident. ADR-020 then built the concept's *operation* half (D1-D7), and the §6 invariant
sweep is complete (LR-051, LR-052, LR-053). **The ladder itself was never built**, and §2's table is
now five gates rather than four.

Proposal: make the ladder a declared structure — an ordered set of levels with, per level, what runs
and what does not, derived from each rule's declared classification rather than from its position in
a 672-line function.

- **Deletes:** the implicit ordering. Every "this must sit above/below X" comment becomes a property
  of the structure, checkable.
- **Does not change:** the pure planners. This is about *when* a decision is invoked, never about
  what it decides. `planForsaken`, Rule L, Rule N and friends keep their tables and their tests.
- **How we would know it worked:** the four-defect table in §2 becomes expressible as a test rather
  than as four incidents.
- **Risk, stated plainly:** this is surgery on the path where every sentinel-mode incident has
  happened, and the failure mode of getting it wrong is another entry in §2. It wants the same
  red-first discipline as a rule change, plus a full soak before and after.
- **Open first (concept §8):** the naming. "Runlevel", "mode", "state" and "phase" are all taken.

### P2 — Mechanise the closure-under-forever criterion

LR-059 produced the criterion that explains all four defects in §2:

> A rule may be stood down by an operation (or by a guard) only if the instance can still reach a
> **settled** state with that rule **permanently absent**.

Today it is doctrine in an ADR. It is mechanisable: a table asserting, per rule, what settlement
depends on — and a test that fails when a suppression covers a rule its own exit needs. That is the
single highest-leverage test in this proposal, because it turns the recurring class into a red.

### P3 — Retire "read all 4,100 lines first" as an onboarding rule

`CLAUDE.md` §7.7 requires reading the entire changelog before touching a reconciliation rule. It has
earned its place — it is why fixes compose rather than collide — but it does not scale, and it worked
this month only because a reader had an unusually large context window. It will not work for a
person joining the project.

Proposal: keep the ledger append-only and authoritative, and add a **per-subsystem index** — "what
governs Rule D, and which entries corrected it" — so the entry cost is ~300 lines of reading rather
than 4,100. Generated from the entries' own `Impacts:` lines if possible, so it cannot drift.

### P4 — Grow the arbitrary-state tier, not the e2e minutes

ADR-020 Phase 4's tier feeds planners states with no story attached; it found LR-056 and LR-057 at
zero cluster cost. The full suite costs 2h29m and cannot be run per change. The leverage is obvious:
extend the cheap tier (more states, more planners, cross-mode) and hold e2e growth flat.

Concretely worth adding: the states this month produced — a bare-majority quorum, two live masters, a
pod on a superseded template, an operation pending on a leaderless instance.

---

## 4. The descope question, with its price tag

A descope was proposed: require a unique `masterName` and auth up front, accept downtime for
reconfiguring legacy instances, delete the rename / capture / operations machinery. The accounting:

| | production | tests |
|---|---|---|
| Rule N (rename planner) | 387 | |
| capture verdict + quarantine | 549 | |
| declared operations | 980 | |
| **descope candidates** | **1,916** | **8,836** (4,923 unit + 3,913 e2e) |
| growth since 30 July | +11,904 | +25,347 |
| **share of growth** | **16%** | **35%** |

So the machinery is **~1,900 production lines, 16% of the growth** — it is not what doubled the code
(failover mode ~4,200, the cluster rollout gate, per-shard work and the bounded-client corrections
are). Where it *does* dominate is verification: ~8,800 test lines and roughly **50 minutes of the
2.5-hour suite** (rename ~15, quarantine ~14, operations ~23). The hassle is disproportionate to the
code, which is the signature of a feature cheap to write and expensive to be sure about.

Three things any descope has to reckon with:

1. **"Documented downtime" is not on the menu; immutability is.** Delete Rule N and leave the field
   editable and you have LR-048's measured state — both names monitored, still there 12m39s later,
   two failover state machines, 56.6s with two different live pods as master. Worse than either
   alternative. The real descope is `masterName` **immutable via CEL**: ~10 lines, a clear admission
   error, and it deletes Rule N, the rename e2e Describe, and ADR-020's only registry member.
2. **Auth rotation is not symmetric with rename.** "Accept downtime" for a rename means delete and
   re-create; the instance re-bootstraps. For a credential rotation the same sentence means **delete
   the dataset** — EmptyDir, no persistence — and rotation is a compliance requirement in most target
   deployments. Either it keeps a mechanism, or the product states plainly that credentials cannot be
   rotated without data loss. That is a legitimate position, but it must be said out loud rather than
   arrived at by deletion.
3. **Descope and the auth default are one decision, not two.** The premise "everyone uses auth" is
   not true today and cannot be made true without flipping the default, which was declined
   (2026-09-05) because it breaks unauthenticated fleets on upgrade. With a unique name and no auth,
   gossip fusion is prevented and the **address-adoption path is not** (ADR-015 §9.4) — so capture
   stays reachable, and the quarantine is what protects the healthy *neighbour*.

**And the work is not wasted either way.** LR-051 (unreachability conflated three facts, so Rule L
could reseed over a live dataset), LR-052 (a guard reading a key Sentinel never emitted), LR-057 (the
lineage gate electing between two live masters) and LR-060 (Rule A suppressing the rule that would
end the failover) are defects in **pre-existing** rules, several data-safety, found only because this
work fed those planners states nobody had fed them before. The machinery may go; the findings stand.

---

## 5. Sentinel-specific consolidation candidates

Ranked by whether they make the reconciler *smaller*:

1. **Take the ADR-010 decision** (retire or rate-limit Rule D's ghost-replica `SENTINEL RESET`). It
   is the self-inflicted trigger for LR-024, it has been deferred since July, and it is **the only
   candidate on this page that deletes a rule and its recovery** rather than reorganising them.
2. **Freeze sentinel's intrusion surface.** No new sentinel rules; new capability goes to failover
   mode. Each new intrusion costs the N×M re-verification ADR-020 exists to bound, and sentinel's N
   is already six rules plus two recoveries plus the quarantine.
3. **Settle ADR-011's graduation** (drop / coexist / replace). Today the mode we recommend for HA and
   the mode carrying most of our complexity are different modes; deciding this determines where the
   next two years of work goes.
4. **Consolidate the status surface** — seven sentinel conditions, with LR-050 already having flagged
   inflation for once-in-a-lifetime events.

---

## 6. What this proposal deliberately does NOT propose

- **No rewrite.** The pure seams, the tables and the red-first discipline stay exactly as they are.
- **No new mode, no persistence, no new intrusion.** A consolidation release that grows the operator
  is not one.
- **No weakening of the ledger.** P3 changes how it is *entered*, never whether it is authoritative.
- **No deletion of the `Forsaken` verdict.** It is ~200 lines and it stops the operator thrashing a
  captured instance ~30×/minute (LR-042). The *quarantine* is the arguable part, and only if auth
  becomes mandatory.

---

## 7. Open questions

1. Is the release's goal **fewer lines**, or **fewer things that must be true at once**? They point
   at different work: the descope is the first, P1/P2 are the second. This proposal argues the second
   is what the incident record actually calls for.
2. Naming for the runlevel ladder (concept §8.1) — cheap, and it blocks writing.
3. Does auth rotation get a mechanism, or a documented "not supported without data loss"? That
   answer also decides whether ADR-020 keeps a member or becomes dormant code to delete.
4. Is `masterName` immutability acceptable as a product statement? It is the cleanest single
   simplification available, and it is user-visible.
5. Does P1 land before or after the next feature? The N×M argument says before; the risk argument
   says a consolidation of the hottest path wants a quiet window.
