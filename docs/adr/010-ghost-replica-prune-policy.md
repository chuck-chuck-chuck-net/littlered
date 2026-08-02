# ADR-010: Ghost-Replica Prune Policy in Sentinel Mode (Rule D) — Draft

## Status
**DRAFT — for discussion. No decision has been made.**

This ADR decides the *prevention* half of LR-024. The *recovery* half already shipped
(operator-led ghost-master recovery; see LR-024, pillar 3.10) and is e2e-verified, so the
ghost-master failover deadlock is **no longer permanent regardless of what we decide here** —
this decision is now about monitoring-signal quality, churn, and philosophy, **not**
correctness. The options and the claims under "Facts" are written to be argued with; fill in
the decision at the end once we agree.

## Context

- Sentinel mode is pure in-memory with **IP-only identity** (pillar 3.7): a pod that restarts
  returns with a **new** IP, so its old IP is meaningless afterward.
- After a failover, Sentinel **deliberately re-adds the deposed master as a replica** of the new
  master, "so that we'll be able to sense / reconfigure the old master" (verbatim from
  `sentinel.c`, Redis and Valkey). Under our model the old master never returns at that IP, so the
  entry becomes a permanent **ghost replica**.
- **Rule D** — the ghost-replica `SENTINEL RESET` (origin commit `c56f7c0` "forget ghosts",
  2026-02-14; ships in the released **v0.2.1**) — exists to prune these ghost replicas.
- **LR-024 RCA:** Rule D's RESET is the *self-inflicted cause* of a permanent ghost-master
  failover deadlock. In the graceful→crash e2e, the prior failover's ghost triggers a
  (legitimately gate-passing) RESET that empties Sentinel's replica list ~4s before the next
  crash; Sentinel then has a dead master and no promotable replica → `-failover-abort-no-good-slave`
  forever.
- **Two source-confirmed constraints** (Redis + Valkey `sentinel.c`, verified 2026-07-31):
  1. **No surgical single-replica prune exists** — the only removers are the whole-list
     `SENTINEL RESET` and the whole-master `SENTINEL REMOVE`. The official docs prescribe
     `SENTINEL RESET` as *the* way to forget a ghost replica.
  2. **A dead replica never ages out** — no TTL, no reaper, no staleness deletion; the
     INFO-refresh is add-only. It persists (and accumulates across failovers) until an operator
     acts.
- **Rule D's track record:** it has been hardened **five times** on the same hazard
  (LR-001 timer-reset, LR-007/008 REMOVE+MONITOR for stuck sentinels, LR-011 healthy-replica
  guard, LR-013 wholeness gate) and *still* produced the LR-024 deadlock.
- **Harm of a lingering ghost replica** (what pruning buys): it is **not** a correctness problem —
  an `s_down` ghost is excluded from promotion (`sentinelSelectSlave`), `HasHealthyKnownReplica`
  ignores it, and CR `status.replicas` derives from the **StatefulSet**, not Sentinel's list — so
  it never corrupts the operator's health verdict. Its cost is a **permanently degraded monitoring
  signal**: it never self-clears, so `lrctl verify` stays "dirty" and accumulates, which is a real
  operational cost (monitoring false positives, alert fatigue).

## Decision to make

**How, if at all, should the operator prune benign ghost *replicas* from Sentinel's view?**

Scope note: this is only about ghost **replicas** (Rule D). Ghost-**master** correction
(LR-008, `SENTINEL REMOVE`+`MONITOR` a living consensus master) is **out of scope and stays** —
it re-points at a live master and re-learns replicas immediately, so it does not carry the
empty-list-then-death hazard.

## Options

Each prunes with the same blunt tool (`SENTINEL RESET`, per constraint 1) — they differ only in
**when** it fires.

### A0 — Keep Rule D as-is (status quo)
Autonomous RESET whenever the wholeness/healthy-replica/living-master gates pass.
- **Pro:** cleanest signal, no new code.
- **Con:** still walks into the LR-024 deadlock window on a graceful→crash (or any two failovers
  within the RESET→rediscovery gap). Recovery now heals it, but every occurrence costs the ~30s
  recovery-cooldown outage. Paying a recurring availability dip for hygiene is a poor trade — and
  it is the very behavior LR-024 traced as the cause.

### A1 — Never prune (retire Rule D)
Stop issuing the ghost-replica RESET entirely; carry ghosts forever (the Redis default per
constraint 2).
- **Pro:** simplest; removes the self-inflicted cause outright; most aligned with pillar 3.5
  (do less when something is fragile); no outage.
- **Con:** the monitoring signal stays permanently dirty unless we also soften how ghosts are
  surfaced (see "Cross-cutting" below).

### A2 — Low-frequency GC
RESET only when the cluster has been **whole** *and* **no failover has occurred** for a long,
configurable quiet window — shrinking the RESET→death race to near-zero.
- **Pro:** restores a clean signal autonomously; residual risk is tiny and recovery covers it.
- **Con:** more logic and a new "quiet window" concept to reason about and test; still a
  non-zero autonomous RESET (philosophically, still "intervening").

### A3 — On-request `lrctl` verb
Never prune autonomously; expose e.g. `lrctl … prune-ghosts` for an operator to run when they
know the topology is stable.
- **Pro:** safest — a human asserting "clean up now" won't do it mid-incident, and recovery covers
  any residual race; fits "operator surfaces, human decides."
- **Con:** ghosts linger until a human acts; adds a CLI verb + its wiring.

### Cross-cutting (applies to A1/A2/A3): soften `lrctl verify`
Present a benign ghost **replica** as *informational*, not `[FAIL]`. This addresses the
alert-fatigue cost at the **presentation** layer rather than by a risky prune — and may on its own
neutralize most of A1's downside.

## Tentative leaning (to be confirmed or overturned)

**A1 (retire Rule D) + soften `lrctl verify`, with A3 (on-request verb) as an optional manual
escape hatch.** Reasoning: recovery makes correctness a non-issue; the *only* remaining value of
pruning is signal quality, and a false-positive that we *label* as benign is no longer a false
positive — so the presentation fix plausibly captures most of the benefit at none of the RESET
risk. A0 is the weakest (keeps the cause). A2 is the strongest "keep it clean autonomously"
option if we decide the signal-cleanliness value is high enough to justify a standing (if rare)
autonomous RESET. **Genuinely open** — the crux is how much we value an always-clean autonomous
signal vs. never issuing a risky autonomous RESET again.

## Open questions for the discussion

1. Is a **labeled-benign** ghost in `lrctl verify` (and any status surface) good enough, or do we
   want the ghost *gone* from Sentinel's actual tables?
2. If gone: do we trust an autonomous **quiet-window** heuristic (A2), or insist on
   **human-in-the-loop** (A3)?
3. Does unbounded ghost **accumulation** across many failovers ever bite in practice (long-lived
   instances, frequent failovers) beyond verify noise — any real Sentinel cost we have not found?
4. Blast-radius sanity check: is there any consumer of Sentinel's replica **list** (ours or a
   third party reading `SENTINEL replicas`) that a lingering ghost misleads, given our own code
   already filters it?
5. Do we want to keep an `allowUnsafe`-style opt-in anywhere here, or is that only a recovery-side
   concern (it is, today)?

## Consequences (by leaning)

- **If A1 + verify-softening:** Rule D and `GhostReplicaResetSafe` are removed; the
  `ghostFound` detection loop that feeds only Rule D goes with it; `lrctl verify` classifies ghost
  replicas as informational. Blast radius is contained (LR-024 design doc §6.1): the operator's
  happy/not-happy verdict is StatefulSet-driven and untouched; `DetermineRealMaster` ghost-master
  suppression, `HasHealthyKnownReplica`, and LR-008 stay. Cluster-mode ghosts (`CLUSTER FORGET`)
  are a separate mechanism, unaffected.
- **If A2/A3:** Rule D's RESET is retained but re-gated (A2) or moved behind a CLI verb (A3);
  either way the autonomous-RESET-in-a-tight-window path that caused LR-024 is closed.

## References
- LR-024 (recovery half) — `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md`; CLAUDE.md pillar 3.10
  ("Ghost-master variant") + §9.
- `docs/GHOST_MASTER_FAILOVER_DEADLOCK_DESIGN.md` — full RCA, the source research (no surgical
  prune; ghosts never age out), the option matrix, and the contained blast radius (§6.1).
- Rule D history: LR-001 / LR-007 / LR-008 / LR-011 / LR-013 (same changelog).
- ADR-003 (ghost master pruning / IP-only identity), ADR-005 (leaderless recovery, the
  `allowUnsafeRebootstrapOnDeadlock` precedent), pillars 3.5 (minimal interference), 3.7 (IP-only
  identity), 3.9 (ghost node healing), 3.10 (leaderless + ghost-master recovery).
