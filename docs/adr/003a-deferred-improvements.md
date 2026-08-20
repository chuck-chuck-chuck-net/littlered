# ADR-003a: Deferred Improvements

## Status

**Reviewed 2026-08-20** against the code. Of the five items deferred alongside ADR-003, two are
implemented, one is superseded, one is now **rejected** (doing it would cause data loss), and one
remains genuinely open — with a sharper reason than the original entry gave.

Kept as a register rather than closed, because the open item is real and the rejected one needs to
stay visible so nobody re-proposes it.

## Context

During the revert to low-interference sentinel reconciliation (ADR-003), several potential
improvements were identified but intentionally deferred to keep the change focused and low-risk.
Much has happened since — LR-006 through LR-024 rebuilt most of the surrounding machinery — so the
original entries are re-stated below with what is actually true now.

## Items

### 1. Sentinel settings reconciliation — **OPEN**, and the original premise was wrong

*Original:* "settings are applied only during bootstrap and after SENTINEL RESET. A dedicated
reconciliation step could periodically verify and correct these settings."

The premise no longer holds. `applySentinelSettings` is called from **four** places: the bootstrap
seed (`seedSentinelsWithMaster`), Rule 0's bare-sentinel re-register, and both ghost-master and
divergent-master corrections. Any operator-initiated `SENTINEL MONITOR` re-applies the full set.
`SENTINEL RESET` is no longer one of them.

But the item stays open, for a reason the original did not state and which is more concrete than
drift: **editing `spec.sentinel.downAfterMilliseconds`, `failoverTimeout` or `parallelSyncs` on a
running instance has no effect.** Every call site above is a *repair* path; none fires on a healthy
instance. The new value therefore sits in the spec, is reported as accepted, and does nothing until
some unrelated event triggers a re-`MONITOR` — a sentinel restart, a ghost correction. A user has no
way to tell the difference between "applied" and "silently pending".

A periodic verify-and-correct step would close it. The care needed is the ADR-003 one: `SENTINEL
SET` during a failover is exactly the interference this ADR exists to avoid, so any such step
belongs behind Rule A's guardrails and must skip while `FailoverActive`.

### 2. Sentinel health monitoring — **IMPLEMENTED**

*Original:* "detect sentinels that lost their configuration after restart and re-bootstrap
individual sentinels. Currently handled by the bootstrap flow and RESET."

This is **Rule 0** (`littlered_controller.go`, "Re-register sentinel pods that started without a
master configured"): a reachable sentinel with `Monitoring == false` is re-registered individually,
with settings, targeted at that pod rather than broadcast. Two later rules complete the coverage:
**Rule L** (LR-015) for the case where *every* sentinel is bare, which Rule 0 cannot fix because it
needs a consensus master to point at; and ghost-master correction (LR-005/LR-008) for a sentinel
that kept a configuration but the wrong one.

The "handled by RESET" half is also stale, and the reverse of current design: `SENTINEL RESET` is
now the most heavily gated action in the loop (LR-001/007/011/013) and is never the mechanism for
restoring a sentinel's configuration — it cannot be, since RESET does not change the monitored
master address (LR-008).

### 3a. `INFO replication` instead of `PING` for master reachability — **OPEN, low value**

Still `PING` (`PING_ATTEMPTS=6`, `PING_DELAY=3`). The original rationale was to distinguish a master
that is alive-but-slow from one that is dead — but the script no longer *branches* meaningfully on
that distinction: after ~18 s it starts **bare** either way and lets Sentinel decide. A richer probe
would produce a better log line and little else. Worth doing only if some future branch actually
needs the distinction.

### 3b. Timeout for the initial Sentinel query loop — **REJECTED**

*Original:* "consider adding a timeout for the initial Sentinel query loop to prevent pods from
hanging indefinitely if all sentinels are down."

**Do not do this.** The loop is unbounded on purpose. A timeout only helps if the pod then does
something — and the only thing it could do is start as a master, which in a pure in-memory
architecture (EmptyDir, pillar 3.1) means an empty pod claiming mastership and every surviving
replica full-syncing *from* it. That destroys exactly the data the operator is trying to preserve.

This is the same lesson as **LR-016**, where the sentinel liveness probe restarted masterless
replicas and wiped the survivors Rule L exists to promote: a single pod cannot distinguish "no
master exists anywhere" from "the master is temporarily unreachable from here", because that
requires the global view. The parked pod is *correct*; the deadlock it reveals is the operator's to
break, and **Rule L** (LR-015, ADR-005) is that break — data-aware, quorum-gated, cooldown-gated,
and refusing outright when two or more pods hold data.

Hanging indefinitely is a real symptom, but the cure is operator-side recovery, not a pod-side
timer.

### 4. Label update debouncing — **SUPERSEDED**

The churn this aimed at was caused by `updateMasterLabel` relabelling every living pod as `orphan`
during a leaderless period. **LR-006** removed the cause rather than damping the effect: the
function is now surgical — with no known master it only strips the `master` label from whoever held
it and leaves everything else untouched. **LR-010** removed the other half of the loop pressure with
a `GenerationChangedPredicate` on the watch, so status writes no longer trigger reconciles and the
periodic timer is the primary driver.

No debouncing layer is needed on top, and adding one now would obscure Rule A, which is the
intentional guard for transitions.

### 5. Status phase granularity (`Degraded`) — **OPEN, but probably unnecessary**

Phases today are `Pending`, `Initializing`, `Running`, `Failed`; there is no `Degraded`. The
granularity the item asked for now exists in a different place: the `Ready` condition carries a
reason and a message spelling out exactly what is missing — e.g. `PodsNotReady`, *"Redis: 0/3,
Sentinels: 3/3, Sentinel-known replicas: 0/2"* — which is strictly more informative than a fourth
phase string, and is what `kubectl describe` and `lrctl status` surface.

Adding `Degraded` would also be an API-visible change to a field users may be alerting on, for
information already available. Kept open only because "running but not fully healthy" is a
legitimate thing to want in a single field; not worth doing on its own.

## References
- [ADR-003: Low-Interference Sentinel Reconciliation](003-low-interference-sentinel-reconciliation.md)
- [ADR-005: Leaderless Bootstrap-Deadlock Recovery](005-leaderless-bootstrap-recovery.md) — item 3b's actual answer
- [Reconciliation Algorithm Changelog](../RECONCILIATION_ALGORITHM_CHANGELOG.md) — LR-006, LR-008, LR-010, LR-013, LR-015, LR-016
- [RECONCILIATION_LOOP_SENTINEL.md](../RECONCILIATION_LOOP_SENTINEL.md) — Rule 0, Rule A, Rule L
