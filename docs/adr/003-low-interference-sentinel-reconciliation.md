# ADR-003: Low-Interference Sentinel Reconciliation

## Status
Accepted

## Context
Starting with commit 89ea364, the sentinel reconciliation loop (`reconcileSentinelCluster`) was progressively expanded to actively heal the cluster by issuing SLAVEOF and SENTINEL MONITOR commands to individual Redis and Sentinel nodes. While well-intentioned, this violated the "Minimal Interference" principle (Section 2.5 of CLAUDE.md) and introduced regressions where the operator fought Sentinel's built-in failover mechanisms.

Specific problems observed:
- **SLAVEOF interference**: The operator issued `SLAVEOF` commands to nodes it believed were misconfigured, sometimes racing with Sentinel's own reconfiguration during failovers.
- **MONITOR interference**: The operator re-issued `SENTINEL MONITOR` to sentinels it believed were idle, potentially disrupting ongoing failover state.
- **Offset-based promotion**: `DetermineRealMaster` included a "last resort" step that picked the node with the highest replication offset and treated it as the master. This caused the operator to act on speculative data rather than waiting for Sentinel consensus.

## Decision
Revert the reconciliation behavior to a low-interference approach:

1. **Remove SLAVEOF healing** (Rule C): The operator no longer issues `SLAVEOF` commands to Redis nodes. Sentinel handles all replica reconfiguration. *(Partially superseded by LR-009: the operator does issue `SLAVEOF <RealMasterIP>` to a pod following the wrong/ghost master — "Rule R, Replica Rescue" — but only on a definitively-wrong `Role`/`MasterHost`, never on transient `link:down`, so it never races Sentinel's own reconfiguration. See LR-009/LR-010 and the 2026-02-19 amendment's supersession note.)*

2. **Narrow MONITOR healing** (Rule B → Rule 0): The operator no longer re-issues `SENTINEL MONITOR` to sentinels that already have a master configured — that was the race-inducing behavior. The sole exception is a sentinel that is reachable but has **no master configured at all** (`Reachable && !Monitoring`). Such a sentinel cannot self-heal via gossip: gossip requires an existing master config to subscribe to the pubsub channel, creating a circular dependency. Issuing `SENTINEL MONITOR` to a blank sentinel is non-disruptive to the rest of the cluster and is the only way to bring it back into the quorum. This targeted form is called **Rule 0** in the code.

3. **Remove offset-based promotion** (Step 5 of `DetermineRealMaster`): The operator no longer picks a "best candidate" master when no clear master exists. It waits for Sentinel to reach consensus.

4. **Keep ghost pruning** (Rule D): `SENTINEL RESET` is still issued when ghost nodes (IPs not belonging to any living pod) are detected in the topology, but only when the sentinel's current master is a valid (non-ghost) IP.

5. **Add terminating guard to label updates**: `updateMasterLabel` skips label updates when any pod is terminating, to avoid label churn during failovers.

6. **Simplify phase check**: `updateSentinelStatus` no longer polls Sentinel for replica count. The phase check uses only StatefulSet readiness and master pod presence.

## Amendment (2026-02-19): zombie-replica self-healing

> **Superseded (2026-07-12, LR-016).** The topology-aware liveness probe described below
> was reduced to a plain local health check. Restarting a data-holding pod to fix its
> replication target is a blunt, EmptyDir-wiping instrument that cannot distinguish a
> zombie (a real master exists — restart re-bootstraps onto it) from a leaderless survivor
> (no master exists — restart destroys the last copy of the data). Both are locally
> identical (`role:slave` + `link:down` + master unreachable). Zombie redirect is now owned
> by **Rule R** (LR-009: `SLAVEOF <RealMasterIP>`, surgical, no restart), and the
> leaderless case by **Rule L** (LR-015). The readiness probe (below) is retained and still
> isolates a zombie from traffic. The historical rationale is kept below for context.

### Zombie replica problem

A race condition was observed: a new Redis pod starts during a Sentinel failover transition, queries Sentinel and receives a ghost master IP (a pod being force-deleted whose network stack is still partially alive), successfully PINGs that IP in the brief window before the kernel tears down the connection, starts as `REPLICAOF <ghost>`, and then the ghost vanishes. Sentinel cannot self-heal this situation because:
- The zombie never syncs to the real master, so the real master doesn't list it as a replica.
- Sentinel only sends `SLAVEOF` to replicas it knows about (from its topology state), not to zombies pointing at ghost IPs.

Adding a `SLAVEOF` fix in the operator reconciler was evaluated and explicitly rejected: it would violate this ADR by having the operator race with Sentinel's reconfiguration logic.

### Self-healing via liveness and readiness probes

The zombie problem is instead solved at the pod level, without operator involvement:

**Liveness probe** (`buildSentinelLivenessProbe`): The probe inspects `INFO replication` and applies this logic:
1. Bootstrap guard (`/data/bootstrap-in-progress` present) → pass immediately.
2. `role:master` → pass.
3. `master_link_status:up` → pass (replica is synced).
4. `master_link_status:down` but the configured master IP is still reachable via `redis-cli PING` → pass (legitimate failover in progress; Sentinel will redirect the replica).
5. `master_link_status:down` and master IP is unreachable → **fail** (zombie: following a ghost).

The `failureThreshold` is computed from `downAfterMilliseconds + failoverTimeout + 15 s buffer` so that a healthy replica is never killed mid-failover. After the threshold is exceeded, Kubernetes restarts the pod, which runs the startup script fresh and joins the actual master.

**Readiness probe** (`buildSentinelReadinessProbe`): Immediately removes a zombie from service endpoints by failing if `master_link_status` is not `up` (and the node is not master). This is faster than the liveness threshold and ensures clients are not routed to a zombie replica.

## Amendment (2026-02-20): ghost-master RESET — targeted extension of Rule D

### Observed failure: non-self-healing sentinel split-brain after dual-failover race

A further failure mode was observed in production e2e testing: two rapid successive failovers (graceful delete → crash delete of the promoted master) can cause two sentinels to race for leadership of the second failover. The elected leader sends `SLAVEOF NO ONE` to the selected replica, but another sentinel simultaneously broadcasts `+config-update-from`, which the remaining sentinels accept. The elected leader — waiting in `failover-state-wait-promotion` — never processes the competing broadcast and never emits `+switch-master`. It is left permanently monitoring the now-dead former master (a ghost IP).

This is **non-self-healing** by design: the stuck sentinel detects its master as `s_down` but cannot reach `o_down` alone (quorum = 2, only 1 sentinel stuck). It can never trigger a corrective failover without a quorum vote, and the other sentinels, which have already moved on, will not vote.

**Forensic evidence** preserved in:
`debug-artifacts-20260220-192511-should-recover-correctly-with-both-mechanisms-active-crash/`

Key log facts from that run:
- `18:23:09` sentinel-1: `+elected-leader master mymaster 10.233.66.153`, `+selected-slave slave 10.233.64.224` — sentinel-1 won election and selected redis-1
- `18:23:10` sentinel-2: `+config-update-from sentinel cd42151c … +switch-master mymaster 10.233.66.153 → 10.233.64.224` — sentinel-0 independently completed failover; sentinel-2 followed
- sentinel-1 log: **no `+switch-master` event** — it was superseded and never recorded the switch
- `18:23:14` sentinel-1: `+sdown master mymaster 10.233.66.153` — stuck at sdown forever
- `lrctl verify` 120 s later: 2 sentinels monitoring `10.233.64.224`, 1 sentinel still monitoring ghost `10.233.66.153`

### Why the original Rule D guard prevented healing

The original guard for ghost masters inside the sentinel loop was `return nil` ("skip RESET until failover completes"). This was correct when a failover was *in progress*, but the condition fires even after the failover is *complete* on 2/3 sentinels. Every subsequent reconcile cycle hit the guard and bailed, leaving the split-brain unhealed indefinitely.

### Decision: extend Rule D to cover ghost masters (targeted RESET)

After Rule A passes (no terminating pods, no `FailoverActive` on any sentinel), if a reachable, monitoring sentinel's master IP is a ghost (confirmed absent from the Kubernetes pod list via `IsGhost()`), issue `SENTINEL RESET mymaster` to **that specific pod's IP** (not via the service). The sentinel discards its stale master config and rediscovers the real master via gossip from the other sentinels, which are already monitoring the correct IP.

This is **not** a violation of minimal interference in the sense of the reverted MONITOR/SLAVEOF healing:

| Property | Reverted MONITOR/SLAVEOF | Ghost-master RESET |
|---|---|---|
| Condition | Operator's opinion ("looks wrong") | Kubernetes-verified fact (pod is gone) |
| Pre-conditions | Weak or none | Rule A: no terminating pods, no `FailoverActive` |
| Directs sentinel to a specific new master? | Yes (`MONITOR <ip>`) | No — RESET clears stale state; gossip decides |
| Target | All sentinels via service | Single rogue sentinel pod IP only |
| Philosophical fit | "operator knows better" | "Kubernetes knows the pod is gone; sentinel doesn't" |

The action is a direct extension of Rule D (ghost pruning): Rule D removes ghost *replicas* from sentinel's topology; this amendment removes a ghost *master* from one stuck sentinel's config. The mechanism and safety conditions are identical.

### Safety argument

1. **Rule A is the primary guard.** If any pod is still terminating, or if any sentinel reports an active failover, the function returns before reaching this code. The ghost-master RESET can only fire in a fully settled post-failover steady state.
2. **`IsGhost()` is grounded in Kubernetes, not speculation.** A ghost IP is one that does not belong to any living (non-terminating) pod. It cannot be a valid master. A sentinel that monitors it is permanently stuck.
3. **RESET does not direct sentinel to any specific master.** Unlike `SENTINEL MONITOR <ip>`, RESET clears state and delegates re-discovery entirely to Sentinel's gossip. The operator cannot cause an incorrect promotion.
4. **Individual pod targeting.** The RESET is issued via the stuck pod's IP directly, not broadcast via the headless service. Other healthy sentinels are unaffected.

## Consequences
- The operator is now a "setup and observe" controller for Sentinel mode: it creates resources, bootstraps Sentinel, prunes ghosts, and updates labels/status.
- Sentinel is the sole authority for failover decisions and replica reconfiguration.
- Recovery from complex failure scenarios (e.g., all pods restarting simultaneously) may take longer, as the operator no longer force-heals the topology. This is acceptable because Sentinel is designed to handle these scenarios.
- Zombie replicas are self-healing: ~~Kubernetes restarts them without operator involvement, within `downAfterMilliseconds + failoverTimeout + buffer` seconds of the ghost master disappearing.~~ *(Superseded by LR-016: the operator redirects a zombie via Rule R `SLAVEOF` without a restart, faster and without wiping data; readiness keeps it out of traffic meanwhile.)*
- Sentinels that are permanently stuck with a ghost master after a dual-failover race are healed by a targeted RESET once the cluster settles (Rule A clears), without operator-directed master promotion.
- The `GetHealActions()` method on `SentinelClusterState` (since renamed `ReplicationState`) remains for CLI diagnostic use but the operator does not act on its recommendations.
