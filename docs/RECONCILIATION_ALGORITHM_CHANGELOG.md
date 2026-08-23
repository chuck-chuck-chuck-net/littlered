# Reconciliation Algorithm Changelog

This document tracks significant changes to the LittleRed reconciliation logic. Its purpose is to prevent regressions where a fix for one failure scenario (e.g., a deadlock) unintentionally breaks another previously fixed scenario.

## Format
- **ID:** Issue ID or descriptive name.
- **Date:** ISO Date.
- **Commit:** Git hash.
- **Problem:** Description of the failure (deadlock, race condition, sync failure).
- **Fix:** Description of the algorithmic change.
- **Regresses:** (Optional) IDs of previously fixed issues that this change might impact.

---

## [LR-001] Sentinel Failover Blocked by Reset Spam (#11)
- **Date:** 2026-02-21
- **Commit:** 3729af490027bc4b0c1c80187ed36f8c7b73d97a
- **Problem:** When a master crashes, its IP becomes a "ghost". The operator's ghost pruning logic issues `SENTINEL RESET`, which resets Sentinel's `s_down` timer. If the reconcile loop runs faster than the detection timeout, failover never triggers.
- **Fix:** Extended Rule A (Guardrails) to skip all sentinel healing if `state.RealMasterIP == ""`. The operator now remains passive during leaderless periods.
- **Impacts:** ADR-003 (Ghost Master Pruning). Rule D Extension is now gated by the presence of a living master.

## [LR-002] Cluster Mode Bus Port Mismatch
- **Date:** 2026-02-21
- **Commit:** 310510870cac8e35ebd7ade64a38f5be060bc732
- **Problem:** Positional `fmt.Sprintf` parameter mismatch caused `--cluster-announce-bus-port` to be configured with the standard port (6379) instead of the bus port (16379).
- **Fix:** Refactored all multi-line scripts in `resources.go` to use `text/template` with named parameters.
- **Regresses:** None. Internal maintenance.

## [LR-003] Cluster Mode Aggressive Failover
- **Date:** 2026-02-21
- **Commit:** 310510870cac8e35ebd7ade64a38f5be060bc732
- **Problem:** A restarted master pod (same IP, lost memory) could observe its replicas still seeing it as master, leading to a deadlock where the master waits for failover and replicas wait for the master.
- **Fix:** Added a yield loop to the cluster startup script. If failover isn't detected via peers within 30s, the starting pod issues an aggressive `CLUSTER FAILOVER TAKEOVER` to its own best-known replica.
- **Impacts:** Cluster Mode Data Safety.

## [LR-004] Sentinel Ghost Master Fallback Race
- **Date:** 2026-02-21
- **Commit:** f3de62a
- **Problem:** If a master pod crashes and restarts quickly with a new IP, it starts as a "master" by default. The operator's master identification logic (fallback mode) would pick this new IP as the authoritative master, bypassing the "leaderless" guardrail. This would then trigger Rule A+ (Ghost pruning) which would RESET Sentinels still monitoring the old (dead) IP, blocking their failover.
- **Fix:** Hardened `DetermineRealMaster` to ignore ghost IPs when counting Sentinel votes, and specifically to DISALLOW fallback to Redis-only master identification if a majority of Sentinels are still monitoring a ghost IP.
- **Impacts:** LR-001 (further hardening). Ensures the operator remains passive while Sentinel is timing out a dead master.

## [LR-005] Sentinel Divergent Master Deadlock
- **Date:** 2026-02-21
- **Commit:** 66dcd83
- **Problem:** A sentinel could miss a failover event and remain stuck monitoring a previous master IP. If that IP was still "living" (e.g. the old master restarted and became a replica), the operator's ghost pruning logic wouldn't trigger, and the sentinel would never converge with the majority.
- **Fix:** Added a new healing rule to `reconcileSentinelCluster`: if a sentinel is monitoring a living but incorrect master (divergent from the majority consensus), issue `SENTINEL REMOVE` + `SENTINEL MONITOR <consensus-master-IP>` to force it onto the correct master. *(Note: initially implemented with `SENTINEL RESET`; superseded by LR-008 which changed all divergent-master correction to REMOVE+MONITOR.)*
- **Impacts:** Sentinel convergence safety.

## [LR-006] Surgical Pod Relabeling
- **Date:** 2026-02-21
- **Commit:** d1be0ca
- **Problem:** During failover (leaderless period), the operator would relabel all living pods as 'orphan'. This caused massive K8s churn and triggered new reconciliation loops that could interfere with Sentinel's convergence.
- **Fix:** Refactored `updateMasterLabel` to be surgical. If no living master is known, the operator only ensures that the 'master' label is removed from whoever held it, leaving other pods untouched. Once a master is elected, ALL pods are reconciled to their correct labels (Master/Replica).
- **Impacts:** Cluster stability during failover.

## [LR-007] Sentinel Failover Blocked by Reset (Regression Fix)
- **Date:** 2026-02-21
- **Commit:** b5bd053
- **Problem:** An earlier attempt to allow ghost-master pruning during leaderless periods caused a regression. If a master died, its IP became a ghost. The operator would then issue `SENTINEL RESET` every 2 seconds. Because `RESET` wipes Sentinel's internal state, it reset the `down-after-milliseconds` timer, preventing failover from ever triggering.
- **Fix:** Re-instated the hard gate: NO `SENTINEL RESET` (for master or replicas) is allowed if the cluster is leaderless (`RealMasterIP == ""`). The operator must remain passive and allow Sentinel to complete its built-in failure detection and election.
- **Impacts:** Sentinel failover reliability.

## [LR-008] Sentinel Ghost Master RESET Ineffectiveness & Failure Detection Suppression
- **Date:** 2026-02-22
- **Commit:** d1c9ff9
- **Problem:**
    1. `SENTINEL RESET` does not change the master IP monitored by Sentinel; it only clears state like replicas and other sentinels. Stuck sentinels monitoring ghost IPs remained stuck even after a reset.
    2. Frequent `SENTINEL RESET` (every 2s) reset the `s_down` timer (5s), preventing failover detection for crashed masters when Rule A was bypassed (e.g. by a fast-restarting pod masquerading as a master).
- **Fix:**
    1. Replaced `SENTINEL RESET` with a `SENTINEL REMOVE` + `SENTINEL MONITOR` sequence for correcting stuck sentinels. This forces the sentinel to immediately point to the correct, living consensus master IP.
    2. Hardened Rule D (ghost pruning): `SENTINEL RESET` is now only issued if the consensus master is confirmed to be a living AND reachable pod. This ensures the operator remains passive during any period where failure detection might be in progress.
- **Impacts:** LR-001, LR-005, LR-007 (further hardening). The REMOVE+MONITOR approach now covers both ghost master IPs (LR-008's primary scope) and divergent-but-living master IPs (LR-005's scope). Ensures failover reliability and guaranteed convergence.

## [LR-009] Missing Replica Rescue (Rule R)
- **Date:** 2026-02-22
- **Commit:** 2b12e92
- **Problem:** A Redis pod could remain in "master" mode (e.g., after a restart or crash) even if a different consensus master was already established by Sentinel. The operator was missing the logic to force these rogue pods back into "replica" mode, leaving Sentinel with an incomplete replica count and preventing the cluster from reaching "Running" phase.
- **Fix:** Implemented **Rule R (Replica Rescue)** in `reconcileSentinelCluster`. The operator now iterates over all Redis pods and issues `SLAVEOF <RealMasterIP>` to any pod that is not the consensus master and is not correctly following it.
- **Impacts:** Cluster convergence to "Running" phase.

## [LR-010] Redundant Reconciliation & Aggressive Rule R
- **Date:** 2026-02-22
- **Commit:** b2ef31e
- **Problem:**
    1. The operator often issued duplicate `SLAVEOF` commands for the same pod within milliseconds. This was caused by status updates triggering immediate reconciliations that collided with the periodic requeue timer.
    2. Rule R was too aggressive, triggering `SLAVEOF` if `LinkStatus == "down"`. Since the replica handshake takes time, a second reconciliation would often trigger and interrupt an ongoing successful handshake.
- **Fix:**
    1. Added `GenerationChangedPredicate` to the `For` watch to ignore status-only updates, ensuring the 2-second timer remains the primary source of truth for periodic healing.
    2. Refined Rule R to only trigger on incorrect `Role` or `MasterHost`. It no longer triggers on `LinkStatus` alone, allowing transient handshakes to complete.
- **Impacts:** Reduction in audit log noise and faster cluster convergence.

## [LR-011] SENTINEL RESET After Failover Wipes Replica Knowledge
- **Date:** 2026-03-24
- **Problem:**
    1. After a `+switch-master` event, the old master's IP becomes a ghost `s_down,slave` within seconds. The operator detects this ghost and issues `SENTINEL RESET` — which wipes Sentinel's knowledge of ALL replicas, including the healthy ones that just reconnected to the new master. If the new master is then killed (e.g., by a test or a second failure), Sentinel cannot promote anyone: `-failover-abort-no-good-slave`.
    2. Secondary: the preStop hook's replica count check (`grep -c "^name$" || echo 0`) produced a multiline value (`0\n0`) because both `grep -c`'s output and the `echo 0` fallback were captured by the command substitution. This broke the `[ $SLAVE_COUNT -ge 2 ]` arithmetic with `Illegal number`.
- **Fix:**
    1. Added a guard to Rule D: `SENTINEL RESET` now requires at least 1 healthy (non-ghost, non-`s_down`) replica to be known to Sentinel before firing. This prevents the race where RESET fires seconds after failover, before Sentinel has re-learned the surviving replicas. The operator logs the skip when no healthy replicas are known yet.
    2. Fixed the preStop hook: replaced `|| echo 0` with `|| true` + `${SLAVE_COUNT:-0}` fallback, so the command substitution captures only the single-line count from `grep -c`.
- **Regresses:** None. The new guard is strictly additive to the existing conditions from LR-008 (living + reachable master). It covers the gap where the master IS reachable but Sentinel hasn't yet re-discovered its replicas after a recent failover.
- **Impacts:** LR-001, LR-007, LR-008 (further hardening of the SENTINEL RESET safety chain).

## [LR-012] Cluster Ground-Truth Gather Stalls on Stale Pod IPs (Slow Convergence)
- **Date:** 2026-06-24
- **Problem:** The cluster repair logic was *correct* but too *slow* to converge under rapid multi-round pod churn — the e2e test `Cluster Mode Chaos Testing > should survive multiple rounds of random pod deletions` timed out while the cluster was mid-heal (it fully recovered minutes after the test gave up). Root cause: `gatherGroundTruth` builds its pod set from the cache-backed `r.Get(pod)`, which during churn returns a **stale `Status.PodIP`** for a recently-recreated pod. `GatherClusterGroundTruth` then dialed every pod IP **serially**, and each dead IP blocked ~25s (go-redis: 5 dial attempts × 5s `DialTimeout`) before failing. With one or two stale IPs in the set, every reconcile spent 25–50s blocked inside gather, so the loop turned over only 2–3 times inside the test's 120s window — too few to FORGET ghosts, re-MEET survivors, and re-assign new replicas. A secondary stall: the ghost `CLUSTER FORGET` loop dialed *every* node including unreachable ones.
- **Fix:**
    1. **Parallelized** `GatherClusterGroundTruth` (identity probes and topology probes both fan out concurrently), so total gather latency is bounded by the slowest single probe rather than their sum. Decomposed into `gatherNodeIdentities` / `gatherTopology` / `probeNodeTopology` / `computePartitions` for testability.
    2. Added a hard per-probe deadline (`ClusterProbeTimeout = 3s`) to the operator's `GetClusterID` / `GetClusterInfo` / `GetClusterNodes`, so a dead IP fails in ~3s instead of ~25s while staying far above a live in-cluster node's sub-second response.
    3. `CLUSTER FORGET` now skips unreachable nodes.
- **Regresses:** None. This changes only gather *latency*, not decision logic or requeue cadence. Critically — and unlike the Sentinel `SENTINEL RESET` timer-reset trap (LR-001/LR-007) where a faster loop *suppressed* failover — the destructive cluster-mode interventions are gated independently of loop speed: quorum-loss `CLUSTER FAILOVER TAKEOVER` fires on a topology condition (`votingMasters <= shards/2`), and orphan force-promotion is gated by a wall-clock `orphanTimeout` (`ClusterNodeTimeout + failoverGracePeriod`) tracked via the persisted `DetectedAt`. A faster gather only lets the operator *observe and act* sooner; it cannot trip these guards prematurely.
- **Impacts:** Cluster-mode convergence speed under churn. No change to Sentinel-mode paths.

## [LR-013] Sentinel RESET Racing a Master Crash Deadlocks Failover
- **Date:** 2026-06-24
- **Problem:** The e2e test `Sentinel Failover > should elect new master after master pod deletion (crash)` (SEN-011) hit a permanent, non-self-healing deadlock: after the master pod was force-deleted, the operator issued a broadcast `SENTINEL RESET` ~1s later (Rule D ghost-replica pruning), and from then on Sentinel stayed stuck monitoring the dead master IP (`s_down,o_down,master`) with an **empty replica list**, so failover aborted with no good slave — the CR reported no master 77 minutes later. Root cause: `SENTINEL RESET` wipes Sentinel's entire replica list, which can only be rebuilt from the master's `INFO` (replicas never self-announce to Sentinel). Because the master was permanently dead, the surviving (live!) replicas could never be re-learned. The RESET fired in a race window: a `--grace-period=0 --force` delete makes the master pod *vanish* (not *terminate*), so Rule A's `anyTerminating` guard was false, and the snapshot-time `healthyReplicas > 0` guard (LR-011) still saw the just-killed master as reachable with healthy replicas. The cluster was *not whole*, but no existing guard noticed.
- **Fix:** Added a **K8s-grounded wholeness gate** to the ghost-replica RESET decision, extracted as a pure, unit-tested predicate `SentinelClusterState.GhostReplicaResetSafe(ghostFound, clusterWhole)`. `clusterWhole` is computed from ground truth already gathered each loop (`reachableRedis == SentinelRedisReplicas`) — **zero extra requests**. RESET now only fires when every expected Redis pod is reachable, on top of the prior LR-008 (living/reachable consensus master) and LR-011 (≥1 healthy known replica) conditions. When not whole, the operator defers: the ghost entry is harmless and is pruned on a later reconcile once the cluster is whole again.
- **Regresses:** None, and the asymmetry is the whole point: deferring a RESET never causes a deadlock, whereas issuing one at the wrong moment does. The gate is strictly additive to LR-008/LR-011 and only suppresses RESET during disruption — exactly when it is dangerous. Gated *only* the destructive ghost-replica RESET, not all of Rule A, so ghost-master REMOVE+MONITOR (LR-008), divergent-master correction (LR-005), and Rule R replica rescue (LR-009) still run during disruption.
- **Follow-up (scoped, not yet implemented):** *Operator-led failover recovery.* When Sentinel reports an `o_down` master with **no known replicas** but K8s shows ≥1 living reachable Redis pod, the operator should break the deadlock itself (pillar 3.5, External Knowledge) by promoting the survivor with the highest replication `Offset` (already gathered) via `SENTINEL REMOVE` + `SENTINEL MONITOR`. This is a safety net for this deadlock *and* unknown future ones; the natural e2e test is fault injection (crash master, inject `SENTINEL RESET`, assert the operator recovers) — which validates recovery, whereas LR-013's prevention is validated by the `GhostReplicaResetSafe` unit table.
- **Impacts:** LR-001, LR-007, LR-008, LR-011 (completes the SENTINEL RESET safety chain with a wholeness precondition). No change to cluster-mode paths.

## [LR-014] Empty Master Wrongly Counted as Healthy Stalls Reattach Past the e2e Window
- **Date:** 2026-06-25
- **Problem:** The e2e test `Cluster Mode Chaos Testing > Continuous Multi-Pod Failure Resilience` intermittently timed out in `verifyClusterTopologySync` with *"Master <pod> has no slots in Status"*. Root cause is a convergence-*speed* defect (the sibling of LR-012, which fixed gather *latency*): a restarted pod returns as an **empty master** (cold-start state — pure in-memory, no `cluster-config-file`), and Step 4 reattaches it via `CLUSTER REPLICATE`. But `IsHealthy` counts only masters *with slots* (`CountMasters`), so `3 slot-masters + 1 empty master + 2 replicas = 6 nodes` passes every health clause. `updateClusterStatus` therefore declared `Phase=Running` and dropped to the **30s steady requeue cadence**. The reattach can transiently fail with `ERR Unknown node` (gossip has not yet propagated the target master's NodeID to the freshly-MEETed empty pod); at a 30s retry cadence those failures compound and the empty-master state — written into `Status.Cluster.Nodes` via the repair fall-through — lingered past the test's 120s window. Reproduced on a live cluster: from first empty master to heal ≈ **2 minutes**, with two identical `Unknown node` failures **28s apart** (the steady-cadence fingerprint). The cluster always *did* fully self-heal — this is purely about how fast.
- **Fix:** Two parts. (1) `IsHealthy` now returns false when `HasEmptyMasters()` is true. An empty master means a shard is under-replicated and a node is dead weight — not a healthy steady state. With health false, `Phase != Running`, so the operator stays on the **fast (2s) requeue cadence** and the reattach completes within seconds (gossip converges ~1–2s after MEET). (2) A least-interference guard in Step 4: `CLUSTER REPLICATE` runs *on* the empty master and needs it to already know the target master's NodeID, so the operator now defers the command until `gt.NodeKnows(emptyMaster, target)` is true instead of issuing a doomed `ERR Unknown node`. The known-node adjacency is already gathered for partition detection (`gather.go`); the guard only *retains* it on `ClusterGroundTruth.KnownNodes`, at **zero extra Redis round-trips**. Validated by `cluster_state_test.go` (`TestIsHealthy` table: healthy 3×1, empty-master, zero-replica, maldistribution-tolerated, plus count/slot/state clauses; `TestNodeKnows`).
- **Why it cannot deadlock:** in cluster mode no healing action is gated on `Running`/healthy — `reconcileCluster` enters `repairCluster` precisely in the *unhealthy* branch, so reporting unhealthy only routes *into* repair and *speeds* the loop. And the new clause is operator-actionable: whenever an empty master exists, some slot-master has `< expectedReplicas` replicas, so Step 4 always has a reattach target. The clause is the *maximal* strengthening for which every predicate is backed by a repair step (no empty masters ← Step 4 in replica mode / Step 3 in zero-replica mode).
- **Deliberately not gated:** replica **maldistribution** that does not involve an empty master (e.g. 2+1+0 across shards). No repair step rebalances existing replicas, so gating health on per-master replica *counts* would be exactly the deadlock the rest of this fix avoids. The operator does not currently *produce* such a state, but the gap is real and tracked separately (littlered-internal) for an eventual replica-rebalance repair step.
- **Regresses:** None. Changes only the *health verdict* (and hence requeue cadence + reported phase) for a state that was already routed through `repairCluster` anyway (`reconcileCluster` already listed `HasEmptyMasters()` as a repair trigger). Phase now briefly reads `Initializing` during normal single-pod recovery, which is accurate. Sentinel-mode paths are untouched (`IsHealthy` is cluster-only).
- **Impacts:** LR-012 (completes the cluster-mode convergence-speed work: LR-012 fixed gather latency, LR-014 fixes the requeue cadence during reattach). `lrctl verify`/`status` now also report an empty-master cluster as not-healthy.

## [LR-015] Leaderless Sentinel Bootstrap Deadlock (Bare Sentinels) Not Self-Healed
- **Date:** 2026-07-09
- **Commit:** (pending)
- **Problem:** A production mass-restart incident (node maintenance). Two sentinel-mode instances sat stuck, not serving, for ~47–56 minutes. All three Sentinels were *bare* (reachable on :26379 but monitoring nothing — `current-epoch 0`, no persisted `sentinel monitor` line), and the Redis pods were `1/2 Running` (`redis-server` parked in the ADR-3.6/3.8 startup wait-loop, waiting for Sentinel to name a master). No reachable Redis node was a master either, so `DetermineRealMaster` returned `RealMasterIP == ""`. Every runtime healing rule — Rule 0 (re-register bare sentinel), LR-008 ghost-master REMOVE+MONITOR, LR-011/LR-013 ghost-replica RESET — is gated on `RealMasterIP != ""`, so all short-circuited. And `bootstrapSentinel()` runs only when `Status.BootstrapRequired == true`, a flag set exactly once when `Status.Phase == ""` and never re-armed. Result: an already-initialized instance whose entire Sentinel quorum lost its config has **no path back to a master**. Recovery required a human to `redis-cli SENTINEL MONITOR` one sentinel by hand (after which Rule 0 propagated to the rest), or a full CR delete+redeploy. Distinct from the LR-013 follow-up (o_down master with no known replicas): here the Sentinels are *bare*, not monitoring a dead master.
- **Fix:** New **Rule L — leaderless recovery** (`recoverLeaderlessDeadlock`), the only rule that runs while `RealMasterIP == ""`. It is deliberately conservative and fires only in the deadlock signature: (a) every reachable Sentinel is bare (`AllSentinelsBare()` — excludes a recent master death, where Sentinels still *monitor* the dead master and can fail over); (b) a reachable Sentinel *quorum* exists, so the seed can form consensus; (c) the state has persisted past a 30s cooldown tracked in `Status.LeaderlessSince` (the 2s fast requeue clears the marker long before the cooldown on a transient rollout blip); and (d) Rule A already passed (no pod terminating, no active failover). The action is **data-aware**, keyed on the count of reachable pods holding keys (the gatherer now collects per-pod key count via `INFO keyspace` and replication id into `RedisNodeState.Keys`/`.Replid`): **0 holders** → seed `redis-0` (shared `seedSentinelsWithMaster`, also now used by `bootstrapSentinel`); this is the common mass-restart case, since a pure-in-memory server that is unreachable or wait-looping has no data. **Exactly 1 holder** → promote that pod — it is necessarily a surviving replica of a dead master (a reachable `role:master` would have set `RealMasterIP`), holding the only copy of the data, so electing it discards nothing; **safe, no opt-in**. **≥2 holders** → electing one discards the others, so it **refuses** unless the owner opted in via `sentinel.allowUnsafeRebootstrapOnDeadlock`, in which case it force-elects the most-complete pod (`BestDataHolder()`: highest replication offset, tiebreak keys, then IP) and logs loudly that data on the other pods will be discarded — and, when the holders span multiple replication lineages (distinct `master_replid`), that genuinely independent writes will be lost. Because a sole/best holder is a *running* replica of a dead master, `electMaster` first issues `REPLICAOF NO ONE` to promote it (Sentinel would not promote a monitored-but-slave instance, and Rule R skips the elected master); a no-data `redis-0` elect starts fresh as master via its startup script and needs no promotion. The decision is factored into a pure function `planLeaderlessRecovery` (no I/O), unit-tested across the full gate/tier matrix — every guard that must block (sentinel monitoring, sub-quorum, no marker, within cooldown, no candidate, ≥2 holders without opt-in) and every action that must fire (seed, promote-survivor, unsafe-elect incl. divergence). Supporting logic is unit-tested too (`AllSentinelsBare`, `DataHolders`, `BestDataHolder` incl. single-holder-among-empty-and-unreachable-peers, `needsPromotion`, `pickBootstrapMasterIP`, `ParseKeyspaceKeys`). End-to-end (`test/e2e/leaderless_recovery_test.go`, Kind) reproduces the deadlock by deleting pods (Sentinel `/data` is EmptyDir ⇒ bare on restart) and asserts real recovery for all three tiers: no-data reseed, single-survivor promotion with data preserved, and the ≥2-holder refuse-gate-then-opt-in.
- **Why it cannot deadlock or wipe data unbidden:** Rule L only *adds* an action in a state where every other rule already gave up (`RealMasterIP == ""`) and where Sentinel provably cannot self-heal (all bare). The all-bare guard is what makes it safe to distinguish from a legitimate in-flight failover, which the pre-existing rules correctly still own (they require and act on `RealMasterIP != ""`). Destructive rebootstrap over live data is off by default and requires explicit per-instance opt-in.
- **Regresses:** None. `RealMasterIP != ""` paths are untouched — Rule L lives entirely inside the former `if RealMasterIP == "" { return nil }` early-out. `bootstrapSentinel` behavior is unchanged (refactored to call `seedSentinelsWithMaster`, which preserves the per-pod-IP MONITOR, idempotent skip, and best-effort semantics). Cluster-mode paths untouched.
- **Impacts:** Complements Rule 0 (bare-sentinel re-registration *with* a consensus master) and the LR-013 operator-led-recovery follow-up. `lrctl verify` now reports per-pod `keys:N`, making "empty vs holding data" visible for sentinel mode.

## [LR-016] Sentinel Liveness Probe Wipes Surviving Replicas During a Leaderless Deadlock
- **Date:** 2026-07-12
- **Commit:** (pending)
- **Problem:** The Rule L (LR-015) e2e multi-holder tier failed: the operator reported `Reseeded` (0 data holders → seed `redis-0`) where the test expected `RefusedDataPresent` (≥2 holders, opt-in off → refuse). The debug artifacts show why, and it is not in Rule L. The sentinel Redis **liveness probe** (`buildSentinelLivenessProbe`, added in the ADR-003 2026-02-19 zombie-replica amendment) restarts any replica that is not `role:master`, not `master_link_status:up`, and whose configured master IP is unreachable, once that state has held for `downAfterMilliseconds + failoverTimeout + 15s buffer`. In a leaderless deadlock — master dead **and** every Sentinel bare — there is no failover to redirect the survivors, so the probe *always* fires. Because Redis storage is EmptyDir (pillar 3.1), the restart returns the pod empty into the startup wait-loop, wiping the very replicated data Rule L exists to preserve. The artifacts confirm the sequence exactly: both surviving replicas logged `Container redis failed liveness probe, will be restarted` ~24s after their master link dropped, restarted with `keys loaded: 0`, and the operator then *correctly* observed 0 holders and reseeded. The defect is a **category error in the probe**: from a pod's local `INFO` a zombie-on-ghost (a real master exists elsewhere; restart→re-bootstrap finds it — the scenario the probe was built for) is indistinguishable from a leaderless survivor (no master exists; restart destroys the last copy). Both read as `role:slave` + `link:down` + master unreachable. The probe was betting "a real master exists somewhere" — right for zombies, catastrophic when leaderless. ADR-003's own consequence note waved leaderless away as "Sentinel handles all-pods-restart"; LR-015 already proved that false.
- **Fix:** Reduce `buildSentinelLivenessProbe` to a **local health check** — the bootstrap-in-progress guard plus a local `redis-cli PING` — identical in spirit to standalone (`buildLivenessProbe`) and cluster mode (already PING-only). The probe no longer inspects replication topology or the computed failover-window `failureThreshold`. A replica whose master is gone is now treated as *healthy and waiting*: it is redirected by **Rule R** (LR-009) when a consensus master exists (`SLAVEOF <RealMasterIP>`, surgical, no restart, no data loss), or preserved and promoted by **Rule L** (LR-015) when none does. The **readiness** probe is unchanged (still requires `role:master` or `link:up`), so a masterless or zombie replica is pulled from client traffic immediately without being killed.
- **Why this is the right layer:** topology is a *global* fact; only something with a cluster-wide view (the operator) can tell a zombie from a leaderless survivor. This change completes a supersession that Rule R already began. ADR-003 decision #1 removed operator `SLAVEOF` ("Sentinel handles all replica reconfiguration") and its 2026-02-19 amendment *explicitly rejected* operator-side zombie redirect, delegating it to the liveness probe. Rule R (LR-009, three days later) reintroduced exactly that operator `SLAVEOF` for wrong-master pods, and LR-010 made it safe (trigger only on wrong `Role`/`MasterHost`, never on `link:down` alone). The probe's fail-and-restart rule was the last remnant of the "operator stays out of replica reconfiguration" stance; removing it aligns the code with the operator-owns-topology reality (pillars 3.4/3.5) that Rule R, LR-008, LR-013, and LR-015 already embody.
- **Also fixed (test):** the multi-holder e2e tier never verified the replicas had actually received the write before killing the master (unlike the single-survivor tier, which waits on `DBSIZE`), so it could fail for a reason unrelated to the gate. Added the same replication wait so the tier establishes its own precondition.
- **Regresses:** None. Zombie replicas still self-heal — now via Rule R (`SLAVEOF` redirect, no restart, no data loss) and *faster* than the old `downAfter+failoverTimeout+buffer` liveness window, with readiness still isolating them from traffic in the meantime. A genuinely broken `redis-server` that answers `PING` but refuses `REPLICAOF` is a distinct failure class — surfaced as a recurring Rule R `Failed to rescue replica` audit error and kept out of traffic by readiness — not the topology probe's responsibility. Standalone and cluster liveness are unchanged (already PING-only).
- **Impacts:** Supersedes ADR-003's 2026-02-19 zombie-replica amendment and closes the loop on ADR-003 decision #1. Makes Rule L's survivor/multi-holder data-preservation guarantee (LR-015, pillar 3.10) actually hold end-to-end.

## [LR-017] Sentinel Reconcile Stalls on Blackholing Dead Pod IPs (Sequential / Unbounded Probes)
- **Date:** 2026-07-28
- **Commit:** (pending)
- **Problem:** On a managed cloud the single-survivor leaderless-recovery e2e failed as "data lost": `status.master.podName` stayed `redis-0` (the killed master, restarted empty into the wait-loop → `GET` returned connection-refused). It was **not** a Rule L logic bug — Rule L is correct (the local control run promoted the survivor with its data). The operator logs show a single reconcile (`reconcileID ba5169a2`) blocked **~146s** (15:35:31 → 15:37:57), the whole recovery window, dialing the freshly-killed Sentinel pods' stale IPs (`172.16.0.38`, `172.16.0.26`) which **blackholed** (`i/o timeout` / `no route to host`) one after another. Root cause is a missing per-probe deadline in two sentinel-mode paths that cluster mode already bounds (LR-012): (1) the ground-truth gather `GatherClusterState` probed every Redis and Sentinel pod **sequentially** (cluster's `gatherNodeIdentities` was made concurrent in LR-012; the sentinel loop never was); (2) the status / master-resolution path (`getMasterPodName` → `SentinelClient.GetMaster` / `GetMasterState`) loops `c.addresses` **sequentially** with `DefaultTimeout` (5s) and go-redis's default retries and **no per-address context deadline**. During pod churn the informer cache still lists killed sentinels as `Ready`, so their stale IPs enter the address list; on a cloud where a dead IP blackholes (drops packets, no RST) each address burns the full `DialTimeout × retries` (~25s) before the loop moves on. The 146s stall froze status at its stale bootstrap value **and** starved Rule L (each reconcile pass was 146s long), so the survivor was never promoted inside the test's window. Local kubeadm never reproduced it: a killed pod's IP tears down fast (RST → immediate `connection refused`), so the same paths return in ~2s.
- **Fix:** (a) `GatherClusterState` now probes all Redis and Sentinel pods **concurrently** (goroutines + barrier, results assembled single-threaded), mirroring `gatherNodeIdentities` — a single unreachable IP can no longer serialize-block the gather. (b) Renamed the cluster-specific `ClusterProbeTimeout` to a mode-neutral **`ProbeTimeout`** (3s) and applied a per-probe `context.WithTimeout(ctx, ProbeTimeout)` to the operator gatherer's `GetRedisState` (the INFO probe) and to **every read-path address loop** in `SentinelClient` (`GetMaster`, `GetMasterState`, `IsFailoverInProgress`, `GetMasterAcrossAll`, `GetReplicas`). A stale/blackholing address now fails in ≤3s regardless of go-redis retries — the context deadline cancels the retries too — bounding a reconcile at roughly one probe timeout instead of `N × (DialTimeout × retries)`.
- **Cross-mode:** this is the sentinel-mode completion of LR-012, which bounded cluster probes only. It is the motivating example for the new cross-mode-parity rule in `CLAUDE.md` §7 (fix the sibling in the same change).
- **Tests:** added `TestGatherClusterState_ProbesRunConcurrently` (red-first: RED against the sequential gather — `elapsed 721ms ≥ 720ms` serial bound, max in-flight 1 — GREEN once concurrent, 0.12s). The per-probe-timeout bound follows the LR-012 idiom and, like LR-012, carries no dedicated unit test; its one-time red is the captured 146s reconcile from the managed cloud, and a local control run confirmed Rule L promotes the survivor once the reconcile is not stalled.
- **Regresses:** None. The 3s bound is far above a live in-cluster node's sub-second response, so healthy gathers and status resolution are unaffected. Write-path sentinel commands (`Monitor`/`Set`/`Reset`/`Remove`) and the pub/sub subscriber are unchanged — they are gated by Rule A during churn and are not on the stall path. Cluster mode already had the bound (LR-012). **⚠ Corrected by LR-040: the write-path exemption is false for Rule 0, which runs *before* Rule A and so issues unbounded writes during exactly the churn Rule A sits out — a blackholing stale sentinel IP stalled one reconcile ~117s inside `MONITOR`. LR-040 further found that a `context.WithTimeout` alone does not bound these calls (go-redis unwinds for roughly another `DefaultTimeout` past the deadline), which qualifies the "fails in ≤3s regardless of go-redis retries" claim above.**
- **Verification:** confirmed on the managed cloud that produced the original 146s red. Re-running the leaderless Describe there: the **no-data** and **single-survivor** tiers pass, the probe timeout fires as designed (`failed to get master addr: context deadline exceeded` instead of a hang), and active recovery completes in ~7s with no reconcile stall (was ~146s). Unit guard (`TestGatherClusterState_ProbesRunConcurrently`) and local kubeadm control also green.
- **Test hardening (multi-holder tier):** the same cloud run surfaced that the multi-holder tier was flaky *for an unrelated reason* — with two surviving data-holding replicas and a **graceful** master delete, the killed master lingered ~30s in `Terminating` (still listed with its IP), so the operator re-registered the returning bare Sentinels onto it and Sentinel performed an ordinary, data-safe failover; the cluster never went leaderless, so the ≥2-holder REFUSE gate (and its `RefusedDataPresent` condition) was never reached. Not a regression and no data lost, but a false negative. Hardened the tier to **force-delete** (`--grace-period=0 --force`) the master + Sentinels so the killed master leaves the pod list at once. This reliably drives leaderless *detection* (Rule L starts its cooldown) — a real improvement over the graceful delete, which recovered via ordinary failover before leaderless was ever detected — but it does **not** guarantee the REFUSE *decision*: the re-verification cloud run showed the operator's own re-registration (Rule 0 / the LR-008 ghost-master correction) can still find a master and recover the cluster during the 30s cooldown, before Rule L counts holders. That is itself a data-safe recovery. So the tier is restructured to accept **either** data-safe outcome (leaderless REFUSE→opt-in→recover, *or* recovery via re-registration/failover) while **always** asserting the surviving key is intact, so a false negative (never reached REFUSE) can never become a false positive (silent data loss). The REFUSE decision matrix itself stays fully guarded by `planLeaderlessRecovery` unit tests; on the cloud it is exercised opportunistically, not deterministically.

## [LR-018] Cluster Consolidated-Shard Deadlock (One Master Owns Multiple Shard Ranges)
- **Date:** 2026-07-29
- **Commit:** (pending)
- **Problem:** A field report (`debug-0720`) from a cluster-mode instance (`shards: 3`, `replicasPerShard: 1`, Redis 8.4.2, EmptyDir) stuck in `phase: Initializing` for ~19h. Redis itself was fine (`cluster_state: ok`, all 16384 slots served) but the topology had drifted: **one master owned two of the three shard ranges** (0-5461 **and** 10923-16383) while **two pods were slotless "empty masters"** with no replicas — `CountMasters() == 2`, `cluster_size == 2`. No repair branch could act. Step 3 (missing shards) checks only that each *range* has *an* owner, never that owners are **distinct per shard**, so it saw nothing missing. Step 4 (empty-master reattach) only targets *under-replicated* slot-masters, and both slot-masters already had their one replica, so the empty masters had nowhere to go. `IsHealthy` failed forever (`CountMasters != shards`, `HasEmptyMasters`), routing into a repair that was a no-op every ~2s. The operator had **no reshard/slot-migration capability at all** (`grep` confirmed: no SETSLOT/MIGRATE/CLUSTER MIGRATION). Most-probable origin: an EmptyDir mass-restart orphaned a shard's range, and Step 3's stale pod-index→shard assumption re-assigned it (`ClusterAddSlots`) to a pod that *already* owned another range — i.e. Step 3 itself created the consolidation it then could not detect.
- **Fix:** Four parts, ASM+dance both validated e2e on the lab.
    1. **Detection (pure seam):** `PlanReshard(gt, shards)` recognises the consolidated state (a reachable master owning >1 expected shard range while a reachable empty master exists) and emits key-preserving move(s): keep the lowest-index shard on the over-consolidated master, relocate the surplus range onto the lowest-PodName reachable empty master (distinctness-only, LR-014 §11.3). Defers on a fragmented/non-aligned range (like Step 3), on no empty master, and on a healthy one-master-per-shard topology.
    2. **Prevention (root cause):** `SafeMissingShardTarget` + Step 3 hardening — a missing shard may be assigned only to a reachable **empty** master, so recovery can never pile a second range onto a master that already owns one (the drift that created the state).
    3. **Recovery (Step 3b `reshardConsolidated`), keys preserved unconditionally:** picks the mechanism by a **free gather-time capability probe** — `ParseClusterInfo` flags `cluster_slot_migration_*` presence, and `ClusterGroundTruth.AtomicSlotMigration` is the AND over all reachable nodes (mixed-version rolling upgrade ⇒ baseline). Redis 8.4+ ⇒ native atomic slot migration (`CLUSTER MIGRATION IMPORT`, re-entrant via `STATUS`). Pre-8.4 ⇒ the incremental `reshardViaDance`: idempotently re-mark IMPORTING/MIGRATING, drain ≤`ReshardMaxKeysPerReconcile` (2000) keys in `ReshardKeyBatchSize` (128) `MIGRATE` batches, and flip `SETSLOT NODE` (broadcast to all reachable masters) **only once the whole range is drained**. Ownership flips only at the end, so the source keeps owning the range in gossip and `PlanReshard` re-emits the same move — the executor **resumes from the cluster's own IMPORTING/MIGRATING markers, no persisted operator state**. Per-reconcile bound is a key *count* (deterministic, keeps each pass short for the single reconcile worker); the anti-hang bound is the per-`MIGRATE` `ReshardMigrateTimeoutMillis` (5000); an un-migratable oversized key stalls loudly. `B` / budget / timeout are advanced `spec.cluster.reshard*` fields.
    4. **Latent parser bug this exposed (commit `1a9733d`):** `ParseClusterNodes` took every trailing `CLUSTER NODES` field as an owned slot (`parts[8:]`), including the per-slot migrating `[slot->-id]` / importing `[slot-<-id]` notations. Nothing in the operator ever marked slots IMPORTING/MIGRATING before the dance, so it was latent. It broke the gather hard: the source's `Slots` gained bracket tokens (`PlanReshard` → "unparseable → defer"); the *importing but slotless* destination had `len(Slots) > 0` → counted as a slot-owning master → `CountMasters == shards`, `HasEmptyMasters == false` → `IsHealthy` wrongly true → the operator abandoned the reshard after one drain pass, stranding keys on a non-owner. Fix: skip tokens starting with `[`.
- **Design decision (preserve keys, no drop-keys opt-in):** the recovery is unconditionally non-lossy — a key-preserving reshard always exists, so (unlike sentinel's `allowUnsafeRebootstrapOnDeadlock`, ADR-005) no unsafe opt-in is warranted. See ADR-006, docs/CLUSTER_CONSOLIDATED_SHARD_RECOVERY.md.
- **Tests / validation:** pure seams red-green — `PlanReshard` (dump's exact 6-node fixture), `SafeMissingShardTarget`, `SlotsNeedingDrain`, `ParseClusterNodes_SkipsMigrationMarkers`, ASM detection (`ParseClusterInfo` + gather AND), `parseMigrationTasks`. E2e on the lab: **ASM path on Redis 8.4.2** (consolidated → resharded via `CLUSTER MIGRATION IMPORT`, 300/300 keys, `cluster_size:3`) and **dance on Redis 7.4.0** (5000-key range drained in three passes 2048/2048/904, resumed from markers, flipped, 5000/5000 keys, no leftover markers). The parser bug was found by the 7.4 e2e via `lrctl verify` — the class of defect unit tests cannot reach because they never build a mid-migration topology.
- **Regresses:** None. Adds a repair step in the already-unhealthy branch and a health verdict for a state that was already routed through `repairCluster`. Sentinel/standalone paths untouched. The parser fix is strictly more correct (owned slots only) and matters to any mid-migration observation, not just LR-018.
- **Impacts:** First cluster-mode ADR-class recovery rule (ADR-006). Completes the family LR-014 flagged (its deferred *replica*-rebalance sibling is tracked separately as LR-019). Reinforces the operator-owns-topology stance (pillars 3.4/3.5) in cluster mode, mirroring the sentinel Rule L/Rule R lineage. New pillar §3.11.

## [LR-020] Per-Shard StatefulSets & Stable Shard Identity (Cluster Mode, 0.3.0 breaking)
- **Date:** 2026-07-30
- **Commit:** (pending)
- **Problem:** Cluster mode was a **single** StatefulSet `{name}-cluster` (`replicas = shards × (1+replicasPerShard)`) with a **striped pod-index→shard model** (pod N = shard N's master; replicas mapped via `(i-shards)%shards`) and identical labels on every pod. The must-have requirement — a shard's master and replica(s) never share a failure domain, so a single node/zone loss can't take a shard's only copies (pure in-memory, EmptyDir ⇒ durability *is* domain diversity) — was **not expressible**. A `topologySpreadConstraint` only isolates pods it can *select*, evaluated at bind time and never re-run (`IgnoredDuringExecution`); it needs a stable, schedule-time, per-shard grouping key. A single StatefulSet cannot carry one: one `spec.template` ⇒ every pod stamped identically; the only per-pod metadata K8s injects (`pod-name`, `apps.kubernetes.io/pod-index`, `controller-revision-hash`) is ordinal/revision identity, never shard-semantic, and `matchLabelKeys` groups by label *equality* (raw ordinal, wrong grain). Operator-patched labels land *after* scheduling (too late); the only single-STS way to a schedule-time shard label is a bespoke mutating webhook. So it is **mandatory, not cosmetic** (full argument: ADR-007). Independently, the striped `(i-shards)%shards` decode is the fragile assumption that produced LR-018.
- **Fix (Milestone 1 — structural split):**
    1. **One StatefulSet per shard** `{name}-shard-K` (K in `0..shards-1`), each sized `1+replicasPerShard`, stamping a static `redis.chuck-chuck-chuck.net/shard: "<K>"` identity label on selector + pod template + STS metadata (`buildClusterShardStatefulSet`).
    2. **Positional-within-shard master identity:** shard K's master is `{name}-shard-K-0`, replicas `-1..R`. New pure seam `ClusterPodRefs(name, shards, replicasPerShard)` (red-first unit test) is the single source of truth for enumeration + master identity, replacing five `{name}-cluster-N` loops and the `(i-shards)%shards` formula across `gatherGroundTruth`, `bootstrapCluster` (slot + replica assignment, MEET seed = `{name}-shard-0-0`, per-shard revision gate), missing-shard recovery, readiness aggregation, and `updateClusterStatus`.
    3. **Shared Services unchanged:** the one headless Service `{name}-cluster` (selector `component=cluster`) governs every shard STS (`serviceName`), so peer discovery + pod DNS keep resolving; only shard STS *selectors* carry the shard label.
    4. **One PDB per shard** `{name}-shard-K-pdb` (redundant shards only), scoping the disruption budget to the shard's failure domain.
- **Data safety (never delete data by default):** the split renames workloads ⇒ EmptyDir clean slate. The operator does **not** auto-delete the legacy `{name}-cluster` STS — it detects it, surfaces a `LegacyClusterTopology` condition + event, and refuses to create per-shard STSs beside it until a human migrates/removes it. Reducing `shards` is refused (`ShardScaleDownRefused`) — it would orphan shard STSs and drop slots with no reshard-away path. In-place upgrade from a pre-0.3.0 cluster is unsupported; documented clean-slate migration (USAGE upgrade notes).
- **Untouched by design:** the reshard/state layers (`PlanReshard`, gatherer, `cluster_state`) are keyed by IP/NodeID/role, never pod ordinal or STS name — zero logic edits. Sentinel/standalone modes are cluster-only-by-nature out of scope (sentinel's single STS already spreads its three data pods).
- **Tests / validation:** `ClusterPodRefs` and `buildClusterShardStatefulSet`/`buildClusterShardPDB` unit tests (red-first); full unit suite + envtest green, `make lint` clean. E2e churn migrated `{name}-cluster-N` → `{name}-shard-K-O` behind semantic helpers (`clusterMasterPod`/`clusterReplicaPod`/`clusterPodNames`). E2e on Kind pending.
- **Regresses:** None to reconcile logic; the striped model is deleted, not altered. Breaking to *topology/naming* (workloads renamed, pods rebuilt) — alpha, called out loudly.
- **Impacts:** Cluster mode is now N StatefulSets (CLAUDE.md §4, new pillar §3.12, ADR-007). **Closes LR-019** (deferred *replica*-rebalance sibling) — subsumed: per-shard identity + placement is the correct home for that concern. Milestone 2 (`spec.placement.shardAntiAffinity` first-class knob + under-provisioning status) rides on this identity and is tracked separately.
- **Correction (first e2e run — "A is not free"):** the initial design (and this entry's premise) assumed per-shard StatefulSets + K8s spread deliver single-domain-loss survivability on their own. The first e2e run falsified that: the operator's own Step 4 empty-master reattach (`for _, m := range gt.Nodes`, shard-blind, random map order) welded every replica to a **different** shard's master **at bootstrap**, decoupling Redis shards from shard StatefulSets — so a per-shard-scoped spread would pin the wrong pods and a shard's master+replica could share a domain. Root cause is structural: OSS Redis/Valkey Cluster has **no failure-domain awareness** (Enterprise-only; Valkey AZ = client read routing), and both the operator's reattach *and* Redis's autonomous replica migration re-pair across shards, topology-blind. Fixes on this branch: **(1) shard-aware reattach** — pure `chooseReattachTarget` (red-first, fixtured on the observed bootstrap scramble) attaches an empty pod to the under-replicated slot-master in *its own* shard STS, cross-shard only as a logged fallback; **(2) `cluster-allow-replica-migration no`** in the cluster config so Redis never autonomously re-pairs across shards. The operator is now the sole topology authority (ADR-007 Decision 6; a thin slice of Direction B that A requires). **New guards:** pure `ClusterGroundTruth.CheckShardColocation` (red-first) flags any replica whose master is in a different shard STS; `lrctl verify` now **fails** on such a violation (it previously reported a scrambled cluster as healthy, since Redis itself was fine) and reports a `[DEGRADED]` warn tier when a replica's replication link is down (reduced redundancy ≠ "healthy and consistent"); the e2e `verifyClusterTopologySync` asserts the same colocation invariant so a regression goes red in CI. Separately, the run surfaced an **availability** (not data-loss; `corruptions: 0`) regression: per-shard STSs roll in parallel, so a config change restarts all masters in one wave — the single STS serialized restarts globally. Fix tracked as **LR-021** (operator-serialized rolling updates across shard STSs).

## [LR-021] Operator-Serialized Rolling Updates Across Shard StatefulSets (Cluster Mode)
- **Date:** 2026-07-30
- **Commit:** (pending)
- **Problem:** LR-020 split cluster mode into one StatefulSet per shard, which silently dropped the *global* one-pod-at-a-time restart serialization the single pre-0.3.0 StatefulSet provided for free. `reconcileClusterStatefulSet` applied the pod template to all N shard StatefulSets in a single pass, so an operator-driven change (config/resource edit) rolled every shard **in parallel**: the StatefulSet controllers restarted each shard's `-0` master in one wave, leaving the whole cluster briefly master-less. The first e2e run measured this as ~24% of operations failing (i/o timeout) during a rolling update — an **availability** regression (`corruptions: 0` — no data loss). Root cause: parallel per-shard rollout with no cross-shard coordination.
- **Fix:** `reconcileClusterStatefulSet` now serializes template **updates** across shards. Missing shard StatefulSets are still created immediately and in parallel (a fresh bootstrap has no data to protect); but for an existing shard whose applied pod-template hash differs from desired, the operator rolls **only that shard** and returns, deferring every later shard until the current one has fully settled. Settle is the pure `clusterShardRolloutSettled` (red-first): `ObservedGeneration == Generation` (the just-applied change is observed, closing the cache-lag race), `UpdateRevision == CurrentRevision` (no roll in progress), and `UpdatedReplicas == ReadyReplicas == Replicas`. Change detection uses a new `AnnotationPodSpecHash` — a 16-char hash of the operator-authored pod template stamped on the template itself — compared cache-safely as a stored annotation value (no diffing of server-defaulted fields, no uncached read needed). Net effect: at most one shard (hence one master) rolls at a time, restoring the pre-split serialization at shard granularity.
- **Scope / limitation:** this governs only rollouts the **operator** triggers (a spec/config change rewriting the pod template). A manual `kubectl rollout restart` of the shard StatefulSets bypasses the operator and is *not* serialized — documented as "roll shards one at a time by hand." On first upgrade to this build, existing shard StatefulSets lack `AnnotationPodSpecHash`, so they roll once (serialized) to acquire it — a one-time, availability-safe rollout.
- **Tests / validation:** `clusterShardRolloutSettled` unit test (red-first, covers the observed-generation cache-lag window). The cluster chaos "rolling restart" e2e was switched from a manual parallel `kubectl rollout restart` (which the operator cannot govern) to an **operator-mediated** rollout (a CR pod-template change) and keeps the availability ≥95% assertion, so it now exercises the serialization end-to-end. Resolves design-doc `docs/PER_SHARD_STATEFULSET_DESIGN.md` open question §7.4.
- **Regresses:** None. Bootstrap create-path is unchanged (parallel); only the update-path is staged. Sentinel/standalone untouched.
- **Impacts:** Cluster-mode config changes/upgrades now roll shard-by-shard (ADR-007, CLAUDE.md pillar 3.12). Independent of the LR-020 shard-colocation fix, but shares its "first e2e run" origin.

## [LR-022] First-Class Per-Shard Placement Knob (`spec.placement.shardAntiAffinity`, Cluster Mode)
- **Date:** 2026-07-30
- **Commit:** (pending)
- **Problem:** LR-020 gave cluster mode the *structure* for single-failure-domain survivability (one StatefulSet per shard + stable shard label), and the operator pins each Redis shard inside its StatefulSet (LR-020 reattach + `cluster-allow-replica-migration no`). But the actual placement — "a shard's master and replica(s) never share a node/zone" — was still only expressible by hand-writing a `topologySpreadConstraint` into `spec.podTemplate`, which users **cannot** practically do: it must select on the operator-owned `redis.chuck-chuck-chuck.net/shard` label they don't control. So Goal 1 (ADR-007) was structurally enabled but ergonomically out of reach (USAGE.md flagged it as a current limitation).
- **Fix (Milestone 2, additive/non-breaking):** new `spec.placement.shardAntiAffinity { topologyKey, whenUnsatisfiable }`. The operator translates it into a **per-shard** `corev1.TopologySpreadConstraint` (`maxSkew: 1`, `labelSelector` = `clusterShardSelectorLabels(lr, K)` = that shard's pods) injected into each shard StatefulSet via the pure `buildShardSpreadConstraint` (red-first), **appended** to any `spec.podTemplate.topologySpreadConstraints` (operator's shard-scoped constraint always applies; users layer more). Injected before the LR-021 pod-spec hash, so enabling the knob triggers one serialized rollout that re-places pods per the constraint.
- **Defaults / decisions:** `topologyKey` defaults to `kubernetes.io/hostname` and `whenUnsatisfiable` to **`ScheduleAnyway` (soft)** — matching CloudNativePG's defaults (`preferred` + hostname; `required` is opt-in) and Strimzi's opt-in posture, and pillar 3.5 ("enable, don't force"): small/dev/single-node clusters still schedule; production opts into `DoNotSchedule` for a hard guarantee (at the cost of `Pending` when domains < a shard's pods). `topologySpreadConstraint` (not `podAntiAffinity`) — `maxSkew`+per-STS selector expresses "spread this shard's pods across domains" directly, no `matchLabelKeys` (no K8s version floor). Cluster-mode only (validation rejects it in other modes; sentinel/standalone spread via `spec.podTemplate`).
- **Deferred (tracked):** the under-provisioning **status condition** (warn when failure domains < 1+replicasPerShard) — it needs cluster-wide `nodes` list/watch RBAC (the operator reads no node topology today) + subtle usable-domain counting. With the soft default pods never `Pending`, and a hard `Pending` is already surfaced by `Ready<Total`/`Initializing`, so the condition is a diagnostic nicety, not a correctness need. Follow-up.
- **Tests / validation:** `buildShardSpreadConstraint` (red-first: nil unset; correct maxSkew/topologyKey/whenUnsatisfiable/shard-scoped selector; self-defaulting), STS-level merge order (user first, operator appended, spec slice not mutated), `PlacementSpec.SetDefaults`, `validatePlacementSpec` (mode gate + enum) — all unit, `make lint` clean, CRD/deepcopy regenerated (no RBAC change). New e2e `Cluster Mode Per-Shard Placement`: with a hard hostname anti-affinity, asserts each shard's pods land on **distinct nodes** (the first test that verifies the failure-domain isolation Direction A exists for), guarded by a schedulable-node-count skip.
- **Regresses:** None. Purely additive API; when unset, behaviour is identical to LR-020/021. Sentinel/standalone untouched.
- **Impacts:** Closes the Goal-1 ergonomics gap (ADR-007, CLAUDE.md pillar 3.12; design-doc §4.4/§7.1 resolved, §7.3 deferred). Direction B (topology-aware master balancing) remains future work.

## [LR-023] Cluster Total-/Partial-Wipe Re-Bootstrap (Operator-Driven Pod Recycle)
- **Date:** 2026-07-31
- **Commit:** (pending)
- **Problem:** The cluster analog of the sentinel leaderless bootstrap deadlock (LR-015), reproduced e2e. When every cluster pod is lost at once, recovery splits by how EmptyDir is affected. A **pod-delete** wipe (node-pool recycle / eviction) returns every pod fresh + isolated and **self-heals** through the normal repair loop (Step 1 MEET → Step 3 assign-missing to fresh intended masters, `SafeMissingShardTarget` → Step 4 shard-aware reattach) — never reaching `bootstrapCluster`, and with no data-safety dilemma (a total wipe leaves zero data holders: cluster data ⟺ slot ownership). A **mass container-crash** (kill-9 / OOM storm) instead keeps pod/IP/EmptyDir, so `nodes.conf` survives and every restarted master enters the startup script's STEP-3 yield loop (LR-003) with no reachable replica to confirm its demotion. With no live replica to fail over to, `TAKEOVER` cannot resolve it, each master parks (`sleep 3600` → liveness-killed → `CrashLoopBackOff`), and the pods never become Ready — so the operator, which gates repair on `allPodsReady`, never gathers or acts. Confirmed on Kind: 5/6 pods `CrashLoopBackOff`, one escaped replica as an empty master, `cluster_state:fail`, 0/16384 slots, no recovery.
- **Fix:** Operator-owned, not script-owned (a parked pod cannot tell a total wipe from a temporarily-unreachable live replica — the STEP-3 park branch is *not* a clean "no live replica" dichotomy; that decision needs the global view, echoing LR-016). New `recoverClusterWipeDeadlock`, called from `reconcileCluster`'s not-all-Ready branch, recycles (deletes) exactly the pods matching the wipe signature so their StatefulSets reschedule them fresh (clean EmptyDir → new node ID); the pod-delete self-heal path then re-bootstraps. Decision lives in the pure `planClusterWipeRecovery` (unit-tested, red-first): recyclable = redis container **not-Ready + restarted (crash-looping) + not OOMKilled**, gated by a `WipeDeadlockSince` cooldown (120s, safely above the script's ~60s yield) mirroring the sentinel `LeaderlessSince`. Startup script is left **untouched** (keeps its conservative yield/park).
- **Why it is data-safe:** the safety gate is the **kubelet's local readiness probe** (authoritative and blackhole-proof — *not* the operator's remote dial, which LR-017 showed a blackhole can fool). In a pure in-memory (EmptyDir) cluster, data lives only in the RAM of a *serving* redis; a not-Ready + crash-looping pod's redis is down, so it holds no data and deleting it loses nothing, by construction. A **Ready** pod (a possible data holder) is **never** recycled — so a partial wipe with a surviving replica keeps that replica, which the existing repair (Step 0/1 orphan promotion) then promotes. OOM kills are excluded (distinct failure mode; recycling would only churn).
- **Tests / validation:** `TestPlanClusterWipeRecovery` (red-first table: clear / ready-holder-excluded / not-restarted-excluded / OOM-excluded / start-cooldown / wait / recycle-stuck-only). New e2e `Cluster Total-Wipe Re-Bootstrap`: Flavor A (pod-delete → self-heal regression guard), Flavor B (mass kill-9 → the operator must recycle-and-recover), and a **partial-wipe** tier that keeps one shard's replica alive with data and asserts it survives (the explicit ADR-001/LR-003 crash-protection regression guard). RBAC: added `delete` on `pods`. New status field `cluster.wipeDeadlockSince`.
- **Validated:** e2e 3/3 on a real 3-node cluster (scm-s2, 2026-07-31). Flavor A (pod-delete) self-heals ~60s; Flavor B (mass kill-9, previously deadlocked) recovers ~2.5min (120s cooldown + reschedule + re-bootstrap); partial-wipe keeps the surviving replica's data intact — the existing Step 0/1 orphan promotion beat any slot-assignment race, so no repair-ordering change was needed.
- **Regresses:** None. The startup yield/park (LR-003) is unchanged; the operator only *adds* an action in a state where pods are already stuck not-Ready and (by the RAM-only invariant) hold no data. Recycling is gated on the crash-loop signature + a cooldown longer than the script's own recovery window, so a pod that would self-recover is never preempted, and Ready data holders are never touched.
- **Impacts:** CLAUDE.md pillar 3.13 + §9; ADR-008. Completes the LR-015 feature-parity item for cluster mode (BACKLOG "Cluster mode — HA correctness gaps").

## [LR-024] Ghost-Master Sentinel Failover Deadlock — Operator-Led Recovery
- **Date:** 2026-07-31
- **Commit:** 4c949d4 (recovery) + 9e4261a (promotion-chain lineage fix)
- **Problem:** The long-standing e2e `Sentinel Failover > should elect new master after master pod deletion (crash)` (SEN-011) deadlocked on a real multi-node cluster (scm-s2; not reproducible on isolated Kind — timing/env-sensitive, cf. LR-017). The `Sentinel Failover` context is `Ordered`: `(graceful)` runs first and passes, then `(crash)` runs on the *same* instance. The graceful failover leaves the old master IP as a ghost replica; the operator's Rule D ghost-replica `SENTINEL RESET` fires ~4s later — and here it is *correctly permitted*, because between the two failovers the cluster is legitimately whole (the graceful-deleted pod returned as a fresh replica), so LR-013's wholeness gate passes. RESET empties Sentinel's replica list; the crash then kills the current master ~4s later, before Sentinel re-learns the replicas from its `INFO` → `-failover-abort-no-good-slave` forever, CR reports no master. The operator could not heal it: it falls in the gap between **LR-008** ghost-*master* correction (needs a living consensus master to MONITOR — none; every pod is slave-of the ghost) and **Rule L** leaderless recovery (needs all Sentinels *bare* — they monitor the ghost). Source-confirmed (Redis + Valkey `sentinel.c`): there is no surgical single-replica prune (only whole-list RESET / whole-master REMOVE) and a dead replica never ages out — so the permanent deadlock is self-inflicted by the RESET (Sentinel's own no-good-slave is otherwise transient and self-heals). Not a fresh regression: Rule D ships in v0.2.1; LR-013 fenced the RESET-after-force-delete ordering, this is a second, uncovered ordering (prior-failover ghost → RESET → next crash).
- **Fix (recovery half — the deferred LR-013 follow-up):** a new "ghost-master stuck" rule, sibling of Rule L, in the same `RealMasterIP == ""` reconcile branch. `recoverGhostMasterDeadlock` (pure `planGhostMasterRecovery`, red-first table) fires when a majority of reachable Sentinels monitor a **ghost** master (`SentinelsMonitorGhostMaster`), Sentinel knows **no** promotable replica (`!HasHealthyKnownReplica` — the discriminator from a recent master death, where a legitimate failover is imminent), a Sentinel quorum is reachable, and the state persists past a 30s `status.ghostMasterStuckSince` cooldown (mirrors `LeaderlessSince`). Action reuses `electMaster` (REMOVE + MONITOR + `REPLICAOF NO ONE`) on the most-complete survivor (`BestDataHolder`).
- **Safety gate — LINEAGE, not holder count (the key difference from Rule L):** the ghost-master survivors are replicas of the *same* dead master, so electing the highest-offset one discards nothing (the losers resync from it). Divergence is keyed on replication lineage (`holdersDiverged`), not the raw holder count Rule L uses: same-lineage survivors elect with **no** opt-in; only genuinely independent lineages still require `sentinel.allowUnsafeRebootstrapOnDeadlock`.
- **Promotion-chain lineage fix (e2e finding, commit 9e4261a):** the first e2e verification (img 01abe98) still refused, seeing the survivors as "divergent". A graceful→crash sequence produces a normal **promotion chain**: a promoted/resynced node rotates its `master_replid` to a new value and shifts the old one into `master_replid2` (observed live: redis-0 `716d42`; redis-1 `716d42` / replid2 `7df3f8`; redis-2 master `1cc4b7` / replid2 `716d42` — one lineage connected through `716d42`, one identical key). But divergence compared the *current* `master_replid` alone and the gather never captured `master_replid2`. Fix: gather `master_replid2` and compute divergence via union-find over each holder's `{replid, replid2}` — holders in one connected component are the same lineage; only truly independent histories (no shared replid) stay divergent. **General lesson: replication-lineage checks must use `master_replid2`, not `master_replid` alone.**
- **Sentinel-repoint fix (second e2e finding, commit 9a0f2a6):** the next run then elected the survivor correctly but the election did not *take* — the operator elected `redis-2` (`role:master`) yet all Sentinels stayed on the ghost, oscillating (elect → re-detect → cooldown → elect) every 30s. Cause: `seedSentinelsWithMaster`'s idempotent guard skipped any Sentinel that "already knows a master", which during the deadlock is the **ghost** master, so it never issued `MONITOR`. And a plain `SENTINEL MONITOR` is rejected while a same-named master is still configured, so even without the guard the repoint would silently no-op. Fixed: skip only when the Sentinel already monitors the **target** master (no churn once correct); otherwise `SENTINEL REMOVE` the stale entry first, then `MONITOR` — so `electMaster` genuinely does REMOVE+MONITOR (LR-008) and reaches a ghost-stuck Sentinel. **Both this and the lineage bug were in the execution path (`electMaster`) past the pure planner, so only e2e caught them — the real-Sentinel repoint has no unit seam.**
- **Prevention deferred (Rule D fate):** the ghost-replica `SENTINEL RESET` is the *self-inflicted* cause, and a lingering ghost replica is correctness-benign (skipped for promotion; `status` counts are StatefulSet-driven, not ghost-driven) but permanently dirties the monitoring signal (it never ages out). Whether to retire it / low-frequency-GC / expose an on-request `lrctl` prune verb is deferred to review (incl. Michael). See `docs/GHOST_MASTER_FAILOVER_DEADLOCK_DESIGN.md`; prospective ADR-010 bundles that decision.
- **Tests / validation:** `TestPlanGhostMasterRecovery` (red-first: gates — bare-vs-leaderless, healthy-replica-known → do not steal a failover, below-quorum, cooldown; tiers — 0-holder seed, 1-holder elect, same-lineage multi-holder elect with no opt-in, **promotion-chain 2-node + real 3-node**, genuinely-independent → refuse, opt-in unsafe-elect) and `TestSentinelsMonitorGhostMaster`. New status field `status.ghostMasterStuckSince` + condition `GhostMasterRecovery`. `make lint` clean; envtest green. The graceful→crash e2e is the already-banked tier-3 red (reproduced 2/2 on scm-s2); e2e green re-verification pending on `e2e-0731` @ 38e9df2.
- **Regresses:** None. The recovery only *adds* an action in a no-living-master state Sentinel cannot resolve itself (`no-good-slave`), gated on `!HasHealthyKnownReplica` + a 30s cooldown so a legitimate in-progress failover is never preempted; the lineage gate preserves data (a genuinely-divergent set still refuses without the opt-in). Rule D is untouched (prevention deferred). Rule L (LR-015) shares `BestDataHolder`; the lineage-aware `holdersDiverged` only *relaxes* false divergence (promotion chains) and never calls independent lineages the same.
- **Impacts:** CLAUDE.md pillar 3.10 (ghost-master variant) + §9; prospective ADR-010 (deferred — bundles the Rule D prune-policy decision). Completes the LR-013 deferred follow-up.

## [ADR-011] Failover Mode — New Reconciliation Algorithm (Cross-Reference)
- **Date:** 2026-08-01
- **Commit:** 380af0d..8e68272 (`feat/failover-mode`, five commits: pure seams, API, resources/startup protocol, reconcile flow, master watcher)
- **Scope note:** this is a **new mode**, not a fix to an existing algorithm, so it gets a cross-reference rather than a full entry — the algorithm itself is specified in ADR-011 and `docs/RECONCILIATION_LOOP_FAILOVER.md`, and future fixes *to* it will land here as normal LR entries. Recorded because the changelog is the regression ledger for reconciliation logic, and reviewers should know the boundary of what this change can regress.
- **What:** `mode: failover` (experimental) — operator-managed HA without Sentinel. One decision table (`planFailover`) replaces the sentinel-mode rule family (bootstrap, Rule L/LR-015, LR-024) for this topology; failure detection is the pure `planMasterDeath` (kubelet-authoritative immediate + corroborated probe evidence); pod roles are assigned via operator-stamped annotations read back through a downward-API volume, epoch-fenced against the ADR-001 kill-9 hazard.
- **Regresses:** None expected by construction — the mode lives in new files (`failover_{plan,intent,reconcile,monitor}.go`, `resources_failover.go`); the shared touchpoints are the mode dispatch, the `bootstrapRequired` initialization (now also armed for `mode: failover`), and the shared monitor-event channel. Sentinel/cluster/standalone reconcile paths are untouched.
- **Verification:** the pure-seam unit tables (`failover_plan_test.go`, `failover_intent_test.go`, `failover_monitor_test.go`) are green; envtest reconcile coverage lives in `failover_controller_test.go`. **e2e-verified 16/16 on a real 3-node cluster (scm-s2, 2026-08-01)** — `test/e2e/failover_mode_test.go` (label `failover-mode`): functional, graceful/crash failover, <15s event path (4.96s measured), hybrid double-failover (the LR-007/LR-008/LR-024 class does not reproduce), kill-9 epoch-yield (chaos corruptions 0, write availability 96.5%), deadlock tiers, rolling update. No operator-code fixes were needed by the run. The ADR-011 §8 graduation-gate remainder (chaos/soak, managed-cloud dogfooding) stays open.

## [LR-025] Legacy→Per-Shard Migration Restart-Safety — Replicate-then-Failover (supersedes the reshard mechanism)
- **Problem (found live on s1, dance path, redis 7.4.0):** the ADR-013 in-place legacy→per-shard migration first drained slots node-to-node (native ASM / MIGRATE dance) from a `Draining` phase and attached the new replicas *afterward* (`ReplicasAttached`). This is not restart-safe: during `Draining` each new per-shard master `{name}-shard-K-0` owns slots on EmptyDir (pillar 3.1) **with no replica yet**, so it is a single point of failure for the whole drain window. A restart mid-drain (observed on s1) both **deadlocks** — the startup STEP-3 guard (LR-003) sees the node still owns slots, finds no replica to `TAKEOVER`, parks → `CrashLoopBackOff` — and **loses data**: the migrated keys were already `MIGRATE`-deleted from the legacy source and lived only in that master's RAM. A design gap, not a bug; latent in the ASM path too (its ~8s atomic window merely made a restart unlikely).
- **Fix — mechanism redesign, replicate-then-failover:** migration no longer moves slots. Each new pod joins as a **slot-less replica** of the legacy master owning its range, full-syncs, then `{name}-shard-K-0` is promoted by a **coordinated `CLUSTER FAILOVER`** (atomic ownership flip; the legacy master demotes to a live replica of it). New nodes reach the slot-owning state only *after* an already-redundant handoff, so the unsafe "owns slots, no synced replica" state never exists. Phases `Standup → Meet → Draining → ReplicasAttached` become `Standup → Meet → Replicate → Failover → Decommission → Complete`. Uses only Redis's most-trusted primitives (`MEET`/`REPLICATE`/coordinated `FAILOVER`/`FORGET`, all already in `cluster_client.go`); the LR-018 reshard executor stays in the tree but migration no longer calls it. **The startup script is unchanged** — at the slot-owning instant a new master always has a synced replica (the demoted legacy master, then its own new replicas), so STEP-3's existing `TAKEOVER` breaker suffices.
- **Why data-safe (all `replicasPerShard`, incl. 0):** every slot has ≥2 live copies at every instant — legacy master + new node(s) during sync; new master + demoted-legacy master (+ new replicas) after failover. The demoted legacy master remains a live replica until Decommission, so even a no-replica cluster is restart-safe *throughout* the migration and returns to its single-copy contract only at the final `FORGET`. Two pure-seam invariants carry this red-first (`internal/redis/migration_plan.go`): (i) a `{name}-shard-K-0` is only ever emitted for `Failover` when it is a link-`up` replica of the range owner; (ii) legacy nodes are only `FORGET`-ed once every new replica is a link-`up` replica of its new master — the redundancy gate, enforced *by the strict phase precedence itself* (Decommission is unreachable until Replicate is fully satisfied against the post-failover owner). As-built: `DeleteLegacy` keys on `!anyLegacyOwnsSlots`, not "no legacy remains" (the latter is `Complete`, which outranks Decommission → would never delete the legacy STS).
- **Tests / validation:** `TestPlanClusterMigration` red-first table (12 cases: standup/meet/replicate incl. `NodeKnows`-deferred, failover one-per-lowest-K + the unreachable-owner `Force` TAKEOVER edge, decommission redundancy gate, complete, both invariant guards, and a full **rps=0** sub-table) observed red against the old reshard body before implementation; `LegacyMigrationReady`/`LegacyShapePreserved` unchanged. e2e (`test/e2e/cluster_migration_test.go`) **live-verified on s1 at 7.4.0** (the environment that deadlocked) + 8.4.2 cross-version: default + hold + a new **restart-during-migration chaos tier** all green. The chaos tier crashes a `{name}-shard-K-0` container (real hostPID PID-1 kill — EmptyDir/`nodes.conf` survive, IP preserved) both pre-failover (slot-less replica) and post-failover (owns slots — the exact old-bug state) and asserts no `CrashLoopBackOff` deadlock (PING-PONG **and** `waiting.reason != CrashLoopBackOff`), byte-for-byte no data loss, and migration still reaching `Complete`. Coexistence sampler: `CLUSTERDOWN=0`, `wrongValue=0` (no slot-move blip, no corruption — contrast the superseded ASM/dance CLUSTERDOWN window). No first-contact code fix was needed; the pure seam handled the old-bug scenario correctly on first real contact.
- **Regresses:** None. The reshard executor (LR-018) is untouched and still used for consolidated-shard recovery; migration simply stops calling it. The startup script (STEP-3, LR-003) is unchanged. The change removes an unsafe state rather than adding a rule.
- **Impacts:** ADR-013 (Status amendment, Context reframe, Decision 4/6, phases, Consequences, Alternatives — the reshard mechanism is now the *rejected* alternative); `docs/LEGACY_CLUSTER_MIGRATION_DESIGN.md` §2.2/§5/§7 + new §10 (pivot + survives/superseded list).

### Addendum — chaos "Can't replicate myself" self-replicate deadlock (found live on scm-s2)
- **Problem (a *second*, distinct chaos failure mode of the replicate-then-failover mechanism):** the restart-during-migration chaos tier deadlocked on scm-s2 (redis 7.4.0), CR stuck at `phase Replicate (2/3)` for 14m+, operator looping `ERR Can't replicate myself` every ~2s. Root cause: crash (B) crashed the intended new master `{name}-shard-0-0` while it owned its slot range; while it was down **Redis natively failed the range over to the new *replica* pod `{name}-shard-0-1`**, promoting it to master. The Replicate planner then computed "the current owner of shard 0's range" — now `{name}-shard-0-1` **itself** — and emitted `CLUSTER REPLICATE <self>` for it, which Redis rejects forever. The planner exempted only `{name}-shard-K-0` from being a replicate target when it owns its range (the normal post-failover case); it did not handle *any* new pod owning the range via native failover. Not a data-safety issue (all 16384 slots stayed served); a pre-`Failover` correctness stall. Env/timing-sensitive: not reproduced on s1 (fast dead-IP RST narrows the native-failover window); scm-s2's slower blackholing widened it so the new replica won the election.
- **Fix (pure seam first, red-first):** in `planReplicates` (`internal/redis/migration_plan.go`), generalize the "already owns its range ⇒ settled, not a replicate target" exemption from *just* `{name}-shard-K-0` to **any** new pod (`nodeOwnsRange(node, …)`). A node is never its own range owner, so `CLUSTER REPLICATE <self>` can no longer be emitted; the crashed-and-restarted `{name}-shard-K-0` is attached onto the new owner instead, and the `Failover` phase then reconciles which `{name}-shard-K-0` is master (roles are fluid in cluster mode). Belt-and-suspenders in the driver (`cluster_migration.go`, `executeMigrationPhase`): a REPLICATE whose target NodeID equals the pod's own NodeID is skipped rather than issued, so a future plan regression cannot wedge the loop.
- **Tests / validation:** red-first unit guards in `TestPlanClusterMigration` (new "replicate CHAOS" table case — a native failover promoted a new replica to own the range) and a dedicated `TestPlanReplicateNeverTargetsSelf` (invariant: the plan never emits a REPLICATE whose target is the replica pod's own NodeID), both observed **red** against the pre-fix planner (emitted `{10.1.0.2 → N01}` self) and green after. The e2e chaos tier's existing `waitMigrationComplete` assertion is the integration guard (it is what timed out in the field); reproduction there stays opportunistic (the native-failover-to-K-1 window is env-sensitive), with the deterministic guard in the unit tests per the tier-3 test discipline.
- **Regresses:** None. The generalized exemption is strictly broader than the old `{name}-shard-K-0`-only check and identical on the normal path (no new pod owns a range until failover).

## [LR-038] Graceful Failover Silently Lost Acknowledged Writes — the Outgoing Master Was Never Fenced (failover mode)
- **Problem (measured on t3e, 2026-08-17; failover mode, `mode: failover`):** a graceful master delete lost **202 of 1171 acknowledged writes** — silently. Every assertion the suite had passed while it happened: `DataCorruptions: 0` (the keys were *gone*, not wrong) and write availability 97.66% (the writes were ACKed; only the data vanished). ADR-011 §7 moved the handover from the pod to the operator, and the operator promotes a replica but **never speaks to the outgoing master**. So on a graceful delete that master keeps running and keeps ACKing writes for its whole ~10s preStop window (`resources_failover.go`), and those writes die with the pod. The loss is **not** a race against how fast the operator promotes, which is how it was first misread: an established TCP connection through the master Service is not re-routed by the operator's label flip, so the client keeps writing into the doomed pod for the entire grace window however fast the operator is. Arithmetic matches exactly — ~10s × 10 writes/s × 2 failovers ≈ 200. Sentinel mode does not have this hole: its preStop runs `SENTINEL failover mymaster` and waits for the master address to change, so **sentinel converts handover into visible write failures where failover-graceful converted it into acknowledged-then-lost writes** — a data-safety difference that the headline availability numbers actively hid (failover *beat* sentinel on write availability in both variants).
- **Why the measurement had to come first:** the two candidate explanations were indistinguishable in the metrics, because `doRead` folded them into one counter — `Get(...)` returning `redis.Nil` for a *missing* key is an `err != nil`, so a vanished write incremented `ReadFailures` exactly like a timeout. The first live run therefore read as "77% read availability", an availability problem. Splitting the classification (pure `classifyRead`: `readOK | readFailed | readLost | readCorrupt`) and adding an exact post-traffic sweep over every acknowledged write (`VerifyAll`) turned the argument into a count: of the 263 "read failures" in the original run, only 19 were real unavailability. A sampled counter could not have carried the assertion either — reads pick a random confirmed key, so one lost key read five times looks like five.
- **Fix — fence the outgoing master (pure `planFailoverFence`, applied in `executeFailoverPlan`):** as part of the promotion, demote the outgoing master (`REPLICAOF <new-master>`) so it answers `-READONLY`. The loss becomes **visible write failures instead of silent data loss** — pillar 3.2's "errors rather than silent data loss", applied to failover instead of to memory pressure. This is **not a new mechanism**: it is the existing straggler repoint (`planFailoverRepoints`) applied to the one pod its caller's conservative secondary-healing gate (`settled && !anyTerminating`) excludes, at the one moment it matters — and it is what Rule R would eventually do anyway, which is why it is best-effort and idempotent rather than fatal. Deliberately narrow: only the master being replaced is fenced; the healthy stragglers keep their gate. Nothing is fenced when the outgoing master is unreachable (the crash path — and no dial is wasted on a dead or blackholing IP, LR-017), already demoted, absent from the gather, or *is* the pod being promoted (a resumed half-applied promotion — fencing it would demote the new master).
- **Why the planned next experiment was dropped:** the proposal on the table was `failover.minReplicasToWrite: 1`, expecting the dying master to refuse writes once its replicas left. It cannot work at the default `replicas: 2`, and the code says so: the straggler repoint is blocked by `!anyTerminating` for the whole grace window, so the second replica stays attached to the dying master and the directive stays **satisfied** throughout. `minReplicasToWrite: 2` would fire, but makes any single replica restart stop writes cluster-wide. Only fencing closes this.
- **Tests / validation:** red-first at both tiers. Unit — `TestPlanFailoverFence` (6 rows) written against a stub and observed red on the load-bearing row (`= "", want "10.0.0.1"`); an "always fence" mutant fails 4 of the 5 guard rows, so the guards have teeth. e2e — the durability bound was added to the `rapid double failover` graceful tier *before* the fix and observed red on t3e at **202 of 1171** against a bound of 5. The bound is derived, not tuned: a correct handover can only lose writes ACKed within the replication lag of the promotion instant (~1 per failover at 10 writes/s, so ~2 here). Two guards keep it from passing vacuously — the sweep must have checked a meaningful number of keys, and unreadable must stay under 10% of them. The crash path is deliberately left unbounded: a kill -9 loses the unreplicated tail by construction, and asserting an unmeasured number would be tuning, not a check.
- **Verified (t3e, 2026-08-17, `rapid double failover` 2/2 green):** the graceful path went from **202 of 1171 acknowledged writes lost to 0 of 990**, and the conversion is visible almost one-for-one — write availability 97.66% → 82.50% (210 *visible* failures replacing ~202 silent losses), read availability 78.05% → **99.17%** (those "read failures" *were* the lost keys). The fence fired exactly twice, once per graceful failover, and never on the crash path (nothing reachable to fence). **The crash path loses 19-20 of ~1194 (two runs), and that is NOT yet explained.** It was first written up here as "the inherent async-replication tail" — that explanation is **refuted**: the full four-cell matrix (below) shows **sentinel-mode crash losing 0**, twice, under an identical cascade, cadence, client and topology. A tail that one mode does not have is not inherent. The crash path stays unbounded for now because the cause is open, not because the loss is accepted.
- **Correction worth recording (the fence was inert on its first landing):** keyed on `state.RedisNodes`, `planFailoverFence` never fired — `reconcileFailoverAssignments` skips terminating pods when building `redisMap` (the `continue` lands *before* the IP is recorded), so the outgoing master is absent from the gather in exactly the situation that needs fencing, and the `rn == nil` guard swallowed every graceful handover. The re-run still lost 196 of 1163 with no fence line in the log. Widening the gather would have been far worse than inert: a reachable, still-mastering terminating pod would then read as a **live master** to `determineFailoverLiveMaster` (so the operator would not fail over at all) and as a candidate to `BestDataHolder` (so a dying pod could be *elected*). The fence is keyed on the **pod views** instead — fencing is an actuation, not a decision input, and it must not enter the ground truth. Reachability and role are consequently unknown and unneeded: `SLAVEOF` is idempotent, the dial is bounded by `ProbeTimeout` (LR-017), and the pod-presence check keeps the crash path free of a wasted dial while honouring ADR-001 strict IP identity. **Lesson: a guard written against the ground truth is only as good as what the ground truth is allowed to contain** — and this one was excluded by design, one function away.
- **Regresses:** Graceful-path write availability *drops* (82.50% measured, from 97.66%) (visible `-READONLY` failures replace silent loss) — visible `-READONLY` failures replace silent loss. That is the intended trade and the point of the change; the tier's existing `WriteAvailability > 0.40` bar is unaffected. No change on the crash path (nothing reachable to fence) or to any other mode. `ReadAvailability` keeps counting a lost key as a non-success, so no pre-existing assertion was weakened by the metric split; `LostKeys`/`KeyLossRate` sit beside it, and all six chaos tiers gained the exact durability verdict with no per-spec change.
- **Impacts:** ADR-011 §7 (graceful handover — the fence is now part of it) + the graduation gate; `CLAUDE.md` pillar 3.14; `test/chaos/client.go` (`classifyRead`, `LostKeys`, `VerifyAll`) and `cmd/littlered-chaos-client`.

### Addendum — the four-cell durability matrix, and a test-parity defect (t3e, 2026-08-17)

Both modes' `rapid double failover` tiers, one harness, exact final-sweep numbers:

| mode / variant | writes | reads | **ACKed writes lost** |
|---|---|---|---|
| failover graceful | 81.98% | 98.66% | **0** of 983 |
| failover crash | 99.50% | 97.16% | **20** of 1193 |
| sentinel graceful | 94.75% | 98.25% | **0** of 1136 |
| sentinel crash | 97.50% | 99.42% | **0** of 1169 |

Three readings, in descending confidence:

1. **The fence works, and durability parity on the graceful path is reached.** 0 lost, replicated across two runs (0 of 990, 0 of 983).
2. **The fence's availability cost is roughly double sentinel's** (81.98% vs 94.75%, i.e. ~216 failed writes vs ~63). The arithmetic points at the cause: ~216 failures at 10 writes/s ≈ 21.6s over two failovers ≈ **the full remaining preStop window each time**. Fencing stops the writes from being *lost*, but the client's established connection stays pinned to the old pod until it dies, so every write in that window fails rather than being served by the new master. Sentinel's pod-led preStop instead *waits for the master address to change* and then exits, cutting the window short. The obvious next step is to stop making the client wait for the pod to die — after the demote, `CLIENT KILL TYPE normal` on the outgoing master forces a reconnect, which the Service then routes to the new master. Untested; it would convert most of that 21.6s into a sub-second reconnect.
3. **failover-crash's 19-20 lost keys are unexplained**, and the leading suspect is a **test-parity defect rather than the mode**: `failoverCR` pins `resources.limits.cpu: "100m"` (`failover_utils_test.go`) while the sentinel chaos CR sets no resources and therefore gets the product defaults — which per pillar 3.3 include **no CPU limit at all**. A replica throttled to 0.1 core lags, and ~20 keys is ~2s of replication lag at the client's 10 writes/s. So the "mirror" tier, whose entire stated purpose is comparability, is not actually comparing like with like. Until that is equalised, no mode-level conclusion should be drawn from the crash cells. The second candidate, if throttling is ruled out, is the replica-selection interaction on the *second* cascade: after failover 1 the killed pod returns and full-resyncs (EmptyDir), so crash 2 chooses between a continuously-attached replica and a resyncing one, and `BestDataHolder`'s offset comparison across a fresh full-resync deserves a direct look.

### Addendum 2 — the pod fences itself, and the matrix closes (t3e, 2026-08-18)

The operator-side fence (above) fixed the graceful path's *durability* and made its
*availability* worse: writes stopped vanishing but started failing for the whole
remaining preStop window, 81.23% against sentinel's ~95%. Both halves are now fixed, by
moving the primary fence into the pod.

**Re-reading LR-016 is what unlocked it.** LR-016 forbids a pod inferring the state of
OTHER nodes — its origin was a probe restarting a replica because its master looked
unreachable. `sleep 10` treated that as "a pod may do nothing", which is a category
error: **"I am being terminated" is local knowledge that cannot be wrong**, and the pod is
the only party holding it instantly. So the preStop now (1) self-fences with
`CONFIG SET min-replicas-to-write 99` — **target-free on purpose**, since needing to know
the new master would reintroduce the very race this removes — and (2) waits for
mastership to move, then exits *at once* instead of sleeping out the window. A replica
exits immediately, having nothing to hand over (which also stops rolling updates paying
the budget per pod). The operator-side fence remains as the backstop; safety no longer
depends on it winning a race.

**Final four-cell matrix, one harness, exact final-sweep numbers:**

| cell | writes | reads | ACKed writes lost |
|---|---|---|---|
| failover graceful | **96.07%** | 98.33% | **0** of 1149 |
| failover crash | **98.50%** | **100.00%** | **0** of 1181 |
| sentinel graceful | 95.41% | 98.25% | **0** of 1143 |
| sentinel crash | 97.58% | 99.50% | **0** of 1169 |

Zero loss in every cell, and failover mode is now at or better than sentinel on **all
four** numbers. For the ADR-011 graduation gate that settles the availability-vs-Sentinel
question in failover mode's favour — but only because the durability number was measured
first: the mode previously looked *better* than sentinel on write availability precisely
while it was the only one of the two losing acknowledged data.

**Honest open question.** The crash cell also went 20 lost → 0 (18/17/18 visible write
failures across three runs, a near-1:1 conversion), and a `--grace-period=0 --force`
delete should not run a preStop hook at all. Leading hypothesis: `--force` removes the API
object while the **kubelet still runs its normal termination path**, hook included, because
it never observed a zero-grace deletion object — which would mean "crash" in this tier was
never SIGKILL-without-a-hook, and the ~2s window came from endpoint removal breaking the
client's pinned connection. Not confirmed: `kubectl logs` cannot read a force-deleted pod,
so the obvious instrument is blind, and the reproducible-but-unexplained result is recorded
as such rather than claimed. The clean control is the **kill-9 tier**, where the container
process dies and no hook can possibly run — if that path loses writes while force-delete
does not, the hypothesis is confirmed and the unconditional operator-side fence gets a real
red to be built against.

### Addendum 3 — the epoch gate protected only the bootstrap master (t3e, 2026-08-19)

Adding the genuinely-named `kill-9` variant to the tier (above) turned it red on the first
run, on a latent bug in the shipped mode: **a kill-9 of a PROMOTED master destroyed 352 of
1145 acknowledged writes** — 31% — with `DataCorruptions: 0` and write availability 95.50%.
The arithmetic (second kill lands ~30–35s into a 120s window at 10 writes/s) says
*everything written before the kill* was wiped, not a tail.

**The epoch gate was answering the wrong kind of question.** It is an **ordering** device —
"is this instruction newer than the last one I used?" — being used to answer an **identity**
question: "was this instruction issued for *this process incarnation*?" The two coincide
everywhere except one place: an **in-place promotion advances the assignment epoch without
restarting the process**, so the start marker is never rewritten and stays behind.

| | annotation | marker |
|---|---|---|
| bootstrap: redis-1 starts as **replica** @1 | `replica@1` | **1** |
| failover 1: stamp `master@2` + `REPLICAOF NO ONE` to the **live** process | `master@2` | **still 1** |
| kill-9 | `2 > 1` → honored → **empty master** | |

The operator then sees a reachable `role:master` at the intended IP, believes it healthy,
and repoints the replicas holding the only surviving copy onto it.

**What was already right, and why this hid for so long.** On promotion the operator
re-stamps *every* pod, so the superseded ex-master does get `assigned-role=replica` — that
case has two layers of protection because it is the one the design imagined (the ADR comment
reads "a kill-9'd ex-master must never reclaim mastership from its **stale** assigned-role
annotation"). The unimagined case is the pod whose master annotation is **current** but was
issued while it was alive. And the e2e coverage matched the blind spot exactly: the only
kill-9 test killed the **bootstrap** master — the single master whose marker happens to equal
its master epoch — which is why this survived 16/16.

**Fix — narrow, because the two roles carry different risk.** A restarted process (marker
present ⇒ no data) may honor a **replica** assignment on the existing ordering rule, since
starting as a replica of the live master loses nothing. A **master** assignment additionally
requires `AnnotationMasterStartAuthorizedEpoch`, which the operator stamps only after it has
observed the restart and established that no data is at risk — permission that **cannot
predate the death it refers to**, closing the race ordering alone cannot. Pure seam
`masterStartAuthorizedFor`: **seed only**. Promotion must never grant it (its target is by
construction a reachable, running data holder — `DataHolders` requires `Reachable && Keys > 0`
— so authorizing a *start* there re-arms the wipe for the next kill-9); bootstrap grants it
because nothing holds data and redis-0 may already carry a marker.

Rejected: "every start waits to be introduced by the operator" (the cluster-mode analogy).
More uniform, but it puts an annotation round-trip through the kubelet's projected-volume
refresh on the critical path of **every** pod start, forever, to protect states that provably
cannot lose data. The narrow gate also makes the promoted-master case behave exactly like the
bootstrap-master case, whose recovery cost is already measured (single kill-9: 0 lost of 1155,
96.33% writes).

**Renamed, because the old name hid the bug:** `/data/littlered-run-epoch` →
`/data/littlered-started-under-epoch`, `MARKER_EPOCH` → `STARTED_UNDER_EPOCH`,
`runMarkerPath` → `startMarkerPath`. It is not "the epoch we are at", it is "what my current
incarnation booted with".

**Verified (3/3 green):** kill-9 **352 lost → 0**; graceful 0 lost / 96.50% writes;
force-delete 0 → **2** lost / 98.58%. Two observations worth keeping: the gate's cost is
real and correctly placed (kill-9 write availability 96.33% → 91.24%, ~5s per kill, the
parked pod now *waiting* for the operator instead of wrongly resuming), and force-delete's
2 lost is the first non-zero on that path — landing exactly on the "~1 per failover" figure
the ≤5 bound was derived from, so the bound was reasoned rather than luckily zero.

### Addendum 4 — second-environment confirmation, and item (4) (s1 + t3e, 2026-08-19)

**Item (4): the straggler repoint is ungated.** It required `settled && !anyTerminating`;
neither half was reasoned for this mode (both came from sentinel mode, where the point is not
to churn while a *competing actor* is mid-failover — there is none here, pillar 3.5 scope).
The gate cost three things, each of which surfaced as its own symptom above: it hid the
outgoing master from the fence, it kept a freshly promoted master replica-less for extra
passes, and it suppressed `min-replicas-to-write` as a self-fence (stripping a dying master
of its last replica *is* a fencing action). `settled` was additionally redundant — reaching
that step already requires the intended master to be reachable and reporting `role:master`.
Repointing **earlier** is also the safer direction for data: a straggler still following the
old master can only drift *further* ahead while we wait, so waiting enlarges the divergence a
resync discards rather than protecting it, and the live master is by construction not behind
(chosen by bootstrap or `BestDataHolder`). A bonus falls out — the outgoing master is itself
a straggler by `planFailoverRepoints`' definition, so ungating demotes it here too, which is
the operator-side fence arriving for free wherever its pod object still exists.

**It makes `minReplicasToWrite: 1` affordable** — which is the measurement that matters for
the default, and the first version of this entry over-read it. On force-delete the knob cost
**78 refused writes before (4)**; after (4) a single run measured **12**, which was reported
here as "indistinguishable from off". Ten passes later that is the *good mode of a bimodal
distribution*: `13, 14, 16, 17, 18, 19, 19, 20, 63, 64` against `16/17/19` at knob=0. So the
honest statement is **free at the median, with a ~20% tail costing ~45 more refused writes
(~4.5s)** — bimodal rather than noisy, so an ordering condition rather than a smear. Item (4)
is still what made it affordable (78 *every time* before it). All cells 0 lost at both knob
settings. Recorded as a caution: a single favourable run is not a distribution, and this
entry made that mistake about its own result.

**Cross-environment confirmation (the numbers now rest on two clusters, one multi-node):**

| cell | s1 (multi-node) | t3e (single node) | lost |
|---|---|---|---|
| sentinel graceful | 94.25% | 95.41% | 0 / 0 |
| sentinel crash | 96.67% | 97.58% | 0 / 0 |
| failover graceful | 96.58% | 96.75% | 0 / 0 |
| failover force-delete | 98.58% | 98.41% | 0 / 0 |
| failover kill-9 | 91.92% | 93.14% | 0 / 0 |

Ten cells, zero acknowledged writes lost in every one, and `failover >= sentinel` on both
comparable variants in both environments. The kill-9 **start-gate fix is confirmed off t3e** —
on the pre-fix build s1 reproduced the bug identically (master stuck, 60s timeout), which also
retires "t3e artifact" as an explanation.

Two cautions recorded so later readers do not over-read single runs: **kill-9 availability is
noisy** (91.24 / 91.92 / 93.14 / 96.50 across single runs on both clusters — do not read a
3pp move from one run), and the near-identical numbers between a loopback single-node cluster
and a multi-node one with real network hops say this tier's availability cost is dominated by
**operator reaction time** (reconcile cadence, label flip, client reconnect), not replication
latency. Which is consistent with where the wins came from: removing waits, not speeding up
replication.

### Addendum 5 — the six-cell matrix over 10 passes (t3e, 2026-08-20)

Ten consecutive passes of both modes' `rapid double failover` tiers (60 chaos runs) on
operator `803eb26` with `minReplicasToWrite` defaulting to 1. **All 10 passes green; 0
MISSING and 0 corruptions in all 60 blocks.**

| cell | n | write avail min/med/max | failed writes |
|---|---|---|---|
| failover graceful | 10 | 95.83 / 96.42 / 96.91 | 37-50 |
| failover force-delete | 10 | 94.66 / 98.46 / 98.92 | 13-64 |
| failover kill-9 | 10 | 85.13 / 92.19 / 95.73 | 51-178 |
| sentinel graceful | 10 | 94.75 / 95.29 / 96.50 | 42-63 |
| sentinel force-delete | 10 | 96.83 / 97.58 / 98.33 | 20-38 |
| sentinel kill-9 | 10 | **43.32 / 54.92 / 73.89** | 313-679 |

**The headline is the kill-9 column, and the distributions do not overlap:**

    failover  85.13  90.24  90.65  92.15  92.15  92.24  93.66  93.99  94.31  95.73
    sentinel  43.32  46.62  49.96  50.67  50.79  59.05  67.58  73.73  73.79  73.89

Sentinel's *best* run (73.89%) is 11pp below failover's *worst* (85.13%), with zero data loss
on both sides. The mechanism is not a defect in either mode: sentinel's kill-9 guard
deliberately **suppresses Redis** on the restarted master and waits for Sentinel to reach
SDOWN, elect, and be observed, whereas failover mode declares death from kubelet readiness
and promotes in ~2s. So on a true crash the operator-led mode recovers availability roughly
twice as fast, which is the strongest single result for the ADR-011 graduation gate — and it
comes from the disruption shape that had **no coverage at all** before this session.

**A mechanism contrast worth keeping** (both guards protect data; they cost differently):
sentinel mode yields on **Sentinel's stored run-id** — an identity signal maintained by a
continuous external observer, which is why it also covers a *promoted* master with no extra
machinery. Failover mode moved that record into the pod (written once at `exec`) and thereby
lost the observer, which is exactly the hole LR-038's start gate had to close with an
operator-stamped authorization.

**Cautions for whoever reads these numbers next.** Two cells are **bimodal**, not noisy —
failover force-delete (13-20 vs 63-64) and sentinel kill-9 (~314 vs ~600+) — so a discrete
ordering condition is at work in both and a single sample of either is meaningless. Diagnosing
it needs operator logs correlated per pass and is left open. And `sentinel kill-9` sits nearest
its assertion floor: 10 passes never dipped below the inherited `WriteAvailability > 0.40`, but
the worst mode clusters at 43-51%, so a margin of ~3pp. If that cell ever fails, read it as
"the yield's cost grew", not as a flaky test — and note the same `> 0.40` bar is uselessly
loose for the failover cells, which measured 85-96%.
## [LR-039] Cross-Instance Sentinel Capture — the Master Name Was a Shared Constant
> **Numbering note:** LR-026 … LR-037 are allocated on the multi-site line and are not present
> here, hence the jump from LR-025 to LR-038. IDs are allocated globally across branches so they
> stay unique through a merge — which is what let LR-038 (failover) and LR-039 (this entry) land
> side by side on this integration branch with no renumbering.
>
> This entry was briefly numbered LR-038 by mistake — the highest ID was checked on the
> multi-site line only (LR-037) and the failover line had already taken 038. **Check every
> branch before claiming an ID**, not just the one you are working next to:
> `for b in $(git branch --format='%(refname:short)'); do git show $b:docs/RECONCILIATION_ALGORITHM_CHANGELOG.md 2>/dev/null | grep -oE '^## \[LR-[0-9]{3}\]'; done | sort -u | tail -1`
> **⚠ Incomplete as shipped — see LR-041:** the per-instance name was threaded into every Sentinel *command* but not into the sentinel-mode *gatherer*, which then probed with an empty name and reported the whole quorum as bare, silently disabling every `sn.Monitoring`-gated healing rule.

- **Date:** 2026-08-19
- **Commits:** `0e28e8e` (fix), `fb8f3d8` + `7403da1` (e2e) — branch `fix/sentinel-master-name-scoping` off `main`.
- **Problem (field incident, operator v0.2.1, a managed cloud):** two unrelated sentinel-mode instances in one namespace on a shared pod network **merged into a single Sentinel quorum**. The larger instance's Sentinel configuration won on config epoch and reassigned the smaller instance's master to a Redis pod belonging to the *other* instance; the demoted master was told `SLAVEOF` and **flushed its dataset** on the first replication attempt. The instance was unrecoverable for 13+ hours and no healing rule fired — not in v0.2.1, and not on `main` either. This is the project's first **safety** failure: every prior LR entry is a liveness failure whose surviving invariant is "we never serve the wrong data". Full analysis, with Redis source citations and the annotated timeline: `docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md`.
- **Root cause:** `SentinelMasterName` was a package constant (`"mymaster"`) shared by every managed instance, and the master **name is the only isolation boundary Sentinel's gossip protocol has**. `sentinelProcessHelloMessage()` does `master = sentinelGetMasterByName(token[4]); if (!master) goto cleanup;` and performs no other check — no instance identifier, no namespace, no authentication between Sentinels beyond the optional password. Three further protocol facts make the merge reachable and permanent: a Sentinel PUBLISHes its hello to masters, replicas **and other Sentinels**, and accepts inbound `PUBLISH __sentinel__:hello` on its own port (`sentinelPublishCommand`), so *holding* a peer's address is enough to introduce yourself to it; stale known-sentinel entries never age out (only runid-conflict resolution or `SENTINEL RESET` remove them); and the introduction in the field came from a **recycled pod IP** inheriting the entry of a previous-generation sentinel of the *same* instance. The operator's own hello was the opening move — it reached out to the stranger, which replied, and one hello carried `+sentinel-invalid-addr`, `+sentinel`, `+new-epoch`, `+config-update-from` and `+switch-master` within 5 ms.
- **Why no rule recovers it:** `DetermineRealMaster` correctly yields `RealMasterIP == ""` (a majority monitor an address that is not a pod, so the Redis-only fallback is suppressed — LR-004), which short-circuits Rule 0, Rule R (LR-009), LR-005 and LR-008. Rule L (LR-015) needs *bare* Sentinels. LR-024's `planGhostMasterRecovery` is one predicate away: `SentinelsMonitorGhostMaster()` is true, but `HasHealthyKnownReplica()` **vetoes** — a veto whose premise ("a promotable replica means Sentinel is about to act") is false here, because from Sentinel's own vantage the stolen master is perfectly healthy (`flags: master`, `last-ok-ping-reply: 100`). There is nothing to fail over from.
- **Fix:** `spec.sentinel.masterName` is **Required**, bounded (`MinLength=1`, `MaxLength=128`) and pattern-checked (`^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$` — the hello payload is comma-split and `sentinel.conf` space-split, so neither character may appear). Recommended value `<namespace>.<name>`. **No default, static or derived**: per LR-033, a derived default cannot be a CRD constant, so it lives in Go, and the reconciler's finalizer `Update` persists it into the user's spec — and here the effective value is a *client-visible contract*, so a later change to the derivation would silently rename every unset instance's master and break every Sentinel-aware client on an operator upgrade. Objects predating the field fall back to a fixed constant (`LegacySentinelMasterName`) via a pure accessor. `redisclient.SentinelMasterName` is **deleted** rather than left unused, so the compiler located all ~20 call sites: healing rules, gatherer, the pod startup script and preStop hook, `GetMaster`/`GetMasterAcrossAll`/`GetHealActions`, and `lrctl` (via `ClusterContext`, populated from the CR by discovery).
- **Runtime warning:** `SentinelMasterNameUnscoped` condition + one-shot event, **never a refusal** (opt-in-not-block, as `ReducedSiteLossResilience`). It fires only when the field is **unset** — setting `mymaster` explicitly is a decision (a legacy client may hardcode it) and is not second-guessed. This condition exists because validation *cannot reach* pre-existing instances: CRD validation ratcheting excuses a required-field violation at any schema location whose value is unchanged, so an instance that never touches `spec.sentinel` keeps reconciling and keeps being editable while silently sharing the legacy identity.
- **Measured, not assumed — how a newly-required field behaves on pre-existing objects** (throwaway envtest probe over six real `kube-apiserver` versions; the in-place CRD tighten was accepted in every case): plain `required` **never blocks the operator's `/status` writes**, as far back as 1.29, because status is a subresource — the pessimistic assumption was wrong. But on ≥1.30 ratcheting also excuses a *user* spec edit that leaves `spec.sentinel` untouched, so nested `required` does **not** deliver "fail loud on upgrade" on any current cluster; it forces the decision on **new** instances only. A spec-level CEL rule *would* fail loud — but only on ≥1.33; below that it rejects the operator's own status writes and stops reconciliation entirely. Plain `required` was chosen deliberately: it never wedges anything on any version, and the runtime condition carries the loudness for the installed base.
- **Downgrade is loud, verified:** `kubectl apply` defaults to strict field validation, so against an older CRD a CR carrying `masterName` is **rejected** (`strict decoding error: unknown field "spec.sentinel.masterName"`) rather than silently pruned back to the shared name.
- **Red-first, all three tiers:**
  1. *Accessor* (`api/v1alpha1`): four assertions observed red — `SentinelMasterName() = "" want "team-a.cache"` — before the accessor existed as more than a stub.
  2. *CRD schema* (envtest): five negative specs observed red with `Expected failure, but got no error` against an unvalidated field. This also **exposed a false pass**: an existing negative spec (`rejects spec.sentinel when mode is not sentinel`) was being rejected for the *new required field* rather than the mode mismatch it tests — the same trap LR-033 hit with `sitesPerShard`, fixed by setting the field in that fixture.
  3. *e2e, on a real cluster*: run against **main's operator and CRD**, both admission specs went red for the right reasons — `rejects … without masterName` failed with `Expected an error to have occurred` after the apply printed `littlered.../mn-missing-... created`, and the awkward-name spec failed on the strict-decoding rejection above. The isolation spec reproduced the incident in 45 s: `instance A is monitoring 10.233.64.117, which is not one of its own pods — it has been captured`, with `config-epoch: 9999`, `num-slaves: 3` (B's replicas adopted), `num-other-sentinels: 3` (quorums merged) and `flags: master` — the stolen master healthy, hence never failed over. Against the fixed code the same spec passes.
- **A note on the isolation spec's positive control:** "nothing happened" is also what a *dud payload* looks like, so the spec would pass having tested nothing if the injected hello never reached the processor. The `PUBLISH` reply is therefore asserted to be `1`, and a companion spec runs the same payload down the same path with one variable changed — the advertised master name matches the receiving instance's own — and requires that instance to follow it. Control green plus isolation green is what makes the pair evidence.
- **Known limitation of the *red* run only:** the injection targets `sentinel-0`, so the other two Sentinels still hold the correct master and the operator's divergent-sentinel correction races the capture; an earlier red run caught the post-reset state instead (`num-other-sentinels: 0`, master IP already corrected back). On **fixed** code there is nothing to correct, so the spec is deterministic — the race exists only in the direction we needed exactly once. In the field all three Sentinels were captured via epoch gossip, so no correction was possible.
- **Not closed by this change:** a unique name ends gossip fusion, not the narrower **address-adoption** path — if another instance's master pod dies and its IP is recycled onto ours, that instance's Sentinels will monitor our master directly, read its `INFO`, adopt our replicas and can `SLAVEOF` them. No hello, no name check. Only authentication closes it, and auth covers Sentinel↔Sentinel links too (`sentinelSendAuthIfNeeded` uses `sentinel-pass`, falling back to `requirepass`). **Automated recovery for an instance already captured is DECLINED, not deferred** (ADR-015 Alternative J, analysis §9.2). The LR-024 predicate split that would detect it is sound, but two facts kill it: nothing survives to salvage (the flush precedes any possible detection by two orders of magnitude, so recovery restores an *empty* instance — which `kubectl delete` + re-apply already does), and the operator **cannot win the reclaim** — `createSentinelRedisInstance` initialises `config_epoch = 0` (sentinel.c:1304), so an operator-issued `SENTINEL MONITOR` loses to the captor's epoch on the next hello, ~2s later, and reissuing it every reconcile wipes the replica list each time — **LR-013's exact hazard**. The rule would convert a broken-but-static instance into one thrashing its own topology. Adopted instead, both implemented and e2e-verified on a real cluster (the diagnostic prints `[OK] No foreign Sentinel contact observed` for a scoped instance and, for a captured one, names the foreign master plus the peer/replica surplus — catching a *partial* capture of one sentinel in three, i.e. firing before a full takeover): an `lrctl verify` cross-instance diagnostic (`DetectCrossInstance` — foreign master/replica addresses filtered by `s_down`/`o_down` so ordinary post-failover debris is not reported, plus peer/replica counts above what was deployed) and a manual runbook in `USAGE.md`; detection is not the gap, since a captured instance sits at `Ready=False` and ordinary alerting catches it.
- **Regresses:** None on existing behaviour; instances that set the field behave exactly as before. **BREAKING (CR-visible):** `spec.sentinel.masterName` is required on create; existing instances keep running and are forced to state it on their next change to `spec.sentinel`. Changing the value is client-visible — Sentinel-aware clients must be reconfigured, clients using the label-routed `{name}` Service are unaffected — and there is no rolling cutover, because monitoring one master under two names runs two independent failover state machines that can promote different replicas.
- **Impacts:** ADR pending. `docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md`; `docs/API_SPEC.md` and `docs/USAGE.md` (field + the client-reconfiguration note); CLAUDE.md pillar 3.7 (IP-only identity now has a named cross-tenant failure mode) and §4. `lrctl debug-dump` now records the Kubernetes version — behaviour a dump is used to explain can depend on it, and this incident's dump did not have it.

## [LR-040] Sentinel Write Paths Unbounded — Rule 0 Stalls a Reconcile on a Blackholing Sentinel IP
- **Date:** 2026-08-21
- **Commit:** (pending)
- **Problem (found on the `e2e-0821` integration branch, local multi-node cluster):** the e2e `LittleRed Sentinel Rolling Update > should maintain sentinel quorum after rolling update` failed: `SENTINEL master <name>` on `sentinel-0` answered `ERR No such master with that name` for the assertion's full 120s. Not a naming defect (the CR, `sentinel.conf` and the query all agreed) and not a merge artifact — the operator simply never registered the freshly-rolled Sentinels. Ground truth from the debug artifacts: the sentinel StatefulSet rolled, replacing all three pods; the operator's Rule 0 (bare-sentinel re-registration) ran against the **old, already-deleted** pod IPs (`.42/.79/.209`) and then blocked inside `SENTINEL MONITOR` to `.209`, which **blackholed** (`redis: connection pool: failed to dial after 5 attempts: dial tcp 10.233.192.209:26379: i/o timeout`). One reconcile spanned 16:22:03 → 16:24:00 (~117s) and the operator logged **nothing at all** for the instance across the entire test window; the new pods (`.155/.119/.203`, up by 14:22:18) were registered only after the run ended, which is why the instance looks healthy post-mortem. Data-safe throughout — this is availability and convergence latency, the LR-012/LR-017 family.
- **Root cause — LR-017's exemption rested on a false premise.** LR-017 bounded every sentinel *read* path with `ProbeTimeout` and consciously exempted the writes, recording: *"Write-path sentinel commands (`Monitor`/`Set`/`Reset`/`Remove`) and the pub/sub subscriber are unchanged — they are gated by Rule A during churn and are not on the stall path."* **Rule 0 runs before Rule A.** In `reconcileSentinelCluster` the re-registration loop is at littlered_controller.go:774 and Rule A's `anyTerminating || FailoverActive` guardrail returns at :813 — the ordering is deliberate (adding a monitor to an unconfigured sentinel is non-disruptive, so it is allowed during transitions). So during exactly the churn Rule A exists to sit out, Rule 0 has already issued unbounded writes to stale IPs. A rolling update is the perfect trigger: `anyTerminating` is true, so Rule A *would* have skipped healing, but Rule 0 already blocked.
- **Second finding — a context deadline alone does not bound these calls.** The first fix mirrored LR-017 exactly (wrap each address iteration in `context.WithTimeout(ctx, ProbeTimeout)`) and was **inert**: against a blackholing peer go-redis returns `context deadline exceeded` *at* the 3s deadline but still spends roughly another `DefaultTimeout` unwinding, so a 3s ctx over a 5s `ReadTimeout` still cost ~5s of wall clock (measured: 5.02s → 5.00s). The red-first unit test is what caught this; the ctx-only change would otherwise have shipped looking correct. **Both halves are required** — the ctx bounds the pool's dial-retry loop (the ~117s blackhole case), the client's own timeouts bound each individual attempt. This qualifies LR-017's read-path claim too, whose "fails in ≤3s regardless of go-redis retries" is optimistic for the read-blackhole variant.
- **Fix:** two constructors, `(*SentinelClient).newBoundedClient(addr)` and package-level `newBoundedRedisClient(addr, password, tlsEnabled)`, that build a single-address client with `DialTimeout`/`ReadTimeout`/`WriteTimeout` = `ProbeTimeout`. Applied to every single-shot per-address operation in `internal/redis/client.go`: the four writes (`Monitor`, `Set`, `Reset`, `Remove`), and — same latent inert-ctx defect — the read/probe helpers `getMasterFromSentinel`, `getReplicasFromSentinel`, `IsMonitoring`, `Ping`, `SlaveOf`, `GetReplicationInfo`. Retains the per-iteration ctx deadline on the writes.
- **Deliberately NOT bounded:** (1) `SentinelClient.Subscribe` — a long-lived pub/sub subscription, not a single-shot command; a 3s read budget would tear it down continuously. (2) cluster mode's `(*ClusterClient).getClient` — slot migration issues `MIGRATE` with its own multi-second budget (`spec.cluster.reshardMigrateTimeoutMillis`), so a blanket `ProbeTimeout` there would abort legitimate long commands. Cluster's stall was already addressed by LR-012 and is not on this path.
- **Cross-mode (CLAUDE.md §7 rule 11):** `SlaveOf` is shared, so failover mode was carrying the same inert bound — its `slaveOfBounded` (added *with* the LR-017 lesson in mind, `failover_reconcile.go`) wrapped `SlaveOf` in a 3s ctx while `SlaveOf` itself used `DefaultTimeout`. Bounding `SlaveOf` fixes sentinel Rule R and failover in one change. The irony is worth recording: LR-017 is itself the worked example for the cross-mode-parity rule, and the newer mode inherited its blind spot rather than its fix.
- **Why it cannot re-open the RESET-spam trap (LR-001/LR-007):** those regressions came from a *faster loop* letting Rule D's `SENTINEL RESET` reset Sentinel's `s_down` timer before failover could trigger. This change does not shorten the requeue cadence (still 2s) — it removes a pathological ~117s stall, i.e. it *restores* the intended cadence rather than exceeding it. And every RESET gate is now state-based, not time-based: living+reachable consensus master (LR-008), ≥1 healthy known replica (LR-011), and K8s-grounded wholeness (LR-013). A faster-returning write cannot trip a state gate early.
- **Tests:** `TestSentinelWritePathsAreProbeTimeoutBounded` (`internal/redis/client_write_timeout_test.go`) — a local listener that accepts and then never replies, asserting each of the four writes returns within `ProbeTimeout + 1s`. Observed **RED** on all four at ~5.02s against the unbounded code, **RED again at ~5.00s** against the inert ctx-only fix (the finding above), **GREEN at ~3.0s** once the client timeouts were bounded. The budget deliberately discriminates 3s from `DefaultTimeout`'s 5s. The dial-blackhole variant that produced the field stall is not reproducible locally (it needs an IP that swallows SYNs); the read-blackhole asserts the same property — that the bound is actually applied — and the one-time real red is the captured 117s reconcile.
- **Regresses:** None. `ProbeTimeout` (3s) is far above a live in-cluster sentinel's sub-second response, so healthy paths are unaffected; a control command is one round-trip. No decision logic, gate or cadence changed — only how long a dead address may hold the loop.
- **Impacts:** LR-012 / LR-017 (completes the bounded-probe work: LR-012 cluster reads, LR-017 sentinel reads, LR-040 sentinel writes + the inert-ctx correction). Corrects LR-017's "Regresses: None" exemption note, which should be read together with this entry.

## [LR-041] Sentinel Gather Queried the Master by an Empty Name — Every Monitoring-Gated Rule Silently Dead
- **Date:** 2026-08-21
- **Commit:** (pending)
- **Problem:** the e2e `LittleRed Sentinel Failover > should restore full cluster after failover` (SEN-013) failed with `num-slaves: 4` where 2 was expected. Ground truth on the live instance (cleanup skipped) showed Sentinel holding **two real replicas** (`.172`, `.166`, `flags slave`, `master-link-status ok`) **plus two ghosts** (`.101`, `.25`, `flags s_down,slave`, link `err`) — dead pod IPs left by the failover churn that Rule D should have pruned. The operator log contained neither the `Issuing SENTINEL RESET to clear ghost nodes` line nor its `Ghost replica detected but skipping SENTINEL RESET` counterpart, and no `Ghost node detected in Sentinel topology` at all: the ghost loop never saw a ghost.
- **Root cause:** LR-039 made the Sentinel master name per-instance and added `operatorGatherer.masterName` to carry it, documented on the field itself as *"Sentinel-mode paths must set it; cluster-mode gatherers never reach GetSentinelState and leave it empty."* Of the four `&operatorGatherer{...}` construction sites, the three that never call `GetSentinelState` (cluster, cluster-migration, failover) correctly leave it empty — and the **sentinel-mode** site (`reconcileSentinelCluster`, littlered_controller.go:769) was the one that did not set it. So every sentinel probe issued `SENTINEL master ""`. Sentinel answers an empty/unknown name with the *same* `ERR No such master with that name` it uses for a genuine miss, which `GetSentinelState` translates into its legitimate not-monitoring state: `Monitoring: false, Reachable: true`. The result is a plausible-looking lie — a permanently, unanimously **bare** Sentinel quorum — rather than an error.
- **Blast radius (everything gated on `sn.Monitoring`, all silently inert):** ghost-**replica** pruning / Rule D (the loop `continue`s before reading `sn.Replicas`, so `ghostFound` is never set — the observed defect); ghost-**master** correction, i.e. LR-005 divergent-master and LR-008 REMOVE+MONITOR; `HasHealthyKnownReplica`, which is LR-024's discriminator between a ghost-master deadlock and an imminent legitimate failover; and `NumSlaves`, which `DetectCrossInstance` (LR-039's own diagnostic) reads. Conversely Rule 0 *over*-fires: seeing a bare quorum every loop it re-registers all three sentinels every ~2s forever — **181 re-registrations in a 4-minute window** in the captured artifacts. The instance still reached `Running` only because `DetermineRealMaster` fell back to the Redis-only path (a reachable `role:master` pod), which is exactly the fallback LR-004 hardened; had it not, `RealMasterIP == ""` plus a unanimously-bare quorum is also Rule L's precondition (LR-015).
- **Interaction with LR-040:** these compound, and LR-040 was found first because of it. This bug makes Rule 0 issue `SENTINEL MONITOR` to all three sentinels on *every* reconcile; LR-040 left those writes unbounded. So the constant Rule 0 traffic is what kept walking into the blackholing stale-IP stall. Fixing LR-041 removes most of that traffic; fixing LR-040 bounds what remains. Neither alone is sufficient.
- **Fix — make the defect unexpressible, not merely detected.** The first landing set `masterName` at the sentinel construction site and added a runtime refusal. Both worked, but both left the *shape* that caused it: a required value held as optional-looking construction state. Contrast the command paths, which never had this bug — they take the name as a parameter (`podSC.Monitor(ctx, sentinelMasterName, …)`), so omitting it does not compile. The gather took it as a struct field, so omitting it compiles and zero-values to `""`.
  So the name is now a **parameter** on `Gatherer.GetSentinelState(ctx, podName, ip, masterName)` and on `GatherReplicationState(ctx, g, redisPods, sentinelPods, masterName)`, and `operatorGatherer.masterName` is **deleted**. The compiler now asks every call site for a value; there is no field left to forget. `cliGatherer` takes the passed name too instead of re-deriving it from its own `ClusterContext`, so the operator and the CLI cannot disagree about which name they queried.
  The runtime refusal stays as defence in depth for an explicit `""` argument, and it must remain a refusal rather than a pass-through: `SENTINEL master ""` draws the same `ERR No such master with that name` as a genuine miss, so the not-monitoring branch would report reachable-but-bare — indistinguishable from ordinary post-restart churn, which is exactly how this hid. It is deliberately narrow: a sentinel that genuinely does not know *this* instance's master is ordinary state and Rule 0 depends on still seeing it as reachable-but-bare. Failover-mode callers pass `""` legitimately (no Sentinels, so no probe is ever issued).
- **Not affected:** `lrctl`'s `cliGatherer` derives the name per call from the discovered CR context (`masterNameOf(g.cCtx)`) rather than from a settable field, so it never had the defect — which is why `lrctl verify` reports this topology correctly and was the tool that exposed the ghosts.
- **Design lesson (the generalizable one):** LR-039 threaded the per-instance name through everything the compiler asked it to, and missed the one place where the compiler did not ask. **A required value stored as optional-looking construction state has no enforcement**, and the zero value of a required string is a plausible input rather than an obvious error. When a value is mandatory, put it in the signature.
- **Tests:** `TestSentinelGatherRequiresMasterName` (`internal/controller/gatherer_mastername_test.go`), red-first, with a fake Sentinel bound to `SentinelPort` that answers every command `-ERR No such master with that name` so the dial succeeds and the reply path is what is under test. Observed **RED** returning exactly the production shape — `&{... Monitoring:false ... Reachable:true Replicas:[]}` with a nil error — and **GREEN** after the guard. A second sub-case pins the narrowness (a real name-miss must still be reported, not error), and it passed before and after, which is what makes the first case's red meaningful rather than a blanket rejection.
- **⚠ Re-enables previously-dead behaviour — review with LR-024 in hand.** Rule D's ghost-replica `SENTINEL RESET` has been inert on this branch; correcting the gather turns it back on. That RESET is precisely the self-inflicted trigger for the LR-024 ghost-master failover deadlock, whose *prevention* half (retiring or rate-limiting Rule D) is still deferred to the prospective ADR-010. So `recoverGhostMasterDeadlock` becomes load-bearing again the moment this lands. This is a restoration of intended behaviour, not a new hazard, but the LR-024 recovery path is now on the critical path for the sentinel e2e tiers and should be watched there.
- **Regresses:** None. The empty-name refusal cannot fire on a correctly-wired path, and the three cluster/failover gatherers never call `GetSentinelState`. No gate, decision or cadence changed — the gather simply reports the truth it was already trying to read.
- **Observed consequence, e2e (2026-08-22):** the warning above came true immediately, though via LR-008 rather than Rule D, and in a *test* rather than production. `Sentinel Cross-Instance Isolation > proves the injection path is live by capturing an instance that shares the name` went red: with the gather restored, an injected hello advertising a foreign master reads as a **ghost master** (the address is not one of the receiving instance's pods), so the LR-008 REMOVE+MONITOR correction fired **in the same second as the `PUBLISH`** (`ghost_master: 10.233.192.3 -> correct_master: 10.233.192.15`, ts identical to the injection) and the spec's 2s poll never observed the capture. The specs now pause the operator (`scaleOperator(0)`) across both injections, restoring it unconditionally first in `AfterAll`.
  Two points worth keeping. (1) **The positive control earned its place**: the *isolation* spec still passed, and would have kept passing whether the master name protected instance A or not, because the operator heals captures sub-second — its conclusion had become unattributable, and only the control detected that. (2) **The operator does not actually "recover from a capture"**, and this is not a quiet reversal of LR-039's decision to decline that. It wins only against a *fresh* single-Sentinel injection: once the quorum has adopted the foreign configuration at a high config epoch, a per-pod REMOVE+MONITOR (which restarts that pod at epoch 0) is immediately overridden by gossip from its peers. Verified live — with the operator running against an established epoch-21001 capture, the correction is issued every loop and the address never changes.
- **Impacts:** LR-039 (completes it — the per-instance master name has to be threaded into the *gather*, not only the commands); LR-040 (the traffic amplifier that surfaced it); LR-024 (its recovery rule is live again, see the warning above); LR-005 / LR-008 / Rule D / Rule 0 (all restored to their designed behaviour).

## [LR-042] A Captured Sentinel Instance Was Managed and Polled Forever — the Forsaken Verdict
- **Date:** 2026-08-22
- **Commit:** (pending)
- **Problem (measured on t3e):** an instance captured by another Sentinel deployment sharing its master name is unrecoverable **by design** — ADR-015 §9.2 declines recovery, because the dataset is flushed ~1s after the `SLAVEOF` (nothing to salvage) and the operator structurally cannot win the reclaim (`SENTINEL MONITOR` creates the entry at `config_epoch = 0` and loses to the captor's epoch on the next hello). But nothing told the operator that. It kept treating the instance as converging: `Phase != Running` selects the **fast** (2s) interval, so it ran **30 reconciles and ~120 log lines per minute, indefinitely**, re-deriving the same dead end. During the *partial*-capture window it is worse than idle — the LR-008 ghost-master correction reissues `REMOVE`+`MONITOR` every pass, exactly the never-converging thrash §9.2 predicted from `sentinel.c`, each one wiping that Sentinel's replica list (LR-013's ingredient).
- **Fix — name the state, then gate on it.** New terminal condition **`Forsaken`** (named `SentinelForsaken` in the first draft of this change and renamed before release — the `Sentinel` prefix is redundant with the instance's own `spec.mode`, which is the only mode that can reach this verdict) plus `status.forsakenSince`, decided by the pure `planForsaken`. When the verdict lands the operator (a) stops healing the instance — returning before Rule 0, so no rule fights a battle §9.2 proved is unwinnable, (b) logs once per transition rather than per reconcile, and (c) re-examines it at the **steady** interval instead of the fast one. The instance stays `Ready=False` and loudly broken, which is the intended end state; only the futile churn stops. The verdict is retracted automatically once the signature clears, so a human running the runbook is picked up on the next steady tick and normal management resumes.
  **⚠ Corrected by LR-045: effect (c) was inert for sentinel mode — the only mode that can ever be forsaken.** The interval switch was wired into `updateStatus`, but sentinel-mode reconciles return through the separate `updateSentinelStatus`, whose not-`Running` branch requeued at the fast interval unconditionally, with no `Forsaken` check at all. Measured live on t3e (LR-044 milestone M4a): **31 reconciles in 114s (~3.7s apart)** while quarantined and `Forsaken=True` — effects (a) and (b) held (no rule fired, one log line per transition), so the churn was cheap rather than noisy, but it was still there. See LR-045.
- **Rejected first attempt, and why it was wrong:** a *global* stall backoff — fall back to the steady interval for ANY instance not-Ready for five minutes, keyed on the Ready condition's `LastTransitionTime`. It was simpler and needed no new state, and it was wrong on principle: it weakens a load-bearing global invariant (a non-Running instance is polled fast because the healing rules are driven by those iterations — LR-012/LR-014/LR-017) to fix one narrow, nameable case, and it silently reinterprets every *other* slow-converging instance as stalled. A five-minute threshold is also a guess with no relationship to anything real. Reverted in favour of a verdict that says what is actually true about the instance. The general lesson: **do not trade a global invariant for a specific defect that can be named.**
- **The predicate is conservative in one direction on purpose.** A false positive parks a live instance; a false negative merely leaves today's behaviour. So all four clauses must hold: (1) at least one reachable **monitoring** Sentinel — bare Sentinels are Rule L's business (LR-015); (2) every reachable monitoring Sentinel agrees on ONE master address — disagreement is a transition, and transitions are not verdicts; (3) that address is not one of our pods **and is not flagged down** — the down flag is what keeps ordinary post-failover debris (a dead ex-master, LR-024's subject) out of this, since an address that is not ours and is still answering means something else is alive there; (4) no reachable Redis pod of ours is a master — while one still is, the instance has something to be healed back toward and the existing rules own it. Plus a 30s `forsakenCooldown`, which exists only to absorb a bad read: a legitimate failover moves mastership to one of OUR pods and so can never produce this signature.
- **Not the controller-side check ADR-015 rejected.** That one was a **collision check** whose silence would have been read as an all-clear it could not give. This condition only ever reports a positive, locally-observed fact — "our own Sentinels are serving someone else's master" — and asserts nothing at all when absent. (Recorded because the first draft of this fix mis-cited Alternative E as forbidding it.)
- **Known gap, NOT fixed here — the other side of a capture is silently healthy.** Verified on the same pair: the captured instance reports `Initializing`/`Ready=False` (loud, as §9.2 promises), while the instance whose master was adopted reports **`Running` / `Ready=True` / "All Redis and Sentinel pods are ready"** — with its Sentinels holding **5 replicas where 2 were deployed**, three of them the other instance's pods. Its own topology is intact, so no rule here fires; but its Sentinel failover-candidate set is poisoned, and on its next master death Sentinel can promote a **foreign** pod as its master. `lrctl verify` flags it (`FAIL`); the operator does not. This qualifies §9.2's "detection is not the gap: a captured instance sits at Ready=False" — true for the victim, false for the captor. Tracked for a decision, deliberately not bundled into this change.
- **Regresses:** None. The fast/steady intervals, and the rule that a non-Running instance is polled fast, are unchanged for every instance that is not forsaken — the gate is one added condition check. `planForsaken` is pure and table-tested across all four clauses plus the cooldown; the sentinel healing chain is untouched apart from the early return. Cluster/failover modes do not reach this path.
- **Impacts:** ADR-015 §9.2 (this is the "park it, do not fight it" half of declining recovery); `docs/API_SPEC.md` (the `Forsaken` condition and `status.forsakenSince`); `docs/USAGE.md` runbook (the condition now names the state the runbook fixes).


## [LR-043] `CLUSTER MEET` at Unattributed Pod IPs — a Source-Confirmed Cross-Instance Cluster Merge
- **Date:** 2026-08-22
- **Commit:** (pending)
- **Status:** **source-confirmed reachable, never observed in the field.** This is not an incident write-up. Nothing merged, no data was lost; the chain below is established from the Redis/Valkey sources plus two of this project's own prior findings, and closed prophylactically. Its sibling in sentinel mode (LR-039) *was* a field incident, which is what prompted looking here.
- **Problem — the operator was MEETing addresses it had not attributed to itself.** The cluster-mode Step 1 partition-healing loop (`cluster_reconcile.go`) picked the largest partition's seed and then, for every other gathered node, issued `CLUSTER MEET seed → node.PodIP` under exactly two guards: "not the seed" and "IP non-empty". `node.PodIP` comes from the **cache-backed** pod read in `gatherGroundTruth`, which is the precise read **LR-012** documented as returning a **stale `Status.PodIP`** during pod churn (and LR-017 revisited when those stale IPs blackholed). **LR-039** then proved, in production, that pod IPs really are recycled across *unrelated* instances on a shared pod network — that is how one instance's Sentinels were introduced to another's Redis. Compose the two: a stale cached IP, recycled onto another instance's cluster-mode Redis pod, gets `CLUSTER MEET`-ed.
- **Why that is a merge and not a failed command** (read first-hand at three versions — Redis 8.4.2 `src/cluster_legacy.c` (6560 lines), Redis 7.2 `src/cluster.c` (7830 lines), Valkey 8.1 `src/cluster_legacy.c` (7509 lines). Line numbers below are Redis 8.4.2 as read; each of the four load-bearing markers is present exactly once in each of the three files):
    1. **MEET has no membership validation at all.** `clusterStartHandshake` (`cluster_legacy.c`:1994, guards at :2000-2016) validates only `inet_pton` (v4, then v6) and a 1-65535 range on both ports. Nothing checks the target's cluster identity, epoch, slots or size — no such field exists in the protocol.
    2. **The receiver trusts an inbound MEET.** :2937-2961 — `if (!sender && type == CLUSTERMSG_TYPE_MEET)` creates the node via `createClusterNode(NULL, CLUSTER_NODE_HANDSHAKE)` (:2950) and adds it; then a **second, identical** guard (:2960-2961) calls `clusterProcessGossipSection(hdr, link)`, ingesting the stranger's entire node table. Upstream comment at :2957-2959: *"we still process the gossip section here since we have to trust the sender because of the message type."*
    3. **The initiator adopts the responder's identity.** On a handshake link, if the responder's node ID is unknown, `clusterRenameNode(link->node, hdr->sender)` runs (:2996) and the handshake flag is cleared — the foreign node becomes a full member. Bidirectional, and transitive via gossip.
    4. **Node-ID keying protects only ESTABLISHED nodes** — and does so exactly as one would hope: :3008-3014, a PONG whose sender ID mismatches the recorded one gets `CLUSTER_NODE_NOADDR`, has its `ip` and all three ports cleared, and its link freed. Upstream states the warm-IP reasoning verbatim in the *gossip-learn* path (:2216, in `clusterProcessGossipSection`'s `else if (!node)` branch): *"we cannot simply start a handshake against this IP/PORT pairs, since IP/PORT can be reused already, otherwise we risk joining another cluster"* — which is why a gossip-learned node is created with the node ID **from the payload** (`createClusterNode(g->nodename, flags)`), never via handshake, and only when the sender is already a known member of our cluster (the `if (sender && ...)` guard on that same branch, whose own comment gives the identical reason). **MEET is the one operation that creates a fresh identity binding, which is why it is the hole.**
    5. **Authentication does not close it.** `grep -n 'requirepass\|masterauth' src/cluster_legacy.c` returns **zero** hits in all three files, confirmed at each version: the cluster bus is unauthenticated, and the merge travels on the bus. So this is not a documentation-of-`spec.auth` matter (contrast LR-039, where auth *is* the remaining mitigation for Sentinel's address-adoption path).
    6. **Provenance of the citations above.** They were first established by a source survey, and were carried into this entry **second-hand**: the implementer that wrote it recorded having verified "only the repo half", because the Redis/Valkey trees are not vendored here. They were re-read first-hand against upstream at all three versions on 2026-08-23 and the line numbers corrected to what was actually read (every one was accurate to within a few lines; nothing was wrong). Recorded because this file's standard is source-confirmed claims, so "verified" has to mean somebody looked — and because a chain of restatements can look like evidence when it holds exactly one link of it.
- **Fix, part 1 (primary guard) — confirm the address at the API server, uncached.** Every variant of this hazard exists because the operator MEETs an address derived from a **stale cache-backed** `Status.PodIP`, and no amount of Redis-side inference can fully close it: the cluster bus carries no instance identity, so there is a floor on what bus evidence can prove. Kubernetes can answer the question exactly — it holds at most one live pod per IP, so if our own pod object still reports this address, whatever answers there **is** our pod; and a recycled IP is by definition no longer our pod's IP. The reconciler therefore gained an **uncached** reader (`APIReader: mgr.GetAPIReader()`, the same `get pods` permission as the cached client — **no new RBAC**), consulted only on the MEET paths:
    - **Step 1**: one uncached pod GET per MEET candidate (and for the seed) at MEET time — `confirmPodIP`, which denies on `pod-gone` / `ip-changed` / `confirm-failed`. Bounded by the node count, and only while `HasPartitions()` is true; the steady loop and the gather are untouched (making the *gather* uncached would put an extra GET per pod on every 2s pass, which is not the same trade).
    - **`bootstrapCluster`**: the existing per-pod cached GET is simply **replaced** by the uncached one — zero extra requests, and the IP it goes on to MEET is by construction the one the API server reports right now, so no separate confirm read is needed.
    - **migration Meet phase**: one GET per not-yet-met pod, before the attribution probe (so a stale address costs no Redis round-trip either). One-shot migration phase, not a steady loop.
  **Residual staleness, narrowed not closed:** the kubelet writes `Status.PodIP`, so a *terminating* pod's object can still report an address the CNI has already released and handed on. That window is bounded by the pod object's removal at the API server, whereas the cached read's window is unbounded informer lag over objects that may not exist at all (LR-017 observed killed pods still listed `Ready`). So the check narrows the exposure sharply and independently of load — it does not eliminate it, which is why attribution stays.
- **Fix, part 2 (defence in depth) — attribute the address before creating an identity binding.** New pure seam in `internal/redis/cluster_state.go`: `AttributeMeetTarget(MeetCandidate, ourNodeIDs) MeetVerdict`, with the Step 1 adapter `(*ClusterGroundTruth).PlanPartitionMeets()`. A target is admitted only when it was **identified this pass** (an identity probe answered *and* its own `CLUSTER NODES` view was gathered) and one of:
    - **member** — its own gossip view names another node of ours. A stranger cannot, without prior contact. This is the genuinely-partitioned-node case Step 1 exists for.
    - **isolated** — its node table names nobody but itself, *whatever slots it holds*. That covers a fresh/restarted/wiped pod (bootstrap's normal case), a survivor whose peers were FORGOTten in Step 2, and an LR-018 consolidated master cut off from its peers.
  Everything else is skipped with a named verdict (`unidentified` / `no-gossip-view` / `unattributed`) on the audit log — a silently suppressed MEET is exactly what makes a future partition-healing bug hard to diagnose. **Zero extra Redis round-trips in the repair loop**: the gossip view is `gt.KnownNodes`, already retained for partition detection (LR-014).
  **What this closes, precisely:** the **established**-foreign-cluster merge — a node naming peers, none of them ours — which is the case that costs, because such a node arrives owning slots *and* carrying a config epoch.
- **The design concession, stated up front rather than buried as a residual: an isolated node cannot be attributed from bus state at all.** The cluster bus carries no instance identity and no authentication, and our own pods are routinely isolated, so a foreign isolated node — peers dead or forgotten, i.e. precisely the LR-023 wipe state this operator's own recovery manufactures — is indistinguishable from our own survivor and is **allowed**. That is why `confirmPodIP` is the *primary* guard and this predicate only the second layer.
  **A slot-alignment clause was built here and then deleted** (`ownsExactlyItsShardRange` / the `survivor` verdict, present in the first draft of this change, never in a release), and the reasoning is worth keeping because it inverted on inspection: (a) its safety value was ~nil — `GenerateSlotRanges` is a pure function of `shards`, so two instances with the same shard count have byte-identical ranges and it admitted an aligned *foreign* isolated owner exactly as readily as our own survivor; its only genuine denial was a foreign cluster with a *different* shard count. And (b) **it could deny a legitimate own node**: it opened with `len(slots) != 1 ⇒ deny`, so an isolated master owning **more than one** range was refused — the **LR-018 consolidated-shard state, which this project has actually seen in the field** (`debug-0720`, stuck ~19h). Consolidated + isolated + partitioned ⇒ Step 1 refuses to MEET it back and the partition cannot heal: the LR-018/LR-023 "repair step that can never fire" shape, bought for no safety. It would also have refused any legacy `{name}-cluster-N` pod (`ShardIndexFromPodName` returns -1 ⇒ deny) — but that one is a latent property with **no reachable call site**, and the first write-up of this deletion got it wrong by calling it "the migration's own MEET targets": Step 1's targets come from `ClusterPodRefs` and the migration's from `facts.NewPodAddrs`, both per-shard names, and the new pods are slot-less anyway so they meet the isolated clause. Recorded because the mistaken version is the more alarming one and would have been believed. Narrow and unobserved — it needed all three conditions at once, and was caught in review before release — but the wrong trade. **The cost of removing it is explicit: we give up the deny for a foreign isolated slot-owner whose shard count differs from ours, inside the `confirmPodIP` window only.** General lesson: **slot alignment was never attribution** — it is a property of the *shape* both instances share, not of *whose* node it is.
- **The seed is attributed too**, because the MEET is issued *at* the seed: an unattributable seed would be told to meet all of our pods — the same merge, in the other direction. A refused seed means no MEET that pass; the fast (2s) cadence retries.
- **Why not the obvious `!node.Reachable` filter.** It closes the hazard but is both too weak and too strong. **Too weak:** a foreign pod that shares our password answers our probes perfectly well, so reachability is no evidence of ownership — the LR-039 shape passes it. **Too strong in the wrong place:** the MEET is executed *at the seed*, so the target does not need to be reachable *from the operator* for it to be useful, and on a cloud where dead IPs blackhole rather than RST (LR-017) a bare reachability filter would skip a live-but-operator-unreachable node — suppressing the very healing Step 1 performs. That cost turns out to be immaterial, and the reasoning is worth recording: **partitions are computed only over operator-reachable nodes** (`computePartitions` keys on `nodeIDtoPod`, which requires `Reachable`), so an unprobeable node is not in any detected partition to begin with — Step 1 fires on ≥2 components of *reachable* nodes, MEETing an unidentified address is speculative rather than corrective, and the node re-enters the plan on the pass where it answers. Identification is therefore required not as a liveness proxy but because it is the only thing that can carry attribution.
- **Residuals of the attribution half, stated rather than papered over — all three now sit behind the primary guard, i.e. they are reachable only inside the API-server staleness window above.** (1) An address answering as a **pristine** Redis (single-entry node table, no slots) is indistinguishable from our own fresh pod — that *is* what our own pods look like at bootstrap, and the bus carries no instance identity to compare. Merging a pristine node costs nothing (no data, no slots, no epoch); the established-foreign-cluster merge, which costs everything, is closed. Nothing short of an authenticated bus or an operator-planted marker closes this half, and neither exists. (2) If **two** of our pod names resolve to two nodes of the *same* foreign cluster, they vouch for each other under the member clause. Two independently-recycled stale IPs landing on one foreign instance is a narrow shape, and no cheaper predicate excludes it. Both are now subsumed by the isolated-node concession above (a pristine node and a mutual-vouching pair are just two shapes of "not attributable from bus state"), and all of it sits behind `confirmPodIP` — so the residual that actually remains after this change is the **API-server staleness window**: a *terminating* pod's object can still report an address the CNI has released and handed on, bounded by the object's removal rather than by unbounded informer lag. Nothing here is closed by `spec.auth`; the bus is unauthenticated.
- **Does the primary guard make an attribution clause dead weight? No — checked clause by clause.** The two guards answer different questions ("is this address still ours?" vs "is what answers there behaving like ours?"). `unidentified` / `no-gossip-view` / `no-address` fire independently of freshness (nothing answered, or answered without a view) and run **first**, which also keeps the API GET off candidates that would be skipped anyway. `unattributed` fires when a confirmed-ours address nonetheless answers as a foreign **established** cluster — reachable inside the staleness window above, and the only thing that would catch a wrong `APIReader` wiring. The two allow clauses are what stop the guard denying our own pods, and they were collapsed from three for exactly that reason (see the concession above). So both halves stay, in that order.
- **Cross-mode / parity audit (CLAUDE.md §7 rule 11).** Every site that creates an identity binding or acts on a cache-backed pod IP was audited:
    | site | verdict | why |
    |---|---|---|
    | Step 1 partition heal (`repairCluster`, MEET) | **FIXED** | the reported hazard; now `PlanPartitionMeets` + a `confirmPodIP` uncached read per candidate and for the seed |
    | `bootstrapCluster` MEET seeding | **FIXED** | same class. Its revision gate bounds *which deployment* a pod object belongs to, not how fresh the cached `Status.PodIP` is, so a recreated pod at an unchanged STS revision passes it with a stale IP. Attribution needs the per-pod view, which bootstrap did not read, so it now issues one `CLUSTER NODES` per pod beside its existing `CLUSTER MYID` — bootstrap-path only, next to a pre-existing `sleep 2`. Seed refused ⇒ requeue without touching the cluster. The per-pod cached GET became an **uncached** one, so freshness costs nothing here |
    | migration `MigrationMeet` (`cluster_migration.go`) | **FIXED (driver)** | same class, reachable via the LR-025 chaos shape (a new pod crashing/rescheduling mid-migration). The *pure planner* cannot make this call: `restrictToLegacyMesh` deletes both "unidentified" and "identified but not yet in the mesh" pods from `gt.Nodes`, so the evidence is gone before `missingNewPodMeets` runs. The driver confirms each un-met target with `confirmPodIP` and then probes it directly (one `CLUSTER NODES`, Meet phase only), applying the same predicate. `migration_plan.go` unchanged |
    | migration MEET **seed** | **not gated — follow-up** | a legacy `{name}-cluster-N` pod of an already self-consistent cluster, identified this pass and behind the `LegacyShapePreserved` facts; it is the node the migration's entire ground truth is anchored on. It also gets **no** `confirmPodIP`, unlike the targets. The reason originally recorded here — that the slot-alignment clause would refuse a legitimate single-node legacy cluster — **dissolved when that clause was deleted**, so gating the seed is now both cheap and safe. Left as a follow-up rather than folded in unreasoned |
    | `CLUSTER REPLICATE` (Step 4, bootstrap, migration) | safe as-is | node-ID-keyed, creates no identity binding, and Step 4 additionally gates on `gt.NodeKnows(...)` (LR-014). Issued at a foreign address it draws `ERR Unknown node` |
    | `CLUSTER FORGET` (Step 2, migration decommission) | safe as-is | node-ID-keyed and skips unreachable nodes (LR-012). At a foreign address: `ERR Unknown node` |
    | `CLUSTER FAILOVER TAKEOVER` (Step 0 quorum loss, Step 1 orphan promotion) | **audited, NOT fixed — see below** | node-ID-free but issued *at* an address. At a recycled IP it would promote a *foreign* instance's replica over its own master |
    | sentinel / failover modes | N/A | no `CLUSTER MEET`; their cross-instance analogue is LR-039/LR-042 (master-name scoping, `Forsaken`) |
- **Deliberately out of scope (recorded so it is not lost): the TAKEOVER sites are reachable from an unattributed address.** A recycled IP read as `role:replica` whose master is not one of our pods lands in the orphan-tracking path (its foreign master's ID has no pod, so it is a "ghost"), and after `clusterNodeTimeout + failoverGracePeriod` the operator force-promotes it — damaging the *foreign* instance, not ours. The honest fix is not another local guard but keeping unattributed nodes out of the ground truth that feeds decisions at all, which is a gather-level change touching ghost detection, partition computation and health verdicts — high regression surface, and the LR-038 correction ("a guard written against the ground truth is only as good as what the ground truth is allowed to contain", and equally: widening or narrowing the gather changes every rule at once) says to do that deliberately, not as a rider. Tracked separately.
- **Tests:** `internal/redis/meet_attribution_test.go`, red-first. `TestAttributeMeetTarget` (9 rows) was authored against a deny-everything stub and observed **RED on 6 rows** — `AttributeMeetTarget = "unattributed", want "member" / "fresh" / "survivor" / "no-address" / "unidentified" / "no-gossip-view"` — plus both plan tests (`seed = <nil>, want … n0`; `SeedVerdict = "", want "unattributed"`). Because a deny-everything stub passes the *deny* rows vacuously — including the hazard fixture, which is the row that matters — an "always allow" mutant was then run and failed **all four deny rows** (`= "member", want "unattributed"`, `Allowed() = true, want false`), so both directions have teeth. The table is 11 rows: a genuinely partitioned own node (must MEET), a fresh/wiped pod (must MEET — bootstrap's normal case), an isolated survivor owning its own range (must MEET), an **isolated own master owning two consolidated ranges** (must MEET — the LR-018 row, added when the slot-alignment clause was deleted and observed **RED** against it: `AttributeMeetTarget = "unattributed", want "fresh"` / `Allowed() = false, want true`), an isolated legacy-named slot owner (must MEET), an unidentified and a view-less address (must not), a foreign **established** cluster node under one of our pod names with a disjoint node table and its own slots (must not — the hazard fixture), and two foreign **isolated** slot owners, aligned and misaligned, both of which the collapse deliberately admits — pinned as rows so the concession and its cost stay visible rather than implicit.
  For the primary guard, `internal/controller/cluster_meet_freshness_test.go` / `TestConfirmPodIP` (4 rows, fake uncached reader) was authored first and observed **RED on 3 of 4** against a `return true, ""` stub — `confirmPodIP ok = true (""), want false` for the moved-IP, pod-gone and no-IP-yet rows. The **aligned-survivor** row is green from birth (it pins existing behaviour rather than driving new code); its mutation check is that forcing `ownsExactlyItsShardRange` to false flips it together with the legitimate-survivor row, which is exactly the point — attribution cannot separate the two, by construction.
- **Not e2e-verified.** The failure mode of this change is **over-suppression** — a MEET skipped that should have been issued, leaving a genuine partition unhealed — and unit tests cannot prove its absence. Step 1 returns early while partitioned, so Steps 2-4 are suspended for as long as it is suppressed; nothing downstream *assumes* Step 1 succeeded (each step re-derives from a fresh gather), so a suppressed MEET is a stall, not a corruption. The tiers that would catch it are `Cluster Mode Chaos Testing` (all three contexts — random and continuous multi-pod deletion is what manufactures partitions), `Cluster Mode Functional Testing > Failover Recovery`, `Cluster Total-Wipe Re-Bootstrap` (both flavors exercise the fresh-pod clause through bootstrap and self-heal), and `Cluster Legacy→Per-Shard In-Place Migration` incl. its restart-during-migration chaos tier. A fully degraded gather is *not* a new deadlock: with nothing reachable, `computePartitions` yields no components, `HasPartitions()` is false and Step 1 never runs.
- **Regresses:** None expected. The uncached reads are confined to the MEET paths (partition healing, bootstrap, migration Meet) — the steady loop, the gather and every other mode are untouched, and `APIReader` needs no additional RBAC. **The reader is defaulted in `SetupWithManager`, and that is deliberate belt-and-braces — do not "clean it up".** Left to the wiring site alone, `APIReader` would be exactly the shape LR-041 warns about: a required value held as optional-looking construction state, with no enforcement. Drop the assignment in `main.go` in some future refactor and the MEET guard silently degrades to the cached read — back to this very bug — with every test still green. `SetupWithManager` already receives the manager, so it defaults the field (`if r.APIReader == nil { r.APIReader = mgr.GetAPIReader() }`); every production path goes through it, so production can no longer forget. The explicit `APIReader: mgr.GetAPIReader()` in `main.go` is kept as intent documentation at the wiring site (redundant but correct), and `apiReader()`'s nil fallback to `Client` remains for the unit/envtest reconcilers that never call `SetupWithManager` — and whose `Client` is itself a direct, uncached client, so the fallback is not a silent downgrade there. **Rejected: making it a constructor parameter**, which is what LR-041's lesson literally prescribes. It would force edits to ~10 unit/envtest constructors for a value those tests do not need; defaulting at the one place the manager is available buys the same enforcement where it matters at a fraction of the blast radius. `TestSetupWithManagerDefaultsAPIReader` pins it (mutation-checked: removing the default fails it with "APIReader is nil after SetupWithManager"). No gate, cadence or decision outside the MEET target set changed; every own-pod state that Step 1 legitimately heals is admitted by one of the three clauses (an own pod reachable enough to gather a view either knows a peer of ours, is pristine, or owns exactly its own range — a lone minority node keeps its peers in view as `fail?`, which the gather does not filter). LR-014's `NodeKnows` adjacency is now read by a second consumer but unchanged; the `fail`/`noaddr`/`handshake` filter was extracted to one definition (`nodeFlagsFailed`) shared by the gather and attribution, behaviour-preserving.
- **Impacts:** `CLAUDE.md` pillar 3.4 (the pod list is only as trustworthy as the cached IP; MEET is where an unattributed address is destructive); `docs/RECONCILIATION_LOOP_CLUSTER.md` Step 1. Completes for cluster mode what LR-039 established for sentinel mode: **a recycled pod IP is a cross-instance identity hazard in every mode, and the operation to guard is the one that creates a new identity binding.**

### Regression and correction — the attribution layer refused a legitimate own node, and a partial wipe could never re-converge (t3e, 2026-08-23)

**The entry above says, twice, that the failure mode of this change is over-suppression and
that unit tests cannot prove its absence. It was right. The e2e caught it on the first
full-suite run after landing, in the one tier whose entire purpose is to protect a surviving
data-holder.**

- **Failing spec:** `Cluster Total-Wipe Re-Bootstrap > Partial wipe keeps a surviving
  data-holder > preserves the surviving replica's data and never recycles it`
  (`test/e2e/cluster_totalwipe_test.go`). Timed out after 480s at `status.phase:
  Initializing`, expected `Running`. All six pods were `2/2 Running`, so `allPodsReady` held
  and `repairCluster` was running every 2s — not a crash-loop, not scheduling.
- **What the operator logged, 180 times, for the same target:**
  `Skipping CLUSTER MEET: address answers as a cluster node not attributable to this
  instance … wipe-partial-…-shard-0-1@10.233.192.143 (unattributed)
  nodeID=66a19469b0bb546caa94c511bd980504648993a5`, with
  `partitions:2 ghosts:5 emptyMasters:true masters:1 allNodesView:11` on every pass, plus one
  `no attributable MEET seed this pass`. **`shard-0-1` is the survivor** — the pod the tier
  exists to prove is preserved and never recycled (8m54s old against the recycled pods'
  6m24s).
- **Mechanism, confirmed on the live cluster rather than inferred.** `kubectl exec` on the
  survivor: its own `CLUSTER NODES` names four former peers as `master,fail?` / `slave,fail?`
  and one as `slave,noaddr`; `cluster myid` on each of the five recycled pods returns a
  **new** ID (`2372383b…`, `36f74258…`, `a3e52b2f…`, `1474c0dd…`, `1ba11fa7…`), none of them
  the ghosts the survivor still lists. `nodeFlagsFailed` filters `fail`/`noaddr`/`handshake`
  but deliberately **not** `fail?` (PFAIL) — that non-filter is what makes a partitioned own
  node vouch for itself, and here it is what keeps the ghosts in the peer set. So in
  `AttributeMeetTarget` the survivor has `peers > 0` and no ID in `ourNodeIDs` ⇒
  `MeetDenyUnattributed`. **And it can never acquire an anchor:** that would require the fresh
  pods to appear in its view, which requires exactly the MEET being refused. Deadlock, not a
  slow convergence. The five fresh pods meanwhile MEETed each other into their own five-node
  partition with `cluster_slots_assigned:0`; Step 1 returns early while partitioned, so Steps
  2-4 stayed suspended for the whole eight minutes.
- **The `MeetAllowMember` clause tolerates ghost IDs *alongside* a known-ours anchor. A wipe
  leaves no anchor at all.** That is the gap: the clause was designed against the partition
  case (peers alive, merely unreachable) and never against the recycle case (peers gone,
  replaced under new identities) — which is the state this operator's own LR-023 wipe
  recovery *manufactures*.
- **Data was NOT lost.** The tier timed out before its data assertion, so this had to be
  checked rather than assumed: on the live instance the survivor reports `dbsize 1` and holds
  `survivor-shard-key`, still owning `0-5461` as `myself,master` — Step 0/1 promoted it and
  `planClusterWipeRecovery` never recycled it, both exactly as LR-023 intends. So the
  severity is **availability and convergence**, not durability. The guard broke the tier that
  protects the data-holder without ever endangering the data.
- **Same failure class as the clause deleted in `f5d0e98` one commit earlier**
  (`ownsExactlyItsShardRange`, which refused an isolated own master owning more than one
  range — the LR-018 state). That deletion was caught in review; this one was not, because
  the surviving clauses were reasoned about one candidate at a time and nobody asked what a
  *recycled* peer set looks like. Two clauses, two ways to deny a legitimate own node, in one
  change.
- **Fix — put the two guards in the right order of authority.** They were never equal in
  evidentiary strength, and the code let the weaker one veto the stronger.
  1. **`confirmPodIP` now also refuses a pod carrying a `deletionTimestamp`**
     (`podIPTerminating`). That closes precisely the residual the entry above named — the
     kubelet writes `Status.PodIP`, so a *terminating* pod's object can keep naming an address
     the CNI has released and handed on — which is the only window in which "our pod object
     claims this IP" is not "this IP is ours". The same check is applied inline in
     `bootstrapCluster`'s uncached per-pod read, which has no `confirmPodIP` call.
  2. **`MeetDenyUnattributed` is demoted from a veto to a logged warning** on an address that
     has been positively confirmed (`MeetVerdict.AdmissibleWhenConfirmed`). Kubernetes decides
     ownership — it holds at most one live pod per IP, so a confirmed address *is* attribution,
     a fact rather than an inference; the bus may inform but not overrule. `PlanPartitionMeets`
     keeps such a node in `Targets` and records it in the new `Unattributed` list, and the
     caller logs the disagreement next to the MEET it describes, so a genuine merge stays
     diagnosable. The **seed** is demoted identically: with two single-node partitions
     `GetLargestPartitionSeed` can pick the survivor, and refusing the seed refuses the whole
     pass — `no attributable MEET seed this pass` is that path, observed once in this very run.
  **The hard denials are NOT relaxed.** `no-address` / `unidentified` / `no-gossip-view` mean
  there is no evidence at all, which no API-server read can supply, and unlike `unattributed`
  they are self-clearing: partitions are computed only over operator-reachable nodes, so an
  unidentified address is in no detected partition and re-enters the plan on the pass where it
  answers. A dedicated test pins that an unidentified seed is still refused.
- **Why this layer and not another clause.** The alternatives were weighed and rejected: admit
  a node whose peers are *all unreachable* (extra probing, and it is one more special case),
  or admit a node whose peer set is disjoint from every address we can see (same). Both add a
  clause to a predicate whose two previous clauses each turned out to deny some legitimate
  state; the pattern to break is the growing pile, not to extend it. Demotion removes the
  *authority* of the weaker evidence instead of patching its content.
- **What the demotion costs, stated exactly.** We give up the deny for a **foreign
  *established* cluster answering at an address our own non-terminating pod object still
  reports**. The claim that no such path remains is *too strong* and is not made here: a pod
  object whose IP is stale without a `deletionTimestamp` is reachable on hard node loss (the
  kubelet is gone, so `Status.PodIP` freezes and no deletion is recorded until the node
  lifecycle controller or a human forces it), and in that window the frozen IP could in
  principle be reallocated. It is narrower than it sounds — most CNIs allocate from a
  node-local pool, and that node is the one that is down — but it is a residual, not a
  closure. Accepted knowingly, because of the asymmetry below.
- **The generalizable lesson, and the reason the trade is not close.** **A guard that can deny
  a legitimate own node is more dangerous here than one that admits a foreign node inside a
  narrow window: the deny is a permanent stall, the admit needs a rare coincidence.** The deny
  cost eight minutes of a suspended repair loop in a tier that was *designed* to exercise this
  exact state, on ordinary hardware, with no adversary and nothing recycled across instances.
  The admit needs a stale-but-not-terminating pod object, an IP reallocated out from under it,
  and another instance's *established* cluster answering there. Corollary for this predicate
  specifically: **bus-state attribution is inference over a protocol that carries no instance
  identity, so it can never be the deciding vote over a Kubernetes fact.** And a second one,
  about method: the first landing reasoned about each clause against each candidate shape it
  could think of, which is exactly how both denials survived review — the missing question was
  not "is this clause right?" but "which legitimate own-node states does the *conjunction*
  refuse, including the ones this operator's own recovery paths create?"
- **Parity audit (CLAUDE.md §7 rule 11) — all three sites call the same predicate:**

  | site | verdict | why |
  |---|---|---|
  | Step 1 partition heal (`PlanPartitionMeets`) | **FIXED** | the reported hole; targets *and* seed demoted, `Unattributed` audited |
  | `bootstrapMeetRound` | **FIXED** | same hole, reachable: `bootstrapCluster` runs on `TotalSlots == 0 && no replicas`, and a mass container-crash that keeps `nodes.conf` on some pods while others are recycled with new IDs yields `peers > 0, none ours`. A refused **seed** there is `return false` ⇒ requeue forever, i.e. the same permanent stall. Its addresses come from the uncached read, so they are confirmed by construction — the terminating check was added inline to make that true |
  | migration Meet (`executeMigrationMeets`) | **FIXED** | same predicate behind the same `confirmPodIP`. `restrictToLegacyMesh` deletes un-met pods from `gt.Nodes`, so `ourIDs` is the legacy mesh plus already-met pods; a new pod that crashed mid-migration (the LR-025 chaos shape) can therefore name peers none of which are in `ourIDs` |
  | `CLUSTER REPLICATE` / `FORGET` / `TAKEOVER` | unchanged | not MEET paths; their audit stands as recorded above, including the deliberately out-of-scope TAKEOVER sites |

- **Tests, red-first, and the red is the real shape.** `internal/redis/meet_attribution_test.go`:
  two new `TestAttributeMeetTarget` rows built from the live survivor's own `CLUSTER NODES`
  (identified, reachable, `peers > 0`, every peer absent from `ourNodeIDs`) in both shapes —
  promoted master owning `0-5461`, and the pre-promotion slotless replica — observed **RED**
  as `AdmissibleWhenConfirmed() = false, want true`. `TestPlanPartitionMeetsAdmitsPostWipeSurvivor`
  rebuilds the whole t3e topology (five fresh isolated pods with the real new node IDs in one
  partition, the survivor with its four ghost peers in the other) and was **RED** with the
  production message: `survivor wipe-shard-0-1 is not a MEET target;
  skipped=[wipe-shard-0-1=unattributed] — the partition can never heal`. The seed half —
  `TestPlanPartitionMeetsAdmitsUnattributedSeedForConfirmation`, which deliberately **inverts**
  what `TestPlanPartitionMeetsRefusesUnattributableSeed` asserted — was **RED** as `seed = nil
  (verdict "unattributed"), want it admitted for confirmPodIP to rule on`. `TestPlanPartitionMeets`
  was **RED** as `targets = [lr-shard-1-0 lr-shard-2-0], want [lr-shard-0-1 lr-shard-1-0
  lr-shard-2-0]`. `TestConfirmPodIP` gained a terminating row, **RED** as `confirmPodIP ok =
  true (""), want false` / `reason = "", want "pod-terminating"`. Mutation-checked in the other
  direction: an "always admissible" mutant fails the three no-evidence rows plus
  `TestPlanPartitionMeetsRefusesUnidentifiedSeed`, so the demotion is not a blanket allow.
- **Still unproven, honestly.** The only real proof is this tier going green on `t3e`, and it
  has not been re-run. Everything above is unit-level plus a confirmed diagnosis on the live
  instance; the e2e tiers that would catch a *remaining* over-suppression are the same ones
  the entry above lists (`Cluster Mode Chaos Testing`, `Failover Recovery`, `Cluster
  Total-Wipe Re-Bootstrap`, the migration chaos tier). The `Unattributed` audit line is the
  operational tell: if it ever fires in a healthy steady state, attribution and Kubernetes are
  disagreeing and one of them is wrong.
- **Regresses:** the LR-043 attribution half is strictly weakened, by design and only where
  `confirmPodIP` says yes; the primary guard is strictly strengthened (terminating pods now
  denied everywhere a MEET address is derived). No gate, cadence or decision outside the MEET
  target set changed. `TestPlanPartitionMeetsRefusesUnattributableSeed` is **deleted** — it
  pinned the behaviour this correction removes; its replacement pins the new contract and a
  separate test keeps the unidentified-seed refusal.

## [LR-044] The Captor Side of a Capture Was Silently Healthy — Forsaken-Gated Quarantine (decision layer)
- **Date:** 2026-08-22
- **Commit:** (pending)
- **Scope:** **milestones 2 and 3 of 4 — the pure decision layer, its status surfaces, and the
  StatefulSet wiring that makes the decision act.** The e2e that captures a victim and asserts both
  sides recover is a later milestone, so the behaviour is still **not observed on a cluster**:
  everything below marked as inference stays inference until that run. The wiring half is recorded in
  its own section at the end of this entry (the two halves landed as separate reviewed commits and the
  ordering matters, but they are one defect and one feature).
- **Problem — the gap LR-042 named and deliberately left open.** A capture has two sides and only one
  is loud. Verified on a live pair: the *victim* reports `Initializing` / `Ready=False` (as ADR-015
  §9.2 promises), while the instance whose master was adopted — the **captor** — reports `Running` /
  `Ready=True` / *"All Redis and Sentinel pods are ready"* with its Sentinels holding **5 replicas
  where 2 were deployed**, three of them the victim's pods. Its own topology is intact, so no rule
  fires; but its Sentinel **failover-candidate set is poisoned**, and on its next master death Sentinel
  can promote a *foreign* pod as its master. `lrctl verify` flags it (`FAIL`); the operator says
  nothing. LR-042 stopped the operator wasting itself on the victim; it did nothing for the neighbour
  the victim is damaging.
- **Why the captor must not be operated on directly.** Its Sentinels are not confused: the victim's
  pods are *genuinely* replicating from its master, so its master's `INFO replication` genuinely
  reports five replicas. Sentinel rebuilds its replica list from the master's `INFO` (replicas never
  self-announce — LR-013), so a `SENTINEL RESET` on the captor clears a list that repopulates seconds
  later — and RESET is the LR-024 hazard. Surgery on the captor cannot work while the cause is alive.
- **Fix (decision layer) — quarantine the victim, and let the captor heal through paths that already
  exist.** New pure `planQuarantine` (`internal/controller/quarantine_plan.go`), gated on LR-042's
  existing `Forsaken` verdict:
    1. **Quarantine.** While forsaken, desired Redis **and** Sentinel replicas are 0. Sentinel is not
       optional: the victim's *sentinels* publish hellos on the captor's master's channel under the
       shared name, so the captor learns them as peers — that is the `num-other-sentinels` inflation
       that distorts the captor's quorum math and puts foreign sentinels in its elections.
    2. **The captor then heals itself.** Once the victim's pods vanish, its master's `INFO` reports the
       right count, the departed entries become ordinary `s_down` ghost replicas (a dead replica never
       ages out — LR-024), and **Rule D's gates all pass**: living+reachable consensus master (LR-008),
       ≥1 healthy known replica (LR-011), K8s-grounded wholeness judged against the *captor's own*
       expected pod count (LR-013). Its `SENTINEL RESET` then prunes them. **This is an inference from
       three independently-documented gates, not a documented design, and it is the load-bearing
       assumption of the whole change — it must be verified live before this can be called done.**
    3. **Settle, then re-bootstrap.** After the settling period the pods are allowed back. They come up
       with all Sentinels bare, no master and **zero data holders** — exactly **Rule L's no-data reseed
       signature, which needs no opt-in** (LR-015). "Re-bootstrap" is therefore handing Rule L a state
       it already handles: no new bootstrap machinery, no re-arming of `bootstrapRequired`.
    4. **Bounded, then latch.** `status.quarantineAttempts` counts the attempts; at the limit the
       instance stays at zero instead of being released again.
- **Not a reversal of the DECLINED automated recovery (ADR-015 §9.2).** Quarantine **reclaims
  nothing**. §9.2 declined recovery on two grounds — nothing survives to salvage, and the operator
  cannot outbid the captor's config epoch — and neither is contradicted here: the operator never speaks
  to a Sentinel about the capture, never issues `MONITOR`, and the victim comes back **empty**. §9.2
  itself names that outcome ("a recovery restores an *empty* instance, which is precisely what deleting
  and recreating the CR already achieves") — the difference is only that the operator now does it
  *without a human*, and the reason it is worth doing is not the victim at all but the healthy
  neighbour it stops damaging.
- **The 120s settling period is derived, not guessed** (LR-042's own lesson was that "five minutes" was
  *"a guess with no relationship to anything real"*). The captor is `Running`, so it reconciles on the
  **steady 30s interval**; the settle must span Sentinel re-reading the master's `INFO`, the departed
  replicas becoming `s_down` ghosts, and Rule D's gate chain passing on a couple of those steady
  passes — so ~4 steady passes. It also shrinks the **warm-IP window**: while the captor still lists
  the victim's *old* addresses as `s_down` replicas, a fresh victim pod landing on one of those
  recycled IPs is the exact coincidence that starts a capture (LR-039). 120s additionally matches the
  existing cluster-mode precedent `status.cluster.wipeDeadlockSince` (LR-023).
- **Predicate hardening, and what it independently closes.** The quarantine deletes pods, so it carries
  its own data clauses on top of the verdict (pure `quarantineDataRisk`):
    - **`atRisk`** — a reachable pod holds keys that are **not** explained by the capture. Keys on a pod
      that is a link-`up` replica of the *captor's* master are the captor's own dataset, replicated in;
      the original is still on the captor, so discarding the copy loses nothing. Keys anywhere else may
      be the only copy in existence. This is what makes the quarantine **provably lossless rather than
      lossless-by-argument**, and it independently closes a worry the design record could only argue
      away: §9.2 holds that data cannot survive a capture *except* on a path where replication is
      blocked before the sync starts, and a scale-down could in principle interrupt a `SLAVEOF`
      mid-flight. The clause covers that **whatever the timing**, which is why it is load-bearing and
      not belt-and-braces.
      **Read this before "simplifying" the clause to "all reachable pods hold 0 keys" — that literal
      formulation (which is how this hardening was first specified) is INERT in the case that matters
      most.** A captured victim whose pods completed their full sync holds *the captor's* keyspace, so
      `Keys > 0` on every one of them and a 0-keys gate would never let the quarantine fire. The field
      incident only showed 0 keys because an RDB version mismatch broke the sync — and the capture
      analysis is explicit that this was luck: *"The loud failure was luck. The RDB version mismatch is
      the only thing separating this from a silent one"*, where a version-compatible victim instead
      "serves `instance-B`'s keyspace to its own clients" with no alarm at all. The silent case is the
      common one, and it is the one with `Keys > 0` everywhere. So the discriminator has to be *whose*
      data the keys are — a link-`up` replica of the captor's master is holding a copy of data that
      still exists on the captor — not *whether there are* keys.
    - **`unverified`** — a pod of ours did not answer, so it cannot be proven empty. The operator's own
      dial is not blackhole-proof (LR-017), so an unreachable pod may still be serving clients.
      Refusing is the safe direction. It does mean a permanently crash-looping pod holds the quarantine
      open (and so keeps the captor dirty); the accepted fix, in the wiring milestone, is to feed this
      input from **kubelet pod readiness** instead of gather reachability — the LR-023 precedent
      exactly, since a not-Ready redis in a pure in-memory instance holds no data and readiness is
      blackhole-proof where a remote dial is not.
    - **Terminating pods are outside the gather**, so a terminating pod holding data is invisible to
      this clause — the guard is only as good as what the ground truth is allowed to contain (LR-038).
      Judged harmless rather than overlooked: a terminating pod's RAM is gone whatever the planner
      decides. Widening the gather to see it would be strictly worse (LR-038: a terminating pod in the
      gather reads as live topology to every other rule).
    - **"never quarantine an instance that has a master of its own"** needs no new clause: `planForsaken`
      clause 4 already refuses the verdict while any **reachable** Redis pod of ours is a master, and the
      asymmetry is structural — a merge resolves by config epoch, the winner keeps its master, the
      loser's pods all become replicas of the winner, so "no master of its own" *is* the loser.
- **Bounded, and why: N = 2, or 1 for a known-dangerous configuration.** Every recapture re-pollutes the
  captor — the victim's pods re-attach to its master and it is dirty again until its own Rule D cleans
  up — so an unbounded retry does not merely fail to fix the victim, it repeatedly degrades a healthy
  neighbour. N=2 gives one re-roll for the lucky case (a capture needs an *address coincidence*, not
  just a shared name, so most are luck) and stops when the evidence says it is not luck. N=**1** when
  the instance's own configuration is what makes capture reachable: auth disabled **and** the effective
  master name is the shared legacy `mymaster`. The predicate deliberately reads the *effective* name
  (`SentinelMasterName() == LegacySentinelMasterName`) rather than `SentinelMasterNameUnscoped()`, which
  reports only that the field is *unset* and — correctly for its own purpose — does not flag an instance
  that sets `mymaster` explicitly. A deliberate `mymaster` is exactly as capturable as an omitted one.
  **Default-on, with no opt-out knob:** the instance is provably empty and unrecoverable while actively
  damaging a neighbour, and an opt-in nobody sets protects nobody.
- **Where the state lives, and why `status` is right here.** The **verdict** needs no persistence — it is
  re-derived every pass. The **attempt counter** goes in `status`, deliberately and not in an annotation.
  ADR-006's "nothing is persisted" was about an *internal engine capability* (async slot migration) that
  a free gather-time probe could answer; "status is a monitoring surface" is exactly why a
  recapture-attempt count belongs there — it is a monitoring metric, not derived engine state. The
  governing distinction is **annotation = intent, status = monitoring** (pillar 3.14), and this is
  monitoring. `status.quarantineAttempts` is also the clearest operational signal this state has:
  *"quarantined twice"* says "your configuration is the problem" better than any condition message,
  which is why it is surfaced on the `Forsaken` condition reason (`Quarantined` / `QuarantineLatched` /
  `QuarantineRefusedDataPresent` / `QuarantineRefusedDataUnknown`) and as one Warning event per
  transition rather than per reconcile.
- **The trap that shapes the whole design: the verdict self-clears once quarantined.** With no pods
  there is no reachable *monitoring* Sentinel, so `planForsaken` clause 1 fails, the state reads "not
  captured", and `clearForsaken` runs — **every pass of the quarantine**. So the *signature* cannot hold
  the state; `status.quarantinedSince` and `status.quarantineAttempts` must, and `clearForsaken` must
  deliberately not touch the counter. Hence the two different lifecycles: `quarantinedSince` is cleared
  by the planner's release decision, the counter **only on SUCCESS** (`Phase == Running`). A counter
  that reset whenever the verdict cleared would reset every cycle and never latch — which is the same
  reason the planner decides an armed quarantine **first and without reference to the verdict**, a
  property that also lets a caller running *before* the gather compute the same `ScaleToZero`.
- **Tests:** `internal/controller/quarantine_plan_test.go`, red-first. `TestPlanQuarantine` (11 rows)
  authored against a zero-value stub and observed **RED on 11 of 11** (10 on `Phase = "", want …`, the
  `not captured` row on `AttemptLimit = 0, want 2`). Because a deny-everything stub passes the refusal
  rows vacuously, an **"always quarantine" mutant** was then run and failed exactly the four rows that
  must never scale to zero — `not captured`, `HoldSuspected`, `HoldDataPresent`, `HoldDataUnknown` — so
  both directions have teeth. `TestQuarantineDataRisk` (5 rows) observed **RED on 3 of 5**
  (`atRisk = false, want true` for keys-not-following-the-captor and for a link-`down` replica;
  `unverified = false, want true` for an unreachable pod). `TestQuarantineConfigDangerous` is green from
  birth (it pins a two-clause predicate rather than driving new code); its mutation check is weakening
  the `&&` to `||`, which fails the two mixed rows.
- **Consequences accepted rather than left open** (reviewed and decided, so a later reader does not
  re-litigate them): the release lands up to ~150s after arming rather than 120s, because a forsaken
  instance is polled at the **steady** 30s interval and 30s granularity on a 120s timer is immaterial.
  **⚠ Corrected by LR-045.** At the time this was written the reasoning was wrong, not just imprecise:
  LR-042's steady-cadence promise was inert for sentinel mode (below), so the M4a live run in fact
  measured the polling as **fast** and the release still landed at 120-122s (see the M4a table above) —
  the ~150s figure was accidentally-correct arithmetic built on a false premise. LR-045 makes the
  steady-cadence choice real for sentinel mode, so the ~150s figure is now the true consequence of
  *this* fix, for the reason originally stated: a forsaken instance is genuinely polled at the steady
  30s interval, and 30s granularity on a 120s settle timer is immaterial.
  Separately, `status.quarantineAttempts` is reset on `Phase == Running`, so an
  instance that genuinely re-bootstrapped, served, and is only *then* recaptured gets a fresh budget.
  The latch therefore bites when recapture happens *before* the instance is healthy — which is the
  intended shape ("self-heal the lucky case, latch when it is not luck") — and an instance that
  oscillates through healthy states between captures will keep re-rolling.
- **Regresses (decision layer):** Nothing in this half acts on the decision, so its own behaviour change
  is confined to the `Forsaken` condition's reason/message and two new status fields. What the wiring half
  adds on top is stated in its own Regresses note below. `planForsaken` is untouched — the data clauses
  live in the quarantine planner rather than the verdict, deliberately: LR-042's verdict answers "is
  this instance still ours to manage", and an instance holding data is still not ours to manage, so
  weakening the verdict would put the operator back to thrashing it. The sentinel healing chain is
  untouched apart from the existing early return.
- **Impacts:** ADR-016 (pending, written from this entry); `docs/API_SPEC.md`
  (`status.quarantinedSince`, `status.quarantineAttempts`, the new `Forsaken` reasons); LR-042 (closes
  its "Known gap, NOT fixed here"); LR-015 (Rule L is the re-bootstrap, unchanged); LR-008 / LR-011 /
  LR-013 (Rule D's gate chain is the captor's healing path, unchanged — and unverified in this role).

### Wiring half (milestone 3) — desired replicas must BE zero, not be set to zero

The decision layer above computes `ScaleToZero` and nothing consumed it. Consuming it turns out to be
constrained in a way worth recording, because the obvious implementation produces a failure mode
strictly worse than the churn LR-042 removed.

- **Why out-of-band scaling cannot work — SSA with `ForceOwnership`.** Both sentinel StatefulSets are
  applied through `LittleRedReconciler.apply`, which is a server-side apply carrying
  `client.FieldOwner(fieldManager)` **and `client.ForceOwnership`**. So whatever `.Spec.Replicas` the
  *build* function computes is authoritative on every pass, unconditionally: scaling the live object
  (a `Scale` subresource write, or a patch from the healing step) is force-overwritten by the next
  reconcile. And the two applies happen EARLY — `reconcileRedisStatefulSetSentinel` /
  `reconcileSentinelStatefulSet` run in `reconcileSentinel` well before `reconcileSentinelCluster`,
  where the verdict lives. Deciding late and acting out-of-band therefore yields a **0→3→0 flap every
  pass**: steps 6/7 force 3 back, step 12 takes it away again, and in between pods are genuinely
  scheduled, come up, and rejoin the captor's quorum — re-polluting the neighbour the quarantine
  exists to protect. The requirement is consequently stronger than "the operator scales it down":
  **zero must be the desired state at build time.**
- **Shape chosen: decide the ARMED quarantine early, from status alone.** New pure
  `sentinelDesiredReplicas(lr, now) (redis, sentinel int32)` runs before either apply and passes
  `planQuarantine` **no capture verdict at all** — only `status.quarantinedSince`,
  `status.quarantineAttempts` and `Dangerous`. That is not a shortcut around the planner, it is the
  planner's documented pre-gather contract (*"an armed quarantine is decided FIRST and without
  reference to the verdict"*, pinned by the existing table row `no verdict this pass (pre-gather)`),
  and it exists because the verdict provably self-clears once the pods are gone. **Arming stays where
  it was**, after the gather, where the verdict and the data-risk clauses live. Both builders now take
  the count as a parameter rather than hardcoding `SentinelRedisReplicas` / `int32(3)`, mirroring how
  the cluster builder takes its shard index: a builder renders a decision, it does not make one.
- **The rejected alternative was hoisting the gather** above the StatefulSet applies so the full
  verdict is available there. It needs no pre-gather contract and avoids a second decision point, but
  it reorders the sentinel reconcile flow — the exact flow whose ordering is load-bearing in LR-013,
  LR-015, LR-024, LR-040 (*"Rule 0 runs before Rule A"*) and LR-041, and whose gather is deliberately
  taken **after** the ConfigMaps/Services/StatefulSets exist and behind the `BootstrapRequired`
  early-out. LR-038's lesson also applies directly: moving or widening the gather changes what every
  rule sees at once. Since the planner already supports the pre-gather decision by design, the reorder
  buys nothing that is needed and risks something that is.
- **What the ordering costs, and why it is not a flap.** Arming happens after the applies, so the
  *arming* pass still applies 3 and the pods go away on the NEXT pass; the release pass symmetrically
  applies 0 one last time and the pods return on the next. Both directions are monotone — 3→3→0 and
  0→0→3 — never 0→3→0, and while a quarantine is armed **every** pass computes 0 before touching the
  API server. One steady interval (≤30s) of latency on each edge, on a 120s settle.
- **`DataUnverified` rekeyed onto kubelet readiness** — the correction milestone 2 deferred here
  because it needs the pod list. Keyed on gather reachability, a permanently crash-looping or
  blackholing pod is "unverified" forever, so it vetoes the quarantine indefinitely and keeps the
  CAPTOR dirty for exactly as long: the one pod that can never answer would block the one action that
  helps the healthy neighbour. LR-023 settled which signal to use for this judgement — the kubelet's
  local readiness probe is authoritative and blackhole-proof where the operator's remote dial can be
  fooled (LR-017), and *"in a pure in-memory cluster a not-Ready redis holds no data, so deleting it
  loses nothing"*. So `quarantineDataRisk` now takes a `map[podName]redisContainerReady` built from the
  same pod list the gather is built from, and only an **unreachable pod the kubelet still calls Ready**
  is unverified. A pod absent from the map is treated like a Ready one: unknown readiness is not
  evidence of emptiness. The seam stays pure — readiness is passed in, no pod is read inside it.
- **Live-safety properties, each pinned by a test rather than argued.** (1) A **fresh** instance cannot
  read as quarantined: with `status.quarantinedSince` nil *or* zero-valued the planner's armed branch is
  not taken and nothing else in the pre-gather input can produce `ScaleToZero` (`Captured` is false).
  (2) A quarantine marker cannot scale down a **non-sentinel** instance — the mode gate is checked in
  `sentinelDesiredReplicas` itself rather than resting on these builders happening to be sentinel-only
  callers, so a mode change or a hand-edited status cannot take a cluster/failover/standalone
  instance's pods away. (3) The scale-down **cannot outlive the feature**: it is a function of
  `status.quarantinedSince`, so clearing that field (with `quarantineAttempts`) returns replicas to
  normal on the next pass — which is also the manual release for a latched instance, and is now stated
  in `docs/API_SPEC.md`. Clearing only the `Forsaken` condition does *not* release it; the marker is
  what holds the state, by design.
- **No `allPodsReady`-style gate was added, deliberately.** `reconcileSentinelCluster` runs
  unconditionally, which is exactly why the release decision is still reachable with zero pods. Gating
  it on pods would strand every quarantined instance permanently — the LR-018/LR-023 "repair step that
  can never fire" trap.
- **Two consequences of 0 replicas being reachable at all, both handled where they surface.**
  `updateSentinelStatus` derives `Status.Replicas.Total` as `Redis.Total - 1`, which would report `-1`;
  it is now floored at 0. And `allReady` requires `Redis.Ready > 0`, so a quarantined instance can
  never report `Running` — which matters more than it looks: `clearForsaken` resets the attempt counter
  only on `Phase == Running`, so the counter cannot be reset by the quarantine it is counting, and the
  latch still bites.
- **Also fixed, pre-existing (found by milestone 2, in the way of observing this one):**
  `setForsaken`/`clearForsaken` wrote the `Forsaken` condition onto a freshly-fetched `latest` object
  and did not mirror it into the in-memory `littleRed.Status.Conditions`. A later
  `updateSentinelStatus` in the same pass reads that condition (to pick the steady requeue interval)
  and writes the whole status back from the stale in-memory object. Self-correcting on the next pass,
  and LR-042 shipped this shape — but it makes the state a human is being asked to act on wrong for one
  interval, and this condition is now load-bearing for exactly that human. Both functions now mirror
  the persisted condition list back after a successful update.
- **Tests:** `internal/controller/quarantine_wiring_test.go`, red-first.
  `TestQuarantinedInstanceNeverGetsItsPodsPutBackByTheBuilders` is the **flap guard** and is written as
  a *sequence* rather than a state, because the failure mode is an interleaving that no single-pass
  assertion can see: it walks fresh → armed → settling → latched → released and asserts what the two
  **builders** stamp on a pass where no gather has happened. Observed **RED on 3 of 5 rows** against
  the pre-wiring builders — `redis StatefulSet .spec.replicas = 3, want 0` and
  `sentinel StatefulSet .spec.replicas = 3, want 0` for the armed, settling and latched rows — i.e. red
  on precisely the passes that would have re-created the pods. `TestFreshInstanceIsNeverReadAsQuarantined`
  and `TestQuarantineNeverScalesDownANonSentinelInstance` are green from birth (they assert an absence),
  so their teeth were shown with an **always-zero mutant**, which failed all five of their rows. In
  `TestQuarantineDataRisk`, the readiness rekey went **RED on exactly one new row** —
  `unverified = true, want false` for *"a pod we cannot dial and whose redis is NOT Ready is provably
  empty"* — against the reachability-keyed body, with the Ready-but-unreachable and
  kubelet-has-no-view rows green throughout, which is what makes that single red attributable.
- **Still not verified live, and M4a's job:** that a real quarantine actually scales both StatefulSets
  to 0 and holds there without a flap; that the captor's Rule D then prunes the departed replicas (the
  load-bearing inference of the whole change); that release + Rule L re-bootstraps the victim empty;
  and the arming/release edge latency on a real steady interval. No cluster work was done in this
  milestone — the operator image tag derives from the git hash, so live validation needs commits.

  **Answered by the M4a live run below: all four hold. The load-bearing inference — the captor heals
  itself through Rule D — is confirmed, twice, and faster than the design assumed.**
- **Regresses (wiring half):** For any instance that is not quarantined, `sentinelDesiredReplicas`
  returns exactly the constants the two builders hardcoded before (3 and 3), so the applied
  StatefulSets are byte-identical and no rollout is triggered by this change. The only new behaviour is
  reachable through `status.quarantinedSince`, which only the quarantine sets, only in sentinel mode.
  The readiness rekey strictly *relaxes* one refusal — an unreachable pod whose redis the kubelet
  reports not-Ready no longer blocks — and relaxes it toward the action, so it is the one clause where
  the conservative direction was deliberately traded for LR-023's stronger evidence. Cluster, failover
  and standalone paths are untouched; the healing chain is untouched.

### Live verification (milestone 4a) — t3e, 2026-08-22, operator `01e2df3`

Two throwaway sentinel instances in one namespace sharing `masterName: lr044.shared`, auth
disabled, same image, operator **running** throughout (unlike the LR-039 isolation specs, which
must pause it — see LR-041's note). A hello advertising the captor's live master at a derived
epoch was PUBLISHed into **all three** of the victim's Sentinels, because `planForsaken` clause 2
requires unanimity among reachable monitoring Sentinels and a 1-of-3 injection reads as a
transition, not a verdict. Every `PUBLISH` answered `1`. Two full cycles were run.

**The capture reproduced the field incident exactly, including the part that makes the data
clauses necessary.** Within ~6s all three victim Sentinels monitored `10.233.192.152` (the
captor's master) at the injected epoch, `flags: master`; all three victim Redis pods became
**link-`up` replicas of the foreign master** and their own 10 keys were **flushed** and replaced
by the captor's 100. So the live shape is the *silent* case the hardening was written for —
`Keys > 0` on every victim pod, all of it the captor's dataset — and the literal "all reachable
pods hold 0 keys" formulation the entry warns about would indeed have been inert. `atRisk` was
false for the right reason, and the quarantine fired.

**1. The verdict fires — CONFIRMED.** `Forsaken` reached `True/Quarantined` with
`status.quarantinedSince` set and `quarantineAttempts: 1`, and the operator logged the transition
**once**, not per reconcile. Both cycles identical.

**2. The wiring takes effect and holds — CONFIRMED, no flap.** Both StatefulSets went to
`.spec.replicas: 0` in the same pass and stayed. Sampled at **1s** across both edges: cycle 2's
sampler ran the whole lifecycle and reads `61 × 3, then 104 × 0, then 79 × 3` — one transition each
way, at a single sample boundary (`23:22:22 → 23:22:23` arming, `23:24:19 → 23:24:20` release), with
no intermediate and no returning value; cycle 1's covers the release edge the same way
(`33 × 0, then 83 × 3`). The
predicted 0→3→0 oscillation was never observed, and the *monotone* ordering the wiring half
predicted (arming pass still applies 3, pods leave on the next) is what happened.

**3. THE LOAD-BEARING ONE — the captor heals itself: CONFIRMED, and it is Rule D.** Not
inferred from counts alone; the operator said so, on the captor, 2-4s after the victim's pods
left:

    Ghost node detected in Sentinel topology  ip=10.233.192.74 flags=s_down,slave sentinel=captor-sentinel-0
    Issuing SENTINEL RESET to clear ghost nodes from topology  master=lr044.shared reachableRedis=3

`reachableRedis=3` is LR-013's wholeness gate passing against the **captor's own** expected pod
count, exactly as the inference required. Sentinel counts, cycle 1: `num-slaves 2 → 5`,
`num-other-sentinels 2 → 5` on capture; then `5 → 0 → 2` and `5 → 2` within ~12s of the pods
leaving (the 0 is the whole-list RESET, repopulating from the master's `INFO`). Cycle 2 reproduced
it in ~5s. **The captor's own 100 keys were intact on all three of its pods at every check**, and
`lrctl verify captor` went `FAIL` (`reports 5 replicas; 2 were deployed`) → `[OK] No foreign
Sentinel contact observed`.

Two corrections to the settle's derivation fall out, both in the safe direction. The premise was
"the captor is `Running`, so it reconciles on the steady 30s interval, so allow ~4 passes". In
practice the captor **briefly leaves `Running`** when its Sentinel-known replica count collapses
(`reasons: Sentinel knows 0/2 replicas as healthy`), so it is polled *fast* and heals in seconds.
120s is therefore generous rather than tight — no change proposed, but the number rests on a
premise that does not hold, and the real bound is the ghost becoming `s_down`
(`down-after-milliseconds`), not the requeue cadence.

**4. The release re-bootstraps the victim empty — CONFIRMED, and the mechanism is Rule L.** Named
in the CR, not guessed: condition `LeaderlessRecovery=False/Reseeded`, *"Leaderless recovery: no
data present, seeded victim-redis-0 as master"*, preceded by `Leaderless bootstrap deadlock
suspected; starting cooldown` and eight `persists; waiting` passes. **Not** the Rule 0 / LR-008
interception LR-017 recorded as a hazard for this tier: the victim's Sentinels come back genuinely
bare and there is no master anywhere to re-register them onto, so nothing can race Rule L. Rule A
did not block it either — the scale-down's pods were fully gone (`pods=0`) long before the release,
so `anyTerminating` was false on every pass of the recovery. Victim returned `Running`/`Ready=True`
with `keys:0` on all three pods, and `quarantineAttempts` dropped back to 0 on `Phase == Running`
exactly as designed.

**Timings (cycle 1 → cycle 2), one steady interval of granularity on the first edge:**

| edge | cycle 1 | cycle 2 |
|---|---|---|
| PUBLISH → all 3 Sentinels captured | ~4s | ~6s |
| capture → first `Captured` verdict (`forsakenSince`) | 30s | 31s |
| verdict → armed (`forsakenCooldown`) | 31s | 31s |
| armed → both `.spec.replicas: 0` | same pass (<1s) | same pass (<1s) |
| replicas 0 → pods gone | ~7s | ~4s |
| pods gone → captor's Sentinels clean | **~12s** | **~5s** |
| armed → release (`quarantineSettlePeriod`) | 122s | 120s |
| release → victim has a master (Rule L) | ~39s | ~38s |
| release → `Running` | ~48s | ~56s |
| **capture → victim serving again** | **~3m51s** | **~3m58s** |

The 30s from capture to the first verdict is not latency in the verdict — the victim was `Running`
when it was captured, so it was on the **steady** interval and the capture landed just after a
pass. It is the honest cost of a capture arriving between two steady reconciles.

**One defect found, NOT fixed here (it does not block the verification, and this milestone's remit
was to observe):** LR-042's third promised effect — *"re-examines it at the steady interval instead
of the fast one"* — **is not in effect for sentinel mode, the only mode that can be forsaken.** The
`Forsaken`-aware interval choice lives in `updateStatus` (littlered_controller.go, the not-`Running`
branch that switches `fast → steady` when the condition is true), but sentinel mode returns through
**`updateSentinelStatus`**, whose not-`Running` branch returns `fast` unconditionally with no such
check. Measured while quarantined and `Forsaken=True`: **31 reconciles in 114s (~3.7s apart)**,
i.e. the churn LR-042 set out to remove is still there. Two consequences: the log-once-per-transition
half of LR-042 *is* working (one line per cycle, verified), so the churn is cheap rather than noisy;
and this entry's accepted consequence *"the release lands up to ~150s after arming rather than 120s,
because a forsaken instance is polled at the steady 30s interval"* is **wrong in its reasoning** and
accidentally right in its conclusion — the release landed at 120-122s precisely *because* the
polling is fast. Fixing the cadence would make that 150s real. Tracked for the next change.

**Not exercised live, so still unverified:** the `Latched` phase (`Attempts >= limit`) — the
counter resets on `Phase == Running` and both cycles recovered, so a second attempt was never
reached; the `Dangerous` limit of 1 (a shared **`mymaster`** with auth off would latch on the first
quarantine, which is also why this run deliberately used a non-legacy shared name — a latched
instance never releases and links 3-4 would have been unobservable); `HoldDataPresent` /
`HoldDataUnknown` refusing a real capture (both were false throughout, correctly); and a *partial*
capture (1 of 3 Sentinels), where the expected behaviour is no verdict plus the LR-008 correction
healing it — the shape LR-041 observed sub-second.

### First full-suite run: one spec flake, and what it says about the operator (t3e, 2026-08-23)

The tier-1 full-cycle spec **failed on its first full-suite run** — and the failure is a **test**
defect, on the *last* assertion in the tier, after every load-bearing step had already passed
(the captor's prune at 15:01:04, Rule L's reseed at 15:02:32). Recorded because the investigation
killed five plausible causes with evidence and the one that survived is a property of the
operator worth knowing.

**What failed:** a bare `Expect(quarantineAttempts(victim)).To(BeEmpty())` — *"the attempt counter
must be reset once the instance is Running again"* — read `1`.

**Why, and it is by construction:** `clearForsaken` gates the reset on
`lr.Status.Phase == PhaseRunning`, i.e. the phase **persisted by the previous pass**. It is called
from `reconcileSentinelCluster` (`littlered_controller.go`:708) while the phase itself is written at
the tail of the same pass by `updateSentinelStatus` (:733). **So the pass that first reports
`Running` structurally cannot also clear the counter; the next pass does.** Measured: phase
`Running` at 13:02:31Z, counter still `1` when the spec read it 0.9s later, cleared by the pass at
13:02:33Z. A bare assertion was racing a ~2s window sampled by a 5s poll — roughly a 40% failure
rate per run, so M4b's single green was ~60% luck rather than evidence. Fixed by making it an
`Eventually` bounded at 90s (one steady interval, LR-045, plus margin); the assertion's intent is
unchanged and it still fails if the counter never clears.

**Not a product defect, and the reasoning matters:** the lag is intentional, bounded by the next
watch event or at worst one steady interval, and touches only a monitoring-surface field. It cannot
re-arm a quarantine, because a quarantined instance can never report `Running` (the `Redis.Ready > 0`
clause), so the latch still bites.

**What the run positively confirmed**, on the first full-suite attempt and with no favourable
ordering: every edge landed inside the previously measured envelope — injection→quorum captured
1.2s, capture→`Quarantined` 41s, verdict→both StatefulSets at 0 29.5s, armed→release 133s,
release→`Running` 46s, **capture→serving 3m40s** against M4a's 3m51s/3m58s and M4b's 3m41s. Suite
load cost nothing measurable. Five hypotheses were killed on evidence: the operator was up for the
whole window (image `f5d0e98`, 0 restarts, continuous logging), no recycled-IP stall, no leftover
state, and `f5d0e98` touches cluster-mode files only.

**Diagnosability gap this exposed:** `clearForsaken` logs **nothing** when it clears, so the
clearing pass had to be *inferred* from a reconcile timestamp plus the CR's final state rather than
read. A state transition that emits no line is a transition someone will have to re-derive next
time. Tracked, not fixed here (a test-only fix keeps the suite re-runnable without rebuilding the
image).

### Committed coverage (milestone 4b) — `test/e2e/sentinel_quarantine_test.go`, t3e, 2026-08-23

M4a proved the behaviour by hand; this milestone makes it repeatable. New Describe
**`Sentinel Forsaken-Gated Quarantine`** (`Label("sentinel")`, three tiers, all three observed
**green in one run on t3e against operator `47d9482`**: `Ran 3 of 122 Specs in 844.905 seconds
… 3 Passed | 0 Failed`). It is also the first committed coverage of the **LR-042 verdict itself**,
which shipped with none and had never executed inside the operator under test — the only other
place a capture is staged (the LR-039 isolation tier) deliberately *pauses* the operator, which is
exactly why the verdict had no coverage.

- **Tier 1 `Full cycle`** — the regression guard: capture → `Forsaken=True/Quarantined` +
  `quarantineAttempts: 1` → both StatefulSets at `.spec.replicas: 0` → held 60s at 2s sampling (the
  flap guard, asserted as a *sequence* for the same reason the unit flap guard is) → pods gone →
  **the captor's Sentinels back to `num-slaves: 2` / `num-other-sentinels: 2`** → release → Rule L
  re-bootstraps the victim empty (`Running`, a master of its own, `DBSIZE 0` on all three, no pod
  monitoring the foreign master) with the captor's own keys readable on all three captor pods at the
  end. The captor-side assertion carries a **positive control**: the polluted counts (`5`/`5`) are
  asserted *before* the healed ones, so "the captor reports 2/2" cannot pass on a capture that never
  touched the captor.
- **Tier 2 `Refusal when a victim pod holds data the captor does not have`** — `HoldDataPresent`:
  reason `QuarantineRefusedDataPresent`, both StatefulSets **stay at 3** and no `quarantinedSince`
  is armed, held 90s.
- **Tier 4 `Latched after the attempt budget is spent`** — the `Dangerous` limit of 1 (auth off +
  effective name `mymaster`) makes the latch deterministic in one cycle: reason `QuarantineLatched`,
  `quarantineAttempts: 1`, and `Consistently` **200s** at 0 replicas with the marker intact — the
  whole claim being a non-event, the window has to outlast the 120s timer that would have produced
  the event.

**Two staging findings, both of which had made a first draft of these specs unsound:**

1. **The three Sentinels are not independent, so "not yet captured" cannot be asserted per pod.**
   Sentinel propagates a higher-epoch config to its peers in its own hellos, so by the time the
   second or third injection is issued that Sentinel may already have converged. Observed exactly
   that: `q-hdp-victim-…-sentinel-2 already monitors the foreign master before the injection`. The
   helper now asserts the precondition **once, over all three, before any injection**, then skips a
   peer that has already converged, and requires ≥1 accepted `PUBLISH` (reply `1`) so the payload's
   positive control stays load-bearing. Worth keeping: injecting all three is still right for
   *clause 2*, but the reason a 1-of-3 injection reads as a transition is the **operator's** LR-008
   correction, not Sentinel's own inertia — gossip alone spreads it.
2. **A data-risk state must be staged BEFORE the capture, not after it.** The first draft broke a
   victim pod's replication link once the capture had landed and lost the race outright: the operator
   had already armed the quarantine and the pod was **gone** (`pods "…-redis-1" not found`) before the
   precondition could be read. `forsakenCooldown` is 30s and staging a capture takes longer than
   that. The mechanism is now pre-armed and is a **bogus `masterauth`** rather than a
   `REPLICAOF <blackhole>`: `REPLICAOF NO ONE` would make the pod `role:master` and dissolve
   `planForsaken` clause 4 entirely; a blackhole address disagrees with what the victim's Sentinels
   believe and Sentinel's own `+fix-slave-config` repoints it back within ~`failoverTimeout`, after
   which it full-resyncs and the keys are the captor's again — the refusal would be a race. A wrong
   `masterauth` against a password-less master keeps `master_host` pointing at the foreign master (so
   Sentinel sees nothing to fix) while every handshake fails on AUTH forever, and the dataset is
   retained because a flush only happens on a **successful** resync. So the pod holds the *victim's
   own* keys — genuinely the only copy in existence, which is what the clause exists to protect —
   and the clause is true on the very first gather that sees the capture, so the quarantine is never
   armed at all.

**Measured on t3e (one run, for judging the timeouts):** injection → all 3 Sentinels captured
**1.3s**; → all victim pods following **5s**; capture → `Quarantined` observed **≤45s** (the arming
edge is still *fast*-polled, because the condition reads `False/CaptureSuspected` until
`forsakenCooldown` elapses); verdict → both StatefulSets at 0 **≤29s** (one steady interval — LR-045
made this real, as it predicted); armed → release **~120-150s**; release → victim `Running` **~46s**;
**capture → victim serving again ~3m41s**, in line with M4a's 3m51s/3m58s. Tier 4: capture → `Latched`
**88s** (cooldown + arm + one steady pass). Every `Eventually` carries roughly one extra steady
interval per edge on top of these, and says so.

**Green from birth, disclosed as such.** These specs assert behaviour that already shipped and was
already verified live in M4a, so no tier could go red for the right reason against `47d9482` — and
the honest red was **not obtainable**: building the pre-LR-042 operator (`e26510b`) and deploying it
to observe tier 1 fail at the verdict assertion was attempted and blocked by the environment's
permission policy before the deploy. What the tiers have instead of a red is the two intermediate
positive controls above (the captor's `5`/`5` pollution before its `2`/`2` heal; the `PUBLISH` reply)
plus tier 2's precondition **re-asserted inside its own `Consistently`**, so a green cannot be earned
by the staged state quietly decaying into "link up, captor's copy, safe to discard". The two staging
findings are themselves evidence the specs were run against reality rather than against their own
assumptions.

**Still uncovered — `HoldDataUnknown` / `QuarantineRefusedDataUnknown`.** Staging it needs a victim
Redis pod that is **Ready per the kubelet** while being **unreachable from the operator**, which is
the whole content of the clause (LR-023's blackhole-proof signal versus LR-017's blackhole). The
kubelet's local exec probe must keep passing while operator→pod traffic is dropped, i.e. a
traffic-shaping capability this suite does not have. **Deferred by decision to a `feat/e2e-harness`
branch** rather than pre-built here; a pointer comment sits where the tier would go, and the
decision matrix stays covered by `TestQuarantineDataRisk` / `TestPlanQuarantine`. Also still
uncovered: a *partial* capture (1 of 3 Sentinels) producing no verdict.

## [LR-045] Forsaken's Steady-Cadence Requeue Was Inert for Sentinel Mode — the Mode That Can Be Forsaken
- **Date:** 2026-08-22
- **Commit:** (pending)
- **Problem — two divergent status/requeue paths, and the generic one was not enough.** LR-042
  gave a captured-and-declined sentinel instance a terminal `Forsaken` verdict and promised three
  effects while it holds: stop healing it, log once per transition, and re-examine it at the
  **steady** requeue interval instead of the fast one. The interval switch was written into
  `updateStatus` (`internal/controller/littlered_controller.go`, its not-`PhaseRunning` branch): it
  picks `steady` over `fast` when `meta.IsStatusConditionTrue(..., ConditionForsaken)`. But
  `ConditionForsaken` is set exclusively in `reconcileSentinelCluster`, i.e. **only sentinel mode can
  ever be forsaken** — and sentinel-mode reconciles do not return through `updateStatus` at all. They
  return through the separate `updateSentinelStatus`, whose not-`Running` branch read, verbatim:
  `if latest.Status.Phase != littleredv1alpha1.PhaseRunning { return ctrl.Result{RequeueAfter: fast}, nil }`
  — no `Forsaken` check, ever. So the one mode this verdict exists for was polled at the fast (2s)
  interval forever, indefinitely, exactly the churn LR-042 was written to remove.
- **Measured (t3e, LR-044 milestone M4a live run, 2026-08-22):** while `Forsaken=True` and
  quarantined, **31 reconciles in 114s (~3.7s apart)**. LR-042's other two effects held throughout —
  no healing rule fired, and exactly one log line was written per transition — so this was wasted
  reconcile/log-write churn, not a correctness defect and not the log spam LR-042 named. Found and
  recorded by LR-044 as a known-but-deferred gap during its own verification run; this entry is the
  fix.
- **This is a rule-11 cross-mode-parity miss of the same shape as LR-041.** LR-041 was a required
  value (the Sentinel master name) that compiled fine as a construction-state field the sentinel
  construction site forgot to set — the compiler asked nothing, because an omitted string is a
  plausible zero value, not an error. This is the same class one level up: a required *behaviour*
  (honour `Forsaken` when picking the requeue interval) was threaded through the path the reviewer's
  eye landed on (`updateStatus`, the generic status function) and never carried to the path that
  actually executes for the only mode the behaviour applies to. Neither the compiler nor a
  same-file read had any way to catch it — the two functions live ~800 lines apart in the same file,
  under similar but not identical not-`Running` requeue blocks.
- **Fix — one pure decision, shared by both call sites.** New `requeueAfterNotRunning(phase,
  conditions, fast, steady) time.Duration` in `littlered_controller.go`: `Running` → `steady`;
  otherwise `steady` iff `Forsaken=True`, else `fast`. `updateStatus` and `updateSentinelStatus` both
  now call it instead of each inlining the condition check. A shared helper was chosen over
  duplicating the predicate with a cross-reference comment because a duplicated predicate is
  literally how this defect happened — `updateStatus` had it, `updateSentinelStatus` didn't, and
  nothing forced the second copy to exist or to match. The helper is written against the generic
  `(phase, conditions)` shape rather than sentinel-specific types, so it is directly reusable and
  **safe by construction** for any future mode that might one day set `Forsaken` — a parity audit for
  the next mode reduces to "does it call the shared helper", not "does it remember the check".
- **Cross-mode parity audit of the sibling status/requeue paths (CLAUDE.md §7 rule 11):**

  | path | verdict | why |
  |---|---|---|
  | `updateStatus` (standalone; also the generic function) | fixed, wired through the helper | already had the check; now shares it instead of inlining it |
  | `updateSentinelStatus` (sentinel) | **fixed — the reported defect** | the only mode `ConditionForsaken` is ever set for; was requeuing `fast` unconditionally |
  | `updateClusterStatus` (`cluster_reconcile.go`, ~:980-984) | correct-by-vacuity, not touched | `ConditionForsaken` is never set outside `reconcileSentinelCluster`, so today this path can never see it true; same `fast`/`steady`-on-`Running` shape as the old sentinel code, unowned by this change (outside `littlered_controller.go`) |
  | `updateFailoverStatus` (`failover_reconcile.go`, ~:975-983) | correct-by-vacuity, not touched | same reasoning — `Forsaken` is a sentinel-only verdict today; unowned by this change |

  Neither cluster nor failover mode has the bug today because neither can reach the state that
  triggers it; both would silently inherit it the moment (if ever) `Forsaken` — or an equivalent
  terminal verdict — becomes settable outside sentinel mode, since they inline the same
  fast-unless-`Running` shape `updateSentinelStatus` used to. They are not touched here (out of this
  change's owned files), but the shared helper existing in `littlered_controller.go` means wiring
  them through it later is a one-line change per site rather than a rediscovery of the predicate.
- **Corrects a wrong explanation in LR-044.** LR-044 recorded an accepted consequence — *"the
  release lands up to ~150s after arming rather than 120s, because a forsaken instance is polled at
  the steady 30s interval"* — whose number was right and whose reasoning was not: polling was fast
  (this defect), which is exactly why M4a measured the release landing at 120-122s, not ~150s. See
  the `⚠ Corrected by LR-045` marker added to that entry. This fix makes the steady interval real for
  sentinel mode, so the ~150s figure becomes the true consequence going forward, for the reason
  LR-044 originally gave.
- **Regresses:** None. The non-forsaken not-`Running` fast cadence — the LR-042 "do not trade a
  global invariant for a specific defect that can be named" invariant — is unchanged and covered by
  `TestRequeueAfterNotRunning`'s `not running, not forsaken -> fast` and `forsaken condition
  explicitly false -> fast` rows. `Running` still always requeues at `steady` regardless of
  `Forsaken`, matching LR-042's design (the verdict only ever affects the not-`Running` branch).
- **Tests:** `internal/controller/requeue_interval_test.go`, red-first. `TestRequeueAfterNotRunning`
  (4 rows) was authored against a stub mirroring the actual pre-fix `updateSentinelStatus` behaviour
  (`Running` → `steady`, otherwise unconditionally `fast`) and observed **RED on 1 of 4** —
  `requeueAfterNotRunning(Initializing, forsaken-conditions) = 2s, want 30s` — the other three rows
  (not-running-not-forsaken, not-running-forsaken-explicitly-false, running) passed vacuously against
  the stub, which is expected: they pin behaviour the stub already had right, and only the forsaken
  row exercises the fix. Implementing the `Forsaken` check turned that row green with no change to
  the other three.
- **Impacts:** LR-042 (corrects its third promised effect for the only mode it applies to); LR-044
  (corrects one accepted-consequence sentence's reasoning, not its number).
