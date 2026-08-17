/*
Copyright 2026 The littlered Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"time"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// linkStatusDown is the master_link_status value a replica reports while its
// replication link to the master is broken (INFO replication).
const linkStatusDown = "down"

// --- master-death predicate (ADR-011 §4) -----------------------------------

// masterDeathAction is what the failover-mode failure detector should do for the
// current observation of the master. It is the output of the pure predicate
// planMasterDeath, so the detection matrix can be unit-tested without any
// Kubernetes or Redis I/O.
type masterDeathAction int

const (
	// masterDeathClearMarker: the master is alive (operator-reachable). Clear any
	// masterDownSince marker and take no action.
	masterDeathClearMarker masterDeathAction = iota
	// masterDeathStartWindow: the master just became operator-unreachable; stamp
	// masterDownSince and wait out the downAfter window.
	masterDeathStartWindow
	// masterDeathWait: still operator-unreachable but the downAfter window has not
	// elapsed. Keep the marker, do nothing this pass.
	masterDeathWait
	// masterDeathHold: operator-unreachable past the window, but the declaration is
	// vetoed — a reachable replica still reports link:up (operator-side network
	// issue, LR-017) or no reachable replica exists to corroborate (the operator's
	// own dial is never sufficient evidence, ADR-008). Keep the marker, do nothing.
	masterDeathHold
	// masterDeathDeclareK8s: Kubernetes-authoritative death — the master pod is
	// gone/replaced, its redis container is not-Ready per kubelet, or the pod is
	// terminating (graceful handover, ADR-011 §7). Declare dead immediately.
	masterDeathDeclareK8s
	// masterDeathDeclareProbe: probe-evidenced, corroborated death — operator-
	// unreachable for >= downAfter AND every reachable replica reports link:down.
	masterDeathDeclareProbe
)

// masterPodView is the Kubernetes view of the current master pod, pre-resolved by
// the caller from the pod list (the pure predicate does no API reads).
type masterPodView struct {
	// present: a pod matching the recorded master (name AND IP — strict IP identity,
	// ADR-001) still exists. A deleted or replaced pod is not present.
	present bool
	// ready: the redis container is Ready per the kubelet's local probe (the
	// blackhole-proof, authoritative signal — ADR-008).
	ready bool
	// terminating: the pod carries a deletionTimestamp (rolling update, drain,
	// deliberate delete — the graceful-handover trigger, ADR-011 §7).
	terminating bool
}

// planMasterDeath is the pure master-death predicate for failover mode (ADR-011
// §4). It performs no I/O: given the K8s view of the master pod, the operator's
// probe result, the reachable replicas' master_link_status values, and the timing
// inputs, it returns what the failure detector should do. Check order:
//
//  1. Kubernetes-authoritative (checked first, acted on immediately, no window):
//     pod gone/replaced, redis container not-Ready per kubelet, or terminating
//     (§7 graceful handover — promote proactively during the grace period, so a
//     terminating master is treated as dead NOW, not after the window).
//  2. Operator-reachable -> alive, clear the marker.
//  3. Unreachable, no marker -> start the downAfter window.
//  4. Unreachable, window not elapsed -> wait (blip filtering; even unanimous
//     link:down does not shortcut the window).
//  5. Window elapsed AND >=1 reachable replica AND all of them report link:down
//     -> dead, corroborated (LR-017: replicas are the independent viewpoints).
//  6. Window elapsed otherwise -> HOLD the marker, no declaration. This covers
//     both "some replica sees link:up" (operator-side network issue) and "no
//     reachable replicas" (nothing to corroborate with — kubelet readiness is the
//     authoritative fallback for a truly dead pod).
//
// Hold-vs-clear on veto (documented design choice): the marker is HELD, not
// cleared. It records operator-observed unreachability, which is factually still
// true; clearing it would restart the window on every replica-link flap and could
// postpone a genuine death declaration indefinitely. Held, the veto lifts the
// instant every reachable replica loses its link — and ">= window unreachable +
// unanimous link:down" is then exactly the corroborated signature. The link-up
// veto gates the declaration, never the timer.
//
// replicaLinks carries the master_link_status of each REACHABLE replica following
// the master (the caller filters); unreachable replicas contribute nothing.
func planMasterDeath(
	pod masterPodView,
	masterReachable bool,
	replicaLinks []string,
	downSince *time.Time,
	now time.Time,
	downAfter time.Duration,
) masterDeathAction {
	// 1. Kubernetes-authoritative: gone/replaced, not-Ready per kubelet, or
	// terminating (graceful handover). Immediate, regardless of probe or marker.
	if !pod.present || !pod.ready || pod.terminating {
		return masterDeathDeclareK8s
	}
	// 2. Operator-reachable -> alive.
	if masterReachable {
		return masterDeathClearMarker
	}
	// 3-4. Sustained-unreachability window.
	if downSince == nil {
		return masterDeathStartWindow
	}
	if now.Sub(*downSince) < downAfter {
		return masterDeathWait
	}
	// 5. Corroboration: at least one reachable replica, and all of them link:down.
	if len(replicaLinks) > 0 {
		corroborated := true
		for _, link := range replicaLinks {
			if link != linkStatusDown {
				corroborated = false
				break
			}
		}
		if corroborated {
			return masterDeathDeclareProbe
		}
	}
	// 6. Vetoed (a replica sees link:up) or uncorroborable (no reachable replicas):
	// hold the marker, declare nothing.
	return masterDeathHold
}

// --- planFailover: the single "who should be master" table (ADR-011 §5/§6) ---

// failoverAction is what the failover-mode reconciler should do about mastership.
// It is the output of the pure decision function planFailover — the one table
// unifying bootstrap seeding, normal failover, and the deadlock matrix that
// sentinel mode needed three separate rules for (Rule L, LR-024, bootstrap).
type failoverAction int

const (
	// failoverNone: a live master exists; no promotion. Straggler repoint is
	// executed elsewhere (the existing Rule R loop), not decided here.
	failoverNone failoverAction = iota
	// failoverWait: no live master, but acting now is not allowed — an unsettled
	// prior transition, the post-transition cooldown, or no eligible bootstrap
	// candidate yet. Do nothing this pass.
	failoverWait
	// failoverSeed: no live master and no data anywhere; seed the bootstrap
	// candidate (deterministic redis-0 preference, pickBootstrapMasterIP).
	failoverSeed
	// failoverPromote: no live master, >=1 data holders, all one replication
	// lineage; promote the most-complete holder. NO opt-in — a post-failover
	// promotion chain is one lineage (LR-024 lesson).
	failoverPromote
	// failoverRefuse: holders span >=2 independent lineages and the unsafe opt-in
	// is off; refuse (electing any one discards independent writes).
	failoverRefuse
	// failoverUnsafeElect: >=2 lineages and the opt-in is on; force-elect the
	// most-complete holder, flagging the divergence loudly.
	failoverUnsafeElect
)

// failoverPlan is the decision returned by planFailover.
type failoverPlan struct {
	action   failoverAction
	masterIP string // pod to elect (seed/promote/unsafe actions only)
	diverged bool   // unsafe action: holders span multiple replication lineages
	holders  int    // number of reachable data holders (for messaging)
}

// planFailover is the pure "who should be master" decision for failover mode
// (ADR-011 §5, guards §6). It performs no I/O: given the gathered ground truth
// (only RedisNodes/ValidIPs are populated in this mode — there are no Sentinels)
// and the timing inputs, it returns what the operator should do. Decision order:
//
//  1. A live master exists (liveMasterIP != "") -> none. Stragglers are repointed
//     by the existing Rule R loop; that is execution, not a mastership decision.
//  2. Unsettled prior transition (the previous assignment epoch's intent not yet
//     observed converged) -> wait.
//  3. Within the post-transition cooldown (transitionSince + cooldown, serializing
//     cascading flips) -> wait. A nil marker means no prior transition; an elapsed
//     marker does not block.
//  4. No live master, 0 data holders -> seed the bootstrap candidate ("" -> wait).
//  5. >=1 holders, all ONE lineage (holdersDiverged over {replid, replid2} is
//     false) -> promote BestDataHolder. NO opt-in: same-lineage losers resync from
//     the winner with no independent writes lost.
//  6. Holders in >=2 lineages -> refuse unless allowUnsafe; then elect
//     BestDataHolder and flag the divergence.
//
// Deliberate contrast with sentinel-mode Rule A (§6): there is NO terminating-pods
// gate — a crash failover is exactly the moment the dead master pod is
// terminating, and its termination must never block promoting a survivor. The
// dead master simply appears in state as an unreachable node and never suppresses
// a decision.
func planFailover(
	state *redisclient.ReplicationState,
	liveMasterIP string,
	allowUnsafe bool,
	bootstrapMasterIP string,
	unsettled bool,
	transitionSince *time.Time,
	now time.Time,
	cooldown time.Duration,
) failoverPlan {
	// 1. A live master exists: nothing to decide. Straggler repoint (Rule R) and
	// resuming a half-applied transition are execution concerns, handled elsewhere.
	if liveMasterIP != "" {
		return failoverPlan{action: failoverNone}
	}
	// 2-3. Serialization gates: an unsettled prior transition, or the
	// post-transition cooldown still running. Note there is deliberately NO
	// terminating-pods gate here (contrast sentinel Rule A) — the dead master's own
	// termination must never block promoting a survivor.
	if unsettled {
		return failoverPlan{action: failoverWait}
	}
	if transitionSince != nil && now.Sub(*transitionSince) < cooldown {
		return failoverPlan{action: failoverWait}
	}

	// 4. No data anywhere: seed the deterministic bootstrap candidate.
	holders := state.DataHolders()
	if len(holders) == 0 {
		if bootstrapMasterIP == "" {
			return failoverPlan{action: failoverWait}
		}
		return failoverPlan{action: failoverSeed, masterIP: bootstrapMasterIP}
	}

	// 5-6. Data present: the safety gate is replication LINEAGE (union-find over
	// {replid, replid2}), not holder count — a post-failover promotion chain is one
	// lineage and elects with no opt-in (LR-024 lesson).
	best, diverged := state.BestDataHolder()
	if best == nil { // defensive; holders is non-empty so this cannot happen
		return failoverPlan{action: failoverWait, holders: len(holders)}
	}
	if diverged {
		if !allowUnsafe {
			return failoverPlan{action: failoverRefuse, holders: len(holders)}
		}
		return failoverPlan{action: failoverUnsafeElect, masterIP: best.IP, diverged: true, holders: len(holders)}
	}
	return failoverPlan{action: failoverPromote, masterIP: best.IP, holders: len(holders)}
}

// planFailoverFence returns the IP of the OUTGOING master that must be demoted
// so that it can no longer accept writes, or "" when there is nothing to fence.
//
// WHY (measured on t3e, 2026-08-17): a graceful master delete lost 202 of 1171
// acknowledged writes, silently — DataCorruptions 0, write availability 97.66%.
// The operator promoted a replica but never spoke to the outgoing master, which
// kept running and kept ACKing writes for its whole ~10s preStop window while an
// established client connection through the master Service stayed pinned to it.
// Demoting it makes those writes fail visibly (-READONLY) instead of vanishing:
// pillar 3.2's "errors rather than silent data loss", applied to failover.
//
// Deliberately narrow. This is the existing straggler repoint
// (planFailoverRepoints) applied to the ONE pod its caller's conservative gate
// (settled && !anyTerminating) excludes, at the one moment it matters. The
// healthy stragglers keep that gate; only the master being replaced is fenced.
//
// Nothing to fence when the outgoing master is unreachable (the crash path — and
// no dial is wasted on a dead or blackholing IP, LR-017), already demoted (so
// re-entry is idempotent), absent from the gather, or IS the pod being promoted
// (a resumed half-applied promotion — fencing it would demote the new master).
func planFailoverFence(state *redisclient.ReplicationState, outgoingIP, newMasterIP string) string {
	if outgoingIP == "" || outgoingIP == newMasterIP {
		return ""
	}
	rn := state.RedisNodes[outgoingIP]
	if rn == nil || !rn.Reachable || rn.Role != RoleMaster {
		return ""
	}
	return outgoingIP
}
