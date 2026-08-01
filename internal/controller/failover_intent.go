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
	"sort"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// ============================================================================
// Failover-mode intent model — pure decision seams (ADR-011 §3/§5/§6).
//
// The operator's assignment annotations on the data pods ARE the intent
// record: nothing load-bearing is persisted in status (ADR-006 discipline).
// These functions re-derive the intent, the live master, the settledness of
// the latest transition, and the re-authorization stamps from the pod list
// and the gathered ground truth — no I/O, so every branch is table-testable.
// ============================================================================

// failoverPodView is the per-pod input to the pure failover-mode decisions,
// pre-resolved by the caller from the K8s pod list and the gather result.
type failoverPodView struct {
	name        string
	ip          string // pod IP; "" while pending
	terminating bool   // deletionTimestamp set
	ready       bool   // redis container Ready per the kubelet's local probe
	restarted   bool   // redis container restartCount > 0
	reachable   bool   // the operator's gather reached redis-server on this pod

	// The operator-stamped assignment (ADR-011 §3), parsed from annotations.
	hasAssignment    bool
	assignedRole     string
	assignedMasterIP string
	epoch            int64
}

// failoverIntent is the operator's current intent, re-derived from the pods'
// assignment annotations (never from status).
type failoverIntent struct {
	// masterName/masterIP identify the INTENDED master: the pod whose
	// assigned-role is master at the highest assignment epoch. Empty when no
	// pod carries a master assignment (fresh instance, or the annotations died
	// with a replaced pod).
	masterName string
	masterIP   string
	// maxEpoch is the highest assignment epoch stamped on ANY pod (any role).
	// An epoch bump is always maxEpoch+1, so stamps stay monotonic even when
	// replica re-authorizations have outrun the master's own epoch.
	maxEpoch int64
}

// failoverStamp is one pod-annotation assignment the operator should apply.
type failoverStamp struct {
	podName  string
	role     string
	masterIP string // empty for a master stamp
	epoch    int64
}

// resolveFailoverIntent derives the current intent from the pods' assignment
// annotations. The intended master is the pod with assigned-role=master at the
// HIGHEST epoch (a superseded ex-master that kept its stale master annotation —
// e.g. it was terminating when the new intent was stamped — loses to the newer
// epoch); ties break to the lexicographically smallest pod name for
// determinism. maxEpoch spans ALL assignments, including replicas and
// terminating pods, so a bump can never collide with a consumed epoch.
func resolveFailoverIntent(pods []failoverPodView) failoverIntent {
	var intent failoverIntent
	var masterEpoch int64
	for _, p := range pods {
		if !p.hasAssignment {
			continue
		}
		if p.epoch > intent.maxEpoch {
			intent.maxEpoch = p.epoch
		}
		if p.assignedRole != RoleMaster {
			continue
		}
		switch {
		case intent.masterName == "",
			p.epoch > masterEpoch,
			p.epoch == masterEpoch && p.name < intent.masterName:
			intent.masterName, intent.masterIP, masterEpoch = p.name, p.ip, p.epoch
		}
	}
	return intent
}

// determineFailoverLiveMaster is the failover-mode replacement for sentinel's
// DetermineRealMaster: the live master is the INTENDED master's IP iff that pod
// is reachable and reports role:master — the operator's intent is the sole
// authority (ADR-011 §2). An unintended reachable role:master (an old master
// still up during a transition, or a bare restarted pod) is a straggler for
// Rule R to repoint, never the live master. Returns "" when there is no intent
// or the intended master is not observably mastering.
func determineFailoverLiveMaster(state *redisclient.ReplicationState, intendedMasterIP string) string {
	if intendedMasterIP == "" {
		return ""
	}
	rn := state.RedisNodes[intendedMasterIP]
	if rn != nil && rn.Reachable && rn.Role == RoleMaster {
		return intendedMasterIP
	}
	return ""
}

// failoverTransitionSettled reports whether the latest stamped intent has been
// observed converged (ADR-011 §6): the intended master pod reports role:master
// AND carries the role=master K8s label. No intent at all is settled (there is
// nothing to converge). roleLabels maps pod name -> current LabelRole value.
func failoverTransitionSettled(intent failoverIntent, state *redisclient.ReplicationState, roleLabels map[string]string) bool {
	if intent.masterName == "" {
		return true
	}
	if determineFailoverLiveMaster(state, intent.masterIP) == "" {
		return false
	}
	return roleLabels[intent.masterName] == RoleMaster
}

// failoverPromotionUnsettled reports whether a NEW mastership decision (the
// planFailover unsettled gate, ADR-011 §6) must wait for an in-flight
// transition. NOTE it is deliberately NOT !failoverTransitionSettled: a
// transition blocks re-election only while its target — the intended master —
// is still ALIVE (reachable) and converging (not yet role:master, or the label
// not yet flipped). A dead/unreachable/terminating intended master never
// blocks: its transition is moot and re-election is the remedy — gating on
// bare unsettledness would deadlock exactly the crash and graceful-handover
// recoveries this mode exists for (the dead master can never again "report
// role:master"). Cascade serialization for that case is the time-based
// post-transition cooldown, not this gate.
func failoverPromotionUnsettled(intent failoverIntent, state *redisclient.ReplicationState, roleLabels map[string]string) bool {
	if intent.masterName == "" {
		return false
	}
	rn := state.RedisNodes[intent.masterIP]
	if rn == nil || !rn.Reachable {
		return false // dead target: the transition is moot, never block re-election
	}
	return !failoverTransitionSettled(intent, state, roleLabels)
}

// planFailoverReauth decides which pods need a fresh assignment stamp while a
// live master exists (the re-authorization loop, ADR-011 §3). Two cases:
//
//   - A brand-new pod (no assignment annotations — scale-up, or recreated by
//     the StatefulSet, which wipes operator-stamped annotations) is stamped
//     replica-of-the-live-master at the CURRENT maxEpoch: nothing was consumed,
//     so no bump is needed and the fresh pod honors it immediately.
//   - A parked pod (has an assignment, redis container restarted and not-Ready,
//     and unreachable to the gather — i.e. redis-server is not running, the
//     startup script is looping on an already-consumed epoch) is stamped
//     replica-of-the-live-master at maxEpoch+1, releasing it. Data-safe: a
//     not-Ready redis in a pure in-memory instance holds nothing (ADR-008).
//
// The INTENDED master pod is never stamped here — a blind master restamp is the
// ADR-001 kill-9 hazard; its recovery goes through planMasterDeath/planFailover
// (a not-Ready intended master is DeclareK8s -> promotion; after the transition
// it is re-stamped replica like any straggler). Pods without an IP, terminating
// pods, and the pod that IS the live master are skipped. Results are sorted by
// pod name for determinism.
//
// Note the parked restamp is deliberately NOT deduplicated: while kubelet
// propagation of the downward-API file lags, the same parked pod may be bumped
// again on the next reconcile. Every such stamp is a consistent
// replica-of-the-live-master assignment at a fresh epoch, so the worst case is
// a harmlessly inflated (still monotonic) epoch counter — never a wrong
// assignment. Skipping instead could park a re-crashed pod forever.
func planFailoverReauth(pods []failoverPodView, intent failoverIntent, liveMasterIP string) []failoverStamp {
	var stamps []failoverStamp
	for _, p := range pods {
		if p.ip == "" || p.terminating || p.name == intent.masterName || p.ip == liveMasterIP {
			continue
		}
		switch {
		case !p.hasAssignment:
			// Fresh pod: nothing consumed, current epoch is honored immediately.
			stamps = append(stamps, failoverStamp{
				podName: p.name, role: RoleReplica, masterIP: liveMasterIP, epoch: max(intent.maxEpoch, 1),
			})
		case !p.ready && p.restarted && !p.reachable:
			// Parked pod: its current epoch is consumed (run-marker equals it),
			// so only a strictly greater epoch releases the wait loop.
			stamps = append(stamps, failoverStamp{
				podName: p.name, role: RoleReplica, masterIP: liveMasterIP, epoch: intent.maxEpoch + 1,
			})
		}
	}
	sort.Slice(stamps, func(i, j int) bool { return stamps[i].podName < stamps[j].podName })
	return stamps
}

// planFailoverRepoints returns the IPs of reachable pods that need
// SLAVEOF <liveMaster> (the Rule R analog): any reachable node that is not the
// live master and either claims role:master (unintended — the intent/label is
// the sole authority) or follows a wrong master IP. A replica already following
// the live master with link:down is NOT repointed — that can be a transient
// handshake state, and re-issuing SLAVEOF would interrupt it (Rule R parity).
// Sorted for determinism. The caller gates execution (settled transition, no
// terminating pods — ADR-011 §6 secondary healing keeps the conservative gate).
func planFailoverRepoints(state *redisclient.ReplicationState, liveMasterIP string) []string {
	var ips []string
	for ip, rn := range state.RedisNodes {
		if !rn.Reachable || ip == liveMasterIP {
			continue
		}
		if rn.Role == RoleMaster || rn.MasterHost != liveMasterIP {
			ips = append(ips, ip)
		}
	}
	sort.Strings(ips)
	return ips
}
