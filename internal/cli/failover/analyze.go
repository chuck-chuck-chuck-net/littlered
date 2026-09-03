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

// Package failover contains the pure verification logic for failover-mode
// instances (ADR-011): intent resolution from the assignment annotations,
// the authority-master computation (intent ∩ observation), and the findings
// classification lrctl verify renders. It mirrors the operator's pure seams
// (resolveFailoverIntent / determineFailoverLiveMaster in
// internal/controller/failover_intent.go) without importing the controller;
// a drift-guard test pins the shared constants.
package failover

import (
	"fmt"
	"sort"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// Annotation and label keys stamped by the operator. These mirror the
// internal/controller constants (AnnotationAssignedRole & friends); the
// drift-guard test in analyze_test.go asserts they stay identical.
const (
	AnnotationAssignedRole     = "redis.chuck-chuck-chuck.net/assigned-role"
	AnnotationAssignedMasterIP = "redis.chuck-chuck-chuck.net/assigned-master-ip"
	AnnotationAssignmentEpoch  = "redis.chuck-chuck-chuck.net/assignment-epoch"
	LabelRole                  = "redis.chuck-chuck-chuck.net/role"
	RoleMaster                 = "master"
	RoleReplica                = "replica"
)

// PodView is the per-data-pod K8s-side input to Analyze, pre-resolved by the
// caller from the pod list (the live Redis view arrives separately via the
// gathered ReplicationState, keyed by IP).
type PodView struct {
	Name        string
	IP          string // pod IP; "" while pending
	Phase       string // pod phase (Running, Pending, ...)
	Ready       bool   // redis container Ready per the kubelet
	Restarted   bool   // redis container restartCount > 0
	Terminating bool   // deletionTimestamp set
	RoleLabel   string // current value of the role label, "" if unset

	// The operator-stamped assignment, parsed from the annotations.
	HasAssignment    bool
	AssignedRole     string
	AssignedMasterIP string
	Epoch            int64
}

// Intent is the operator's current intent re-derived from the assignment
// annotations (the failoverIntent mirror): the intended master is the pod
// with assigned-role=master at the highest epoch, ties broken to the
// lexicographically smallest pod name.
type Intent struct {
	MasterName  string
	MasterIP    string
	MasterEpoch int64
	MaxEpoch    int64
}

// Severity classifies a finding: FAIL fails verification (exit non-zero),
// WARN degrades it ([DEGRADED], exit zero) — the cluster-mode convention.
type Severity string

const (
	SeverityFail Severity = "FAIL"
	SeverityWarn Severity = "WARN"
)

// Finding is one verification finding.
type Finding struct {
	Severity Severity `json:"severity"`
	Message  string   `json:"message"`
}

// Analysis is the result of Analyze.
type Analysis struct {
	Intent Intent

	// AuthorityPod/AuthorityIP identify the authority master: the INTENDED
	// master iff it is reachable and observed role:master (intent ∩
	// observation — the determineFailoverLiveMaster mirror). Empty otherwise.
	AuthorityPod string
	AuthorityIP  string

	// Parked lists pods in the epoch-yield state: an assignment is stamped
	// but consumed (run-marker), the redis container restarted and is
	// not-Ready, and redis-server is unreachable — the startup script is
	// waiting for an operator epoch bump.
	Parked []string

	Findings []Finding
}

// Failed reports whether any finding is FAIL severity.
func (a *Analysis) Failed() bool {
	for _, f := range a.Findings {
		if f.Severity == SeverityFail {
			return true
		}
	}
	return false
}

// Degraded reports whether the analysis carries warnings but no failures.
func (a *Analysis) Degraded() bool {
	return !a.Failed() && len(a.Findings) > 0
}

// Healthy reports whether the analysis found nothing wrong.
func (a *Analysis) Healthy() bool {
	return len(a.Findings) == 0
}

// Analyze computes the failover-mode verification verdict from the K8s pod
// views and the gathered per-pod replication state. Pure: no I/O.
//
// The intent and authority computations mirror the operator's
// resolveFailoverIntent / determineFailoverLiveMaster; findings gated on the
// authority existing (straggler, wrong-follow, link-down, label agreement)
// mirror the operator's healthy-path healing, which also requires a live
// master — while a transition/detection window is in flight those states are
// expected and the single "no authority master" FAIL covers the situation.
func Analyze(pods []PodView, state *redisclient.ReplicationState) Analysis {
	a := Analysis{Intent: resolveIntent(pods)}

	anyAssignment := false
	for _, p := range pods {
		anyAssignment = anyAssignment || p.HasAssignment
	}

	a.checkAssignmentRecord(pods, anyAssignment)
	a.computeAuthority(state, anyAssignment)
	for _, p := range pods {
		a.checkPod(p, state, anyAssignment)
	}
	sort.Strings(a.Parked)
	a.checkLineage(state)

	return a
}

func (a *Analysis) fail(format string, args ...any) {
	a.Findings = append(a.Findings, Finding{SeverityFail, fmt.Sprintf(format, args...)})
}

func (a *Analysis) warn(format string, args ...any) {
	a.Findings = append(a.Findings, Finding{SeverityWarn, fmt.Sprintf(format, args...)})
}

// checkAssignmentRecord flags a missing intent record (no assignments at all,
// or no master assignment) and duplicated/stale master assignments.
func (a *Analysis) checkAssignmentRecord(pods []PodView, anyAssignment bool) {
	switch {
	case !anyAssignment:
		a.fail("no assignment annotations on any pod (instance not yet bootstrapped, or the intent record died with the pods)")
	case a.Intent.MasterName == "":
		a.fail("no master assignment on any pod (the master intent died with its pod; the operator should re-elect)")
	}

	var dupSame, dupStale []string
	for _, p := range pods {
		if !p.HasAssignment || p.AssignedRole != RoleMaster || p.Name == a.Intent.MasterName {
			continue
		}
		if p.Epoch == a.Intent.MasterEpoch {
			dupSame = append(dupSame, p.Name)
		} else if !p.Terminating {
			dupStale = append(dupStale, fmt.Sprintf("%s@%d", p.Name, p.Epoch))
		}
	}
	sort.Strings(dupSame)
	sort.Strings(dupStale)
	if len(dupSame) > 0 {
		a.fail("duplicate master assignment at epoch %d on %v (intent is ambiguous; tie-broken to %s)",
			a.Intent.MasterEpoch, dupSame, a.Intent.MasterName)
	}
	for _, d := range dupStale {
		a.warn("stale master assignment on %s (superseded by %s@%d, not yet re-stamped)",
			d, a.Intent.MasterName, a.Intent.MasterEpoch)
	}
}

// computeAuthority resolves the authority master (intent ∩ observation) and
// flags its absence.
func (a *Analysis) computeAuthority(state *redisclient.ReplicationState, anyAssignment bool) {
	if a.Intent.MasterIP != "" {
		if rn := state.RedisNodes[a.Intent.MasterIP]; rn != nil && rn.Reachable && rn.Role == RoleMaster {
			a.AuthorityPod, a.AuthorityIP = a.Intent.MasterName, a.Intent.MasterIP
			return
		}
	}
	if !anyAssignment || a.Intent.MasterName == "" {
		return // already covered by checkAssignmentRecord
	}
	reason := "intended master " + a.Intent.MasterName + " "
	switch rn := state.RedisNodes[a.Intent.MasterIP]; {
	case rn == nil || !rn.Reachable:
		reason += "is unreachable"
	default:
		reason += "reports role:" + rn.Role
	}
	a.fail("no authority master (%s; transition in flight or awaiting operator recovery)", reason)
}

// checkPod classifies one pod: parked / awaiting authorization / unreachable,
// and — against a live authority — straggler, wrong-follow, link-down, and
// role-label agreement.
func (a *Analysis) checkPod(p PodView, state *redisclient.ReplicationState, anyAssignment bool) {
	if p.Terminating || p.IP == "" {
		return
	}
	rn := state.RedisNodes[p.IP]
	reachable := rn != nil && rn.Reachable

	switch {
	case !reachable && p.HasAssignment && !p.Ready && p.Restarted:
		a.Parked = append(a.Parked, p.Name)
		a.warn("pod %s is PARKED (assignment epoch %d consumed; not-Ready, restarted, redis-server not running — waiting for an operator epoch bump)",
			p.Name, p.Epoch)
		return
	case !p.HasAssignment && anyAssignment:
		a.warn("pod %s has no assignment yet (fresh pod awaiting operator authorization)", p.Name)
		return
	case !reachable:
		a.warn("pod %s is unreachable (reduced redundancy)", p.Name)
		return
	}

	// Observation checks: only meaningful against a live authority.
	if a.AuthorityIP == "" {
		return
	}
	if p.IP != a.AuthorityIP {
		switch {
		case rn.Role == RoleMaster:
			a.fail("pod %s is a straggler: reports role:master but the authority master is %s (must be repointed)",
				p.Name, a.AuthorityPod)
		case rn.MasterHost != a.AuthorityIP:
			a.fail("replica %s follows the wrong master %s (authority master is %s at %s)",
				p.Name, rn.MasterHost, a.AuthorityPod, a.AuthorityIP)
		case rn.LinkStatus != "up":
			a.warn("replica %s follows the authority master but link:down (transient resync, reduced redundancy)", p.Name)
		}
	}
	switch {
	case p.RoleLabel == RoleMaster && p.IP != a.AuthorityIP:
		a.fail("pod %s carries the master role label but the authority master is %s (the master Service routes traffic to a non-master)",
			p.Name, a.AuthorityPod)
	case p.IP == a.AuthorityIP && p.RoleLabel != RoleMaster:
		a.fail("authority master %s does not carry the master role label (the master Service is not routing to it)", p.Name)
	}
}

// checkLineage flags data holders spanning independent replication lineages
// (the holdersDiverged mirror; replid/replid2 union — LR-024).
func (a *Analysis) checkLineage(state *redisclient.ReplicationState) {
	holders := state.DataHolders()
	if len(holders) < 2 {
		return
	}
	if _, diverged, _ := state.BestDataHolder(); diverged {
		names := make([]string, 0, len(holders))
		for _, h := range holders {
			names = append(names, h.PodName)
		}
		sort.Strings(names)
		a.fail("data holders %v span independent replication lineages (replid/replid2 disjoint) — electing any one discards writes", names)
	}
}

// resolveIntent mirrors the operator's resolveFailoverIntent: the intended
// master is the pod with assigned-role=master at the HIGHEST epoch, ties
// broken to the lexicographically smallest pod name; MaxEpoch spans all
// assignments on all pods, any role.
func resolveIntent(pods []PodView) Intent {
	var intent Intent
	for _, p := range pods {
		if !p.HasAssignment {
			continue
		}
		if p.Epoch > intent.MaxEpoch {
			intent.MaxEpoch = p.Epoch
		}
		if p.AssignedRole != RoleMaster {
			continue
		}
		switch {
		case intent.MasterName == "",
			p.Epoch > intent.MasterEpoch,
			p.Epoch == intent.MasterEpoch && p.Name < intent.MasterName:
			intent.MasterName, intent.MasterIP, intent.MasterEpoch = p.Name, p.IP, p.Epoch
		}
	}
	return intent
}
