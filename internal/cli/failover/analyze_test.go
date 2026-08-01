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

package failover

import (
	"strings"
	"testing"

	controller "github.com/littlered-operator/littlered-operator/internal/controller"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// TestConstantsMatchController pins the CLI's copies of the operator-stamped
// annotation/label keys to the controller's authoritative constants, so the
// two cannot silently drift (the controller package is deliberately not
// linked into the lrctl binary; this test-only import is the guard).
func TestConstantsMatchController(t *testing.T) {
	pairs := []struct{ name, cli, op string }{
		{"AnnotationAssignedRole", AnnotationAssignedRole, controller.AnnotationAssignedRole},
		{"AnnotationAssignedMasterIP", AnnotationAssignedMasterIP, controller.AnnotationAssignedMasterIP},
		{"AnnotationAssignmentEpoch", AnnotationAssignmentEpoch, controller.AnnotationAssignmentEpoch},
		{"LabelRole", LabelRole, controller.LabelRole},
		{"RoleMaster", RoleMaster, controller.RoleMaster},
		{"RoleReplica", RoleReplica, controller.RoleReplica},
	}
	for _, p := range pairs {
		if p.cli != p.op {
			t.Errorf("%s: cli %q != controller %q", p.name, p.cli, p.op)
		}
	}
}

// node is a shorthand for building the gathered per-pod replication state.
type node struct {
	pod, ip, role, masterHost, link string
	offset, keys                    int64
	replid, replid2                 string
	unreachable                     bool
}

func mkState(nodes ...node) *redisclient.ReplicationState {
	s := redisclient.NewReplicationState()
	for _, n := range nodes {
		s.ValidIPs[n.ip] = true
		s.RedisNodes[n.ip] = &redisclient.RedisNodeState{
			PodName:    n.pod,
			IP:         n.ip,
			Role:       n.role,
			MasterHost: n.masterHost,
			LinkStatus: n.link,
			Offset:     n.offset,
			Keys:       n.keys,
			Replid:     n.replid,
			Replid2:    n.replid2,
			Reachable:  !n.unreachable,
		}
	}
	return s
}

func assigned(name, ip, role, masterIP string, epoch int64) PodView {
	return PodView{
		Name: name, IP: ip, Phase: "Running", Ready: true,
		RoleLabel:     map[bool]string{true: RoleMaster, false: RoleReplica}[role == RoleMaster],
		HasAssignment: true, AssignedRole: role, AssignedMasterIP: masterIP, Epoch: epoch,
	}
}

func hasFinding(a Analysis, sev Severity, substr string) bool {
	for _, f := range a.Findings {
		if f.Severity == sev && strings.Contains(f.Message, substr) {
			return true
		}
	}
	return false
}

func findingsStr(a Analysis) string {
	var b strings.Builder
	for _, f := range a.Findings {
		b.WriteString("[" + string(f.Severity) + "] " + f.Message + "; ")
	}
	return b.String()
}

const (
	ip0 = "10.0.0.10"
	ip1 = "10.0.0.11"
	ip2 = "10.0.0.12"

	pod0 = "r-0"
	pod1 = "r-1"
	pod2 = "r-2"
)

// healthyPods returns the canonical settled 1M+2R instance at epoch 2.
func healthyPods() []PodView {
	return []PodView{
		assigned(pod0, ip0, RoleMaster, "", 2),
		assigned(pod1, ip1, RoleReplica, ip0, 2),
		assigned(pod2, ip2, RoleReplica, ip0, 2),
	}
}

// healthyState matches healthyPods, including a promotion-chain lineage on
// one replica (replid rotated, old lineage in replid2) — which must NOT be
// flagged as divergence (the LR-024 lesson).
func healthyState() *redisclient.ReplicationState {
	return mkState(
		node{pod: pod0, ip: ip0, role: "master", offset: 100, keys: 5, replid: "AAAA"},
		node{pod: pod1, ip: ip1, role: "slave", masterHost: ip0, link: "up", offset: 100, keys: 5, replid: "AAAA"},
		node{pod: pod2, ip: ip2, role: "slave", masterHost: ip0, link: "up", offset: 90, keys: 5, replid: "BBBB", replid2: "AAAA"},
	)
}

func TestAnalyzeHealthy(t *testing.T) {
	a := Analyze(healthyPods(), healthyState())
	if a.Intent.MasterName != pod0 || a.Intent.MasterIP != ip0 {
		t.Fatalf("intent = %+v, want master r-0/%s", a.Intent, ip0)
	}
	if a.Intent.MasterEpoch != 2 || a.Intent.MaxEpoch != 2 {
		t.Errorf("epochs = %d/%d, want 2/2", a.Intent.MasterEpoch, a.Intent.MaxEpoch)
	}
	if a.AuthorityPod != pod0 || a.AuthorityIP != ip0 {
		t.Errorf("authority = %s/%s, want r-0/%s", a.AuthorityPod, a.AuthorityIP, ip0)
	}
	if !a.Healthy() {
		t.Errorf("want healthy, got findings: %s", findingsStr(a))
	}
}

func TestAnalyzeIntentHighestEpochWins(t *testing.T) {
	// r-0 kept a stale master assignment at epoch 1 (superseded); r-1 carries
	// the current master assignment at epoch 3. r-0 was already repointed.
	pods := []PodView{
		assigned(pod0, ip0, RoleMaster, "", 1),
		assigned(pod1, ip1, RoleMaster, "", 3),
		assigned(pod2, ip2, RoleReplica, ip1, 3),
	}
	pods[0].RoleLabel = RoleReplica
	state := mkState(
		node{pod: pod0, ip: ip0, role: "slave", masterHost: ip1, link: "up", keys: 5, replid: "AAAA"},
		node{pod: pod1, ip: ip1, role: "master", offset: 100, keys: 5, replid: "AAAA"},
		node{pod: pod2, ip: ip2, role: "slave", masterHost: ip1, link: "up", keys: 5, replid: "AAAA"},
	)
	a := Analyze(pods, state)
	if a.Intent.MasterName != pod1 || a.Intent.MasterEpoch != 3 {
		t.Fatalf("intent = %+v, want r-1@3", a.Intent)
	}
	if a.AuthorityPod != pod1 {
		t.Errorf("authority = %q, want r-1", a.AuthorityPod)
	}
	if !hasFinding(a, SeverityWarn, "stale master assignment") {
		t.Errorf("want WARN stale master assignment on r-0, got: %s", findingsStr(a))
	}
	if a.Failed() {
		t.Errorf("stale assignment must not FAIL, got: %s", findingsStr(a))
	}
}

func TestAnalyzeDuplicateMasterAssignmentSameEpoch(t *testing.T) {
	pods := []PodView{
		assigned(pod0, ip0, RoleMaster, "", 2),
		assigned(pod1, ip1, RoleMaster, "", 2),
	}
	state := mkState(
		node{pod: pod0, ip: ip0, role: "master", keys: 1, replid: "AAAA"},
		node{pod: pod1, ip: ip1, role: "slave", masterHost: ip0, link: "up", keys: 1, replid: "AAAA"},
	)
	a := Analyze(pods, state)
	// Deterministic tie-break: lexicographically smallest pod name.
	if a.Intent.MasterName != pod0 {
		t.Fatalf("intent = %+v, want tie-break to r-0", a.Intent)
	}
	if !hasFinding(a, SeverityFail, "duplicate master assignment") {
		t.Errorf("want FAIL duplicate master assignment, got: %s", findingsStr(a))
	}
}

func TestAnalyzeNoAssignmentsAtAll(t *testing.T) {
	pods := []PodView{
		{Name: pod0, IP: ip0, Phase: "Running"},
		{Name: pod1, IP: ip1, Phase: "Running"},
	}
	a := Analyze(pods, mkState())
	if a.Intent.MasterName != "" || a.AuthorityIP != "" {
		t.Fatalf("want empty intent/authority, got %+v / %s", a.Intent, a.AuthorityIP)
	}
	if !hasFinding(a, SeverityFail, "no assignment annotations") {
		t.Errorf("want FAIL no assignment annotations, got: %s", findingsStr(a))
	}
}

func TestAnalyzeNoMasterAssignment(t *testing.T) {
	// The master pod (and its annotations) died; survivors carry only
	// replica assignments. No intent, no authority.
	pods := []PodView{
		assigned(pod1, ip1, RoleReplica, ip0, 2),
		assigned(pod2, ip2, RoleReplica, ip0, 2),
	}
	state := mkState(
		node{pod: pod1, ip: ip1, role: "slave", masterHost: ip0, link: "down", keys: 5, replid: "AAAA"},
		node{pod: pod2, ip: ip2, role: "slave", masterHost: ip0, link: "down", keys: 5, replid: "AAAA"},
	)
	a := Analyze(pods, state)
	if a.Intent.MasterName != "" {
		t.Fatalf("intent = %+v, want none", a.Intent)
	}
	if a.Intent.MaxEpoch != 2 {
		t.Errorf("maxEpoch = %d, want 2", a.Intent.MaxEpoch)
	}
	if !hasFinding(a, SeverityFail, "no master assignment") {
		t.Errorf("want FAIL no master assignment, got: %s", findingsStr(a))
	}
}

func TestAnalyzeAuthorityRequiresReachable(t *testing.T) {
	// Intended master unreachable (crashed): no authority.
	pods := healthyPods()
	state := healthyState()
	state.RedisNodes[ip0].Reachable = false
	a := Analyze(pods, state)
	if a.AuthorityIP != "" {
		t.Fatalf("authority = %q, want none (intended master unreachable)", a.AuthorityIP)
	}
	if !hasFinding(a, SeverityFail, "no authority master") {
		t.Errorf("want FAIL no authority master, got: %s", findingsStr(a))
	}
}

func TestAnalyzeAuthorityRequiresObservedRoleMaster(t *testing.T) {
	// Intended master reachable but still role:slave (converging transition):
	// intent alone is not authority.
	pods := healthyPods()
	state := healthyState()
	state.RedisNodes[ip0].Role = "slave"
	a := Analyze(pods, state)
	if a.AuthorityIP != "" {
		t.Fatalf("authority = %q, want none (intended master not role:master)", a.AuthorityIP)
	}
	if !hasFinding(a, SeverityFail, "no authority master") {
		t.Errorf("want FAIL no authority master, got: %s", findingsStr(a))
	}
}

func TestAnalyzeStragglerMaster(t *testing.T) {
	// r-2 claims role:master although the intent (and authority) is r-0.
	pods := healthyPods()
	state := healthyState()
	state.RedisNodes[ip2].Role = "master"
	state.RedisNodes[ip2].MasterHost = ""
	state.RedisNodes[ip2].LinkStatus = ""
	a := Analyze(pods, state)
	if a.AuthorityPod != pod0 {
		t.Fatalf("authority = %q, want r-0 (straggler is never the authority)", a.AuthorityPod)
	}
	if !hasFinding(a, SeverityFail, "straggler") {
		t.Errorf("want FAIL straggler on r-2, got: %s", findingsStr(a))
	}
}

func TestAnalyzeReplicaFollowsWrongIP(t *testing.T) {
	pods := healthyPods()
	state := healthyState()
	state.RedisNodes[ip2].MasterHost = "10.9.9.9" // dead ex-master
	state.RedisNodes[ip2].LinkStatus = "down"
	a := Analyze(pods, state)
	if !hasFinding(a, SeverityFail, "wrong master") {
		t.Errorf("want FAIL follows wrong master, got: %s", findingsStr(a))
	}
}

func TestAnalyzeLinkDownIsDegradedOnly(t *testing.T) {
	// Following the authority with link:down is a transient resync: WARN.
	pods := healthyPods()
	state := healthyState()
	state.RedisNodes[ip2].LinkStatus = "down"
	a := Analyze(pods, state)
	if !hasFinding(a, SeverityWarn, "link:down") {
		t.Errorf("want WARN link:down, got: %s", findingsStr(a))
	}
	if a.Failed() {
		t.Errorf("link:down must not FAIL, got: %s", findingsStr(a))
	}
	if !a.Degraded() {
		t.Errorf("want degraded")
	}
}

func TestAnalyzeParkedPod(t *testing.T) {
	// r-2 is in the epoch-yield park: assignment stamped but consumed,
	// container restarted + not-Ready, redis-server unreachable.
	pods := healthyPods()
	pods[2].Ready = false
	pods[2].Restarted = true
	state := healthyState()
	state.RedisNodes[ip2].Reachable = false
	a := Analyze(pods, state)
	if len(a.Parked) != 1 || a.Parked[0] != pod2 {
		t.Fatalf("parked = %v, want [r-2]", a.Parked)
	}
	if !hasFinding(a, SeverityWarn, "PARKED") {
		t.Errorf("want WARN PARKED, got: %s", findingsStr(a))
	}
	if a.Failed() {
		t.Errorf("a parked pod alongside a live authority must not FAIL, got: %s", findingsStr(a))
	}
}

func TestAnalyzeUnreachableReplicaWarns(t *testing.T) {
	// Unreachable but not parked (container Ready per last kubelet probe):
	// degraded redundancy, not a failure.
	pods := healthyPods()
	state := healthyState()
	state.RedisNodes[ip1].Reachable = false
	a := Analyze(pods, state)
	if !hasFinding(a, SeverityWarn, "unreachable") {
		t.Errorf("want WARN unreachable, got: %s", findingsStr(a))
	}
	if a.Failed() {
		t.Errorf("unreachable replica must not FAIL, got: %s", findingsStr(a))
	}
}

func TestAnalyzeLabelDisagreement(t *testing.T) {
	// The role label authority is the operator: a master label on a
	// non-authority pod (traffic to a non-master!) and a missing master
	// label on the authority both FAIL.
	pods := healthyPods()
	pods[0].RoleLabel = RoleReplica // authority missing its master label
	pods[1].RoleLabel = RoleMaster  // misplaced master label
	a := Analyze(pods, healthyState())
	if !hasFinding(a, SeverityFail, "carries the master role label") {
		t.Errorf("want FAIL misplaced master label on r-1, got: %s", findingsStr(a))
	}
	if !hasFinding(a, SeverityFail, "does not carry the master role label") {
		t.Errorf("want FAIL missing master label on r-0, got: %s", findingsStr(a))
	}
}

func TestAnalyzeLineageDivergence(t *testing.T) {
	// Two data holders on genuinely independent lineages (no replid/replid2
	// connection): electing either discards writes — FAIL loudly.
	pods := []PodView{
		assigned(pod1, ip1, RoleReplica, ip0, 2),
		assigned(pod2, ip2, RoleReplica, ip0, 2),
	}
	state := mkState(
		node{pod: pod1, ip: ip1, role: "slave", masterHost: ip0, link: "down", keys: 5, replid: "AAAA"},
		node{pod: pod2, ip: ip2, role: "slave", masterHost: ip0, link: "down", keys: 7, replid: "CCCC", replid2: "DDDD"},
	)
	a := Analyze(pods, state)
	if !hasFinding(a, SeverityFail, "independent replication lineages") {
		t.Errorf("want FAIL lineage divergence, got: %s", findingsStr(a))
	}
}

func TestAnalyzePromotionChainIsNotDivergence(t *testing.T) {
	// healthyState contains a promotion chain (r-2: replid rotated, old
	// lineage in replid2) — the LR-024 lesson: this is ONE lineage.
	a := Analyze(healthyPods(), healthyState())
	if hasFinding(a, SeverityFail, "independent replication lineages") {
		t.Errorf("promotion chain wrongly flagged as divergence: %s", findingsStr(a))
	}
}

func TestAnalyzeFreshPodAwaitingAuthorization(t *testing.T) {
	// A recreated pod has no annotations yet (the StatefulSet wiped them):
	// transient re-auth state, WARN only.
	pods := healthyPods()
	pods[2] = PodView{Name: pod2, IP: ip2, Phase: "Running", RoleLabel: RoleReplica}
	state := healthyState()
	state.RedisNodes[ip2].Reachable = false
	a := Analyze(pods, state)
	if !hasFinding(a, SeverityWarn, "no assignment yet") {
		t.Errorf("want WARN no assignment yet on r-2, got: %s", findingsStr(a))
	}
	if a.Failed() {
		t.Errorf("fresh pod must not FAIL while authority is live, got: %s", findingsStr(a))
	}
}
