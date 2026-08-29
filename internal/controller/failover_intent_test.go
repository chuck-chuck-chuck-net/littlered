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
	"reflect"
	"testing"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// --- resolveFailoverIntent (ADR-011 §3: annotations ARE the intent record) ---

func TestResolveFailoverIntent(t *testing.T) {
	assigned := func(name, ip, role, masterIP string, epoch int64) failoverPodView {
		return failoverPodView{name: name, ip: ip, hasAssignment: true,
			assignedRole: role, assignedMasterIP: masterIP, epoch: epoch}
	}

	tests := []struct {
		name string
		pods []failoverPodView
		want failoverIntent
	}{
		{
			name: "no pods -> no intent, epoch 0",
			pods: nil,
			want: failoverIntent{},
		},
		{
			name: "unstamped pods only -> no intent",
			pods: []failoverPodView{{name: podRedis0, ip: ipMaster}},
			want: failoverIntent{},
		},
		{
			name: "bootstrap set: one master, two replicas at epoch 1",
			pods: []failoverPodView{
				assigned(podRedis0, ipMaster, RoleMaster, "", 1),
				assigned(podRedis1, ipReplica, RoleReplica, ipMaster, 1),
				assigned(podRedis2, ipNode3, RoleReplica, ipMaster, 1),
			},
			want: failoverIntent{masterName: podRedis0, masterIP: ipMaster, maxEpoch: 1},
		},
		{
			name: "two master assignments: highest epoch wins (stale ex-master superseded)",
			pods: []failoverPodView{
				assigned(podRedis0, ipMaster, RoleMaster, "", 1), // terminating ex-master, kept stale stamp
				assigned(podRedis1, ipReplica, RoleMaster, "", 2),
				assigned(podRedis2, ipNode3, RoleReplica, ipReplica, 2),
			},
			want: failoverIntent{masterName: podRedis1, masterIP: ipReplica, maxEpoch: 2},
		},
		{
			name: "two master assignments at the SAME epoch: lowest name wins (deterministic)",
			pods: []failoverPodView{
				assigned(podRedis2, ipNode3, RoleMaster, "", 3),
				assigned(podRedis1, ipReplica, RoleMaster, "", 3),
			},
			want: failoverIntent{masterName: podRedis1, masterIP: ipReplica, maxEpoch: 3},
		},
		{
			name: "master pod replaced (annotations died with it): replica stamps remain, no intended master",
			pods: []failoverPodView{
				{name: podRedis0, ip: ipNode9}, // recreated, unstamped
				assigned(podRedis1, ipReplica, RoleReplica, ipMaster, 4),
				assigned(podRedis2, ipNode3, RoleReplica, ipMaster, 4),
			},
			want: failoverIntent{maxEpoch: 4},
		},
		{
			name: "maxEpoch spans replica re-auth stamps beyond the master's own epoch",
			pods: []failoverPodView{
				assigned(podRedis0, ipMaster, RoleMaster, "", 2),
				assigned(podRedis1, ipReplica, RoleReplica, ipMaster, 5), // re-authed parked pod
			},
			want: failoverIntent{masterName: podRedis0, masterIP: ipMaster, maxEpoch: 5},
		},
		{
			name: "terminating intended master still resolves (graceful handover sees the intent)",
			pods: []failoverPodView{
				func() failoverPodView {
					v := assigned(podRedis0, ipMaster, RoleMaster, "", 1)
					v.terminating = true
					return v
				}(),
				assigned(podRedis1, ipReplica, RoleReplica, ipMaster, 1),
			},
			want: failoverIntent{masterName: podRedis0, masterIP: ipMaster, maxEpoch: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveFailoverIntent(tt.pods)
			if got != tt.want {
				t.Errorf("resolveFailoverIntent() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// --- determineFailoverLiveMaster (ADR-011 §2: intent is the sole authority) --
//
// This is the failover-mode replacement for sentinel's DetermineRealMaster and
// closes the inherited gap that DetermineRealMaster never had a direct table
// test (ADR-011 Consequences).

func TestDetermineFailoverLiveMaster(t *testing.T) {
	node := func(ip, role string, reachable bool) *redisclient.RedisNodeState {
		return &redisclient.RedisNodeState{IP: ip, Role: role, Reachable: reachable}
	}
	stateOf := func(nodes ...*redisclient.RedisNodeState) *redisclient.ReplicationState {
		s := redisclient.NewReplicationState()
		for _, n := range nodes {
			s.RedisNodes[n.IP] = n
			s.AddLiveTopologyIP(n.IP)
		}
		return s
	}

	tests := []struct {
		name       string
		state      *redisclient.ReplicationState
		intendedIP string
		want       string
	}{
		{
			name:       "no intent -> no live master",
			state:      stateOf(node(ipMaster, RoleMaster, true)),
			intendedIP: "",
			want:       "",
		},
		{
			name:       "intended master reachable and role:master -> live",
			state:      stateOf(node(ipMaster, RoleMaster, true), node(ipReplica, roleSlave, true)),
			intendedIP: ipMaster,
			want:       ipMaster,
		},
		{
			name:       "intended master reachable but still role:slave (promotion not applied) -> not live",
			state:      stateOf(node(ipMaster, roleSlave, true)),
			intendedIP: ipMaster,
			want:       "",
		},
		{
			name:       "intended master unreachable -> not live",
			state:      stateOf(node(ipMaster, RoleMaster, false)),
			intendedIP: ipMaster,
			want:       "",
		},
		{
			name:       "intended master unknown to the gather -> not live",
			state:      stateOf(),
			intendedIP: ipMaster,
			want:       "",
		},
		{
			// The load-bearing case: an unintended reachable role:master (old
			// master still up mid-transition, or a bare restarted pod) is a
			// STRAGGLER — it must never be adopted as the live master.
			name:       "unintended reachable role:master is a straggler, never the live master",
			state:      stateOf(node(ipNode9, RoleMaster, true), node(ipMaster, roleSlave, true)),
			intendedIP: ipMaster,
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := determineFailoverLiveMaster(tt.state, tt.intendedIP); got != tt.want {
				t.Errorf("determineFailoverLiveMaster() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- failoverTransitionSettled (ADR-011 §6: unsettled-transition definition) -

func TestFailoverTransitionSettled(t *testing.T) {
	intent := failoverIntent{masterName: podRedis1, masterIP: ipReplica, maxEpoch: 2}
	stateWith := func(role string, reachable bool) *redisclient.ReplicationState {
		s := redisclient.NewReplicationState()
		s.RedisNodes[ipReplica] = &redisclient.RedisNodeState{IP: ipReplica, Role: role, Reachable: reachable}
		return s
	}

	tests := []struct {
		name   string
		intent failoverIntent
		state  *redisclient.ReplicationState
		labels map[string]string
		want   bool
	}{
		{
			name:   "no intent -> settled (nothing to converge)",
			intent: failoverIntent{},
			state:  redisclient.NewReplicationState(),
			labels: nil,
			want:   true,
		},
		{
			name:   "intended master role:master + master label -> settled",
			intent: intent,
			state:  stateWith(RoleMaster, true),
			labels: map[string]string{podRedis1: RoleMaster},
			want:   true,
		},
		{
			name:   "intended master role:master but label not yet flipped -> unsettled",
			intent: intent,
			state:  stateWith(RoleMaster, true),
			labels: map[string]string{podRedis1: RoleReplica},
			want:   false,
		},
		{
			name:   "intended master reachable but still role:slave -> unsettled",
			intent: intent,
			state:  stateWith(roleSlave, true),
			labels: map[string]string{podRedis1: RoleMaster},
			want:   false,
		},
		{
			name:   "intended master unreachable -> unsettled",
			intent: intent,
			state:  stateWith(RoleMaster, false),
			labels: map[string]string{podRedis1: RoleMaster},
			want:   false,
		},
		{
			name:   "intended master unknown to gather -> unsettled",
			intent: intent,
			state:  redisclient.NewReplicationState(),
			labels: map[string]string{podRedis1: RoleMaster},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := failoverTransitionSettled(tt.intent, tt.state, tt.labels); got != tt.want {
				t.Errorf("failoverTransitionSettled() = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- failoverPromotionUnsettled (the §6 promotion gate) ----------------------
//
// The load-bearing distinction vs failoverTransitionSettled: a DEAD intended
// master must never block a new election — bare unsettledness would deadlock
// the crash and graceful-handover recoveries (the dead master can never again
// report role:master).

func TestFailoverPromotionUnsettled(t *testing.T) {
	intent := failoverIntent{masterName: podRedis1, masterIP: ipReplica, maxEpoch: 2}
	stateWith := func(role string, reachable bool) *redisclient.ReplicationState {
		s := redisclient.NewReplicationState()
		s.RedisNodes[ipReplica] = &redisclient.RedisNodeState{IP: ipReplica, Role: role, Reachable: reachable}
		return s
	}
	masterLabel := map[string]string{podRedis1: RoleMaster}
	replicaLabel := map[string]string{podRedis1: RoleReplica}

	tests := []struct {
		name   string
		intent failoverIntent
		state  *redisclient.ReplicationState
		labels map[string]string
		want   bool
	}{
		{
			name:   "no intent: nothing in flight",
			intent: failoverIntent{},
			state:  redisclient.NewReplicationState(),
			want:   false,
		},
		{
			name:   "converged transition: not blocking",
			intent: intent,
			state:  stateWith(RoleMaster, true),
			labels: masterLabel,
			want:   false,
		},
		{
			name:   "intended master ALIVE, promotion not yet observed: blocking",
			intent: intent,
			state:  stateWith(roleSlave, true),
			labels: replicaLabel,
			want:   true,
		},
		{
			name:   "intended master ALIVE role:master, label not yet flipped: blocking",
			intent: intent,
			state:  stateWith(RoleMaster, true),
			labels: replicaLabel,
			want:   true,
		},
		{
			// The graceful-handover / crash regression: the intended master is
			// unreachable (terminating pod excluded from the gather, or its
			// container crashed and parks). Its transition is moot — a new
			// election must NOT be blocked, or recovery deadlocks.
			name:   "intended master DEAD (unreachable): NOT blocking",
			intent: intent,
			state:  stateWith(RoleMaster, false),
			labels: masterLabel,
			want:   false,
		},
		{
			name:   "intended master unknown to the gather entirely: NOT blocking",
			intent: intent,
			state:  redisclient.NewReplicationState(),
			labels: masterLabel,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := failoverPromotionUnsettled(tt.intent, tt.state, tt.labels); got != tt.want {
				t.Errorf("failoverPromotionUnsettled() = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- planFailoverReauth (ADR-011 §3: the re-authorization loop) --------------

func TestPlanFailoverReauth(t *testing.T) {
	const liveMaster = ipMaster
	intent := failoverIntent{masterName: podRedis0, masterIP: liveMaster, maxEpoch: 3}

	masterPod := failoverPodView{name: podRedis0, ip: liveMaster, ready: true, reachable: true,
		hasAssignment: true, assignedRole: RoleMaster, epoch: 3}
	healthyReplica := failoverPodView{name: podRedis1, ip: ipReplica, ready: true, reachable: true,
		hasAssignment: true, assignedRole: RoleReplica, assignedMasterIP: liveMaster, epoch: 3}

	tests := []struct {
		name string
		pods []failoverPodView
		want []failoverStamp
	}{
		{
			name: "steady state: nothing to stamp",
			pods: []failoverPodView{masterPod, healthyReplica},
			want: nil,
		},
		{
			name: "brand-new pod (no assignment): replica at CURRENT epoch, no bump",
			pods: []failoverPodView{masterPod, healthyReplica,
				{name: podRedis2, ip: ipNode7}},
			want: []failoverStamp{
				{podName: podRedis2, role: RoleReplica, masterIP: liveMaster, epoch: 3},
			},
		},
		{
			name: "new pod without an IP yet: skipped",
			pods: []failoverPodView{masterPod, healthyReplica,
				{name: podRedis2}},
			want: nil,
		},
		{
			name: "parked pod (restarted, not-Ready, unreachable, consumed epoch): replica at maxEpoch+1",
			pods: []failoverPodView{masterPod, healthyReplica,
				{name: podRedis2, ip: ipNode3, restarted: true, // parked in the wait loop
					hasAssignment: true, assignedRole: RoleReplica, assignedMasterIP: liveMaster, epoch: 3}},
			want: []failoverStamp{
				{podName: podRedis2, role: RoleReplica, masterIP: liveMaster, epoch: 4},
			},
		},
		{
			name: "restarted but reachable (syncing replica, readiness lagging): NOT restamped",
			pods: []failoverPodView{masterPod,
				{name: podRedis1, ip: ipReplica, restarted: true, reachable: true,
					hasAssignment: true, assignedRole: RoleReplica, assignedMasterIP: liveMaster, epoch: 3}},
			want: nil,
		},
		{
			name: "not-Ready but never restarted (first boot honoring a fresh stamp): NOT restamped",
			pods: []failoverPodView{masterPod,
				{name: podRedis1, ip: ipReplica,
					hasAssignment: true, assignedRole: RoleReplica, assignedMasterIP: liveMaster, epoch: 3}},
			want: nil,
		},
		{
			// The kill-9 hazard guard: the INTENDED master parks after a
			// container restart, but a blind master restamp is exactly the
			// ADR-001 hazard — its path is planMasterDeath/planFailover.
			name: "parked INTENDED master: never blind-restamped here",
			pods: []failoverPodView{
				{name: podRedis0, ip: liveMaster, restarted: true,
					hasAssignment: true, assignedRole: RoleMaster, epoch: 3},
				healthyReplica,
			},
			want: nil,
		},
		{
			name: "terminating pod: skipped",
			pods: []failoverPodView{masterPod,
				{name: podRedis2, ip: ipNode3, terminating: true}},
			want: nil,
		},
		{
			name: "mixed: new pod at current epoch, parked pod at bumped epoch, sorted by name",
			pods: []failoverPodView{masterPod,
				{name: podRedis2, ip: ipNode3, restarted: true,
					hasAssignment: true, assignedRole: RoleReplica, assignedMasterIP: liveMaster, epoch: 3},
				{name: podRedis1, ip: ipNode9}},
			want: []failoverStamp{
				{podName: podRedis1, role: RoleReplica, masterIP: liveMaster, epoch: 3},
				{podName: podRedis2, role: RoleReplica, masterIP: liveMaster, epoch: 4},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := planFailoverReauth(tt.pods, intent, liveMaster)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("planFailoverReauth() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// --- planFailoverRepoints (Rule R analog) ------------------------------------

func TestPlanFailoverRepoints(t *testing.T) {
	const liveMaster = ipMaster
	stateOf := func(nodes ...*redisclient.RedisNodeState) *redisclient.ReplicationState {
		s := redisclient.NewReplicationState()
		for _, n := range nodes {
			s.RedisNodes[n.IP] = n
		}
		return s
	}
	node := func(ip, role, masterHost, link string, reachable bool) *redisclient.RedisNodeState {
		return &redisclient.RedisNodeState{IP: ip, Role: role, MasterHost: masterHost, LinkStatus: link, Reachable: reachable}
	}

	tests := []struct {
		name  string
		state *redisclient.ReplicationState
		want  []string
	}{
		{
			name: "healthy topology: nothing to repoint",
			state: stateOf(
				node(liveMaster, RoleMaster, "", "", true),
				node(ipReplica, roleSlave, liveMaster, "up", true),
			),
			want: nil,
		},
		{
			name: "unintended role:master straggler: repointed",
			state: stateOf(
				node(liveMaster, RoleMaster, "", "", true),
				node(ipNode9, RoleMaster, "", "", true),
			),
			want: []string{ipNode9},
		},
		{
			name: "replica following a wrong (dead) master IP: repointed",
			state: stateOf(
				node(liveMaster, RoleMaster, "", "", true),
				node(ipReplica, roleSlave, "10.0.0.99", "down", true),
			),
			want: []string{ipReplica},
		},
		{
			name: "replica on the right master with link:down (handshake): NOT repointed",
			state: stateOf(
				node(liveMaster, RoleMaster, "", "", true),
				node(ipReplica, roleSlave, liveMaster, "down", true),
			),
			want: nil,
		},
		{
			name: "unreachable pod: skipped",
			state: stateOf(
				node(liveMaster, RoleMaster, "", "", true),
				node(ipReplica, RoleMaster, "", "", false),
			),
			want: nil,
		},
		{
			name: "multiple stragglers: sorted by IP",
			state: stateOf(
				node(liveMaster, RoleMaster, "", "", true),
				node("10.0.0.5", RoleMaster, "", "", true),
				node(ipNode3, roleSlave, "10.0.0.5", "up", true),
			),
			want: []string{ipNode3, "10.0.0.5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := planFailoverRepoints(tt.state, liveMaster)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("planFailoverRepoints() = %v, want %v", got, tt.want)
			}
		})
	}
}
