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

package redis

import "testing"

// Fixture addresses. Named rather than repeated so the table reads as a topology
// and not as a wall of literals.
const (
	fsPromotedIP = "10.0.0.2"
	fsOtherIP    = "10.0.0.3"
	fsMasterIP   = "10.0.0.1"
	fsSentinel0  = "10.1.0.1"
	fsSentinel1  = "10.1.0.2"
	fsSentinel2  = "10.1.0.3"

	fsRoleSlave     = "slave"
	fsFlagsFailover = "master,failover_in_progress"
	fsStateWaitProm = "wait_promotion"
	fsStateUpdateCf = "update_config"
)

// LR-060 / D2. A Sentinel latched in RECONF_SLAVES reports a failover that can
// never end: sentinelFailoverDetectEnd returns at its FIRST statement when the
// promoted replica is s_down (redis 8.0 sentinel.c:5185, valkey 8.1 :4989),
// BEFORE the elapsed > failover_timeout force-end at :5201/:5004, and
// sentinelAbortFailover cannot be reached from that state
// (serverAssert(failover_state <= WAIT_PROMOTION), :5342/:5129).
//
// FailoverStalled reconstructs that exact predicate from flags we already
// gather. It is deliberately POSITIVE-EVIDENCE-ONLY: the promoted_slave == NULL
// half of the upstream condition is conceded, because an absent `promoted` entry
// is indistinguishable from a failed `SENTINEL replicas` read (the gather drops
// the error, LR-041's class), and reading absence as evidence would suppress the
// guard on no evidence at all. Everything unknown must therefore read as NOT
// stalled, so the guard holds.
func TestFailoverStalled(t *testing.T) {
	promoted := func(flags string) ReplicaInfo { return ReplicaInfo{IP: fsPromotedIP, Flags: flags} }

	tests := []struct {
		name string
		sn   *SentinelNodeState
		want bool
	}{
		{
			name: "the latch: reconf_slaves with an s_down promoted replica",
			sn: &SentinelNodeState{
				Reachable: true, Monitoring: true,
				MasterFlags:         "s_down," + fsFlagsFailover,
				MasterFailoverState: failoverStateReconfSlaves,
				Replicas:            []ReplicaInfo{promoted("slave,s_down,promoted"), {IP: fsOtherIP, Flags: fsRoleSlave}},
			},
			want: true,
		},
		{
			name: "MEASURED CONTROL (LR-060 run 2): reconf_slaves, promoted replica healthy — held 179s by not_reconfigured, NOT latched",
			sn: &SentinelNodeState{
				Reachable: true, Monitoring: true,
				MasterFlags:         "s_down," + fsFlagsFailover + ",force_failover",
				MasterFailoverState: failoverStateReconfSlaves,
				Replicas:            []ReplicaInfo{{IP: fsOtherIP, Flags: fsRoleSlave + ",reconf_inprog"}, promoted("slave,promoted")},
			},
			want: false,
		},
		{
			name: "an earlier state is bounded by Sentinel's own abort timer, never stalled",
			sn: &SentinelNodeState{
				Reachable: true, Monitoring: true,
				MasterFlags:         fsFlagsFailover,
				MasterFailoverState: fsStateWaitProm,
				Replicas:            []ReplicaInfo{promoted("slave,s_down,promoted")},
			},
			want: false,
		},
		{
			name: "update_config is transient and self-clearing, never stalled",
			sn: &SentinelNodeState{
				Reachable: true, Monitoring: true,
				MasterFlags:         fsFlagsFailover,
				MasterFailoverState: fsStateUpdateCf,
				Replicas:            []ReplicaInfo{promoted("slave,s_down,promoted")},
			},
			want: false,
		},
		{
			name: "CONCEDED: no promoted entry is a failed read, not evidence of NULL",
			sn: &SentinelNodeState{
				Reachable: true, Monitoring: true,
				MasterFlags:         fsFlagsFailover,
				MasterFailoverState: failoverStateReconfSlaves,
				Replicas:            nil,
			},
			want: false,
		},
		{
			name: "no failover reported at all",
			sn: &SentinelNodeState{
				Reachable: true, Monitoring: true,
				MasterFlags: roleMaster,
				Replicas:    []ReplicaInfo{promoted("slave,s_down,promoted")},
			},
			want: false,
		},
		{
			name: "an unreachable Sentinel tells us nothing",
			sn: &SentinelNodeState{
				Reachable:           false,
				MasterFlags:         fsFlagsFailover,
				MasterFailoverState: failoverStateReconfSlaves,
				Replicas:            []ReplicaInfo{promoted("slave,s_down,promoted")},
			},
			want: false,
		},
		{
			name: "nil is not stalled",
			sn:   nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sn.FailoverStalled(); got != tt.want {
				t.Errorf("FailoverStalled() = %v, want %v", got, tt.want)
			}
		})
	}
}

// LR-060 / D1 + D2. Rule A must suppress healing only while a failover can still
// PROGRESS. FailoverReported keeps the Q1 meaning ("a Sentinel says a failover is
// running") for lrctl, DetermineRealMaster step 4 and Rule N G3; FailoverProgressing
// is the Q2 answer Rule A needs.
//
// Both are an OR over reachable monitoring Sentinels, and the OR is required rather
// than merely conservative: only the election LEADER carries
// SRI_FAILOVER_IN_PROGRESS (LR-052 measured 1 of 3), so a majority test would make
// the field permanently false again.
func TestDetermineRealMasterFailoverProgressing(t *testing.T) {
	stalled := &SentinelNodeState{
		PodName: "s0", IP: fsSentinel0, Reachable: true, Monitoring: true, MasterIP: fsMasterIP,
		MasterFlags:         "s_down," + fsFlagsFailover,
		MasterFailoverState: failoverStateReconfSlaves,
		Replicas:            []ReplicaInfo{{IP: fsPromotedIP, Flags: fsRoleSlave + ",s_down,promoted"}},
	}
	progressing := &SentinelNodeState{
		PodName: "s1", IP: fsSentinel1, Reachable: true, Monitoring: true, MasterIP: fsMasterIP,
		MasterFlags:         fsFlagsFailover,
		MasterFailoverState: failoverStateReconfSlaves,
		Replicas:            []ReplicaInfo{{IP: fsPromotedIP, Flags: fsRoleSlave + ",promoted"}},
	}
	idle := &SentinelNodeState{
		PodName: "s2", IP: fsSentinel2, Reachable: true, Monitoring: true, MasterIP: fsMasterIP,
		MasterFlags: roleMaster,
	}

	tests := []struct {
		name                   string
		sentinels              []*SentinelNodeState
		wantReported, wantProg bool
	}{
		{"idle quorum", []*SentinelNodeState{idle, idle, idle}, false, false},
		{"one leader mid-failover: reported AND progressing", []*SentinelNodeState{progressing, idle, idle}, true, true},
		{"THE FIX: a latched leader is reported but NOT progressing", []*SentinelNodeState{stalled, idle, idle}, true, false},
		{"a stalled leader beside a progressing one still suppresses", []*SentinelNodeState{stalled, progressing, idle}, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewReplicationState()
			for _, sn := range tt.sentinels {
				s.SentinelNodes[sn.IP] = sn
			}
			s.AddLiveTopologyIP(fsMasterIP)
			s.RedisNodes[fsMasterIP] = &RedisNodeState{IP: fsMasterIP, Reachable: true, Role: roleMaster}
			s.DetermineRealMaster()
			if s.FailoverReported != tt.wantReported {
				t.Errorf("FailoverReported = %v, want %v", s.FailoverReported, tt.wantReported)
			}
			if s.FailoverProgressing != tt.wantProg {
				t.Errorf("FailoverProgressing = %v, want %v", s.FailoverProgressing, tt.wantProg)
			}
		})
	}
}
