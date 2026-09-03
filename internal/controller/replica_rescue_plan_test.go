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
	"testing"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// LR-060. Rule A no longer suppresses Rule R while a failover is reported, so the
// one pod Rule R must not touch in that window needs a guard.
//
// The rows that are NOT about the new clause pin LR-009/LR-010's unchanged
// trigger, and they are what make this table a regression net rather than a
// single assertion: if the extraction to a pure seam had changed Rule R's
// behaviour at all, they would say so.
func TestPlanReplicaRescue(t *testing.T) {
	const (
		master    = "10.0.0.1"
		promotedA = "10.0.0.2"
		syncingB  = "10.0.0.3"
		strayC    = "10.0.0.4"
		elsewhere = "10.9.9.9"
		sentinelA = "10.1.0.1"

		roleSlave = "slave"
		linkDown  = "down"
		podMaster = "redis-0"
		podOther  = "redis-1"
	)

	// A Sentinel mid-failover: it has promoted .2, and the quorum has not yet
	// caught up, so RealMasterIP is still the OUTGOING master .1.
	sentinelMidFailover := &redisclient.SentinelNodeState{
		PodName: "s0", IP: sentinelA, Reachable: true, Monitoring: true, MasterIP: master,
		MasterFlags: RoleMaster + ",failover_in_progress", MasterFailoverState: "reconf_slaves",
		Replicas: []redisclient.ReplicaInfo{
			{IP: promotedA, Flags: roleSlave + ",promoted"},
			{IP: syncingB, Flags: roleSlave + ",reconf_inprog"},
		},
	}
	idleSentinel := &redisclient.SentinelNodeState{
		PodName: "s0", IP: sentinelA, Reachable: true, Monitoring: true, MasterIP: master,
		MasterFlags: RoleMaster,
		Replicas: []redisclient.ReplicaInfo{
			{IP: promotedA, Flags: roleSlave}, {IP: syncingB, Flags: roleSlave},
		},
	}

	tests := []struct {
		name     string
		sentinel *redisclient.SentinelNodeState
		nodes    map[string]*redisclient.RedisNodeState
		want     []string
	}{
		{
			name:     "a pod following the wrong master is rescued",
			sentinel: idleSentinel,
			nodes: map[string]*redisclient.RedisNodeState{
				master:    {PodName: podMaster, IP: master, Reachable: true, Role: RoleMaster},
				promotedA: {PodName: podOther, IP: promotedA, Reachable: true, Role: roleSlave, MasterHost: elsewhere, LinkStatus: "up"},
			},
			want: []string{podOther},
		},
		{
			name:     "a pod that thinks it is a master is rescued",
			sentinel: idleSentinel,
			nodes: map[string]*redisclient.RedisNodeState{
				master:    {PodName: podMaster, IP: master, Reachable: true, Role: RoleMaster},
				promotedA: {PodName: podOther, IP: promotedA, Reachable: true, Role: RoleMaster},
			},
			want: []string{podOther},
		},
		{
			name:     "LR-010: a replica mid-sync from the CORRECT master is left alone",
			sentinel: idleSentinel,
			nodes: map[string]*redisclient.RedisNodeState{
				master:    {PodName: podMaster, IP: master, Reachable: true, Role: RoleMaster},
				promotedA: {PodName: podOther, IP: promotedA, Reachable: true, Role: roleSlave, MasterHost: master, LinkStatus: linkDown},
			},
			want: nil,
		},
		{
			name:     "an unreachable pod is skipped",
			sentinel: idleSentinel,
			nodes: map[string]*redisclient.RedisNodeState{
				master:    {PodName: podMaster, IP: master, Reachable: true, Role: RoleMaster},
				promotedA: {PodName: podOther, IP: promotedA, Reachable: false, Role: RoleMaster},
			},
			want: nil,
		},
		{
			name:     "THE FIX: the promoted pod is never demoted, even though the majority still names the outgoing master",
			sentinel: sentinelMidFailover,
			nodes: map[string]*redisclient.RedisNodeState{
				master: {PodName: podMaster, IP: master, Reachable: true, Role: RoleMaster},
				// The promoted pod: already role:master, and RealMasterIP is still .1,
				// so LR-010's trigger fires on it. It must NOT be rescued.
				promotedA: {PodName: podOther, IP: promotedA, Reachable: true, Role: RoleMaster},
			},
			want: nil,
		},
		{
			name:     "POSITIVE CONTROL: in the same mid-failover window a genuinely wrong-mastered pod IS still rescued",
			sentinel: sentinelMidFailover,
			nodes: map[string]*redisclient.RedisNodeState{
				master:    {PodName: podMaster, IP: master, Reachable: true, Role: RoleMaster},
				promotedA: {PodName: podOther, IP: promotedA, Reachable: true, Role: RoleMaster},
				strayC:    {PodName: "redis-3", IP: strayC, Reachable: true, Role: roleSlave, MasterHost: elsewhere},
			},
			want: []string{"redis-3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := redisclient.NewReplicationState()
			s.RealMasterIP = master
			s.SentinelNodes[tt.sentinel.IP] = tt.sentinel
			for ip, rn := range tt.nodes {
				s.RedisNodes[ip] = rn
				s.AddLiveTopologyIP(ip)
			}
			got := planReplicaRescue(s)
			names := make([]string, 0, len(got))
			for _, rn := range got {
				names = append(names, rn.PodName)
			}
			if len(names) != len(tt.want) {
				t.Fatalf("planReplicaRescue = %v, want %v", names, tt.want)
			}
			for i := range names {
				if names[i] != tt.want[i] {
					t.Fatalf("planReplicaRescue = %v, want %v", names, tt.want)
				}
			}
		})
	}
}
