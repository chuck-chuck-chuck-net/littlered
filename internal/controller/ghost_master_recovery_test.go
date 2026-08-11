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
	"time"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// --- builders -------------------------------------------------------------

// ghostSnSpec is a Sentinel monitoring a specific master IP (empty = bare) with a set of
// known replicas. Reuses rnSpec (from leaderless_recovery_test.go) for the Redis nodes.
type ghostSnSpec struct {
	reachable bool
	masterIP  string // "" => bare (not monitoring); a ghost IP is one absent from validIPs
	replicas  []redisclient.ReplicaInfo
}

const ghostMasterIP = "10.9.9.9" // never added to ValidIPs -> a ghost

// buildGhostState builds a ReplicationState. Redis node IPs are marked valid (live
// pods); the ghost master IP is deliberately NOT, so IsGhost(masterIP) holds.
func buildGhostState(sentinels []ghostSnSpec, redis []rnSpec) *redisclient.ReplicationState {
	s := redisclient.NewReplicationState()
	for i, sn := range sentinels {
		ip := "10.0.3." + string(rune('0'+i))
		s.SentinelNodes[ip] = &redisclient.SentinelNodeState{
			IP:         ip,
			Reachable:  sn.reachable,
			Monitoring: sn.masterIP != "",
			MasterIP:   sn.masterIP,
			Replicas:   sn.replicas,
		}
	}
	for _, rn := range redis {
		s.ValidIPs[rn.ip] = true
		s.RedisNodes[rn.ip] = &redisclient.RedisNodeState{
			IP: rn.ip, PodName: "pod-" + rn.ip, Reachable: rn.reachable,
			Keys: rn.keys, Offset: rn.offset, Replid: rn.replid, Replid2: rn.replid2, Role: rn.role,
		}
	}
	return s
}

// three reachable Sentinels all pinned to the same ghost master, no known replicas — the
// ghost-master deadlock signature.
func ghostQuorum() []ghostSnSpec {
	return []ghostSnSpec{
		{reachable: true, masterIP: ghostMasterIP},
		{reachable: true, masterIP: ghostMasterIP},
		{reachable: true, masterIP: ghostMasterIP},
	}
}

// --- detection ------------------------------------------------------------

func TestSentinelsMonitorGhostMaster(t *testing.T) {
	tests := []struct {
		name      string
		sentinels []ghostSnSpec
		redis     []rnSpec
		want      bool
	}{
		{name: "all bare -> false", sentinels: []ghostSnSpec{{reachable: true}, {reachable: true}, {reachable: true}}, want: false},
		{
			name:      "majority monitor a live master -> false",
			sentinels: []ghostSnSpec{{reachable: true, masterIP: ipNode5}, {reachable: true, masterIP: ipNode5}, {reachable: true, masterIP: ipNode5}},
			redis:     []rnSpec{{ip: ipNode5, reachable: true, role: RoleMaster}},
			want:      false,
		},
		{name: "majority monitor a ghost master -> true", sentinels: ghostQuorum(), want: true},
		{
			name:      "only a minority monitor a ghost (rest bare) -> false",
			sentinels: []ghostSnSpec{{reachable: true, masterIP: ghostMasterIP}, {reachable: true}, {reachable: true}},
			want:      false,
		},
		{
			name:      "ghost majority but only one sentinel reachable -> true (majority of reachable)",
			sentinels: []ghostSnSpec{{reachable: true, masterIP: ghostMasterIP}, {reachable: false}, {reachable: false}},
			want:      true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := buildGhostState(tc.sentinels, tc.redis)
			if got := s.SentinelsMonitorGhostMaster(); got != tc.want {
				t.Fatalf("SentinelsMonitorGhostMaster() = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- planGhostMasterRecovery: every gate and tier -------------------------

func TestPlanGhostMasterRecovery(t *testing.T) {
	const (
		cooldown = 30 * time.Second
		quorum   = 2
	)
	now := time.Unix(2_000_000, 0)
	elapsed := func() *time.Time { u := now.Add(-cooldown - time.Second); return &u }
	fresh := func() *time.Time { u := now.Add(-5 * time.Second); return &u }

	// a healthy (live, non-s_down) replica known to a Sentinel -> Sentinel can still fail
	// over on its own; we must NOT intervene.
	healthyReplica := []redisclient.ReplicaInfo{{IP: "10.0.0.20", Flags: roleSlave}}
	healthyRedis := []rnSpec{{ip: "10.0.0.20", reachable: true, keys: 3, offset: 5, replid: "A", role: roleSlave}}

	tests := []struct {
		name         string
		sentinels    []ghostSnSpec
		redis        []rnSpec
		allowUnsafe  bool
		bootstrapIP  string
		since        *time.Time
		wantAction   recoveryAction
		wantMasterIP string
		wantDiverged bool
		wantHolders  int
	}{
		// --- GATES --------------------------------------------------------
		{
			name:       "gate: bare sentinels (leaderless, not ghost) -> clear",
			sentinels:  []ghostSnSpec{{reachable: true}, {reachable: true}, {reachable: true}},
			since:      elapsed(),
			wantAction: recoveryClearMarker,
		},
		{
			name:       "gate: ghost master BUT a healthy replica is known (recent death, failover imminent) -> clear",
			sentinels:  []ghostSnSpec{{reachable: true, masterIP: ghostMasterIP, replicas: healthyReplica}, {reachable: true, masterIP: ghostMasterIP, replicas: healthyReplica}, {reachable: true, masterIP: ghostMasterIP, replicas: healthyReplica}},
			redis:      healthyRedis,
			since:      elapsed(),
			wantAction: recoveryClearMarker,
		},
		{
			name:       "gate: ghost master, no healthy replica, but below quorum reachable -> clear",
			sentinels:  []ghostSnSpec{{reachable: true, masterIP: ghostMasterIP}, {reachable: false}, {reachable: false}},
			since:      elapsed(),
			wantAction: recoveryClearMarker,
		},
		{
			name:       "gate: deadlock detected but no marker yet -> start cooldown",
			sentinels:  ghostQuorum(),
			since:      nil,
			wantAction: recoveryStartCooldown,
		},
		{
			name:       "gate: within cooldown -> wait (even with a survivor present)",
			sentinels:  ghostQuorum(),
			redis:      []rnSpec{{ip: ipMaster, reachable: true, keys: 5, role: roleSlave, replid: "A"}},
			since:      fresh(),
			wantAction: recoveryWait,
		},
		{
			name:        "gate: cooldown elapsed, 0 survivors with data, no bootstrap IP -> wait",
			sentinels:   ghostQuorum(),
			bootstrapIP: "",
			since:       elapsed(),
			wantAction:  recoveryWait,
		},

		// --- FUNCTIONALITY ------------------------------------------------
		{
			name:         "func: 0 holders, bootstrap IP set -> seed it",
			sentinels:    ghostQuorum(),
			redis:        []rnSpec{{ip: ipMaster, reachable: true, keys: 0, role: roleSlave}},
			bootstrapIP:  ipNode7,
			since:        elapsed(),
			wantAction:   recoverySeedNoData,
			wantMasterIP: ipNode7,
		},
		{
			name:         "func: exactly 1 survivor with data -> elect it, no opt-in",
			sentinels:    ghostQuorum(),
			redis:        []rnSpec{{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: "A", role: roleSlave}},
			since:        elapsed(),
			wantAction:   recoveryPromoteSurvivor,
			wantMasterIP: ipMaster,
			wantHolders:  1,
		},
		{
			name:      "func: 2 survivors SAME lineage -> elect highest-offset, NO opt-in (key difference from Rule L)",
			sentinels: ghostQuorum(),
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: "A", role: roleSlave},
				{ip: ipReplica, reachable: true, keys: 5, offset: 250, replid: "A", role: roleSlave},
			},
			since:        elapsed(),
			wantAction:   recoveryPromoteSurvivor,
			wantMasterIP: ipReplica,
			wantDiverged: false,
			wantHolders:  2,
		},
		{
			name:      "func: 2 survivors DIVERGED lineage, opt-in OFF -> refuse",
			sentinels: ghostQuorum(),
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: "A", role: roleSlave},
				{ip: ipReplica, reachable: true, keys: 9, offset: 90, replid: "B", role: roleSlave},
			},
			allowUnsafe: false,
			since:       elapsed(),
			wantAction:  recoveryRefuse,
			wantHolders: 2,
		},
		{
			name:      "func: 2 survivors DIVERGED lineage, opt-in ON -> unsafe-elect best",
			sentinels: ghostQuorum(),
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 300, replid: "A", role: roleSlave},
				{ip: ipReplica, reachable: true, keys: 9, offset: 90, replid: "B", role: roleSlave},
			},
			allowUnsafe:  true,
			since:        elapsed(),
			wantAction:   recoveryUnsafeElect,
			wantMasterIP: ipMaster,
			wantDiverged: true,
			wantHolders:  2,
		},

		// --- promotion chains: same lineage despite rotated replids (the real bug) -------
		{
			name:      "func: survivor was promoted (replid rotated to replid2) -> SAME lineage, elect, no opt-in",
			sentinels: ghostQuorum(),
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: testReplid0, role: roleSlave},
				{ip: ipReplica, reachable: true, keys: 5, offset: 250, replid: testReplid1, replid2: testReplid0, role: RoleMaster},
			},
			since:        elapsed(),
			wantAction:   recoveryPromoteSurvivor,
			wantMasterIP: ipReplica,
			wantDiverged: false,
			wantHolders:  2,
		},
		{
			name:      "func: 3-node promotion chain (the real graceful->crash state) -> one lineage, elect highest offset",
			sentinels: ghostQuorum(),
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 1, offset: 100, replid: testReplid0, role: roleSlave},
				{ip: ipReplica, reachable: true, keys: 1, offset: 100, replid: testReplid0, replid2: "7df3f8", role: roleSlave},
				{ip: ipNode3, reachable: true, keys: 1, offset: 120, replid: testReplid1, replid2: testReplid0, role: RoleMaster},
			},
			since:        elapsed(),
			wantAction:   recoveryPromoteSurvivor,
			wantMasterIP: ipNode3,
			wantDiverged: false,
			wantHolders:  3,
		},
		{
			name:      "func: genuinely independent lineages (no shared replid history) -> diverged, refuse",
			sentinels: ghostQuorum(),
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: testReplidA, replid2: "PPP", role: RoleMaster},
				{ip: ipReplica, reachable: true, keys: 5, offset: 90, replid: testReplidB, replid2: "QQQ", role: RoleMaster},
			},
			allowUnsafe: false,
			since:       elapsed(),
			wantAction:  recoveryRefuse,
			wantHolders: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := buildGhostState(tc.sentinels, tc.redis)
			got := planGhostMasterRecovery(s, quorum, tc.allowUnsafe, tc.bootstrapIP, tc.since, now, cooldown)
			if got.action != tc.wantAction {
				t.Fatalf("action = %v, want %v", got.action, tc.wantAction)
			}
			if tc.wantMasterIP != "" && got.masterIP != tc.wantMasterIP {
				t.Errorf("masterIP = %q, want %q", got.masterIP, tc.wantMasterIP)
			}
			if got.diverged != tc.wantDiverged {
				t.Errorf("diverged = %v, want %v", got.diverged, tc.wantDiverged)
			}
			if tc.wantHolders != 0 && got.holders != tc.wantHolders {
				t.Errorf("holders = %d, want %d", got.holders, tc.wantHolders)
			}
		})
	}
}
