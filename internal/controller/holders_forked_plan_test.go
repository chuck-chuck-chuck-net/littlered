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
	"maps"
	"testing"
	"time"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

const (
	fkReplidD = "dddddddddddddddddddddddddddddddddddddddd"
	fkReplidA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fkReplidB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	fkGhostIP = "10.0.0.99"
	fkMasterA = "10.0.0.1"
	fkMasterB = "10.0.0.2"
	fkSlave   = "slave"
)

// twoForkedMasters builds the LR-057 state: two reachable, data-holding pods that
// each report role:master, each carrying its own rotated replid with the SAME
// ancestor in replid2 — so holdersDiverged sees one lineage and the opt-in branch
// is skipped, while in fact each has been independently writable.
func twoForkedMasters() *redisclient.ReplicationState {
	s := redisclient.NewReplicationState()
	for ip, keys := range map[string]int64{fkMasterA: 500, fkMasterB: 400} {
		s.RedisNodes[ip] = &redisclient.RedisNodeState{
			PodName: "redis-" + ip, IP: ip, Reachable: true, Role: RoleMaster,
			Keys: keys, Offset: 900 - keys, Replid: fkReplidA, Replid2: fkReplidD,
		}
		s.AddLiveTopologyIP(ip)
	}
	s.RedisNodes[fkMasterB].Replid = fkReplidB
	return s
}

// LR-057, sentinel mode. The ghost-master recovery elects with NO opt-in whenever
// the holders read as one lineage, so on this state it promotes one of two live
// masters and the loser full-syncs — its acknowledged writes replaced, on EmptyDir,
// with allowUnsafeRebootstrapOnDeadlock never consulted.
func TestGhostMasterRecoveryRefusesTwoLiveMasters(t *testing.T) {
	state := twoForkedMasters()
	// The ghost-master signature: every Sentinel pinned to a dead address, and no
	// healthy known replica (what a whole-list SENTINEL RESET leaves behind).
	for i, ip := range []string{"10.1.0.1", "10.1.0.2", "10.1.0.3"} {
		state.SentinelNodes[ip] = &redisclient.SentinelNodeState{
			PodName: "sentinel-" + ip, IP: ip, Reachable: true, Monitoring: true,
			MasterIP: fkGhostIP, MasterFlags: RoleMaster,
		}
		_ = i
	}

	// Preconditions asserted, not assumed: a green after the fix must not be green
	// because the fixture drifted into not being this state at all.
	if !state.SentinelsMonitorGhostMaster() {
		t.Fatal("precondition: the quorum must be pinned to a ghost master")
	}
	if state.HasHealthyKnownReplica() {
		t.Fatal("precondition: Sentinel must know no healthy replica")
	}
	if _, diverged, _ := state.BestDataHolder(); diverged {
		t.Fatal("precondition: the union-find must see ONE lineage — that is the defect")
	}

	stuck := time.Now().Add(-60 * time.Second)
	plan := planGhostMasterRecovery(state, 2, false, fkMasterA, &stuck, time.Now(), 30*time.Second)

	if plan.action != recoveryRefuse {
		t.Errorf("planGhostMasterRecovery action = %v, want recoveryRefuse: two live masters "+
			"must never be resolved by electing one of them (elected %q)", plan.action, plan.masterIP)
	}

	// POSITIVE CONTROL: the rule must still break the wedge it exists for. The same
	// signature with ordinary survivors of one dead master must elect, no opt-in.
	safe := twoForkedMasters()
	safe.RedisNodes[fkMasterA].Role = fkSlave
	safe.RedisNodes[fkMasterB].Role = fkSlave
	maps.Copy(safe.SentinelNodes, state.SentinelNodes)
	if got := planGhostMasterRecovery(safe, 2, false, fkMasterA, &stuck, time.Now(), 30*time.Second); got.action != recoveryPromoteSurvivor {
		t.Errorf("positive control: action = %v, want recoveryPromoteSurvivor — the rule must "+
			"still break the deadlock it was built for", got.action)
	}
}

// LR-057, failover mode. planFailover step 5 is structurally identical — ">=1
// holders, one lineage -> promote BestDataHolder, NO opt-in" — so the same state
// elects the same way. Rule 11: the twin is fixed in the same change.
func TestPlanFailoverRefusesTwoLiveMasters(t *testing.T) {
	state := twoForkedMasters()

	if _, diverged, _ := state.BestDataHolder(); diverged {
		t.Fatal("precondition: the union-find must see ONE lineage")
	}

	plan := planFailover(state, "", false, fkMasterA, false, nil, time.Now(), 10*time.Second)
	if plan.action != failoverRefuse {
		t.Errorf("planFailover action = %v, want failoverRefuse: two live masters must never be "+
			"resolved by electing one of them (elected %q)", plan.action, plan.masterIP)
	}

	// POSITIVE CONTROL: ordinary survivors of one dead master must still promote.
	safe := twoForkedMasters()
	safe.RedisNodes[fkMasterA].Role = fkSlave
	safe.RedisNodes[fkMasterB].Role = fkSlave
	if got := planFailover(safe, "", false, fkMasterA, false, nil, time.Now(), 10*time.Second); got.action != failoverPromote {
		t.Errorf("positive control: action = %v, want failoverPromote", got.action)
	}
}
