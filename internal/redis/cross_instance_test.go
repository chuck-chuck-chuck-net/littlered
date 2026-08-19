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

import (
	"reflect"
	"testing"
)

const (
	ipForeignMaster  = "10.9.9.9"
	ipForeignReplica = "10.9.9.8"
	ipOurDeadPod     = "10.0.0.9"
)

// The evidence this reports is what `lrctl verify` shows a human who is already
// suspicious. Each signal carries a false-positive filter, because the states it must
// NOT fire on are ordinary: a recently failed-over instance has a dead ex-master and
// dead ex-replica entries that are equally "not one of our pods".
//
// The discriminator throughout is `s_down`/`o_down`. A *dead* address is Sentinel's
// normal debris. An address that is not ours AND is reported healthy means something
// else is alive there — which is precisely the captured state, where the stolen master
// looks fine and therefore never triggers a failover.
func TestDetectCrossInstance(t *testing.T) {
	ourPods := map[string]bool{"10.0.0.1": true, ipPod1: true, "10.0.0.3": true}

	sentinel := func(pod, masterIP, masterFlags string, peers, slaves int, reps ...ReplicaInfo) *SentinelNodeState {
		return &SentinelNodeState{
			PodName: pod, Monitoring: true, Reachable: true,
			MasterIP: masterIP, MasterFlags: masterFlags,
			NumOtherSentinels: peers, NumSlaves: slaves, Replicas: reps,
		}
	}
	state := func(nodes ...*SentinelNodeState) *SentinelClusterState {
		s := &SentinelClusterState{
			SentinelNodes: map[string]*SentinelNodeState{},
			ValidIPs:      ourPods,
			RedisNodes:    map[string]*RedisNodeState{},
		}
		for i, n := range nodes {
			s.SentinelNodes[string(rune('a'+i))] = n
		}
		return s
	}

	t.Run("healthy instance reports nothing", func(t *testing.T) {
		s := state(
			sentinel("s-0", "10.0.0.1", "master", 2, 2,
				ReplicaInfo{IP: ipPod1}, ReplicaInfo{IP: "10.0.0.3"}),
			sentinel("s-1", "10.0.0.1", "master", 2, 2),
		)
		if got := s.DetectCrossInstance(3, 2); got.Any() {
			t.Fatalf("clean instance produced evidence: %+v", got)
		}
	})

	// The incident: A's Sentinels monitor B's master, which is alive and healthy.
	t.Run("captured instance reports the foreign master", func(t *testing.T) {
		s := state(sentinel("s-0", ipForeignMaster, "master", 8, 6))
		got := s.DetectCrossInstance(3, 2)
		if !reflect.DeepEqual(got.ForeignMasterIPs, []string{ipForeignMaster}) {
			t.Errorf("ForeignMasterIPs = %v, want [10.9.9.9]", got.ForeignMasterIPs)
		}
		if len(got.PeerSurplus) != 1 || got.PeerSurplus[0].Reported != 8 || got.PeerSurplus[0].Expected != 2 {
			t.Errorf("PeerSurplus = %+v, want one entry reported=8 expected=2", got.PeerSurplus)
		}
		if len(got.ReplicaSurplus) != 1 || got.ReplicaSurplus[0].Reported != 6 || got.ReplicaSurplus[0].Expected != 2 {
			t.Errorf("ReplicaSurplus = %+v, want one entry reported=6 expected=2", got.ReplicaSurplus)
		}
	})

	// A dead ex-master after a failover is ordinary debris, not a foreign deployment.
	t.Run("a dead ghost master is not reported as foreign", func(t *testing.T) {
		s := state(sentinel("s-0", ipOurDeadPod, "s_down,o_down,master", 2, 2))
		if got := s.DetectCrossInstance(3, 2); got.Any() {
			t.Fatalf("dead ghost master reported as cross-instance evidence: %+v", got)
		}
	})

	t.Run("live foreign replicas are reported, dead ones are not", func(t *testing.T) {
		s := state(sentinel("s-0", "10.0.0.1", "master", 2, 2,
			ReplicaInfo{IP: ipPod1},                              // ours
			ReplicaInfo{IP: ipOurDeadPod, Flags: "s_down,slave"}, // our dead ex-pod: debris
			ReplicaInfo{IP: ipForeignReplica, Flags: "slave"},    // alive and not ours
		))
		got := s.DetectCrossInstance(3, 2)
		if !reflect.DeepEqual(got.ForeignReplicaIPs, []string{ipForeignReplica}) {
			t.Errorf("ForeignReplicaIPs = %v, want [10.9.9.8]", got.ForeignReplicaIPs)
		}
	})

	t.Run("results are deduplicated and sorted for stable output", func(t *testing.T) {
		s := state(
			sentinel("s-0", ipForeignMaster, "master", 2, 2, ReplicaInfo{IP: "10.9.9.7"}),
			sentinel("s-1", ipForeignMaster, "master", 2, 2, ReplicaInfo{IP: "10.9.9.5"}),
		)
		got := s.DetectCrossInstance(3, 2)
		if !reflect.DeepEqual(got.ForeignMasterIPs, []string{ipForeignMaster}) {
			t.Errorf("ForeignMasterIPs = %v, want deduped [10.9.9.9]", got.ForeignMasterIPs)
		}
		if !reflect.DeepEqual(got.ForeignReplicaIPs, []string{"10.9.9.5", "10.9.9.7"}) {
			t.Errorf("ForeignReplicaIPs = %v, want sorted [10.9.9.5 10.9.9.7]", got.ForeignReplicaIPs)
		}
	})

	// An unreachable or bare Sentinel has no view to contribute; counting it would
	// invent evidence out of a gather failure.
	t.Run("unreachable and bare sentinels contribute nothing", func(t *testing.T) {
		bare := &SentinelNodeState{PodName: "s-1", Reachable: true, Monitoring: false}
		down := &SentinelNodeState{PodName: "s-2", Reachable: false, MasterIP: ipForeignMaster, NumOtherSentinels: 99}
		if got := state(bare, down).DetectCrossInstance(3, 2); got.Any() {
			t.Fatalf("unreachable/bare sentinels produced evidence: %+v", got)
		}
	})

	// Fewer peers than deployed is a different problem (a partition, a restart) and is
	// not this check's business — it must not masquerade as a collision.
	t.Run("a deficit is not a surplus", func(t *testing.T) {
		s := state(sentinel("s-0", "10.0.0.1", "master", 0, 0))
		if got := s.DetectCrossInstance(3, 2); got.Any() {
			t.Fatalf("a peer/replica deficit was reported as evidence: %+v", got)
		}
	})
}
