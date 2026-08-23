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

const (
	idForeign1   = "foreign1"
	idForeign2   = "foreign2"
	podShard10   = "lr-shard-1-0"
	podShard01   = "lr-shard-0-1"
	rangeShard1  = "5462-10922"
	rangeForeign = "0-8191"
)

// TestAttributeMeetTarget is the LR-043 decision table: which addresses the operator
// may CLUSTER MEET. MEET is the one Redis operation that creates a *fresh* identity
// binding (the initiator adopts whatever node ID the responder returns), so a target
// this instance has not attributed to itself must never be MEETed.
func TestAttributeMeetTarget(t *testing.T) {
	// ourIDs: the node IDs this instance identified for its own pod names this pass.
	ourIDs := map[string]bool{"n0": true, "n1": true, "n2": true, idForeign1: true}

	tests := []struct {
		name string
		c    MeetCandidate
		want MeetVerdict
	}{
		{
			// A genuinely partitioned node of ours: it still names peers of ours in
			// its own gossip view (a minority node can only mark them pfail, which
			// keeps them in the view), so it vouches for itself. MUST be MEETed —
			// this is what Step 1 exists to heal.
			name: "partitioned own node knows one of ours",
			c: MeetCandidate{
				PodName: podShard10, PodIP: "10.0.0.11", NodeID: "n1",
				Identified: true, ViewKnown: true,
				KnownIDs: []string{"n1", "n0"},
				Slots:    []string{rangeShard1},
			},
			want: MeetAllowMember,
		},
		{
			// A fresh or wiped pod of ours: single-entry node table, no slots. This is
			// bootstrap's normal case and MUST be MEETed.
			name: "fresh isolated pod of ours",
			c: MeetCandidate{
				PodName: "lr-shard-2-1", PodIP: "10.0.0.21", NodeID: "fresh",
				Identified: true, ViewKnown: true,
				KnownIDs: []string{"fresh"},
			},
			want: MeetAllowFresh,
		},
		{
			// An isolated survivor of ours, still owning its shard's range (e.g. its
			// peers were FORGOTten as ghosts in Step 2). It must be MEETed back or the
			// partition never heals — admitted because it is isolated, NOT because the
			// range aligns (slot alignment is not attribution; see the collapse note in
			// AttributeMeetTarget).
			name: "isolated survivor owning its own shard range",
			c: MeetCandidate{
				PodName: podShard10, PodIP: "10.0.0.11", NodeID: "n1",
				Identified: true, ViewKnown: true,
				KnownIDs: []string{"n1"},
				Slots:    []string{rangeShard1},
			},
			want: MeetAllowFresh,
		},
		{
			// THE HAZARD FIXTURE (LR-043). A recycled pod IP, handed to us stale by the
			// K8s cache under our own pod name, now answers for ANOTHER instance's
			// established cluster node: its node table names peers we never deployed and
			// it owns its own instance's slots. MEETing it merges two clusters.
			name: "foreign established cluster node under one of our pod names",
			c: MeetCandidate{
				PodName: podShard01, PodIP: ipOurDeadPod, NodeID: idForeign1,
				Identified: true, ViewKnown: true,
				KnownIDs: []string{idForeign1, idForeign2, "foreign3"},
				Slots:    []string{rangeForeign},
			},
			want: MeetDenyUnattributed,
		},
		{
			// LR-018 CONSOLIDATED-SHARD STATE, seen in the field (debug-0720, stuck ~19h):
			// an own master owning MORE than one shard range. If it is also isolated and
			// partitioned, Step 1 must still MEET it back — refusing it is a repair step
			// that can never fire, the LR-018/LR-023 shape. Allowed as an isolated node.
			name: "isolated own master owning two consolidated ranges",
			c: MeetCandidate{
				PodName: podShard10, PodIP: "10.0.0.111", NodeID: "n1",
				Identified: true, ViewKnown: true,
				KnownIDs: []string{"n1"},
				Slots:    []string{"0-5461", rangeShard1},
			},
			want: MeetAllowFresh,
		},
		{
			// THE DELIBERATE CONCESSION, pinned so it stays visible. A FOREIGN
			// instance's isolated master — peers dead or forgotten, precisely the LR-023
			// wipe state this operator's own recovery manufactures — is allowed, and no
			// bus-state predicate can do better: it looks exactly like our own isolated
			// pods, whatever slots it holds (a slot-alignment check bought ~nothing here,
			// since GenerateSlotRanges is a pure function of `shards`, while refusing
			// legitimate own nodes — hence removed). This node arrives owning slots and
			// carrying a config epoch, so it CAN take live slots off our masters: that is
			// why confirmPodIP, not this predicate, is the primary guard. LR-043.
			name: "foreign isolated slot owner (indistinguishable from our own survivor)",
			c: MeetCandidate{
				PodName: podShard10, PodIP: ipOurDeadPod, NodeID: "otherinstance",
				Identified: true, ViewKnown: true,
				KnownIDs: []string{"otherinstance"},
				Slots:    []string{rangeShard1},
			},
			want: MeetAllowFresh,
		},
		{
			// THE COST of collapsing the isolated clauses, stated rather than absorbed: a
			// foreign single-node cluster (shards:1) owning 0-16383 with no peers used to
			// be DENIED on range mismatch and is now allowed. That deny was the clause's
			// only genuine one — a foreign cluster with a DIFFERENT shard count — and it
			// is given up in exchange for never refusing a legitimate isolated own node
			// (see the LR-018 row). Reachable only inside the confirmPodIP window.
			name: "foreign isolated slot owner with a misaligned range (deny given up)",
			c: MeetCandidate{
				PodName: podShard10, PodIP: ipOurDeadPod, NodeID: "otherid",
				Identified: true, ViewKnown: true,
				KnownIDs: []string{"otherid"},
				Slots:    []string{"0-16383"},
			},
			want: MeetAllowFresh,
		},
		{
			name: "no address",
			c: MeetCandidate{
				PodName: podShard01, NodeID: "n0", Identified: true, ViewKnown: true,
				KnownIDs: []string{"n0"},
			},
			want: MeetDenyNoAddress,
		},
		{
			// Unreachable / unidentified: we know nothing about what answers there, so
			// we must not create an identity binding to it.
			name: "unidentified address",
			c: MeetCandidate{
				PodName: podShard01, PodIP: ipOurDeadPod,
				Identified: false, ViewKnown: false,
			},
			want: MeetDenyUnidentified,
		},
		{
			// Identity probe answered but CLUSTER NODES did not: no view means no
			// attribution evidence at all — deny rather than read it as "isolated".
			name: "identified but no gossip view",
			c: MeetCandidate{
				PodName: podShard01, PodIP: ipOurDeadPod, NodeID: "n0",
				Identified: true, ViewKnown: false,
			},
			want: MeetDenyNoView,
		},
		{
			// A legacy ({name}-cluster-N) pod name has no per-shard range to compare, so
			// the removed slot-alignment clause could never attribute it and refused it —
			// which would have blocked the legacy→per-shard migration's own MEETs. Now
			// allowed as isolated, uniformly with every other isolated node.
			name: "isolated slot owner with a legacy (non-per-shard) pod name",
			c: MeetCandidate{
				PodName: "lr-cluster-1", PodIP: ipOurDeadPod, NodeID: "n1",
				Identified: true, ViewKnown: true,
				KnownIDs: []string{"n1"},
				Slots:    []string{rangeShard1},
			},
			want: MeetAllowFresh,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := AttributeMeetTarget(tc.c, ourIDs)
			if got != tc.want {
				t.Errorf("AttributeMeetTarget = %q, want %q", got, tc.want)
			}
			if got.Allowed() != tc.want.Allowed() {
				t.Errorf("Allowed() = %v, want %v", got.Allowed(), tc.want.Allowed())
			}
		})
	}
}

// TestPlanPartitionMeets drives the Step 1 adapter over a ground truth that contains
// one genuinely partitioned own node and one recycled IP answering for a foreign
// cluster — the state the LR-043 hazard produces.
func TestPlanPartitionMeets(t *testing.T) {
	gt := NewClusterGroundTruth()
	add := func(pod, ip, id string, reachable bool, slots ...string) {
		gt.Nodes[pod] = &ClusterNodeState{
			PodName: pod, PodIP: ip, NodeID: id, Role: roleMaster,
			MasterNodeID: "-", Slots: slots, Reachable: reachable,
		}
		gt.AllNodeIDs[id] = true
	}
	// Seed partition: n0 + n2 see each other.
	add("lr-shard-0-0", "10.0.0.1", "n0", true, "0-5461")
	add("lr-shard-2-0", "10.0.0.3", "n2", true, "10923-16383")
	// Our own partitioned node: knows n0 (pfail), so attributable.
	add(podShard10, "10.0.0.2", "n1", true, rangeShard1)
	// A recycled IP under one of our replica pod names, answering for a foreign cluster.
	add(podShard01, ipOurDeadPod, idForeign1, true, rangeForeign)
	// An unreachable pod (no identity, no view).
	add("lr-shard-1-1", "10.0.0.12", "", false)

	gt.KnownNodes = map[string][]string{
		"n0":       {"n0", "n2"},
		"n2":       {"n2", "n0"},
		"n1":       {"n1", "n0"},
		idForeign1: {idForeign1, idForeign2, "foreign3"},
	}
	gt.Partitions = [][]string{{"n0", "n2"}, {"n1"}, {idForeign1}}

	plan := gt.PlanPartitionMeets()

	if plan.Seed == nil || plan.Seed.NodeID != "n0" {
		t.Fatalf("seed = %+v, want the largest partition's node n0 (verdict %q)", plan.Seed, plan.SeedVerdict)
	}

	gotTargets := make([]string, 0, len(plan.Targets))
	for _, tgt := range plan.Targets {
		gotTargets = append(gotTargets, tgt.PodName)
	}
	want := []string{podShard10, "lr-shard-2-0"}
	if len(gotTargets) != len(want) {
		t.Fatalf("targets = %v, want %v", gotTargets, want)
	}
	for i := range want {
		if gotTargets[i] != want[i] {
			t.Fatalf("targets = %v, want %v (deterministic pod-name order)", gotTargets, want)
		}
	}

	skipped := map[string]MeetVerdict{}
	for _, s := range plan.Skipped {
		skipped[s.PodName] = s.Verdict
	}
	if v := skipped[podShard01]; v != MeetDenyUnattributed {
		t.Errorf("foreign recycled IP skipped with %q, want %q", v, MeetDenyUnattributed)
	}
	if v := skipped["lr-shard-1-1"]; v != MeetDenyUnidentified {
		t.Errorf("unreachable pod skipped with %q, want %q", v, MeetDenyUnidentified)
	}
}

// TestPlanPartitionMeetsRefusesUnattributableSeed pins the other half: the MEET is
// issued AT the seed, so a seed we cannot attribute would be told to meet all of our
// pods — the same merge, in the other direction. No seed ⇒ no MEETs this pass.
func TestPlanPartitionMeetsRefusesUnattributableSeed(t *testing.T) {
	gt := NewClusterGroundTruth()
	gt.Nodes["lr-shard-0-0"] = &ClusterNodeState{
		PodName: "lr-shard-0-0", PodIP: ipOurDeadPod, NodeID: idForeign1, Role: roleMaster,
		MasterNodeID: "-", Slots: []string{rangeForeign}, Reachable: true,
	}
	gt.Nodes[podShard10] = &ClusterNodeState{
		PodName: podShard10, PodIP: "10.0.0.2", NodeID: "n1", Role: roleMaster,
		MasterNodeID: "-", Slots: []string{rangeShard1}, Reachable: true,
	}
	gt.AllNodeIDs[idForeign1] = true
	gt.AllNodeIDs["n1"] = true
	gt.KnownNodes = map[string][]string{
		idForeign1: {idForeign1, idForeign2},
		"n1":       {"n1"},
	}
	gt.Partitions = [][]string{{idForeign1}, {"n1"}}

	plan := gt.PlanPartitionMeets()
	if plan.Seed != nil {
		t.Fatalf("seed = %+v, want nil (unattributable seed)", plan.Seed)
	}
	if plan.SeedVerdict != MeetDenyUnattributed {
		t.Errorf("SeedVerdict = %q, want %q", plan.SeedVerdict, MeetDenyUnattributed)
	}
	if len(plan.Targets) != 0 {
		t.Errorf("targets = %v, want none when the seed is refused", plan.Targets)
	}
}
