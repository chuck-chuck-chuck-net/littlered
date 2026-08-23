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
	rangeShard0  = "0-5461"

	// The t3e partial-wipe survivor, verbatim from its own CLUSTER NODES (2026-08-23).
	// See the regression section of changelog LR-043.
	ipSurvivor     = "10.233.192.143"
	idSurvivor     = "66a19469"
	idGhostShard00 = "b79cb312"
	idGhostShard10 = "169b5274"
	idGhostShard20 = "2cc27122"
	idGhostShard21 = "0bb58e88"
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
		// wantAdmissible is the verdict's answer once the address has been POSITIVELY
		// CONFIRMED at the API server (confirmPodIP). It differs from Allowed() for
		// exactly one verdict — `unattributed` — because bus-state inference must not
		// veto a Kubernetes-confirmed own address. See the LR-043 regression section.
		wantAdmissible bool
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
			want: MeetAllowMember, wantAdmissible: true,
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
			want: MeetAllowFresh, wantAdmissible: true,
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
			want: MeetAllowFresh, wantAdmissible: true,
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
			want: MeetDenyUnattributed, wantAdmissible: true,
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
				Slots:    []string{rangeShard0, rangeShard1},
			},
			want: MeetAllowFresh, wantAdmissible: true,
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
			want: MeetAllowFresh, wantAdmissible: true,
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
			want: MeetAllowFresh, wantAdmissible: true,
		},
		{
			// THE LR-043 REGRESSION FIXTURE (t3e, 2026-08-23), reproduced from the live
			// survivor's own CLUSTER NODES. A partial wipe left this pod alive holding the
			// only copy of shard 0's data; every other pod was recycled and came back with
			// a NEW node ID. So the survivor's own view still names its FORMER peers —
			// present as `master,fail?`/`slave,fail?`, which the gossip filter deliberately
			// keeps — and not one of those IDs is in ourNodeIDs. peers > 0 with no anchor
			// ⇒ `unattributed`, and it can never acquire an anchor, because that would
			// require the fresh pods to appear in its view, which requires exactly the MEET
			// being refused. The operator suppressed the MEET 180 times and the cluster
			// never re-converged. Attribution must therefore NOT veto a confirmed address.
			name: "post-wipe survivor whose every peer is a ghost of a recycled pod",
			c: MeetCandidate{
				PodName: podShard01, PodIP: ipSurvivor, NodeID: idSurvivor,
				Identified: true, ViewKnown: true,
				KnownIDs: []string{idSurvivor, idGhostShard00, idGhostShard10, idGhostShard20, idGhostShard21},
				Slots:    []string{rangeShard0},
			},
			want: MeetDenyUnattributed, wantAdmissible: true,
		},
		{
			// The same survivor one reconcile earlier, before Step 0/1 promoted it: still a
			// replica of a master that no longer exists, so it holds no slots. Same verdict,
			// same permanent stall — the hole is in the peer set, not in the slots.
			name: "post-wipe survivor still a slotless replica of a vanished master",
			c: MeetCandidate{
				PodName: podShard01, PodIP: ipSurvivor, NodeID: idSurvivor,
				Identified: true, ViewKnown: true,
				KnownIDs: []string{idSurvivor, idGhostShard00, idGhostShard10},
			},
			want: MeetDenyUnattributed, wantAdmissible: true,
		},
		{
			name: "no address",
			c: MeetCandidate{
				PodName: podShard01, NodeID: "n0", Identified: true, ViewKnown: true,
				KnownIDs: []string{"n0"},
			},
			want: MeetDenyNoAddress, wantAdmissible: false,
		},
		{
			// Unreachable / unidentified: we know nothing about what answers there, so
			// we must not create an identity binding to it.
			name: "unidentified address",
			c: MeetCandidate{
				PodName: podShard01, PodIP: ipOurDeadPod,
				Identified: false, ViewKnown: false,
			},
			want: MeetDenyUnidentified, wantAdmissible: false,
		},
		{
			// Identity probe answered but CLUSTER NODES did not: no view means no
			// attribution evidence at all — deny rather than read it as "isolated".
			name: "identified but no gossip view",
			c: MeetCandidate{
				PodName: podShard01, PodIP: ipOurDeadPod, NodeID: "n0",
				Identified: true, ViewKnown: false,
			},
			want: MeetDenyNoView, wantAdmissible: false,
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
			want: MeetAllowFresh, wantAdmissible: true,
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
			if got.AdmissibleWhenConfirmed() != tc.wantAdmissible {
				t.Errorf("AdmissibleWhenConfirmed() = %v, want %v",
					got.AdmissibleWhenConfirmed(), tc.wantAdmissible)
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
	add("lr-shard-0-0", "10.0.0.1", "n0", true, rangeShard0)
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
	// podShard01 (the recycled IP / unattributed node) is now a target too: attribution
	// no longer vetoes, it warns. The caller MEETs it only after confirmPodIP says the
	// address is still this pod's.
	want := []string{podShard01, podShard10, "lr-shard-2-0"}
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
	if _, ok := skipped[podShard01]; ok {
		t.Errorf("unattributed node %s is in Skipped; it must be a warned target instead", podShard01)
	}
	warned := map[string]MeetVerdict{}
	for _, w := range plan.Unattributed {
		warned[w.PodName] = w.Verdict
	}
	if v := warned[podShard01]; v != MeetDenyUnattributed {
		t.Errorf("unattributed node warned with %q, want %q", v, MeetDenyUnattributed)
	}
	if v := skipped["lr-shard-1-1"]; v != MeetDenyUnidentified {
		t.Errorf("unreachable pod skipped with %q, want %q", v, MeetDenyUnidentified)
	}
}

// TestPlanPartitionMeetsAdmitsUnattributedSeedForConfirmation is the seed half of the
// LR-043 regression fix, and it deliberately INVERTS what the first landing asserted.
//
// The MEET is issued AT the seed, so refusing an unattributable seed refuses the whole
// pass — and after a partial wipe the survivor can BE the seed (with two single-node
// partitions, GetLargestPartitionSeed picks whichever comes first). Vetoing it on bus
// state is then a permanent stall with no way out, which is exactly what the live t3e run
// showed ("no attributable MEET seed this pass"). Kubernetes decides ownership: the plan
// hands the seed over and the caller's confirmPodIP has the final word.
func TestPlanPartitionMeetsAdmitsUnattributedSeedForConfirmation(t *testing.T) {
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
	if plan.Seed == nil {
		t.Fatalf("seed = nil (verdict %q), want it admitted for confirmPodIP to rule on", plan.SeedVerdict)
	}
	if plan.SeedVerdict != MeetDenyUnattributed {
		t.Errorf("SeedVerdict = %q, want %q — the verdict must still be reported, only not enforced",
			plan.SeedVerdict, MeetDenyUnattributed)
	}
	if len(plan.Targets) != 1 || plan.Targets[0].PodName != podShard10 {
		t.Errorf("targets = %+v, want just %s", plan.Targets, podShard10)
	}
}

// TestPlanPartitionMeetsRefusesUnidentifiedSeed pins what the demotion did NOT relax: a
// seed nothing answered for carries no evidence at all and no API-server confirmation can
// supply any, so there is nothing to issue a MEET at. Unlike the unattributed case this is
// self-clearing — the node re-enters the plan on the pass where it answers.
func TestPlanPartitionMeetsRefusesUnidentifiedSeed(t *testing.T) {
	gt := NewClusterGroundTruth()
	gt.Nodes["lr-shard-0-0"] = &ClusterNodeState{
		PodName: "lr-shard-0-0", PodIP: ipOurDeadPod, Role: roleMaster,
		MasterNodeID: "-", Reachable: false,
	}
	gt.Partitions = nil

	plan := gt.PlanPartitionMeets()
	if plan.Seed != nil {
		t.Fatalf("seed = %+v, want nil (nothing answered at that address)", plan.Seed)
	}
	if plan.SeedVerdict.AdmissibleWhenConfirmed() {
		t.Errorf("SeedVerdict %q is admissible-when-confirmed; a view-less seed must never be",
			plan.SeedVerdict)
	}
}

// TestPlanPartitionMeetsAdmitsPostWipeSurvivor reproduces the exact t3e state that this
// regression produced, from the live pods' own CLUSTER NODES output: five pods recycled by
// the LR-023 wipe recovery came back fresh and isolated with NEW node IDs and formed their
// own five-node partition, while the one surviving data-holder sat alone still naming its
// five former peers. None of those five IDs exists any more, so the survivor had no
// "known-ours" anchor and could never gain one — the MEET that would give it one was the
// thing being refused. 180 suppressed MEETs, phase stuck at Initializing for 8 minutes.
func TestPlanPartitionMeetsAdmitsPostWipeSurvivor(t *testing.T) {
	// Node IDs and addresses as observed on t3e, 2026-08-23.
	const survivorPod = "wipe-shard-0-1"
	freshIDs := []string{"2372383b", "36f74258", "a3e52b2f", "1474c0dd", "1ba11fa7"}
	freshPods := []string{
		"wipe-shard-0-0", "wipe-shard-1-0", "wipe-shard-1-1", "wipe-shard-2-0", "wipe-shard-2-1",
	}
	// The survivor's stale peers: recycled pods' PREVIOUS incarnations, still in its
	// node table as `fail?` (pfail is NOT filtered out — only fail/noaddr/handshake are).
	ghostIDs := []string{idGhostShard00, idGhostShard10, idGhostShard20, idGhostShard21}

	gt := NewClusterGroundTruth()
	for i, pod := range freshPods {
		gt.Nodes[pod] = &ClusterNodeState{
			PodName: pod, PodIP: "10.233.192." + string(rune('a'+i)), NodeID: freshIDs[i],
			Role: roleMaster, MasterNodeID: "-", Reachable: true,
		}
		gt.AllNodeIDs[freshIDs[i]] = true
	}
	gt.Nodes[survivorPod] = &ClusterNodeState{
		PodName: survivorPod, PodIP: ipSurvivor, NodeID: idSurvivor,
		Role: roleMaster, MasterNodeID: "-", Slots: []string{rangeShard0}, Reachable: true,
	}
	gt.AllNodeIDs[idSurvivor] = true

	gt.KnownNodes = map[string][]string{}
	for _, id := range freshIDs {
		gt.KnownNodes[id] = append([]string{}, freshIDs...)
	}
	gt.KnownNodes[idSurvivor] = append([]string{idSurvivor}, ghostIDs...)
	gt.Partitions = [][]string{append([]string{}, freshIDs...), {idSurvivor}}

	plan := gt.PlanPartitionMeets()
	if plan.Seed == nil {
		t.Fatalf("seed = nil (verdict %q); the five-node partition must supply one", plan.SeedVerdict)
	}
	var found bool
	for _, tgt := range plan.Targets {
		if tgt.PodName == survivorPod {
			found = true
		}
	}
	if !found {
		skipped := make([]string, 0, len(plan.Skipped))
		for _, s := range plan.Skipped {
			skipped = append(skipped, s.PodName+"="+string(s.Verdict))
		}
		t.Fatalf("survivor %s is not a MEET target; skipped=%v — the partition can never heal",
			survivorPod, skipped)
	}
	if len(plan.Unattributed) != 1 || plan.Unattributed[0].PodName != survivorPod {
		t.Errorf("Unattributed = %+v, want exactly the survivor recorded for the audit log",
			plan.Unattributed)
	}
}
