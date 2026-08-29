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
	"context"
	"errors"
	"testing"
	"time"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// LR-053 — a terminating pod of OURS is still ours.
//
// `ValidIPs` answered two different questions with one set (concept R5). As "does
// this address count as live topology?" it is right, and LR-038 requires the
// terminating-pod filter it carries. As "is this address ours?" it is wrong by
// exactly that filter: the instant we delete a pod its object carries a
// deletionTimestamp, the gather stops probing it, and its address — which the pod
// still holds and still answers on for the whole preStop window — reads as
// somebody else's.
//
// That is the LR-050 shape at the level of the concept rather than of the timing.
// LR-050 closed it with a rollout gate, and the gate is a margin over the
// StatefulSet's settledness; these tests pin the state the gate cannot see, with
// `rolling` explicitly FALSE, so they fail if the split is ever undone even while
// the gate stands.
//
// Reachability, stated honestly rather than dramatised: the operator's own
// StatefulSet reads settled while one of its pods is Terminating-but-Ready
// (`status.replicas` and `status.readyReplicas` both still count such a pod, and a
// plain pod delete bumps no generation), which in sentinel mode is the master's
// whole preStop window — measured at ~21s in LR-048. Whether a verdict is actually
// reached from there additionally needs the signature to outlast
// `forsakenCooldown`, which an ordinary graceful handover ends long before. So this
// is a LATENT defect closed at its source, on LR-043's precedent, not an observed
// incident.

const (
	// The instance: a master we have just deleted, two live replicas, three Sentinels.
	ownedTerminatingMaster = "10.0.0.1"
	ownedLiveReplica1      = "10.0.0.2"
	ownedLiveReplica2      = "10.0.0.3"
	ownedSentinel1         = "10.0.1.1"
	ownedSentinel2         = "10.0.1.2"
	ownedSentinel3         = "10.0.1.3"
)

// terminatingMasterGatherer answers for the pods the gather is handed and refuses
// everything else. The terminating master is deliberately NOT among them: the
// operator's pod maps exclude it (LR-038), which is precisely why its address had
// no set of its own to belong to.
type terminatingMasterGatherer struct{ sentinels map[string]bool }

func (g *terminatingMasterGatherer) GetRedisState(
	_ context.Context, podName, ip string,
) (*redisclient.RedisNodeState, error) {
	return &redisclient.RedisNodeState{
		PodName: podName, IP: ip, Reachable: true, Role: roleSlave,
		MasterHost: ownedTerminatingMaster, LinkStatus: "up",
	}, nil
}

func (g *terminatingMasterGatherer) GetSentinelState(
	_ context.Context, podName, ip, _ string,
) (*redisclient.SentinelNodeState, error) {
	if !g.sentinels[ip] {
		return nil, errors.New("not a sentinel")
	}
	// Every Sentinel still names the pod we are deleting as the master, with clean
	// flags: it is alive and answering (its preStop is still running), so Sentinel
	// has no reason to flag it down for a whole down-after-milliseconds.
	return &redisclient.SentinelNodeState{
		PodName: podName, IP: ip, Reachable: true, Monitoring: true,
		MasterIP: ownedTerminatingMaster, MasterFlags: RoleMaster,
		MonitoredMasters: []redisclient.MonitoredMaster{
			{Name: "ns.inst", IP: ownedTerminatingMaster, Flags: RoleMaster},
		},
	}, nil
}

func (g *terminatingMasterGatherer) GetClusterID(context.Context, string, string) (string, error) {
	return "", errors.New("not a cluster")
}

func (g *terminatingMasterGatherer) GetClusterInfo(
	context.Context, string, string,
) (*redisclient.ClusterInfo, error) {
	return nil, errors.New("not a cluster")
}

func (g *terminatingMasterGatherer) GetClusterNodes(
	context.Context, string, string,
) ([]redisclient.ClusterNodeInfo, error) {
	return nil, errors.New("not a cluster")
}

// gatherTerminatingMasterFleet runs the REAL gather, so the attribution set is
// exercised as the reconciler supplies it — through the parameter — rather than
// hand-built on the state, which could be made to say anything (LR-051's precedent).
func gatherTerminatingMasterFleet() *redisclient.ReplicationState {
	redisPods := map[string]string{
		// The terminating master is absent: DeletionTimestamp != nil, so
		// reconcileSentinelCluster does not put it in the probe map.
		ownedLiveReplica1: "redis-1",
		ownedLiveReplica2: "redis-2",
	}
	sentinelPods := map[string]string{
		ownedSentinel1: "sentinel-0",
		ownedSentinel2: "sentinel-1",
		ownedSentinel3: "sentinel-2",
	}
	// ...but it IS one of ours, and the reconciler knows it: ownedIPs is built from
	// the unfiltered pod list.
	owned := map[string]bool{
		ownedTerminatingMaster: true,
		ownedLiveReplica1:      true,
		ownedLiveReplica2:      true,
		ownedSentinel1:         true,
		ownedSentinel2:         true,
		ownedSentinel3:         true,
	}
	g := &terminatingMasterGatherer{sentinels: map[string]bool{
		ownedSentinel1: true, ownedSentinel2: true, ownedSentinel3: true,
	}}
	return redisclient.GatherReplicationState(
		context.Background(), g, redisPods, sentinelPods, "ns.inst", owned)
}

// TestTerminatingPodOfOursIsNotAForeignCaptor is the headline assertion: our own
// pod, one second after we deleted it, must never be read as another deployment's
// live master.
func TestTerminatingPodOfOursIsNotAForeignCaptor(t *testing.T) {
	state := gatherTerminatingMasterFleet()

	// Preconditions, asserted rather than assumed — without them a green below could
	// be green for the wrong reason.
	if state.IsGhost(ownedTerminatingMaster) != true {
		t.Fatalf("precondition: the terminating master must NOT be live topology "+
			"(IsGhost = %v, want true) — LR-038's filter is what this test is about",
			state.IsGhost(ownedTerminatingMaster))
	}
	for _, rn := range state.RedisNodes {
		if rn.Role == RoleMaster {
			t.Fatalf("precondition: no reachable pod of ours may be a master (clause 4), got %s", rn.PodName)
		}
	}
	if len(state.SentinelNodes) != 3 {
		t.Fatalf("precondition: want 3 gathered Sentinels, got %d", len(state.SentinelNodes))
	}

	if !state.IsOurs(ownedTerminatingMaster) {
		t.Errorf("IsOurs(%s) = false, want true — a pod of ours that is terminating is still ours",
			ownedTerminatingMaster)
	}

	// rolling = false ON PURPOSE. LR-050's gate is a margin over the StatefulSet's
	// settledness and it is not what this test is about; the point is that the
	// verdict must not rest on it.
	got := planForsaken(state, nil, time.Now(), false)
	if got.Captured {
		t.Errorf("planForsaken.Captured = true (foreign_master=%s), want false: the address is our own "+
			"terminating master, not another deployment's", got.ForeignMaster)
	}
}

// TestStaleMasterNameG5DoesNotCallOurTerminatingPodForeign is the same address, the
// same window, Rule N's copy of the same discriminator. G5 refuses to prune in
// either reading, so what is at stake is the sentence: `Foreign` emits a Warning
// telling the owner they may be captured and must not rename — at the moment their
// own pod is merely on its way out.
func TestStaleMasterNameG5DoesNotCallOurTerminatingPodForeign(t *testing.T) {
	state := gatherTerminatingMasterFleet()
	// A leftover name from an earlier rename, pointing at the pod we are deleting.
	for _, sn := range state.SentinelNodes {
		sn.MonitoredMasters = append(sn.MonitoredMasters, redisclient.MonitoredMaster{
			Name: staleName, IP: ownedTerminatingMaster, Flags: RoleMaster,
		})
	}

	got := planStaleMasterNames(state, "ns.inst", 2, false, false)
	if got.Reason == staleNamesForeign {
		t.Errorf("planStaleMasterNames.Reason = %q, want anything but Foreign: %s",
			got.Reason, got.Message)
	}
}

// TestOwnedIPsFailSafeWhenTheCallerSuppliesNothing pins the zero value's direction.
//
// The dangerous mistake with a new attribution set is not forgetting to widen it, it
// is a caller passing nil and every monitored address then reading as foreign —
// capture verdicts manufactured everywhere. The gather unions the probed addresses
// in, so nil degrades to exactly the pre-split behaviour instead.
func TestOwnedIPsFailSafeWhenTheCallerSuppliesNothing(t *testing.T) {
	g := &terminatingMasterGatherer{sentinels: map[string]bool{ownedSentinel1: true}}
	state := redisclient.GatherReplicationState(context.Background(), g,
		map[string]string{ownedLiveReplica1: "redis-1"},
		map[string]string{ownedSentinel1: "sentinel-0"}, "ns.inst", nil)

	for _, ip := range []string{ownedLiveReplica1, ownedSentinel1} {
		if !state.IsOurs(ip) {
			t.Errorf("IsOurs(%s) = false with a nil ownedIPs, want true: a probed address is "+
				"always one of ours, or the zero value manufactures foreign masters", ip)
		}
	}
	if state.IsOurs(ownedTerminatingMaster) {
		t.Errorf("IsOurs(%s) = true, want false: nothing told the gather about it", ownedTerminatingMaster)
	}
}

// TestPlanForsakenCannotArmWhileAVictimPodHoldsItsOwnData is the e2e failure of
// 2026-08-29 reproduced at the planner level, and it is NOT a defect of the
// LR-053 split — it is LR-050's accepted residual, reached from a direction that
// entry did not anticipate. Read it as a characterisation test: it pins what the
// operator does today, which is nothing.
//
// The state is the `HoldDataPresent` fixture of `Sentinel Forsaken-Gated Quarantine`
// and of `Sentinel Master Name Rename Under Capture`: a victim pod pre-armed with a
// bogus `masterauth` so its sync from the captor's master can never succeed, and
// which therefore still holds the victim's OWN keys — genuinely the only copy in
// existence, which is exactly what LR-044's `atRisk` clause exists to protect.
//
// THE TENSION, which is the finding: a pod that could not sync from the captor has
// `master_link_status:down`, and sentinel-mode readiness requires `role:master` or
// `link:up`. So the pod is not Ready, so `ReadyReplicas < Replicas`, so
// `statefulSetRolloutSettled` is false, so LR-050's gate withholds attribution and
// `planForsaken` cannot ARM. **The very state `atRisk` exists to protect is a state
// that makes the instance unsettled.** The verdict can only ever be armed by winning
// a race between the operator's first post-capture gather and the readiness probe —
// which is what both e2e tiers had been doing, undeclared.
//
// Measured on t3e (2026-08-29, this milestone's investigation), same recipe both
// times, capture signature verified unanimous on all three Sentinels:
//
//	race WON  — injected while Ready 3/3: forsakenSince at +10s, readiness fell to
//	            2/3 at +23s, True/QuarantineRefusedDataPresent at +44s.
//	race LOST — pinned pod made not-Ready BEFORE the injection: no condition at all,
//	            80s+, on BOTH the split build (8898ff0) and the PRE-split build
//	            (6340724). Identical. The split is not implicated.
//
// The failure direction is the safe one — no verdict means no quarantine, so no pod
// is deleted and the data survives — but the capture goes undiagnosed and the CAPTOR
// stays poisoned, which is the whole point of ADR-016. Recorded here rather than
// fixed: narrowing the gate to "a pod object of ours is missing" and dropping the
// readiness clause was considered and is WRONG, because a pod replaced at the same
// ordinal returns with a new IP while `status.replicas` is already back to full — the
// old address is still in the air and only the readiness clause still says so. That
// is LR-050's own reason for the clause, and it holds.
func TestPlanForsakenCannotArmWhileAVictimPodHoldsItsOwnData(t *testing.T) {
	const foreignMaster = "10.9.9.9"

	// The full capture signature: every Sentinel of ours unanimously serving the
	// captor's live master, and no pod of ours a master any more.
	captured := func() *redisclient.ReplicationState {
		s := redisclient.NewReplicationState()
		for _, ip := range []string{ownedTerminatingMaster, ownedLiveReplica1, ownedLiveReplica2,
			ownedSentinel1, ownedSentinel2, ownedSentinel3} {
			s.AddLiveTopologyIP(ip)
		}
		for _, sip := range []string{ownedSentinel1, ownedSentinel2, ownedSentinel3} {
			s.SentinelNodes[sip] = &redisclient.SentinelNodeState{
				PodName: "sentinel-" + sip, IP: sip, Reachable: true, Monitoring: true,
				MasterIP: foreignMaster, MasterFlags: RoleMaster,
			}
		}
		for _, rip := range []string{ownedTerminatingMaster, ownedLiveReplica1, ownedLiveReplica2} {
			s.RedisNodes[rip] = &redisclient.RedisNodeState{
				PodName: "redis-" + rip, IP: rip, Reachable: true, Role: roleSlave,
				MasterHost: foreignMaster, LinkStatus: "down",
			}
		}
		return s
	}

	// Precondition: settled, this state IS a capture. Without this row the assertion
	// below could pass because the fixture is simply not a capture at all.
	if got := planForsaken(captured(), nil, time.Now(), false); !got.Captured {
		t.Fatalf("precondition: a settled instance must read this as captured, got %+v", got)
	}

	// The reported state: the pinned pod's broken link makes the StatefulSet unsettled,
	// so nothing arms — and with nothing armed the gate keeps withholding, forever.
	got := planForsaken(captured(), nil, time.Now(), true)
	if got.Captured || got.Forsaken {
		t.Errorf("planForsaken = %+v, want the empty verdict: LR-050's gate withholds "+
			"attribution while our own StatefulSet is unsettled, and a victim pod that "+
			"cannot sync from the captor is exactly what makes it unsettled", got)
	}
}
