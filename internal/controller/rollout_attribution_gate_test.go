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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// LR-050 — while our own Redis StatefulSet is mid-rollout, the operator does not
// ATTRIBUTE addresses.
//
// The measured defect (design §16): a supported rename of a healthy sentinel instance,
// with no other Sentinel deployment anywhere on the cluster, drove a settled
// Forsaken=True and quarantined it — scaling both StatefulSets to 0 with EmptyDir
// storage. One address was called the instance's OWN ghost master and, two seconds
// later, a foreign captor.
//
// The window is 42.5s: preStop stall ~21s + down-after-milliseconds 30s + election
// ~1.5s, against a 30s forsakenCooldown. The verdict fires at T+30, i.e. 12.5s BEFORE
// the instance heals itself.
//
// A just-replaced pod of ours and a captor's master are structurally indistinguishable
// from Sentinel's vantage — both are absent from ValidIPs, both unflagged. The
// discriminator that DOES exist is not in the gather at all: whether the pod that used
// to hold that address is one we are in the middle of replacing. The StatefulSet
// answers exactly that, and needs no new state.
//
// The gate suppresses ARMING a verdict. It must NOT make planForsaken return "not
// captured", because the call site's default branch calls clearForsaken — so a naive
// "no verdict while rolling" would RETRACT a capture that was correctly diagnosed
// before the rename, reopening the §7.3 trap WP4b exists to close. The invariant rows
// below are the ones that pin this.

// justReplacedIP is the address of a pod the rollout has just taken down: no longer in
// ValidIPs (the pod object is gone), and not yet flagged down by Sentinel, because
// s_down needs a whole down-after-milliseconds.
const justReplacedIP = "10.233.192.110"

// k9Shape is WP0's measured rename window, at t0+89.1s: the quorum is monitoring again
// (Rule 0 re-registered it), all three agree on ONE address, that address is the
// just-replaced redis-0 — ghost, not flagged down — and no reachable pod of ours is a
// master. All four planForsaken clauses hold.
func k9Shape() *redisclient.ReplicationState {
	s := redisclient.NewReplicationState()
	for _, ip := range []string{ipMaster, ipReplica, senIP1, senIP2, senIP3} {
		s.ValidIPs[ip] = true
	}
	for _, sip := range []string{senIP1, senIP2, senIP3} {
		withMonitoredSentinel(s, sip, true, justReplacedIP, "master",
			mon(desiredName, justReplacedIP, "master", ""))
	}
	// The two pods that are up are wait-looping replicas of an address nobody serves.
	s.RedisNodes[ipMaster] = &redisclient.RedisNodeState{
		PodName: podRedis1, IP: ipMaster, Reachable: true, Role: roleSlave,
		MasterHost: justReplacedIP, LinkStatus: linkStatusDown,
	}
	s.RedisNodes[ipReplica] = &redisclient.RedisNodeState{
		PodName: podRedis2, IP: ipReplica, Reachable: true, Role: roleSlave,
		MasterHost: justReplacedIP, LinkStatus: linkStatusDown,
	}
	return s
}

// bareShape is the same instance one moment later: the Sentinels have been reset and
// monitor nothing, so the capture SIGNATURE is gone. Used only by the "cannot clear"
// invariant row.
func bareShape() *redisclient.ReplicationState {
	s := redisclient.NewReplicationState()
	for _, ip := range []string{ipMaster, ipReplica, senIP1, senIP2, senIP3} {
		s.ValidIPs[ip] = true
	}
	for _, sip := range []string{senIP1, senIP2, senIP3} {
		withMonitoredSentinel(s, sip, false, "", "")
	}
	return s
}

func TestPlanForsakenRolloutGate(t *testing.T) {
	now := time.Date(2026, 8, 26, 18, 31, 57, 0, time.UTC)
	ago := func(d time.Duration) *metav1.Time { t := metav1.NewTime(now.Add(-d)); return &t }

	cases := []struct {
		name         string
		state        *redisclient.ReplicationState
		since        *metav1.Time
		rolling      bool
		wantCaptured bool
		wantForsaken bool
	}{
		{
			// THE DEFECT. Nothing else on this cluster; the "captor" is our own pod,
			// 0.6s into being replaced by the rename's rollout.
			name:  "K9: the rename window must not arm a capture verdict while we are rolling",
			state: k9Shape(), since: nil, rolling: true,
			wantCaptured: false, wantForsaken: false,
		},
		{
			// The positive control for the row above: the SAME gather on a SETTLED
			// instance is a genuine capture and must still arm. Without this row the
			// gate could be a blanket suppression and the table would not notice.
			name:  "the same signature on a settled instance is still a capture",
			state: k9Shape(), since: nil, rolling: false,
			wantCaptured: true, wantForsaken: false,
		},
		{
			// THE INVARIANT. A capture diagnosed on a settled instance stays diagnosed
			// through the roll it triggers — an owner who panics and renames a captured
			// instance (design §7.3) must not thereby retract the verdict, because the
			// quarantine is the only thing that heals the CAPTOR. e2e tier 2 asserts
			// exactly this on a live cluster.
			name:  "INVARIANT: an already-armed verdict survives a roll (signature present)",
			state: k9Shape(), since: ago(time.Hour), rolling: true,
			wantCaptured: true, wantForsaken: true,
		},
		{
			// And it survives even when the signature itself has momentarily gone: the
			// gate withholds attribution in BOTH directions, so a roll can neither
			// start a verdict nor end one. Retracting here would reach clearForsaken.
			name:  "INVARIANT: an already-armed verdict survives a roll (signature absent)",
			state: bareShape(), since: ago(time.Hour), rolling: true,
			wantCaptured: true, wantForsaken: true,
		},
		{
			// Nothing armed, nothing observed: the gate must not manufacture a verdict
			// out of its own hold.
			name:  "a roll on an unarmed instance with no signature is not a verdict",
			state: bareShape(), since: nil, rolling: true,
			wantCaptured: false, wantForsaken: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planForsaken(tc.state, tc.since, now, tc.rolling)
			if got.Captured != tc.wantCaptured {
				t.Errorf("Captured = %v, want %v", got.Captured, tc.wantCaptured)
			}
			if got.Forsaken != tc.wantForsaken {
				t.Errorf("Forsaken = %v, want %v", got.Forsaken, tc.wantForsaken)
			}
		})
	}
}

// Rule N's G5 half of the same defect (design §9.2): the discriminator
// `!ValidIPs[ip] && !flaggedDown(flags)` is byte-identical to planForsaken clause 3, so
// during the same window a stale entry pointing at the just-replaced pod reads as
// somebody else's master and the operator emits a Warning reading "this instance may be
// captured — do not rename to escape a capture" at the exact moment the owner is
// performing the rename the runbook asked for.
//
// While rolling, such an address is UNATTRIBUTABLE, not foreign: Deferred, naming the
// gate. No accusation, no Warning — and, unchanged, no prune. Note the deliberate
// asymmetry: Rule N still RUNS during churn (§7.5, which is the whole point of it
// sitting before Rule A), it just stops attributing.
func TestPlanStaleMasterNamesRolloutGate(t *testing.T) {
	t.Run("while rolling, a departed address of ours is unattributable, not foreign", func(t *testing.T) {
		got := planStaleMasterNames(renameWindow(), desiredName, 2, false, true)
		if got.Reason != staleNamesDeferred {
			t.Errorf("Reason = %q, want %q (message %q)", got.Reason, staleNamesDeferred, got.Message)
		}
		if !strings.Contains(got.Message, "G5") || !strings.Contains(got.Message, "rollout") {
			t.Errorf("message must name the gate and the rollout, got %q", got.Message)
		}
		if len(got.Prune) != 0 {
			t.Errorf("Prune = %v, want nothing pruned", got.Prune)
		}
	})

	t.Run("the same entry on a settled instance is still Foreign", func(t *testing.T) {
		got := planStaleMasterNames(renameWindow(), desiredName, 2, false, false)
		if got.Reason != staleNamesForeign {
			t.Errorf("Reason = %q, want %q", got.Reason, staleNamesForeign)
		}
	})

	t.Run("the gate does not stand Rule N down: an attributable stale entry still prunes", func(t *testing.T) {
		s := staleBase()
		for _, sen := range []struct{ ip, pod string }{
			{senIP1, senPod1}, {senIP2, senPod2}, {senIP3, senPod3},
		} {
			setMasters(s, sen.ip, sen.pod,
				mon(desiredName, ipMaster, "master", ""),
				mon(staleName, ipMaster, "master", ""))
		}
		got := planStaleMasterNames(s, desiredName, 2, false, true)
		if got.Reason != staleNamesPruning {
			t.Errorf("Reason = %q, want %q (message %q)", got.Reason, staleNamesPruning, got.Message)
		}
		if len(got.Prune) != 3 {
			t.Errorf("Prune = %d entries, want 3", len(got.Prune))
		}
	})
}
