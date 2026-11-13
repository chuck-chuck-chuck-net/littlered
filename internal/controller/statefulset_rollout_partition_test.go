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
)

// The sentinel/failover StatefulSets' partition cursor (ADR-017 Amendment, LR-065) is the
// cluster planner with the redundancy clause dropped and the availability clause added. These
// rows are the availability gate: the next pod is released only once every pod at or above the
// partition is at UpdateRevision, Ready, and has been Ready for minReadySeconds — the gate the
// StatefulSet controller itself applied before Kubernetes v1.37 began deleting every
// not-yet-available old pod at once.

const availMinReady = int32(35) // sentinel-mode Redis default

// availPod is a pod with no redundancy facts at all — these StatefulSets have none to offer.
func availPod(ordinal int, rev string, ready bool, readyFor time.Duration) shardRolloutPod {
	p := shardRolloutPod{Ordinal: ordinal, Revision: rev, Ready: ready}
	if readyFor > 0 {
		p.ReadySince = rolloutNow.Add(-readyFor)
	}
	return p
}

func availRolling(partition *int32, pods ...shardRolloutPod) shardRolloutInput {
	return shardRolloutInput{
		HighestOrdinal: 2, MinReadySeconds: availMinReady, SkipRedundancyClause: true,
		DesiredHash: "h2", AppliedHash: "h2",
		Generation: 7, ObservedGeneration: 7,
		UpdateRevision: rolloutRevNew, CurrentRevision: rolloutRevOld,
		AppliedPartition: partition, Pods: pods, Now: rolloutNow,
	}
}

func TestPlanStatefulSetAvailabilityPartition(t *testing.T) {
	const old, upd = rolloutRevOld, rolloutRevNew
	long := time.Hour
	tests := []struct {
		name        string
		in          shardRolloutInput
		wantVerdict shardRolloutVerdict
		wantPart    *int32
		wantHold    shardRolloutHold
	}{
		{
			name: "settled at the desired template: partition 0, nothing to gate (creation, steady state)",
			in: shardRolloutInput{
				HighestOrdinal: 2, MinReadySeconds: availMinReady, SkipRedundancyClause: true,
				DesiredHash: "h1", AppliedHash: "h1", Generation: 1, ObservedGeneration: 1,
				UpdateRevision: old, CurrentRevision: old, Now: rolloutNow,
			},
			wantVerdict: rolloutComplete, wantPart: new(int32(0)),
		},
		{
			name: "template change first seen: gate at the highest ordinal",
			in: shardRolloutInput{
				HighestOrdinal: 2, MinReadySeconds: availMinReady, SkipRedundancyClause: true,
				DesiredHash: "h2", AppliedHash: "h1", Generation: 6, ObservedGeneration: 6,
				UpdateRevision: old, CurrentRevision: old, AppliedPartition: new(int32(0)),
				Pods: []shardRolloutPod{availPod(0, old, true, long), availPod(1, old, true, long), availPod(2, old, true, long)},
				Now:  rolloutNow,
			},
			wantVerdict: rolloutStart, wantPart: new(int32(2)),
		},
		{
			name: "STS without the hash annotation yet (upgrade): treated as a change, gate at the top",
			in: shardRolloutInput{
				HighestOrdinal: 2, MinReadySeconds: availMinReady, SkipRedundancyClause: true,
				DesiredHash: "h2", AppliedHash: "", Generation: 6, ObservedGeneration: 6,
				UpdateRevision: old, CurrentRevision: old, Now: rolloutNow,
			},
			wantVerdict: rolloutStart, wantPart: new(int32(2)),
		},
		{
			// THE ROW THAT MATTERS: v1.37 deletes every not-yet-available old pod at once,
			// so the replacement must be AVAILABLE, not merely Ready, before the next goes.
			name: "top pod updated and Ready for 2s (< minReadySeconds): hold",
			in: availRolling(new(int32(2)),
				availPod(0, old, true, long), availPod(1, old, true, long), availPod(2, upd, true, 2*time.Second)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdAvailability,
		},
		{
			name: "top pod updated and available: release exactly one more",
			in: availRolling(new(int32(2)),
				availPod(0, old, true, long), availPod(1, old, true, long), availPod(2, upd, true, 40*time.Second)),
			wantVerdict: rolloutAdvance, wantPart: new(int32(1)),
		},
		{
			name: "Ready with no readiness timestamp is not evidence of availability: hold",
			in: availRolling(new(int32(2)),
				availPod(0, old, true, long), availPod(1, old, true, long), availPod(2, upd, true, 0)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdAvailability,
		},
		{
			name: "released ordinal deleted and not recreated yet: hold",
			in: availRolling(new(int32(1)),
				availPod(0, old, true, long), availPod(2, upd, true, long)),
			wantVerdict: rolloutHold, wantPart: new(int32(1)), wantHold: holdPodAbsent,
		},
		{
			name: "released ordinal not replaced yet: hold",
			in: availRolling(new(int32(1)),
				availPod(0, old, true, long), availPod(1, old, true, long), availPod(2, upd, true, long)),
			wantVerdict: rolloutHold, wantPart: new(int32(1)), wantHold: holdRevision,
		},
		{
			name: "new revision never becomes Ready: hold, however long — no timer",
			in: availRolling(new(int32(2)),
				availPod(0, old, true, long), availPod(1, old, true, long), availPod(2, upd, false, 0)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdReadiness,
		},
		{
			name: "every pod at or above 1 available: release ordinal 0",
			in: availRolling(new(int32(1)),
				availPod(0, old, true, long), availPod(1, upd, true, 36*time.Second), availPod(2, upd, true, long)),
			wantVerdict: rolloutAdvance, wantPart: new(int32(0)),
		},
		{
			// #137409's stuck-rollout shape: an OLD pod that is not Ready is below the cursor,
			// so it says nothing about the handover — the cursor reaches it and the controller
			// replaces it then.
			name: "crash-looping OLD pod below the partition does not hold the cursor",
			in: availRolling(new(int32(1)),
				availPod(0, old, false, 0), availPod(1, upd, true, long), availPod(2, upd, true, long)),
			wantVerdict: rolloutAdvance, wantPart: new(int32(0)),
		},
		{
			name: "crash-looping OLD pod AT the partition: hold on revision (the controller replaces it)",
			in: availRolling(new(int32(2)),
				availPod(0, old, true, long), availPod(1, old, true, long), availPod(2, old, false, 0)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdRevision,
		},
		{
			name: "at 0 with every pod available but the StatefulSet not settled: hold at 0",
			in: availRolling(new(int32(0)),
				availPod(0, upd, true, long), availPod(1, upd, true, long), availPod(2, upd, true, long)),
			wantVerdict: rolloutHold, wantPart: new(int32(0)), wantHold: holdSettling,
		},
		{
			// A second edit while the cursor is low: the pods already rolled to the previous
			// revision are OLD again, and several of them are young. Re-raising in the same
			// write is what keeps the controller from deleting them all at once.
			name: "second spec edit mid-roll: the cursor re-raises to the top",
			in: shardRolloutInput{
				HighestOrdinal: 2, MinReadySeconds: availMinReady, SkipRedundancyClause: true,
				DesiredHash: "h3", AppliedHash: "h2", Generation: 9, ObservedGeneration: 9,
				UpdateRevision: upd, CurrentRevision: old, AppliedPartition: new(int32(0)),
				Pods: []shardRolloutPod{availPod(0, upd, true, 3*time.Second), availPod(1, upd, true, 5*time.Second), availPod(2, upd, true, long)},
				Now:  rolloutNow,
			},
			wantVerdict: rolloutStart, wantPart: new(int32(2)),
		},
		{
			name: "scale-down mid-roll: a partition above the new top is clamped DOWN, never raised",
			in: shardRolloutInput{
				HighestOrdinal: 1, MinReadySeconds: availMinReady, SkipRedundancyClause: true,
				DesiredHash: "h2", AppliedHash: "h2", Generation: 7, ObservedGeneration: 7,
				UpdateRevision: upd, CurrentRevision: old, AppliedPartition: new(int32(3)),
				Pods: []shardRolloutPod{availPod(0, old, true, long), availPod(1, upd, true, 2*time.Second)},
				Now:  rolloutNow,
			},
			wantVerdict: rolloutHold, wantPart: new(int32(1)), wantHold: holdAvailability,
		},
		{
			name: "quarantine (0 replicas): nothing to gate, no partition at all",
			in: shardRolloutInput{
				HighestOrdinal: -1, MinReadySeconds: availMinReady, SkipRedundancyClause: true,
				DesiredHash: "h2", AppliedHash: "h2", Now: rolloutNow,
			},
			wantVerdict: rolloutUngated,
		},
		{
			// The Sentinel-process StatefulSet sets no minReadySeconds: Ready is available.
			name: "minReadySeconds 0: Ready is enough",
			in: func() shardRolloutInput {
				in := availRolling(new(int32(2)),
					availPod(0, old, true, long), availPod(1, old, true, long), availPod(2, upd, true, time.Second))
				in.MinReadySeconds = 0
				return in
			}(),
			wantVerdict: rolloutAdvance, wantPart: new(int32(1)),
		},
		{
			// These StatefulSets have no slot owner; with the redundancy clause dropped a pod's
			// missing replication facts must not hold it (and must never read as blocked).
			name: "no redundancy facts and the clause skipped: availability alone decides",
			in: availRolling(new(int32(1)),
				availPod(0, old, true, long), availPod(1, upd, true, time.Hour), availPod(2, upd, true, time.Hour)),
			wantVerdict: rolloutAdvance, wantPart: new(int32(0)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := planShardRolloutPartition(tt.in)
			if got.Verdict != tt.wantVerdict {
				t.Errorf("Verdict = %q, want %q", got.Verdict, tt.wantVerdict)
			}
			switch {
			case tt.wantPart == nil && got.Partition != nil:
				t.Errorf("Partition = %d, want no partition set", *got.Partition)
			case tt.wantPart != nil && got.Partition == nil:
				t.Errorf("Partition = <nil>, want %d", *tt.wantPart)
			case tt.wantPart != nil && *got.Partition != *tt.wantPart:
				t.Errorf("Partition = %d, want %d", *got.Partition, *tt.wantPart)
			}
			if got.Hold != tt.wantHold {
				t.Errorf("Hold = %q, want %q", got.Hold, tt.wantHold)
			}
			if got.Blocked || len(got.BlockedPods) != 0 {
				t.Errorf("Blocked = %v %v: the redundancy-only stall report must never fire here", got.Blocked, got.BlockedPods)
			}
		})
	}
}

// TestAvailabilityGateNeverReleasesTwoYoungPods is the property the whole change exists for,
// stated over every cursor and pod shape rather than by example: whenever the planner LOWERS the
// partition, every pod at or above the old partition is updated and available — so the one pod
// the step releases is the only pod that can be deleted.
func TestAvailabilityGateNeverReleasesTwoYoungPods(t *testing.T) {
	shapes := make([]shardRolloutPod, 0, 12)
	for _, rev := range []string{rolloutRevOld, rolloutRevNew} {
		for _, ready := range []bool{false, true} {
			for _, readyFor := range []time.Duration{0, 2 * time.Second, time.Hour} {
				shapes = append(shapes, shardRolloutPod{Revision: rev, Ready: ready, ReadySince: func() time.Time {
					if readyFor == 0 {
						return time.Time{}
					}
					return rolloutNow.Add(-readyFor)
				}()})
			}
		}
	}
	for applied := int32(0); applied <= 2; applied++ {
		for _, a := range shapes {
			for _, b := range shapes {
				for _, c := range shapes {
					pods := []shardRolloutPod{a, b, c}
					for i := range pods {
						pods[i].Ordinal = i
					}
					got := planShardRolloutPartition(availRolling(new(applied), pods...))
					if got.Verdict != rolloutAdvance {
						continue
					}
					for _, p := range pods[applied:] {
						available := p.Ready && !p.ReadySince.IsZero() &&
							p.ReadySince.Add(time.Duration(availMinReady)*time.Second).Before(rolloutNow)
						if p.Revision != rolloutRevNew || !available {
							t.Fatalf("partition %d lowered to %d while ordinal %d is %+v", applied, *got.Partition, p.Ordinal, p)
						}
					}
				}
			}
		}
	}
}
