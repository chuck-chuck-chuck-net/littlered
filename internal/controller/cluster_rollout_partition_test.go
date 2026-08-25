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
	"reflect"
	"testing"
	"time"
)

const (
	rolloutRevOld = "rev1"
	rolloutRevNew = "rev2"
)

var rolloutNow = time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)

// oldPod is a pod still carrying the pre-change revision: by construction it has not been
// asked to update yet, so its readiness/redundancy say nothing about the handover.
func oldPod(ordinal int) shardRolloutPod {
	return shardRolloutPod{
		Ordinal: ordinal, Revision: rolloutRevOld, Ready: true,
		ReadySince:      rolloutNow.Add(-1 * time.Hour),
		AttachedToOwner: true, SyncedWithOwner: true,
	}
}

// freshPod is the field shape of the defect (ADR-017 Context): a replaced pod comes back on a
// wiped EmptyDir with a NEW node ID, so until the operator has FORGOTten the old ID, MEETed it,
// CLUSTER REPLICATEd it and let it full-sync it has no cluster contact at all — while answering
// PING, hence Ready per the kubelet.
func freshPod(ordinal int, readyFor time.Duration) shardRolloutPod {
	return shardRolloutPod{
		Ordinal: ordinal, Revision: rolloutRevNew, Ready: true,
		ReadySince:      rolloutNow.Add(-readyFor),
		AttachedToOwner: false, SyncedWithOwner: false,
	}
}

// syncedPod is a replaced pod that has completed the whole reattach: at UpdateRevision, Ready,
// and a link-up replica of its shard's slot owner. The only shape that may lower the partition.
func syncedPod(ordinal int) shardRolloutPod {
	return shardRolloutPod{
		Ordinal: ordinal, Revision: rolloutRevNew, Ready: true,
		ReadySince:      rolloutNow.Add(-30 * time.Second),
		AttachedToOwner: true, SyncedWithOwner: true,
	}
}

// syncingPod is attached to the right owner but its replication link is still down — a full
// sync in flight. Legitimate progress, however long it takes (ADR-017 Consequences).
func syncingPod(ordinal int, readyFor time.Duration) shardRolloutPod {
	return shardRolloutPod{
		Ordinal: ordinal, Revision: rolloutRevNew, Ready: true,
		ReadySince:      rolloutNow.Add(-readyFor),
		AttachedToOwner: true, SyncedWithOwner: false,
	}
}

// rolling is the input shape of a shard whose new template has already been applied: desired ==
// applied hash, and the StatefulSet controller has observed it but not finished (partition > 0
// holds CurrentRevision back, so it never equals UpdateRevision mid-rollout).
func rolling(rps int, partition *int32, pods ...shardRolloutPod) shardRolloutInput {
	return shardRolloutInput{
		ShardIdx: 1, ReplicasPerShard: rps,
		DesiredHash: "h2", AppliedHash: "h2",
		Generation: 7, ObservedGeneration: 7,
		UpdateRevision: rolloutRevNew, CurrentRevision: rolloutRevOld,
		AppliedPartition: partition, Pods: pods, Now: rolloutNow,
	}
}

func TestPlanShardRolloutPartition(t *testing.T) {
	tests := []struct {
		name        string
		in          shardRolloutInput
		wantVerdict shardRolloutVerdict
		wantPart    *int32 // nil = no partition may be set at all
		wantHold    shardRolloutHold
		wantBlocked []int // nil = must not report blocked
	}{
		{
			name: "template change first seen: gate at the shard's highest ordinal",
			in: shardRolloutInput{
				ShardIdx: 1, ReplicasPerShard: 2,
				DesiredHash: "h2", AppliedHash: "h1",
				Generation: 6, ObservedGeneration: 6,
				UpdateRevision: rolloutRevOld, CurrentRevision: rolloutRevOld,
				AppliedPartition: nil,
				Pods:             []shardRolloutPod{oldPod(0), oldPod(1), oldPod(2)},
				Now:              rolloutNow,
			},
			wantVerdict: rolloutStart, wantPart: new(int32(2)),
		},
		{
			name: "template change first seen is the ONLY raise: an applied 0 goes back up",
			in: shardRolloutInput{
				ShardIdx: 1, ReplicasPerShard: 2,
				DesiredHash: "h3", AppliedHash: "h2",
				Generation: 8, ObservedGeneration: 8,
				UpdateRevision: rolloutRevNew, CurrentRevision: rolloutRevNew,
				AppliedPartition: new(int32(0)),
				Pods:             []shardRolloutPod{oldPod(0), oldPod(1), oldPod(2)},
				Now:              rolloutNow,
			},
			wantVerdict: rolloutStart, wantPart: new(int32(2)),
		},
		{
			// THE ROW THAT MATTERS: the exact state the 2026-08-23 run took a master down in.
			name: "updated and Ready but NOT a synced replica: hold, never advance",
			in: rolling(2, new(int32(2)),
				oldPod(0), oldPod(1), freshPod(2, 10*time.Second)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdRedundancy,
		},
		{
			name: "all three clauses satisfied: lower by exactly one",
			in: rolling(2, new(int32(2)),
				oldPod(0), oldPod(1), syncedPod(2)),
			wantVerdict: rolloutAdvance, wantPart: new(int32(1)),
		},
		{
			name: "partway down a 3-pod shard: every pod at or above the partition must qualify",
			in: rolling(2, new(int32(1)),
				oldPod(0), syncedPod(1), syncedPod(2)),
			wantVerdict: rolloutAdvance, wantPart: new(int32(0)),
		},
		{
			name: "one pod above the partition still unsynced blocks the whole step",
			in: rolling(2, new(int32(1)),
				oldPod(0), syncedPod(1), freshPod(2, 10*time.Second)),
			wantVerdict: rolloutHold, wantPart: new(int32(1)), wantHold: holdRedundancy,
		},
		{
			name: "a pod at the old revision BELOW the partition is ignored",
			in: rolling(2, new(int32(2)),
				shardRolloutPod{Ordinal: 0, Revision: rolloutRevOld, Ready: false},
				shardRolloutPod{Ordinal: 1, Revision: rolloutRevOld, Ready: false},
				syncedPod(2)),
			wantVerdict: rolloutAdvance, wantPart: new(int32(1)),
		},
		{
			name: "at 0 and settled: complete",
			in: shardRolloutInput{
				ShardIdx: 1, ReplicasPerShard: 2,
				DesiredHash: "h2", AppliedHash: "h2",
				Generation: 8, ObservedGeneration: 8,
				UpdateRevision: rolloutRevNew, CurrentRevision: rolloutRevNew,
				AppliedPartition: new(int32(0)),
				Pods: []shardRolloutPod{
					// The shard's own master owns the slots, so it is nobody's replica. A
					// settled shard must never be reported as holding on redundancy.
					{Ordinal: 0, Revision: rolloutRevNew, Ready: true, ReadySince: rolloutNow.Add(-1 * time.Hour)},
					syncedPod(1), syncedPod(2),
				},
				Now: rolloutNow,
			},
			wantVerdict: rolloutComplete, wantPart: new(int32(0)),
		},
		{
			name: "settled but the generation is not yet observed: hold, never complete",
			in: shardRolloutInput{
				ShardIdx: 1, ReplicasPerShard: 2,
				DesiredHash: "h2", AppliedHash: "h2",
				Generation: 9, ObservedGeneration: 8,
				UpdateRevision: rolloutRevNew, CurrentRevision: rolloutRevNew,
				AppliedPartition: new(int32(2)),
				Pods:             []shardRolloutPod{oldPod(0), oldPod(1), oldPod(2)},
				Now:              rolloutNow,
			},
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdRevision,
		},
		{
			name: "replicasPerShard 0: ungated, no partition at all",
			in: shardRolloutInput{
				ShardIdx: 1, ReplicasPerShard: 0,
				DesiredHash: "h2", AppliedHash: "h1",
				Generation: 6, ObservedGeneration: 6,
				UpdateRevision: rolloutRevOld, CurrentRevision: rolloutRevOld,
				Pods: []shardRolloutPod{oldPod(0)},
				Now:  rolloutNow,
			},
			wantVerdict: rolloutUngated, wantPart: nil,
		},
		{
			name: "monotone: holding at 0 never climbs back toward the highest ordinal",
			in: rolling(2, new(int32(0)),
				freshPod(0, 10*time.Second), syncedPod(1), syncedPod(2)),
			wantVerdict: rolloutHold, wantPart: new(int32(0)), wantHold: holdRedundancy,
		},
		{
			name: "a partition above the highest ordinal is clamped down, never up",
			in: rolling(2, new(int32(5)),
				oldPod(0), oldPod(1), freshPod(2, 10*time.Second)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdRedundancy,
		},
		{
			name:        "an absent pod at or above the partition holds",
			in:          rolling(2, new(int32(2)), oldPod(0), oldPod(1)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdPodAbsent,
		},
		{
			name: "not-Ready at the partition holds on readiness, not redundancy",
			in: rolling(2, new(int32(2)), oldPod(0), oldPod(1),
				shardRolloutPod{Ordinal: 2, Revision: rolloutRevNew, Ready: false}),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdReadiness,
		},
		{
			name: "clauses satisfied at partition 0 but the StatefulSet has not settled: hold at 0",
			in: rolling(2, new(int32(0)),
				syncedPod(0), syncedPod(1), syncedPod(2)),
			wantVerdict: rolloutHold, wantPart: new(int32(0)), wantHold: holdSettling,
		},
		{
			name: "BLOCKED: Ready past the reattach budget with no attachment at all",
			in: rolling(2, new(int32(2)),
				oldPod(0), oldPod(1), freshPod(2, 10*time.Minute)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdRedundancy,
			wantBlocked: []int{2},
		},
		{
			name: "HOLDING, not blocked: still inside the reattach budget",
			in: rolling(2, new(int32(2)),
				oldPod(0), oldPod(1), freshPod(2, clusterRolloutReattachBudget-time.Second)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdRedundancy,
		},
		{
			name: "HOLDING, not blocked: a full sync in flight is progress, however long",
			in: rolling(2, new(int32(2)),
				oldPod(0), oldPod(1), syncingPod(2, 45*time.Minute)),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdRedundancy,
		},
		{
			name: "HOLDING, not blocked: no readiness timestamp is no evidence",
			in: rolling(2, new(int32(2)), oldPod(0), oldPod(1),
				shardRolloutPod{Ordinal: 2, Revision: rolloutRevNew, Ready: true}),
			wantVerdict: rolloutHold, wantPart: new(int32(2)), wantHold: holdRedundancy,
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
			if len(tt.wantBlocked) == 0 {
				if got.Blocked || len(got.BlockedPods) != 0 {
					t.Errorf("Blocked = %v %v, want not blocked", got.Blocked, got.BlockedPods)
				}
			} else {
				if !got.Blocked {
					t.Errorf("Blocked = false, want true")
				}
				if !reflect.DeepEqual(got.BlockedPods, tt.wantBlocked) {
					t.Errorf("BlockedPods = %v, want %v", got.BlockedPods, tt.wantBlocked)
				}
			}
		})
	}
}

// TestPlanShardRolloutPartitionIsMonotone pins the property that makes the cursor flap-proof:
// the emitted partition is never GREATER than the applied one unless this pass is the first
// sight of a template change. A partition that oscillates back up is harmless; one that
// oscillates back DOWN releases the master while the replica is unsynced, which is the defect.
func TestPlanShardRolloutPartitionIsMonotone(t *testing.T) {
	pods := []shardRolloutPod{
		freshPod(0, time.Minute), syncedPod(1), syncingPod(2, time.Minute),
	}
	for applied := int32(0); applied <= 3; applied++ {
		for _, subset := range [][]shardRolloutPod{nil, pods[:1], pods[:2], pods} {
			in := rolling(2, new(applied), subset...)
			got := planShardRolloutPartition(in)
			if got.Verdict == rolloutStart || got.Partition == nil {
				continue
			}
			if want := int32(in.currentPartition()); *got.Partition > want {
				t.Errorf("applied=%d pods=%d: Partition = %d, want <= %d (never raised without a template change)",
					applied, len(subset), *got.Partition, want)
			}
		}
	}
}
