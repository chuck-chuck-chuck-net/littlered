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
	hfReplidD = "dddddddddddddddddddddddddddddddddddddddd" // the common ancestor
	hfReplidA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hfReplidB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hfSlave   = "slave"
	hfIPA     = surveyOurIP
	hfIPB     = surveyOurIP2
)

// LR-057. holdersDiverged is a union-find over {replid, replid2}, and a promotion
// rotates the replid with the old one shifted into replid2 — so it connects a
// promotion CHAIN and a promotion FORK identically:
//
//	chain  D -> A -> B     A={A,D}  B={B,A}    one component
//	fork   D -> A, D -> B  A={A,D}  B={B,D}    one component
//
// Only the chain is safe. In a fork each node has been independently writable
// since it parted, so their offsets are positions on two branches that share only
// an origin, and electing the higher one discards the other's writes.
//
// holdersForked is the discriminator, and it is deliberately NOT another lineage
// predicate: it asks how many holders have been WRITABLE, which is what the
// offset comparison actually assumes and what lineage cannot establish.
func TestHoldersForked(t *testing.T) {
	holder := func(ip, role string, keys int64) *RedisNodeState {
		return &RedisNodeState{IP: ip, Reachable: true, Role: role, Keys: keys}
	}

	tests := []struct {
		name    string
		holders []*RedisNodeState
		want    bool
	}{
		{
			name: "THE DEFECT: two data-holding masters forked from one ancestor",
			holders: []*RedisNodeState{
				holder(hfIPA, roleMaster, 500),
				holder(hfIPB, roleMaster, 400),
			},
			want: true,
		},
		{
			name: "LR-024's premise, and it must stay safe: survivors of one dead master",
			holders: []*RedisNodeState{
				holder(hfIPA, hfSlave, 500),
				holder(hfIPB, hfSlave, 400),
			},
			want: false,
		},
		{
			name: "one master and its replicas — the ordinary post-promotion chain",
			holders: []*RedisNodeState{
				holder(hfIPA, roleMaster, 500),
				holder(hfIPB, hfSlave, 500),
				holder("10.0.0.3", hfSlave, 499),
			},
			want: false,
		},
		{
			name:    "a single holder can never have forked from anything",
			holders: []*RedisNodeState{holder(hfIPA, roleMaster, 500)},
			want:    false,
		},
		{
			name:    "no holders",
			holders: nil,
			want:    false,
		},
		{
			name: "three writable holders is still a fork",
			holders: []*RedisNodeState{
				holder(hfIPA, roleMaster, 500),
				holder(hfIPB, roleMaster, 400),
				holder("10.0.0.3", roleMaster, 300),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := holdersForked(tt.holders); got != tt.want {
				t.Errorf("holdersForked() = %v, want %v", got, tt.want)
			}
		})
	}
}

// The empty-master transient is why LR-057 deferred the role-based discriminator:
// "a restarted pod returns as an empty master by default (LR-004, LR-014), so 'two
// holders report role:master' may be an ordinary transient of the very scenario the
// rule rescues".
//
// It cannot be. DataHolders is `Reachable && Keys > 0` and storage is EmptyDir, so a
// restarted pod returns with ZERO keys and is never a holder at all. This pins that,
// because it is the reasoning the whole fix turns on.
func TestAnEmptyRestartedMasterIsNeverAHolder(t *testing.T) {
	s := NewReplicationState()
	s.RedisNodes[hfIPA] = &RedisNodeState{IP: hfIPA, Reachable: true, Role: hfSlave, Keys: 500}
	s.RedisNodes[hfIPB] = &RedisNodeState{IP: hfIPB, Reachable: true, Role: roleMaster, Keys: 0}

	holders := s.DataHolders()
	if len(holders) != 1 || holders[0].IP != hfIPA {
		t.Fatalf("DataHolders() = %v, want only the pod holding data", holders)
	}
	if holdersForked(holders) {
		t.Errorf("holdersForked() = true; an empty restarted master must not make the set look forked")
	}
}

// BestDataHolder must report the fork separately from the lineage divergence: they
// lead to the same refusal but they are different situations, and the human reading
// the message is deciding whether to set allowUnsafeRebootstrapOnDeadlock.
func TestBestDataHolderReportsForkSeparatelyFromDivergence(t *testing.T) {
	s := NewReplicationState()
	// Two masters forked from one ancestor: SAME lineage by union-find, forked in fact.
	s.RedisNodes[hfIPA] = &RedisNodeState{
		IP: hfIPA, Reachable: true, Role: roleMaster, Keys: 500, Offset: 900,
		Replid: hfReplidA, Replid2: hfReplidD,
	}
	s.RedisNodes[hfIPB] = &RedisNodeState{
		IP: hfIPB, Reachable: true, Role: roleMaster, Keys: 400, Offset: 800,
		Replid: hfReplidB, Replid2: hfReplidD,
	}

	best, diverged, forked := s.BestDataHolder()
	if best == nil {
		t.Fatal("BestDataHolder() = nil")
	}
	if diverged {
		t.Errorf("diverged = true, want false: the union-find genuinely sees ONE lineage here — "+
			"that is the defect, and forked is what must catch it (best=%s)", best.IP)
	}
	if !forked {
		t.Errorf("forked = false, want true: two data-holding masters have each been " +
			"independently writable, so their offsets are not comparable")
	}
}
