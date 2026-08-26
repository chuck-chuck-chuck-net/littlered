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
	"strings"
	"testing"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// Rule N's pure seam: which stale Sentinel master-name entries may be REMOVEd, and —
// far more importantly — every case in which they may not be. `SENTINEL REMOVE` is a
// destructive primitive aimed by a predicate, and this is the aim.
//
// The gates are design §9's G0-G6; each row below names the one it exercises.

const (
	// The rename under test: an instance moving off the shared legacy name.
	desiredName = "team-a.cache"
	staleName   = "mymaster"
	otherStale  = "leftover.name"

	senIP1, senIP2, senIP3    = "10.0.1.1", "10.0.1.2", "10.0.1.3"
	senPod1, senPod2, senPod3 = "r-sentinel-0", "r-sentinel-1", "r-sentinel-2"

	// The address the LR-044 M4a capture put in the victim's Sentinels: the CAPTOR's
	// live master, answering with clean flags, which is why no failover ever fires.
	capturedForeignIP = "10.233.192.152"

	// An ordinary post-failover ghost: a dead ex-master of ours whose pod is gone, so
	// its address is no longer in ValidIPs, and which Sentinel has flagged down.
	deadExMasterIP = "10.0.0.99"
)

func mon(name, ip, flags, failoverState string) redisclient.MonitoredMaster {
	return redisclient.MonitoredMaster{Name: name, IP: ip, Flags: flags, FailoverState: failoverState}
}

// setMasters overwrites what one Sentinel monitors. MasterIP/MasterFlags/Monitoring are
// kept consistent with the DESIRED name's entry (that is what the single-name probe
// reports), so a fixture cannot accidentally disagree with itself.
func setMasters(s *redisclient.ReplicationState, ip, pod string, masters ...redisclient.MonitoredMaster) {
	sn := &redisclient.SentinelNodeState{PodName: pod, IP: ip, Reachable: true, MonitoredMasters: masters}
	for _, m := range masters {
		if m.Name == desiredName {
			sn.Monitoring, sn.MasterIP, sn.MasterFlags = true, m.IP, m.Flags
		}
	}
	s.SentinelNodes[ip] = sn
}

// staleBase is a healthy, converged instance: three Redis pods (one master), three
// Sentinels, every one of them monitoring exactly the desired name at our master.
func staleBase() *redisclient.ReplicationState {
	s := redisclient.NewReplicationState()
	for _, ip := range []string{ipMaster, ipReplica, ipNode3, senIP1, senIP2, senIP3} {
		s.ValidIPs[ip] = true
	}
	s.RedisNodes[ipMaster] = &redisclient.RedisNodeState{PodName: podRedis0, IP: ipMaster, Reachable: true, Role: RoleMaster}
	s.RedisNodes[ipReplica] = &redisclient.RedisNodeState{
		PodName: podRedis1, IP: ipReplica, Reachable: true, Role: roleSlave, MasterHost: ipMaster, LinkStatus: "up",
	}
	s.RedisNodes[ipNode3] = &redisclient.RedisNodeState{
		PodName: podRedis2, IP: ipNode3, Reachable: true, Role: roleSlave, MasterHost: ipMaster, LinkStatus: "up",
	}
	s.RealMasterIP = ipMaster
	for i, ip := range []string{senIP1, senIP2, senIP3} {
		setMasters(s, ip, []string{senPod1, senPod2, senPod3}[i], mon(desiredName, ipMaster, "master", ""))
	}
	return s
}

// midRename is staleBase plus the leftover entry a rename leaves behind: every Sentinel
// carries BOTH names, the stale one still pointing at our own master.
func midRename() *redisclient.ReplicationState {
	s := staleBase()
	for i, ip := range []string{senIP1, senIP2, senIP3} {
		setMasters(s, ip, []string{senPod1, senPod2, senPod3}[i],
			mon(desiredName, ipMaster, "master", ""),
			mon(staleName, ipMaster, "master", ""),
		)
	}
	return s
}

func allThreePrune(names ...string) map[string][]string {
	return map[string][]string{senPod1: names, senPod2: names, senPod3: names}
}

func TestPlanStaleMasterNames(t *testing.T) {
	cases := []struct {
		name        string
		state       *redisclient.ReplicationState
		desired     string
		quorum      int
		forsaken    bool
		wantReason  string
		wantPrune   map[string][]string
		wantSkipped []string
		wantMsg     []string
	}{
		{
			name:       "converged: every Sentinel monitors exactly the desired name",
			state:      staleBase(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesConverged,
		},
		{
			name:       "one stale name on a healthy instance is pruned everywhere",
			state:      midRename(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesPruning,
			wantPrune:  allThreePrune(staleName),
			wantMsg:    []string{staleName},
		},
		{
			name: "two stale names on ONE Sentinel are both pruned",
			state: func() *redisclient.ReplicationState {
				s := midRename()
				setMasters(s, senIP1, senPod1,
					mon(desiredName, ipMaster, "master", ""),
					mon(staleName, ipMaster, "master", ""),
					mon(otherStale, ipMaster, "master", ""),
				)
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesPruning,
			wantPrune: map[string][]string{
				senPod1: {otherStale, staleName},
				senPod2: {staleName},
				senPod3: {staleName},
			},
			wantMsg: []string{staleName, otherStale},
		},
		{
			name: "stale on one of three Sentinels only: the other two are untouched",
			state: func() *redisclient.ReplicationState {
				s := staleBase()
				setMasters(s, senIP2, senPod2,
					mon(desiredName, ipMaster, "master", ""),
					mon(staleName, ipMaster, "master", ""),
				)
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesPruning,
			wantPrune:  map[string][]string{senPod2: {staleName}},
			wantMsg:    []string{senPod2},
		},
		{
			name: "G2: no master of ours at all (RealMasterIP empty) — defer, never prune",
			state: func() *redisclient.ReplicationState {
				s := midRename()
				s.RealMasterIP = ""
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesDeferred,
			wantMsg:    []string{"G2"},
		},
		{
			name: "G2: the consensus master is a ghost (not one of our pods) — defer",
			// The stale entries point at a DOWN address here, so G5 is satisfied and
			// this row isolates G2's second clause rather than colliding with G5.
			state: func() *redisclient.ReplicationState {
				s := staleBase()
				for i, ip := range []string{senIP1, senIP2, senIP3} {
					setMasters(s, ip, []string{senPod1, senPod2, senPod3}[i],
						mon(desiredName, ipMaster, "master", ""),
						mon(staleName, deadExMasterIP, "s_down,master", ""),
					)
				}
				delete(s.ValidIPs, ipMaster)
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesDeferred,
			wantMsg:    []string{"G2"},
		},
		{
			name: "G2 third clause: the master's own Redis entry is unreachable — defer",
			state: func() *redisclient.ReplicationState {
				s := midRename()
				s.RedisNodes[ipMaster].Reachable = false
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesDeferred,
			wantMsg:    []string{"G2"},
		},
		{
			name: "G2 third clause: the master's own Redis entry says role:slave — defer",
			state: func() *redisclient.ReplicationState {
				s := midRename()
				s.RedisNodes[ipMaster].Role = roleSlave
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesDeferred,
			wantMsg:    []string{"G2"},
		},
		{
			name: "G3: the STALE entry reports a failover in progress — defer",
			state: func() *redisclient.ReplicationState {
				s := midRename()
				setMasters(s, senIP1, senPod1,
					mon(desiredName, ipMaster, "master", ""),
					mon(staleName, ipMaster, "master", "select_slave"),
				)
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesDeferred,
			wantMsg:    []string{"G3"},
		},
		{
			name: "G3: the DESIRED entry reports a failover in progress (flag only) — defer",
			state: func() *redisclient.ReplicationState {
				s := midRename()
				setMasters(s, senIP3, senPod3,
					mon(desiredName, ipMaster, "master,failover_in_progress", ""),
					mon(staleName, ipMaster, "master", ""),
				)
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesDeferred,
			wantMsg:    []string{"G3"},
		},
		{
			name: "G4: below quorum — do not operate on a minority",
			state: func() *redisclient.ReplicationState {
				s := midRename()
				s.SentinelNodes[senIP2].Reachable = false
				s.SentinelNodes[senIP3].Reachable = false
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesDeferred,
			wantMsg:    []string{"G4"},
		},
		{
			name: "G5: the stale entry points at a FOREIGN LIVE master — Foreign, prune nothing",
			// The LR-044 M4a capture shape: our Sentinels serve the captor's master
			// under the shared name, flags clean (from Sentinel's vantage it is
			// healthy, which is why no failover ever fires), and our own pods are
			// replicas of it. Renaming to escape that must not silently delete the
			// only evidence of the capture.
			state: func() *redisclient.ReplicationState {
				s := staleBase()
				for i, ip := range []string{senIP1, senIP2, senIP3} {
					setMasters(s, ip, []string{senPod1, senPod2, senPod3}[i],
						mon(desiredName, ipMaster, "master", ""),
						mon(staleName, capturedForeignIP, "master", ""),
					)
				}
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesForeign,
			wantMsg:    []string{capturedForeignIP, staleName},
		},
		{
			name: "G5: a stale entry pointing at a DOWN address is ordinary debris — prune",
			state: func() *redisclient.ReplicationState {
				s := staleBase()
				for i, ip := range []string{senIP1, senIP2, senIP3} {
					setMasters(s, ip, []string{senPod1, senPod2, senPod3}[i],
						mon(desiredName, ipMaster, "master", ""),
						mon(staleName, deadExMasterIP, "s_down,o_down,master", ""),
					)
				}
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesPruning,
			wantPrune:  allThreePrune(staleName),
		},
		{
			name: "G6: a Sentinel not yet carrying the desired name is SKIPPED and named",
			state: func() *redisclient.ReplicationState {
				s := midRename()
				setMasters(s, senIP2, senPod2, mon(staleName, ipMaster, "master", ""))
				return s
			}(),
			desired:    desiredName,
			quorum:     2,
			wantReason: staleNamesPruning,
			wantPrune: map[string][]string{
				senPod1: {staleName},
				senPod3: {staleName},
			},
			wantSkipped: []string{senPod2},
			// R3 says "no leftover entry, EVER", so an invisible skip is a defect:
			// "one Sentinel lagging by a pass" must be distinguishable from "one
			// Sentinel permanently stuck".
			wantMsg: []string{senPod2},
		},
		{
			name:       "G1: an empty desired name is a plausible-looking lie, not an instruction",
			state:      midRename(),
			desired:    "",
			quorum:     2,
			wantReason: staleNamesDeferred,
			wantMsg:    []string{"G1"},
		},
		{
			name: "G0: forsaken beats an otherwise-perfect prune case",
			// The s_down-captor hole G5 alone cannot see: a captor mid-failover flags
			// its master down, so every per-entry test passes and the prune would
			// fire, erasing a capture that is both undiagnosed and unrecoverable.
			// Only a verdict that does not depend on the name closes it.
			state:      midRename(),
			desired:    desiredName,
			quorum:     2,
			forsaken:   true,
			wantReason: staleNamesForeign,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planStaleMasterNames(tc.state, tc.desired, tc.quorum, tc.forsaken)

			if got.Reason != tc.wantReason {
				t.Errorf("Reason = %q, want %q (message: %q)", got.Reason, tc.wantReason, got.Message)
			}

			gotPrune := map[string][]string{}
			for _, e := range got.Prune {
				gotPrune[e.SentinelPod] = e.Names
			}
			want := tc.wantPrune
			if want == nil {
				want = map[string][]string{}
			}
			if !reflect.DeepEqual(gotPrune, want) {
				t.Errorf("Prune = %v, want %v", gotPrune, want)
			}

			var wantSkipped []string
			wantSkipped = append(wantSkipped, tc.wantSkipped...)
			if !reflect.DeepEqual(got.Skipped, wantSkipped) {
				t.Errorf("Skipped = %v, want %v", got.Skipped, wantSkipped)
			}

			for _, want := range tc.wantMsg {
				if !strings.Contains(got.Message, want) {
					t.Errorf("Message = %q, want it to mention %q", got.Message, want)
				}
			}

			// The property the whole feature's safety rests on. Asserted on EVERY row,
			// not only the prune ones: REMOVE of the desired name is LR-005/LR-008's
			// job and must never be reached from here.
			for _, e := range got.Prune {
				for _, n := range e.Names {
					if n == tc.desired {
						t.Errorf("Prune contains the DESIRED name %q for %s", n, e.SentinelPod)
					}
				}
			}
		})
	}
}

// A prune must be reported with a stable order, or the condition message churns from
// pass to pass over an unchanged topology and an operator cannot tell a new event from
// a re-render of the old one. Map iteration order makes this a real hazard here.
func TestPlanStaleMasterNamesIsDeterministic(t *testing.T) {
	s := midRename()
	setMasters(s, senIP1, senPod1,
		mon(desiredName, ipMaster, "master", ""),
		mon(otherStale, ipMaster, "master", ""),
		mon(staleName, ipMaster, "master", ""),
	)

	first := planStaleMasterNames(s, desiredName, 2, false)
	for range 20 {
		got := planStaleMasterNames(s, desiredName, 2, false)
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("a repeated call differs:\n got %+v\nwant %+v", got, first)
		}
	}
	if len(first.Prune) != 3 {
		t.Fatalf("Prune covers %d Sentinels, want 3", len(first.Prune))
	}
	if first.Prune[0].SentinelPod != senPod1 || first.Prune[2].SentinelPod != senPod3 {
		t.Errorf("Prune order = %s..%s, want %s..%s",
			first.Prune[0].SentinelPod, first.Prune[2].SentinelPod, senPod1, senPod3)
	}
	if got := first.Prune[0].Names; !reflect.DeepEqual(got, []string{otherStale, staleName}) {
		t.Errorf("names = %v, want them sorted", got)
	}
}
