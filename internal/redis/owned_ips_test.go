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

// LR-053, the internal/redis half: the two attribution surfaces this package owns
// must read the OWNED set, not the live-topology one.
//
// `ClassifyMonitoredName` is the sharpest consumer in the tree — it is the one
// definition of "ours / somebody else's / debris" and it feeds both `planForsaken`
// and Rule N's G5 through the survey, plus `lrctl inspect`, which has pod addresses
// and no gathered state at all. `DetectCrossInstance` is `lrctl verify`'s capture
// diagnostic and asks the identical question of a master and of every known replica.
//
// The state under test is our own pod mid-termination: no longer live topology (the
// gather stopped probing it the moment its object grew a deletionTimestamp), still
// holding its address, still answering, and therefore not flagged down by Sentinel
// for a whole down-after-milliseconds.

const (
	ownedLiveIP        = "10.0.0.2"
	ownedTerminatingIP = "10.0.0.1"
	ownedForeignIP     = "10.9.9.9"
	// ownedSentinelPod is the one Sentinel these fixtures need.
	ownedSentinelPod = "sentinel-0"
)

// ourPodsMidTermination: one live pod, one of ours on its way out.
func ourPodsMidTermination() *ReplicationState {
	st := NewReplicationState()
	st.AddLiveTopologyIP(ownedLiveIP)
	st.AddOwnedIP(ownedTerminatingIP)
	return st
}

// Green from birth, and disclosed as such: ClassifyMonitoredName takes the address
// set as a PARAMETER, so it was never the defect — the defect is which map its
// callers hand it, which the next test covers. Its teeth are the mutation of passing
// `st.LiveTopologyIPs` here, which fails the first row with
// `= "foreign", want "stale"`.
func TestClassifyMonitoredNameAttributesOurTerminatingPod(t *testing.T) {
	st := ourPodsMidTermination()

	if st.IsGhost(ownedTerminatingIP) != true {
		t.Fatalf("precondition: a terminating pod is not live topology (IsGhost = false, want true)")
	}
	if !st.IsOurs(ownedTerminatingIP) {
		t.Fatalf("precondition: a terminating pod of ours is still ours (IsOurs = false, want true)")
	}

	cases := []struct {
		name, ip, flags, want string
	}{
		{
			// The row this milestone exists for.
			name: surveyStale, ip: ownedTerminatingIP, flags: roleMaster,
			want: MasterNameStale,
		},
		{
			// Positive control: a genuinely foreign live master must STILL be foreign,
			// so the widening cannot pass as a blanket "everything is ours".
			name: surveyStale, ip: ownedForeignIP, flags: roleMaster,
			want: MasterNameForeign,
		},
		{
			// Unchanged: an address of ours that is live topology.
			name: surveyStale, ip: ownedLiveIP, flags: roleMaster,
			want: MasterNameStale,
		},
	}
	for _, tc := range cases {
		got := ClassifyMonitoredName(tc.name, tc.ip, tc.flags, "ns.inst", st.OwnedIPs)
		if got != tc.want {
			t.Errorf("ClassifyMonitoredName(%s at %s) = %q, want %q", tc.name, tc.ip, got, tc.want)
		}
	}
}

// TestSurveyMonitoredNamesAttributesOurTerminatingPod pins that the survey feeds
// ClassifyMonitoredName the owned set. The classification is right in isolation and
// wrong in place if the state hands it the wrong map, which is exactly the shape of
// defect this milestone is about.
func TestSurveyMonitoredNamesAttributesOurTerminatingPod(t *testing.T) {
	st := ourPodsMidTermination()
	st.SentinelNodes[ownedLiveIP] = &SentinelNodeState{
		PodName: ownedSentinelPod, IP: ownedLiveIP, Reachable: true, Monitoring: true,
		MonitoredMasters: []MonitoredMaster{
			{Name: "ns.inst", IP: ownedTerminatingIP, Flags: roleMaster},
			{Name: surveyStale, IP: ownedTerminatingIP, Flags: roleMaster},
		},
	}

	scope := st.SurveyMonitoredNames("ns.inst")
	if len(scope.Foreign) != 0 {
		t.Errorf("SurveyMonitoredNames.Foreign = %v, want none: the address is our own terminating pod",
			scope.Foreign)
	}
}

// TestDetectCrossInstanceDoesNotReportOurTerminatingPod is lrctl verify's capture
// diagnostic. Reporting a pod we deleted a second ago as a foreign master turns the
// project's own ground-truth tool into a false accusation, on the routine operation
// (a graceful master handover) that produces this state every time.
func TestDetectCrossInstanceDoesNotReportOurTerminatingPod(t *testing.T) {
	st := ourPodsMidTermination()
	st.SentinelNodes[ownedLiveIP] = &SentinelNodeState{
		PodName: ownedSentinelPod, IP: ownedLiveIP, Reachable: true, Monitoring: true,
		MasterIP: ownedTerminatingIP, MasterFlags: roleMaster,
		Replicas:          []ReplicaInfo{{IP: ownedTerminatingIP, Flags: flagSlave}},
		NumOtherSentinels: 0, NumSlaves: 1,
	}

	ev := st.DetectCrossInstance(1, 1)
	if len(ev.ForeignMasterIPs) != 0 {
		t.Errorf("DetectCrossInstance.ForeignMasterIPs = %v, want none: that is our own terminating pod",
			ev.ForeignMasterIPs)
	}
	if len(ev.ForeignReplicaIPs) != 0 {
		t.Errorf("DetectCrossInstance.ForeignReplicaIPs = %v, want none: that is our own terminating pod",
			ev.ForeignReplicaIPs)
	}

	// Positive control, same call: a genuinely foreign address must still be reported,
	// so a green above cannot come from the diagnostic having gone blind.
	st.SentinelNodes[ownedLiveIP].MasterIP = ownedForeignIP
	if ev := st.DetectCrossInstance(1, 1); len(ev.ForeignMasterIPs) != 1 {
		t.Errorf("DetectCrossInstance.ForeignMasterIPs = %v, want [%s]", ev.ForeignMasterIPs, ownedForeignIP)
	}
}
