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
	surveyDesired = "ns.inst"
	// surveyStale is the historic shared name a half-finished rename leaves behind.
	surveyStale     = "mymaster"
	surveyPod0      = "s-0"
	surveyPod1      = "s-1"
	surveyPod0IP    = "10.0.1.1"
	surveyPod1IP    = "10.0.1.2"
	surveyFlagsDown = "s_down,o_down,master"
	surveyOurIP     = "10.0.0.1"
	surveyOurIP2    = "10.0.0.2"
	surveyForeign   = "10.9.9.9"
)

// stateWithSentinels builds a ReplicationState whose ValidIPs are our two pod IPs.
func stateWithSentinels(sns ...*SentinelNodeState) *ReplicationState {
	st := NewReplicationState()
	st.ValidIPs[surveyOurIP] = true
	st.ValidIPs[surveyOurIP2] = true
	for _, sn := range sns {
		st.SentinelNodes[sn.IP] = sn
	}
	return st
}

// TestSurveyMonitoredNames is the rendering half of Rule N's G5 discriminator: an
// address in ValidIPs or flagged down is debris of ours, anything else is somebody
// else's live master. The classes must not drift from the planner's, because the two
// are read side by side by whoever is deciding whether a rename is safe.
func TestSurveyMonitoredNames(t *testing.T) {
	cases := []struct {
		name       string
		state      *ReplicationState
		want       []MonitoredNameFinding
		stale      []string
		foreign    []string
		unreported []string
		converged  bool
	}{
		{
			name: "converged: one Sentinel, only the desired name",
			state: stateWithSentinels(&SentinelNodeState{
				PodName: surveyPod0, IP: surveyPod0IP, Reachable: true, Monitoring: true,
				MonitoredMasters: []MonitoredMaster{{Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster}},
			}),
			want: []MonitoredNameFinding{
				{SentinelPod: surveyPod0, Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster, Class: MasterNameDesired},
			},
			converged: true,
		},
		{
			name: "the LR-048 shape: two monitor lines, the stale one at one of our pods",
			state: stateWithSentinels(&SentinelNodeState{
				PodName: surveyPod0, IP: surveyPod0IP, Reachable: true, Monitoring: true,
				MonitoredMasters: []MonitoredMaster{
					{Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster},
					{Name: surveyStale, IP: surveyOurIP, Flags: roleMaster},
				},
			}),
			want: []MonitoredNameFinding{
				{SentinelPod: surveyPod0, Name: surveyStale, IP: surveyOurIP, Flags: roleMaster, Class: MasterNameStale},
				{SentinelPod: surveyPod0, Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster, Class: MasterNameDesired},
			},
			stale: []string{surveyStale},
		},
		{
			name: "a stale name at a flagged-down address is ordinary debris, not foreign",
			state: stateWithSentinels(&SentinelNodeState{
				PodName: surveyPod0, IP: surveyPod0IP, Reachable: true, Monitoring: true,
				MonitoredMasters: []MonitoredMaster{
					{Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster},
					{Name: surveyStale, IP: surveyForeign, Flags: surveyFlagsDown},
				},
			}),
			want: []MonitoredNameFinding{
				{SentinelPod: surveyPod0, Name: surveyStale, IP: surveyForeign,
					Flags: surveyFlagsDown, Class: MasterNameStale},
				{SentinelPod: surveyPod0, Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster, Class: MasterNameDesired},
			},
			stale: []string{surveyStale},
		},
		{
			name: "a stale name at a live address that is not ours is FOREIGN",
			state: stateWithSentinels(&SentinelNodeState{
				PodName: surveyPod0, IP: surveyPod0IP, Reachable: true, Monitoring: true,
				MonitoredMasters: []MonitoredMaster{
					{Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster},
					{Name: surveyStale, IP: surveyForeign, Flags: roleMaster},
				},
			}),
			want: []MonitoredNameFinding{
				{SentinelPod: surveyPod0, Name: surveyStale, IP: surveyForeign, Flags: roleMaster, Class: MasterNameForeign},
				{SentinelPod: surveyPod0, Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster, Class: MasterNameDesired},
			},
			foreign: []string{surveyStale},
		},
		{
			name: "an entry with no address cannot be attributed to us, so it is foreign",
			state: stateWithSentinels(&SentinelNodeState{
				PodName: surveyPod0, IP: surveyPod0IP, Reachable: true, Monitoring: true,
				MonitoredMasters: []MonitoredMaster{{Name: surveyStale}},
			}),
			want: []MonitoredNameFinding{
				{SentinelPod: surveyPod0, Name: surveyStale, Class: MasterNameForeign},
			},
			foreign: []string{surveyStale},
		},
		{
			name: "mid-rename: Monitoring is false for the new name, the old entry is still there",
			state: stateWithSentinels(&SentinelNodeState{
				PodName: surveyPod0, IP: surveyPod0IP, Reachable: true, Monitoring: false,
				MonitoredMasters: []MonitoredMaster{{Name: surveyStale, IP: surveyOurIP2, Flags: roleMaster}},
			}),
			want: []MonitoredNameFinding{
				{SentinelPod: surveyPod0, Name: surveyStale, IP: surveyOurIP2, Flags: roleMaster, Class: MasterNameStale},
			},
			stale: []string{surveyStale},
		},
		{
			name: "an unreachable Sentinel has no view and contributes nothing",
			state: stateWithSentinels(&SentinelNodeState{
				PodName: surveyPod0, IP: surveyPod0IP, Reachable: false,
				MonitoredMasters: []MonitoredMaster{{Name: surveyStale, IP: surveyForeign}},
			}),
			converged: true,
		},
		{
			name: "an unread master list is no evidence, and is reported as such",
			state: stateWithSentinels(&SentinelNodeState{
				PodName: surveyPod0, IP: surveyPod0IP, Reachable: true, Monitoring: true,
			}),
			unreported: []string{surveyPod0},
			converged:  true,
		},
		{
			name: "findings are ordered by Sentinel pod then name, and names deduped across Sentinels",
			state: stateWithSentinels(
				&SentinelNodeState{
					PodName: surveyPod1, IP: surveyPod1IP, Reachable: true, Monitoring: true,
					MonitoredMasters: []MonitoredMaster{
						{Name: surveyStale, IP: surveyOurIP, Flags: roleMaster},
						{Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster},
					},
				},
				&SentinelNodeState{
					PodName: surveyPod0, IP: surveyPod0IP, Reachable: true, Monitoring: true,
					MonitoredMasters: []MonitoredMaster{
						{Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster},
						{Name: surveyStale, IP: surveyOurIP, Flags: roleMaster},
					},
				},
			),
			want: []MonitoredNameFinding{
				{SentinelPod: surveyPod0, Name: surveyStale, IP: surveyOurIP, Flags: roleMaster, Class: MasterNameStale},
				{SentinelPod: surveyPod0, Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster, Class: MasterNameDesired},
				{SentinelPod: surveyPod1, Name: surveyStale, IP: surveyOurIP, Flags: roleMaster, Class: MasterNameStale},
				{SentinelPod: surveyPod1, Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster, Class: MasterNameDesired},
			},
			stale: []string{surveyStale},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.state.SurveyMonitoredNames(surveyDesired)
			if !reflect.DeepEqual(got.Findings, tc.want) {
				t.Errorf("Findings = %#v, want %#v", got.Findings, tc.want)
			}
			if !reflect.DeepEqual(got.Stale, tc.stale) {
				t.Errorf("Stale = %#v, want %#v", got.Stale, tc.stale)
			}
			if !reflect.DeepEqual(got.Foreign, tc.foreign) {
				t.Errorf("Foreign = %#v, want %#v", got.Foreign, tc.foreign)
			}
			if !reflect.DeepEqual(got.Unreported, tc.unreported) {
				t.Errorf("Unreported = %#v, want %#v", got.Unreported, tc.unreported)
			}
			if got.Converged() != tc.converged {
				t.Errorf("Converged() = %v, want %v", got.Converged(), tc.converged)
			}
		})
	}
}

// TestSurveyMonitoredNamesEmptyDesired pins that an empty desired name is not treated
// as "everything is stale" — the failure mode LR-041 names, where a required string's
// zero value is a plausible input rather than an obvious error. With no name to compare
// against there is nothing to say, so the survey says nothing rather than accusing every
// entry it can see. Rule N refuses to act on the same input (gate G1).
func TestSurveyMonitoredNamesEmptyDesired(t *testing.T) {
	st := stateWithSentinels(&SentinelNodeState{
		PodName: surveyPod0, IP: surveyPod0IP, Reachable: true, Monitoring: true,
		MonitoredMasters: []MonitoredMaster{{Name: surveyDesired, IP: surveyOurIP, Flags: roleMaster}},
	})
	got := st.SurveyMonitoredNames("")
	if len(got.Stale) != 0 || len(got.Foreign) != 0 || len(got.Findings) != 0 {
		t.Fatalf("empty desired name produced a verdict: %#v", got)
	}
	if !got.Converged() {
		t.Fatalf("Converged() = false, want true (nothing was surveyed)")
	}
}
