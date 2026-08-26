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

package cmd

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/types"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// podsWithIPs builds a pod list from alternating name/IP arguments.
func podsWithIPs(nameIP ...string) []corev1.Pod {
	var pods []corev1.Pod
	for i := 0; i+1 < len(nameIP); i += 2 {
		p := corev1.Pod{}
		p.Name = nameIP[i]
		p.Status.PodIP = nameIP[i+1]
		pods = append(pods, p)
	}
	return pods
}

const (
	// scopeStale is the historic shared name a half-finished rename leaves behind;
	// taken from the API constant so the fixture cannot drift from the product.
	scopeStale     = littleredv1alpha1.LegacySentinelMasterName
	scopeForeignIP = "10.9.9.9"
	scopeOurIP2    = "10.0.0.2"
	scopeSentIP    = "10.0.1.1"
	scopeSentPod   = "inst-sentinel-0"
	scopeRedisPod  = "inst-redis-0"
	scopePod0      = "s-0"
	tokenFail      = "[FAIL]"
)

// scopeState builds a ReplicationState carrying the given Sentinel views, with the
// two pod IPs of "our" instance marked valid.
func scopeState(sns ...*redisclient.SentinelNodeState) *redisclient.ReplicationState {
	st := redisclient.NewReplicationState()
	st.ValidIPs[ipMaster] = true
	st.ValidIPs[scopeOurIP2] = true
	for _, sn := range sns {
		st.SentinelNodes[sn.IP] = sn
	}
	return st
}

// TestRenderMasterNameScope is the regression guard for the whole of WP5: before it,
// `verify` asked each Sentinel about ONE name and so reported a two-name instance —
// two `sentinel monitor` lines, two failover state machines over the same three pods
// — as entirely healthy (design §10 WP5, measured on t3e).
func TestRenderMasterNameScope(t *testing.T) {
	cases := []struct {
		name     string
		state    *redisclient.ReplicationState
		wantFail bool
		contains []string
		absent   []string
	}{
		{
			name: "converged: unchanged output, and verification still passes",
			state: scopeState(&redisclient.SentinelNodeState{
				PodName: scopePod0, IP: scopeSentIP, Reachable: true, Monitoring: true,
				MonitoredMasters: []redisclient.MonitoredMaster{
					{Name: nameDesired, IP: ipMaster, Flags: roleMaster},
				},
			}),
			wantFail: false,
			contains: []string{`[OK]`, nameDesired},
			absent:   []string{tokenFail, redisclient.MasterNameStale, redisclient.MasterNameForeign},
		},
		{
			name: "the LR-048 shape: both names are printed and verification FAILS",
			state: scopeState(&redisclient.SentinelNodeState{
				PodName: scopePod0, IP: scopeSentIP, Reachable: true, Monitoring: true,
				MonitoredMasters: []redisclient.MonitoredMaster{
					{Name: nameDesired, IP: ipMaster, Flags: roleMaster},
					{Name: scopeStale, IP: ipMaster, Flags: roleMaster},
				},
			}),
			wantFail: true,
			contains: []string{
				tokenFail, scopeStale, nameDesired, ipMaster, "flags:" + roleMaster, scopePod0, redisclient.MasterNameStale,
			},
			absent: []string{"[OK]"},
		},
		{
			name: "a stale name at a live address that is not ours is reported as FOREIGN",
			state: scopeState(&redisclient.SentinelNodeState{
				PodName: scopePod0, IP: scopeSentIP, Reachable: true, Monitoring: true,
				MonitoredMasters: []redisclient.MonitoredMaster{
					{Name: nameDesired, IP: ipMaster, Flags: roleMaster},
					{Name: scopeStale, IP: scopeForeignIP, Flags: roleMaster},
				},
			}),
			wantFail: true,
			contains: []string{tokenFail, redisclient.MasterNameForeign, scopeForeignIP, "capture"},
		},
		{
			name: "a stale name at a flagged-down address is ordinary debris, not a foreign master",
			state: scopeState(&redisclient.SentinelNodeState{
				PodName: scopePod0, IP: scopeSentIP, Reachable: true, Monitoring: true,
				MonitoredMasters: []redisclient.MonitoredMaster{
					{Name: nameDesired, IP: ipMaster, Flags: roleMaster},
					{Name: scopeStale, IP: scopeForeignIP, Flags: "s_down,o_down," + roleMaster},
				},
			}),
			wantFail: true,
			contains: []string{tokenFail, redisclient.MasterNameStale},
			absent:   []string{redisclient.MasterNameForeign, "capture"},
		},
		{
			name: "an unread master list warns and does not fail — absence of evidence is not evidence",
			state: scopeState(&redisclient.SentinelNodeState{
				PodName: scopePod0, IP: scopeSentIP, Reachable: true, Monitoring: true,
			}),
			wantFail: false,
			contains: []string{"[WARN]", scopePod0},
			absent:   []string{tokenFail},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scope := tc.state.SurveyMonitoredNames(nameDesired)
			lines, fail := renderMasterNameScope(scope, nameDesired)
			out := strings.Join(lines, "\n")
			// Compared case-insensitively: the verdict tokens and the class labels
			// carry deliberate emphasis, and the assertions are about which facts
			// are stated, not how loudly.
			hay := strings.ToLower(out)
			if fail != tc.wantFail {
				t.Errorf("fail = %v, want %v\noutput:\n%s", fail, tc.wantFail, out)
			}
			for _, want := range tc.contains {
				if !strings.Contains(hay, strings.ToLower(want)) {
					t.Errorf("output does not mention %q:\n%s", want, out)
				}
			}
			for _, no := range tc.absent {
				if strings.Contains(hay, strings.ToLower(no)) {
					t.Errorf("output unexpectedly mentions %q:\n%s", no, out)
				}
			}
		})
	}
}

// TestReportCrossInstanceFailsOnAStaleName wires the survey through the function
// `verify` actually calls, and pins the two judgements that shape it: a stale name
// makes verification fail (a behaviour change to an existing command), and a captured
// instance is not accused twice in two voices — one capture runbook pointer, once.
func TestReportCrossInstanceFailsOnAStaleName(t *testing.T) {
	cCtx := &types.ClusterContext{
		Name: "inst", Namespace: "ns", SentinelMasterName: nameDesired,
		SentinelPods: podsWithIPs(scopeSentPod, scopeSentIP),
		RedisPods:    podsWithIPs(scopeRedisPod, ipMaster, "inst-redis-1", scopeOurIP2),
	}

	t.Run("a stale name fails verification", func(t *testing.T) {
		state := scopeState(&redisclient.SentinelNodeState{
			PodName: scopeSentPod, IP: scopeSentIP, Reachable: true, Monitoring: true,
			MasterIP: ipMaster, MasterFlags: roleMaster, NumOtherSentinels: 0, NumSlaves: 1,
			MonitoredMasters: []redisclient.MonitoredMaster{
				{Name: nameDesired, IP: ipMaster, Flags: roleMaster},
				{Name: scopeStale, IP: ipMaster, Flags: roleMaster},
			},
		})
		var fail bool
		out := captureStdout(t, func() { fail = reportCrossInstance(state, cCtx) })
		if !fail {
			t.Errorf("reportCrossInstance() = false, want true (a stale master name is a failure)\n%s", out)
		}
		if !strings.Contains(out, scopeStale) {
			t.Errorf("the stale name is not reported:\n%s", out)
		}
	})

	t.Run("a converged instance still passes and is unchanged", func(t *testing.T) {
		state := scopeState(&redisclient.SentinelNodeState{
			PodName: scopeSentPod, IP: scopeSentIP, Reachable: true, Monitoring: true,
			MasterIP: ipMaster, MasterFlags: roleMaster, NumSlaves: 1,
			MonitoredMasters: []redisclient.MonitoredMaster{
				{Name: nameDesired, IP: ipMaster, Flags: roleMaster},
			},
		})
		var fail bool
		out := captureStdout(t, func() { fail = reportCrossInstance(state, cCtx) })
		if fail {
			t.Errorf("reportCrossInstance() = true, want false for a converged instance\n%s", out)
		}
		if !strings.Contains(out, "No foreign Sentinel contact observed") {
			t.Errorf("the pre-existing cross-instance verdict is gone:\n%s", out)
		}
	})

	t.Run("a captured instance is not accused twice", func(t *testing.T) {
		state := scopeState(&redisclient.SentinelNodeState{
			PodName: scopeSentPod, IP: scopeSentIP, Reachable: true, Monitoring: true,
			MasterIP: scopeForeignIP, MasterFlags: roleMaster, NumOtherSentinels: 3, NumSlaves: 5,
			MonitoredMasters: []redisclient.MonitoredMaster{
				{Name: scopeStale, IP: scopeForeignIP, Flags: roleMaster},
			},
		})
		var fail bool
		out := captureStdout(t, func() { fail = reportCrossInstance(state, cCtx) })
		if !fail {
			t.Errorf("reportCrossInstance() = false, want true for a foreign master name\n%s", out)
		}
		if n := strings.Count(out, "docs/USAGE.md"); n != 1 {
			t.Errorf("the capture runbook is pointed at %d times, want exactly 1:\n%s", n, out)
		}
	})
}

// TestSentinelVerifyJSONCarriesTheMasterNameScope pins the machine-readable half:
// a two-name instance must not be able to read as `"healthy": true` for a consumer
// that never looks at the text output.
//
// Green from birth — it asserts a surface added in the same change rather than
// driving it — so its teeth are shown by mutation (drop `scope.Converged()` from the
// health verdict, and the first assertion fails).
func TestSentinelVerifyJSONCarriesTheMasterNameScope(t *testing.T) {
	state := scopeState(&redisclient.SentinelNodeState{
		PodName: scopeSentPod, IP: scopeSentIP, Reachable: true, Monitoring: true,
		MasterIP: ipMaster, MasterFlags: roleMaster, NumSlaves: 1,
		MonitoredMasters: []redisclient.MonitoredMaster{
			{Name: nameDesired, IP: ipMaster, Flags: roleMaster},
			{Name: scopeStale, IP: ipMaster, Flags: roleMaster},
		},
	})
	state.RealMasterIP = ipMaster
	state.RedisNodes[ipMaster] = &redisclient.RedisNodeState{
		PodName: scopeRedisPod, IP: ipMaster, Role: roleMaster, Reachable: true,
	}

	out := buildSentinelVerifyJSON("inst", "ns", map[string]string{ipMaster: scopeRedisPod},
		state, nameDesired, nameDesired, 1, 1)

	if out.Healthy {
		t.Errorf("Healthy = true for an instance monitoring two master names")
	}
	if out.MasterNameScope == nil {
		t.Fatalf("MasterNameScope is absent for a target whose CR names the master name")
	}
	if out.MasterNameScope.Converged {
		t.Errorf("MasterNameScope.Converged = true, want false")
	}
	if len(out.MasterNameScope.Stale) != 1 || out.MasterNameScope.Stale[0] != scopeStale {
		t.Errorf("MasterNameScope.Stale = %#v, want [mymaster]", out.MasterNameScope.Stale)
	}
	if len(out.Sentinels) != 1 || len(out.Sentinels[0].MonitoredMasters) != 2 {
		t.Fatalf("per-Sentinel monitored masters missing: %#v", out.Sentinels)
	}
	classes := map[string]string{}
	for _, m := range out.Sentinels[0].MonitoredMasters {
		classes[m.Name] = m.Class
	}
	if classes[nameDesired] != redisclient.MasterNameDesired || classes[scopeStale] != redisclient.MasterNameStale {
		t.Errorf("classes = %#v, want desired/stale", classes)
	}
}

// TestMonitoredMastersJSONClassifiesWithoutAGather is `lrctl inspect`'s path: it has
// the pod addresses but no gathered ReplicationState, and must classify identically
// to `verify`. Green from birth; mutated by pointing it at an empty address set,
// which reclassifies the stale entry as foreign.
func TestMonitoredMastersJSONClassifiesWithoutAGather(t *testing.T) {
	ourIPs := map[string]bool{ipMaster: true}
	got := monitoredMastersJSON([]redisclient.MonitoredMaster{
		{Name: nameDesired, IP: ipMaster, Flags: roleMaster},
		{Name: scopeStale, IP: ipMaster, Flags: roleMaster},
		{Name: "someone-else", IP: scopeForeignIP, Flags: roleMaster},
		{Name: "dead-debris", IP: scopeForeignIP, Flags: "s_down," + roleMaster},
	}, nameDesired, ourIPs)

	want := []string{
		redisclient.MasterNameDesired,
		redisclient.MasterNameStale,
		redisclient.MasterNameForeign,
		redisclient.MasterNameStale,
	}
	for i, w := range want {
		if got[i].Class != w {
			t.Errorf("%q classified %q, want %q", got[i].Name, got[i].Class, w)
		}
	}
}

// TestReportCrossInstanceSkipsTheCheckWithoutACR pins that an --unmanaged target,
// where the wanted name is a fallback guess rather than a CR field, is not accused of
// carrying a stale name for using a different one.
func TestReportCrossInstanceSkipsTheCheckWithoutACR(t *testing.T) {
	cCtx := &types.ClusterContext{
		Name: "inst", Namespace: "ns", // SentinelMasterName deliberately unset
		SentinelPods: podsWithIPs(scopeSentPod, scopeSentIP),
		RedisPods:    podsWithIPs(scopeRedisPod, ipMaster),
	}
	state := scopeState(&redisclient.SentinelNodeState{
		PodName: scopeSentPod, IP: scopeSentIP, Reachable: true, Monitoring: true,
		MasterIP: ipMaster, MasterFlags: roleMaster,
		MonitoredMasters: []redisclient.MonitoredMaster{
			{Name: "someone-elses-name", IP: ipMaster, Flags: roleMaster},
		},
	})
	var fail bool
	out := captureStdout(t, func() { fail = reportCrossInstance(state, cCtx) })
	if fail {
		t.Errorf("reportCrossInstance() = true; a guessed name must not accuse\n%s", out)
	}
	if !strings.Contains(out, "not known") {
		t.Errorf("the skip is not explained:\n%s", out)
	}
}
