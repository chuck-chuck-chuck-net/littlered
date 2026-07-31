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

const linkStatusUp = "up" // test-side counterpart of linkStatusDown

// --- planMasterDeath: the full detection matrix (ADR-011 §4) ---------------

func TestPlanMasterDeath(t *testing.T) {
	const downAfter = 5 * time.Second
	now := time.Unix(3_000_000, 0)
	elapsed := func() *time.Time { u := now.Add(-downAfter - time.Second); return &u }
	fresh := func() *time.Time { u := now.Add(-time.Second); return &u }

	livePod := masterPodView{present: true, ready: true}

	tests := []struct {
		name      string
		pod       masterPodView
		reachable bool
		links     []string
		downSince *time.Time
		want      masterDeathAction
	}{
		// --- alive ---------------------------------------------------------
		{
			name:      "alive: operator-reachable -> clear marker (stale marker discarded)",
			pod:       livePod,
			reachable: true,
			downSince: elapsed(),
			want:      masterDeathClearMarker,
		},

		// --- Kubernetes-authoritative: immediate, no window -----------------
		{
			name:      "k8s: pod deleted/replaced -> dead immediately, no marker needed",
			pod:       masterPodView{present: false},
			reachable: false,
			downSince: nil,
			want:      masterDeathDeclareK8s,
		},
		{
			name:      "k8s: redis container not-Ready per kubelet -> dead even though operator can dial it",
			pod:       masterPodView{present: true, ready: false},
			reachable: true,
			downSince: nil,
			want:      masterDeathDeclareK8s,
		},
		{
			name:      "k8s: terminating master -> dead immediately (graceful handover, ADR-011 s7)",
			pod:       masterPodView{present: true, ready: true, terminating: true},
			reachable: true,
			downSince: nil,
			want:      masterDeathDeclareK8s,
		},

		// --- probe-evidenced: window + corroboration ------------------------
		{
			name:      "probe: unreachable, no marker -> start window (even with all replica links down)",
			pod:       livePod,
			reachable: false,
			links:     []string{linkStatusDown, linkStatusDown},
			downSince: nil,
			want:      masterDeathStartWindow,
		},
		{
			name:      "probe: unreachable, window not elapsed -> wait (unanimous link:down does not shortcut it)",
			pod:       livePod,
			reachable: false,
			links:     []string{linkStatusDown, linkStatusDown},
			downSince: fresh(),
			want:      masterDeathWait,
		},
		{
			name:      "probe: window elapsed + every reachable replica link:down -> dead (corroborated)",
			pod:       livePod,
			reachable: false,
			links:     []string{linkStatusDown, linkStatusDown},
			downSince: elapsed(),
			want:      masterDeathDeclareProbe,
		},
		{
			name:      "probe: window elapsed but a replica still sees link:up -> vetoed, hold marker (LR-017)",
			pod:       livePod,
			reachable: false,
			links:     []string{linkStatusDown, linkStatusUp},
			downSince: elapsed(),
			want:      masterDeathHold,
		},
		{
			name:      "probe: window elapsed, zero reachable replicas -> no corroboration -> hold (dial alone never suffices)",
			pod:       livePod,
			reachable: false,
			links:     nil,
			downSince: elapsed(),
			want:      masterDeathHold,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := planMasterDeath(tc.pod, tc.reachable, tc.links, tc.downSince, now, downAfter)
			if got != tc.want {
				t.Fatalf("planMasterDeath() = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- planFailover: every gate and tier of the one table (ADR-011 §5/§6) ----

func TestPlanFailover(t *testing.T) {
	const cooldown = 15 * time.Second
	now := time.Unix(4_000_000, 0)
	elapsed := func() *time.Time { u := now.Add(-cooldown - time.Second); return &u }
	fresh := func() *time.Time { u := now.Add(-5 * time.Second); return &u }

	// a single same-lineage survivor holding data — the plain crash-failover input.
	survivor := []rnSpec{{ip: "10.0.0.2", reachable: true, keys: 5, offset: 100, replid: "A", role: "slave"}}

	tests := []struct {
		name         string
		redis        []rnSpec
		liveMasterIP string
		allowUnsafe  bool
		bootstrapIP  string
		unsettled    bool
		since        *time.Time
		wantAction   failoverAction
		wantMasterIP string
		wantDiverged bool
		wantHolders  int
	}{
		// --- GATES ----------------------------------------------------------
		{
			name: "gate: live master exists -> none (stragglers are Rule R's job, not a promotion)",
			redis: []rnSpec{
				{ip: "10.0.0.1", reachable: true, keys: 5, offset: 100, replid: "A", role: "master"},
				{ip: "10.0.0.2", reachable: true, keys: 5, offset: 90, replid: "A", role: "master"}, // straggler
			},
			liveMasterIP: "10.0.0.1",
			since:        elapsed(),
			wantAction:   failoverNone,
		},
		{
			name:         "gate: live master short-circuits even an unsettled transition -> none (executor resumes it)",
			redis:        []rnSpec{{ip: "10.0.0.1", reachable: true, keys: 5, replid: "A", role: "master"}},
			liveMasterIP: "10.0.0.1",
			unsettled:    true,
			wantAction:   failoverNone,
		},
		{
			name:       "gate: unsettled prior transition -> wait (even with a survivor ready to promote)",
			redis:      survivor,
			unsettled:  true,
			since:      elapsed(),
			wantAction: failoverWait,
		},
		{
			name:       "gate: within post-transition cooldown -> wait (cascades are serialized)",
			redis:      survivor,
			since:      fresh(),
			wantAction: failoverWait,
		},
		{
			name:        "gate: 0 holders, no bootstrap candidate yet -> wait",
			redis:       []rnSpec{{ip: "10.0.0.1", reachable: false}},
			bootstrapIP: "",
			since:       elapsed(),
			wantAction:  failoverWait,
		},

		// --- FUNCTIONALITY ---------------------------------------------------
		{
			name:         "func: cooldown elapsed -> marker alone does not block; promote the survivor",
			redis:        survivor,
			since:        elapsed(),
			wantAction:   failoverPromote,
			wantMasterIP: "10.0.0.2",
			wantHolders:  1,
		},
		{
			name:         "func: no prior transition (nil marker) -> act immediately; promote the survivor",
			redis:        survivor,
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: "10.0.0.2",
			wantHolders:  1,
		},
		{
			name:         "func: 0 holders + bootstrap candidate -> seed it (bootstrap is a row of the same table)",
			redis:        []rnSpec{{ip: "10.0.0.1", reachable: false}, {ip: "10.0.0.2", reachable: true, keys: 0}},
			bootstrapIP:  "10.0.0.1",
			since:        nil,
			wantAction:   failoverSeed,
			wantMasterIP: "10.0.0.1",
		},
		{
			name: "func: 2 holders ONE lineage -> promote highest offset, NO opt-in",
			redis: []rnSpec{
				{ip: "10.0.0.1", reachable: true, keys: 5, offset: 100, replid: "A", role: "slave"},
				{ip: "10.0.0.2", reachable: true, keys: 5, offset: 250, replid: "A", role: "slave"},
			},
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: "10.0.0.2",
			wantDiverged: false,
			wantHolders:  2,
		},
		{
			name: "func: promotion chain (replid rotated, linked via replid2) -> ONE lineage, promote, no opt-in (LR-024)",
			redis: []rnSpec{
				{ip: "10.0.0.1", reachable: true, keys: 1, offset: 100, replid: "716d42", role: "slave"},
				{ip: "10.0.0.2", reachable: true, keys: 1, offset: 120, replid: "1cc4b7", replid2: "716d42", role: "master"},
			},
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: "10.0.0.2",
			wantDiverged: false,
			wantHolders:  2,
		},
		{
			name: "func: terminating dead master never blocks promotion (contrast sentinel Rule A)",
			redis: []rnSpec{
				// the crashed master: pod still terminating (its IP is still a valid pod IP),
				// unreachable — it must not suppress the decision.
				{ip: "10.0.0.9", reachable: false, role: "master"},
				{ip: "10.0.0.2", reachable: true, keys: 5, offset: 100, replid: "A", role: "slave"},
			},
			liveMasterIP: "",
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: "10.0.0.2",
			wantHolders:  1,
		},
		{
			name: "func: diverged lineages, opt-in OFF -> refuse",
			redis: []rnSpec{
				{ip: "10.0.0.1", reachable: true, keys: 5, offset: 100, replid: "AAA", replid2: "PPP", role: "master"},
				{ip: "10.0.0.2", reachable: true, keys: 9, offset: 90, replid: "BBB", replid2: "QQQ", role: "master"},
			},
			allowUnsafe: false,
			since:       nil,
			wantAction:  failoverRefuse,
			wantHolders: 2,
		},
		{
			name: "func: diverged lineages, opt-in ON -> unsafe-elect best + diverged flag",
			redis: []rnSpec{
				{ip: "10.0.0.1", reachable: true, keys: 5, offset: 300, replid: "AAA", role: "master"},
				{ip: "10.0.0.2", reachable: true, keys: 9, offset: 90, replid: "BBB", role: "master"},
			},
			allowUnsafe:  true,
			since:        nil,
			wantAction:   failoverUnsafeElect,
			wantMasterIP: "10.0.0.1",
			wantDiverged: true,
			wantHolders:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// failover mode has no Sentinels: only RedisNodes/ValidIPs are populated.
			state := buildState(nil, tc.redis)
			got := planFailover(state, tc.liveMasterIP, tc.allowUnsafe, tc.bootstrapIP,
				tc.unsettled, tc.since, now, cooldown)
			if got.action != tc.wantAction {
				t.Fatalf("action = %v, want %v", got.action, tc.wantAction)
			}
			if tc.wantMasterIP != "" && got.masterIP != tc.wantMasterIP {
				t.Errorf("masterIP = %q, want %q", got.masterIP, tc.wantMasterIP)
			}
			if got.diverged != tc.wantDiverged {
				t.Errorf("diverged = %v, want %v", got.diverged, tc.wantDiverged)
			}
			if tc.wantHolders != 0 && got.holders != tc.wantHolders {
				t.Errorf("holders = %d, want %d", got.holders, tc.wantHolders)
			}
		})
	}
}
