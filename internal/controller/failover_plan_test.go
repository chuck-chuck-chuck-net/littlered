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

		// --- ARITY (LR-038 / handover gap 3) -------------------------------
		// Every row above uses two witnesses or none. `replicas: 1` is the CRD
		// minimum and therefore a supported, reachable topology, and it has
		// exactly ONE witness — an arity this table never exercised.
		//
		// The policy question it forces, which ADR-011 never visibly answered:
		// IS A SINGLE WITNESS SUFFICIENT CORROBORATION TO DECLARE A MASTER DEAD?
		// The answer encoded here is YES, and it is a deliberate choice rather
		// than an accident of `len(links) > 0`:
		//   - LR-017's lesson is that the OPERATOR'S OWN DIAL is never sufficient,
		//     because a blackholing network fools exactly one viewpoint — the
		//     operator's. A replica is an independent viewpoint, and at
		//     `replicas: 1` it is the only one that exists.
		//   - Requiring two would make `replicas: 1` permanently undead: no
		//     failover could ever be declared on probe evidence, so the mode's
		//     minimum topology would silently lose its HA.
		//   - The kubelet-authoritative branch is unaffected either way, so a
		//     genuinely dead pod is still caught with no witnesses at all.
		{
			name:      "arity 1: single witness reporting link:down IS sufficient corroboration (replicas:1 is supported)",
			pod:       livePod,
			reachable: false,
			links:     []string{linkStatusDown},
			downSince: elapsed(),
			want:      masterDeathDeclareProbe,
		},
		{
			name:      "arity 1: the single witness still sees link:up -> vetoed (one witness can also veto)",
			pod:       livePod,
			reachable: false,
			links:     []string{linkStatusUp},
			downSince: elapsed(),
			want:      masterDeathHold,
		},
		{
			name:      "arity 3: unanimous link:down -> dead (no majority arithmetic; ALL must agree)",
			pod:       livePod,
			reachable: false,
			links:     []string{linkStatusDown, linkStatusDown, linkStatusDown},
			downSince: elapsed(),
			want:      masterDeathDeclareProbe,
		},
		{
			name:      "arity 3: a single dissenting link:up vetoes the majority (unanimity, not quorum)",
			pod:       livePod,
			reachable: false,
			links:     []string{linkStatusDown, linkStatusDown, linkStatusUp},
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
	survivor := []rnSpec{{ip: ipReplica, reachable: true, keys: 5, offset: 100, replid: "A", role: roleSlave}}

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
				{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: "A", role: RoleMaster},
				{ip: ipReplica, reachable: true, keys: 5, offset: 90, replid: "A", role: RoleMaster}, // straggler
			},
			liveMasterIP: ipMaster,
			since:        elapsed(),
			wantAction:   failoverNone,
		},
		{
			name:         "gate: live master short-circuits even an unsettled transition -> none (executor resumes it)",
			redis:        []rnSpec{{ip: ipMaster, reachable: true, keys: 5, replid: "A", role: RoleMaster}},
			liveMasterIP: ipMaster,
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
			redis:       []rnSpec{{ip: ipMaster, reachable: false}},
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
			wantMasterIP: ipReplica,
			wantHolders:  1,
		},
		{
			name:         "func: no prior transition (nil marker) -> act immediately; promote the survivor",
			redis:        survivor,
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: ipReplica,
			wantHolders:  1,
		},
		{
			name:         "func: 0 holders + bootstrap candidate -> seed it (bootstrap is a row of the same table)",
			redis:        []rnSpec{{ip: ipMaster, reachable: false}, {ip: ipReplica, reachable: true, keys: 0}},
			bootstrapIP:  ipMaster,
			since:        nil,
			wantAction:   failoverSeed,
			wantMasterIP: ipMaster,
		},
		{
			name: "func: 2 holders ONE lineage -> promote highest offset, NO opt-in",
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: "A", role: roleSlave},
				{ip: ipReplica, reachable: true, keys: 5, offset: 250, replid: "A", role: roleSlave},
			},
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: ipReplica,
			wantDiverged: false,
			wantHolders:  2,
		},
		{
			name: "func: promotion chain (replid rotated, linked via replid2) -> ONE lineage, promote, no opt-in (LR-024)",
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 1, offset: 100, replid: testReplid0, role: roleSlave},
				{ip: ipReplica, reachable: true, keys: 1, offset: 120, replid: testReplid1, replid2: testReplid0, role: RoleMaster},
			},
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: ipReplica,
			wantDiverged: false,
			wantHolders:  2,
		},
		// --- ARITY (LR-038 / handover gap 3) -------------------------------
		// planFailover is set-based (union-find over lineages), so nothing here
		// scales with the holder count — which is exactly the claim these rows
		// pin. `replicas: 1` yields at most ONE holder, and that single holder is
		// then the only copy of the data in existence.
		{
			name:         "arity 1: the sole holder is promoted with no opt-in (replicas:1 — it is the ONLY copy)",
			redis:        []rnSpec{{ip: ipReplica, reachable: true, keys: 7, offset: 300, replid: "A", role: roleSlave}},
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: ipReplica,
			wantHolders:  1,
		},
		{
			name: "arity 3: three holders, one lineage -> promote highest offset, still no opt-in",
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: "A", role: roleSlave},
				{ip: ipReplica, reachable: true, keys: 5, offset: 250, replid: "A", role: roleSlave},
				{ip: ipNode9, reachable: true, keys: 5, offset: 180, replid: "A", role: roleSlave},
			},
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: ipReplica,
			wantHolders:  3,
		},
		{
			name: "arity 3: one divergent lineage among three refuses — divergence is set-based, not a vote",
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: "A", role: roleSlave},
				{ip: ipReplica, reachable: true, keys: 5, offset: 250, replid: "A", role: roleSlave},
				{ip: ipNode9, reachable: true, keys: 5, offset: 180, replid: "B", role: RoleMaster},
			},
			since:       nil,
			wantAction:  failoverRefuse,
			wantHolders: 3,
		},
		{
			name: "func: terminating dead master never blocks promotion (contrast sentinel Rule A)",
			redis: []rnSpec{
				// the crashed master: pod still terminating (its IP is still a valid pod IP),
				// unreachable — it must not suppress the decision.
				{ip: ipNode9, reachable: false, role: RoleMaster},
				{ip: ipReplica, reachable: true, keys: 5, offset: 100, replid: "A", role: roleSlave},
			},
			liveMasterIP: "",
			since:        nil,
			wantAction:   failoverPromote,
			wantMasterIP: ipReplica,
			wantHolders:  1,
		},
		{
			name: "func: diverged lineages, opt-in OFF -> refuse",
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 100, replid: testReplidA, replid2: "PPP", role: RoleMaster},
				{ip: ipReplica, reachable: true, keys: 9, offset: 90, replid: testReplidB, replid2: "QQQ", role: RoleMaster},
			},
			allowUnsafe: false,
			since:       nil,
			wantAction:  failoverRefuse,
			wantHolders: 2,
		},
		{
			name: "func: diverged lineages, opt-in ON -> unsafe-elect best + diverged flag",
			redis: []rnSpec{
				{ip: ipMaster, reachable: true, keys: 5, offset: 300, replid: testReplidA, role: RoleMaster},
				{ip: ipReplica, reachable: true, keys: 9, offset: 90, replid: testReplidB, role: RoleMaster},
			},
			allowUnsafe:  true,
			since:        nil,
			wantAction:   failoverUnsafeElect,
			wantMasterIP: ipMaster,
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

// --- planFailoverFence: closing the graceful-handover write-loss window -----
//
// MEASURED, not hypothesised (t3e, 2026-08-17): the rapid-double-failover chaos
// tier lost 202 of 1171 acknowledged writes on the GRACEFUL path, with
// DataCorruptions 0 and write availability 97.66% — the loss was invisible to
// every assertion the suite had. Cause: the operator promotes a replica but never
// speaks to the outgoing master, which keeps running and keeps ACKing writes for
// its whole ~10s preStop window (resources_failover.go), while an established TCP
// connection through the master Service is not re-routed by the label flip. Those
// writes die with the pod.
//
// The fix is to demote the outgoing master as part of the promotion, so it starts
// answering -READONLY: the loss becomes VISIBLE write failures instead of silent
// data loss (pillar 3.2's principle, applied to failover).
//
// The input is the POD VIEWS, not the gathered Redis state. A first attempt keyed
// on state.RedisNodes was inert in the field (196 of 1163 still lost) because
// reconcileFailoverAssignments omits terminating pods from the gather — so the
// outgoing master is missing from the ground truth exactly when it needs fencing.
// Reachability and role are therefore unknown here and are not needed: SLAVEOF is
// idempotent and the dial is bounded (LR-017).
func TestPlanFailoverFence(t *testing.T) {
	const (
		oldIP, newIP   = "10.0.0.1", "10.0.0.2"
		podOld, podNew = "redis-0", "redis-1"
		podNoIP        = "redis-3"
	)

	tests := []struct {
		name       string
		views      []failoverPodView
		outgoingIP string
		newIP      string
		want       string
	}{
		{
			// The graceful window: the pod is terminating, but redis is alive and
			// still mastering, so it can still ACK a write. THE case this exists
			// for — and the case the gather cannot see.
			name: "outgoing master still present and terminating -> fence it",
			views: []failoverPodView{
				{name: podOld, ip: oldIP, terminating: true},
				{name: podNew, ip: newIP},
			},
			outgoingIP: oldIP,
			newIP:      newIP,
			want:       oldIP,
		},
		{
			// Crash path: the pod is already gone, so there is nothing alive to
			// accept a write and no dial to waste on a dead or blackholing IP.
			name: "outgoing master pod gone -> nothing to fence, no wasted dial",
			views: []failoverPodView{
				{name: podNew, ip: newIP},
			},
			outgoingIP: oldIP,
			newIP:      newIP,
			want:       "",
		},
		{
			// ADR-001 strict IP identity: a same-named pod that came back with a
			// new IP is a different node. The old IP must not be dialed.
			name: "outgoing master pod replaced with a new IP -> nothing to fence",
			views: []failoverPodView{
				{name: podOld, ip: "10.0.0.9"},
				{name: podNew, ip: newIP},
			},
			outgoingIP: oldIP,
			newIP:      newIP,
			want:       "",
		},
		{
			// Resume of a half-applied promotion: the intent already names the pod
			// being promoted. Fencing it would demote the new master.
			name: "outgoing is the pod being promoted -> never fence the new master",
			views: []failoverPodView{
				{name: podNew, ip: newIP},
			},
			outgoingIP: newIP,
			newIP:      newIP,
			want:       "",
		},
		{
			// Seed path: no prior intent, so there is no outgoing master at all.
			name: "no outgoing master (seed) -> nothing to fence",
			views: []failoverPodView{
				{name: podNew, ip: newIP},
			},
			outgoingIP: "",
			newIP:      newIP,
			want:       "",
		},
		{
			// A pod that has not got an IP yet must never absorb the "" lookup.
			name: "pods without IPs do not match an empty outgoing IP",
			views: []failoverPodView{
				{name: podNoIP, ip: ""},
			},
			outgoingIP: "",
			newIP:      newIP,
			want:       "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := planFailoverFence(tc.views, tc.outgoingIP, tc.newIP); got != tc.want {
				t.Errorf("planFailoverFence() = %q, want %q", got, tc.want)
			}
		})
	}
}

// --- masterStartAuthorizedFor: the guard that keeps an emptied ex-master down --
//
// MEASURED (t3e, 2026-08-19): a kill-9 of a PROMOTED master destroyed 352 of 1145
// acknowledged writes, with DataCorruptions 0 and write availability 95.50%. The
// promoted pod had started as a replica (start marker = epoch 1), was promoted
// in place (annotation master@2, no restart, so the marker stayed 1), and on
// restart read 2 > 1 and came back as an EMPTY master — whereupon the operator
// believed it healthy and repointed the replicas holding the only copy onto it.
//
// The fix is that a restarted process may start as master ONLY with explicit
// operator authorization, which cannot predate the death it refers to. This table
// pins WHICH actions grant it: seeding (zero data holders, target may be parked)
// and nothing else. A promotion must never grant it, because its target is by
// construction a running data holder — authorizing a start there re-arms exactly
// the wipe above for the next kill-9.
func TestMasterStartAuthorizedFor(t *testing.T) {
	tests := []struct {
		action failoverAction
		want   bool
		why    string
	}{
		{failoverSeed, true, "zero data holders; the seeded pod may be parked by the start gate"},
		{failoverPromote, false, "in-place promotion of a REACHABLE data holder; no start involved"},
		{failoverUnsafeElect, false, "still an in-place promotion, just with divergence authorized"},
		{failoverNone, false, "no mastership decision at all"},
		{failoverWait, false, "no mastership decision at all"},
		{failoverRefuse, false, "explicitly declining to elect"},
	}

	for _, tc := range tests {
		if got := masterStartAuthorizedFor(tc.action); got != tc.want {
			t.Errorf("masterStartAuthorizedFor(%v) = %v, want %v — %s", tc.action, got, tc.want, tc.why)
		}
	}
}

// TestFailoverStartupGateRequiresMasterAuthorization pins the pod-side half: the
// generated startup script must refuse a master role for a RESTARTED process
// unless the operator's authorization is newer than what the process started
// under. Structural, because the logic lives in shell.
func TestFailoverStartupGateRequiresMasterAuthorization(t *testing.T) {
	script := buildRedisContainerFailover(newFailoverTestLittleRed()).Command[2]

	for _, want := range []string{
		// the start marker is read, and named for what it is
		"START_MARKER",
		"STARTED_UNDER_EPOCH",
		// the master-role gate exists and consults the authorization annotation
		`if [ "$ASSIGNED_ROLE" = "master" ] && [ -n "$STARTED_UNDER_EPOCH" ]`,
		AnnotationMasterStartAuthorizedEpoch,
		`"$MASTER_START_AUTH" -gt "$STARTED_UNDER_EPOCH"`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("startup script missing %q — a restarted process could start as an empty master", want)
		}
	}

	// The marker must be recorded before exec, or nothing detects the restart.
	markerAt := strings.Index(script, `> "$START_MARKER"`)
	execAt := strings.Index(script, "exec redis-server")
	if markerAt < 0 || execAt < 0 || markerAt > execAt {
		t.Errorf("the start marker must be written BEFORE exec (marker at %d, exec at %d)", markerAt, execAt)
	}
}
