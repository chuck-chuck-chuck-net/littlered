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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

const (
	// wrongPassReply / noAuthReply are the two server replies that matter, and the
	// two that point at OPPOSITE remedies: WRONGPASS means the Secret moved and the
	// pods did not, NOAUTH means the operator lost a password the pods still enforce.
	wrongPassReply = "WRONGPASS invalid username-password pair or user is disabled."
	noAuthReply    = "NOAUTH Authentication required."

	// authVetoCooldown is deliberately NOT the production 30s: these tests are about
	// the veto, not about the timer, and a literal that happens to match production
	// invites a reader to think one depends on the other.
	authVetoCooldown = 5 * time.Second

	// Pod addresses for these fixtures. Redis reuses the shared ipMaster/ipReplica/
	// ipNode3; the Sentinel addresses are file-local.
	authVetoS1 = "10.0.1.1"
	authVetoS2 = "10.0.1.2"
	authVetoS3 = "10.0.1.3"

	authVetoRedis0    = "redis-0"
	authVetoRedis1    = "redis-1"
	authVetoRedis2    = "redis-2"
	authVetoSentinel0 = "sentinel-0"
)

// wrongPassGatherer is the auth design §3.5a Path C fleet, as the operator sees it
// after a password rotation followed by an unrelated Sentinel-only restart:
//
//   - the SENTINEL pods restarted, so they came back on the NEW password (the
//     operator can read them) and BARE (EmptyDir, pillar 3.1);
//   - the REDIS pods did not restart, so they still enforce the OLD password and
//     every probe comes back WRONGPASS — while holding the entire dataset.
//
// This is not a contrived shape. Every ingredient is a supported operation: editing
// the password in the Secret rolls nothing (auth design §3.6), and a drain, an
// eviction, an OOM or `kubectl rollout restart statefulset/<n>-sentinel` — the
// escape hatch docs/USAGE.md itself documents — restarts only the Sentinels.
type wrongPassGatherer struct {
	// sentinelIPs answer normally (bare); every other address refuses the credential.
	sentinelIPs map[string]bool
}

func (g *wrongPassGatherer) GetRedisState(_ context.Context, _, _ string) (*redisclient.RedisNodeState, error) {
	return nil, errors.New("failed to get info: " + wrongPassReply)
}

func (g *wrongPassGatherer) GetSentinelState(
	_ context.Context, podName, ip, _ string,
) (*redisclient.SentinelNodeState, error) {
	if !g.sentinelIPs[ip] {
		return nil, errors.New("failed to get master: " + wrongPassReply)
	}
	// Reachable and bare — restarted onto the new password with an empty EmptyDir.
	return &redisclient.SentinelNodeState{PodName: podName, IP: ip, Reachable: true, Monitoring: false}, nil
}

func (g *wrongPassGatherer) GetClusterID(context.Context, string, string) (string, error) {
	return "", errors.New("not a cluster")
}

func (g *wrongPassGatherer) GetClusterInfo(context.Context, string, string) (*redisclient.ClusterInfo, error) {
	return nil, errors.New("not a cluster")
}

func (g *wrongPassGatherer) GetClusterNodes(context.Context, string, string) ([]redisclient.ClusterNodeInfo, error) {
	return nil, errors.New("not a cluster")
}

// gatherWrongPassFleet runs the REAL gather against the stub above, so this test
// exercises the whole chain — probe error -> classification -> carried on the state
// -> read by the planner — rather than a hand-built state that could be made to say
// anything.
func gatherWrongPassFleet() *redisclient.ReplicationState {
	redisPods := map[string]string{
		ipMaster:  authVetoRedis0,
		ipReplica: authVetoRedis1,
		ipNode3:   authVetoRedis2,
	}
	sentinelPods := map[string]string{
		authVetoS1: authVetoSentinel0,
		authVetoS2: "sentinel-1",
		authVetoS3: "sentinel-2",
	}
	g := &wrongPassGatherer{sentinelIPs: map[string]bool{
		authVetoS1: true, authVetoS2: true, authVetoS3: true,
	}}
	return redisclient.GatherReplicationState(context.Background(), g, redisPods, sentinelPods, "ns.inst")
}

// TestLeaderlessRecoveryRefusesWhenPodsRefuseOurCredential is LR-051's headline red.
//
// Rule L's data-safety gate keys on DataHolders(), which filters on Reachable. Under
// a credential mismatch every Redis pod reads Reachable:false / Keys:0, so the
// operator counts ZERO data holders and takes the no-opt-in `recoverySeedNoData`
// branch: it points the whole Sentinel quorum at redis-0 on the belief that the
// instance holds nothing, while the instance holds everything. The damage is
// delivered later by ordinary mechanisms — when the Redis pods do restart onto the
// new password, redis-0 returns EMPTY as the Sentinel-blessed master and every other
// pod full-syncs from it.
//
// The >=2-holder REFUSE — the ONLY gate in the product whose purpose is to stop the
// operator discarding data, and the only one requiring
// allowUnsafeRebootstrapOnDeadlock — can never fire, because the operator cannot see
// a single holder. See the auth design §3.5/§3.5a and LR-051.
func TestLeaderlessRecoveryRefusesWhenPodsRefuseOurCredential(t *testing.T) {
	state := gatherWrongPassFleet()

	// Precondition, asserted rather than assumed: this must be the leaderless
	// bare-Sentinel deadlock signature, or the test would pass for the wrong reason.
	if bare, reachable := state.AllSentinelsBare(); !bare || reachable != 3 {
		t.Fatalf("fixture is not the bare-Sentinel deadlock: bare=%v reachable=%d", bare, reachable)
	}
	if state.RealMasterIP != "" {
		t.Fatalf("fixture is not leaderless: RealMasterIP = %q", state.RealMasterIP)
	}
	if got := len(state.DataHolders()); got != 0 {
		t.Fatalf("fixture precondition: the operator must see 0 data holders "+
			"(that is the defect), got %d", got)
	}
	if got := len(state.AuthFailedRedisNodes()); got != 3 {
		t.Fatalf("all three Redis pods must be classified AuthFailed, got %d", got)
	}

	now := time.Unix(1_000_000, 0)
	elapsed := now.Add(-authVetoCooldown - time.Second)

	plan := planLeaderlessRecovery(state, 2, false, ipMaster, &elapsed, now, authVetoCooldown)
	if plan.action != recoveryRefuseUnverified {
		t.Fatalf("planLeaderlessRecovery = %v, want recoveryRefuseUnverified — the operator "+
			"cannot prove these pods are empty (they refused its credential), so seeding "+
			"redis-0 would discard the whole dataset", plan.action)
	}
	if plan.action == recoverySeedNoData {
		t.Errorf("this is the LR-051 defect: reseeding an instance that holds everything")
	}

	// The opt-in must NOT unlock it. allowUnsafeRebootstrapOnDeadlock authorizes
	// discarding a SET OF HOLDERS the owner could see; here the operator cannot see
	// the set at all, so the authorization was never given with knowledge of what it
	// authorizes — and the remedy (fix the credential) is trivial and non-destructive.
	plan = planLeaderlessRecovery(state, 2, true, ipMaster, &elapsed, now, authVetoCooldown)
	if plan.action != recoveryRefuseUnverified {
		t.Errorf("with allowUnsafe=true: action = %v, want recoveryRefuseUnverified", plan.action)
	}
}

// TestGhostMasterRecoveryRefusesWhenPodsRefuseOurCredential is the same veto in the
// sibling planner (LR-024). It elects a survivor from BestDataHolder, which is
// DataHolders() with a sort on top — so under a credential mismatch it sees no
// survivors either, and its 0-holder branch seeds the bootstrap master over a live
// dataset exactly as Rule L's does.
func TestGhostMasterRecoveryRefusesWhenPodsRefuseOurCredential(t *testing.T) {
	state := gatherWrongPassFleet()

	// Stage the ghost-master signature on top of the credential mismatch: a majority
	// of reachable Sentinels monitoring an address that is not one of our pods, with
	// no healthy known replica.
	for _, sn := range state.SentinelNodes {
		sn.Monitoring = true
		sn.MasterIP = "10.9.9.9" // not in ValidIPs -> a ghost
	}
	if !state.SentinelsMonitorGhostMaster() || state.HasHealthyKnownReplica() {
		t.Fatalf("fixture is not the ghost-master deadlock")
	}

	now := time.Unix(1_000_000, 0)
	elapsed := now.Add(-authVetoCooldown - time.Second)

	plan := planGhostMasterRecovery(state, 2, false, ipMaster, &elapsed, now, authVetoCooldown)
	if plan.action != recoveryRefuseUnverified {
		t.Fatalf("planGhostMasterRecovery = %v, want recoveryRefuseUnverified", plan.action)
	}
	plan = planGhostMasterRecovery(state, 2, true, ipMaster, &elapsed, now, authVetoCooldown)
	if plan.action != recoveryRefuseUnverified {
		t.Errorf("with allowUnsafe=true: action = %v, want recoveryRefuseUnverified", plan.action)
	}
}

// TestPlanAuthReport pins the condition's rendering.
//
// Authored AFTER the implementation and disclosed as such — it is a rendering, not
// a decision, so there was no honest red available beyond "the function does not
// exist". Its teeth are shown by mutation: dropping the reply from the message, or
// reporting True with no failures, each fails a named assertion below.
//
// What it must carry is not cosmetic. "The operator cannot authenticate to its own
// pods" is the sentence that turns auth design §3.5a Path B from silent-forever
// into diagnosable, and the SERVER'S REPLY is the diagnosis: WRONGPASS and NOAUTH
// point at opposite remedies.
func TestPlanAuthReport(t *testing.T) {
	t.Run("no failures reports False and never accuses anyone", func(t *testing.T) {
		rep := planAuthReport(nil)
		if rep.Status != metav1.ConditionFalse || rep.Reason != reasonCredentialAccepted {
			t.Fatalf("got %v/%s, want False/%s", rep.Status, rep.Reason, reasonCredentialAccepted)
		}
		if len(rep.Pods) != 0 {
			t.Errorf("Pods = %v, want none", rep.Pods)
		}
	})

	t.Run("failures name the pods and what each distinct server said", func(t *testing.T) {
		rep := planAuthReport([]authFailure{
			{PodName: authVetoRedis1, Reply: wrongPassReply},
			{PodName: authVetoRedis2, Reply: wrongPassReply},
			{PodName: authVetoSentinel0, Reply: noAuthReply},
		})
		if rep.Status != metav1.ConditionTrue || rep.Reason != reasonCredentialRejected {
			t.Fatalf("got %v/%s, want True/%s", rep.Status, rep.Reason, reasonCredentialRejected)
		}
		for _, pod := range []string{authVetoRedis1, authVetoRedis2, authVetoSentinel0} {
			if !strings.Contains(rep.Message, pod) {
				t.Errorf("message does not name %s: %s", pod, rep.Message)
			}
		}
		// The reply is the diagnosis; a message without it leaves the reader guessing
		// which of two opposite remedies applies.
		if !strings.Contains(rep.Message, "WRONGPASS") || !strings.Contains(rep.Message, "NOAUTH") {
			t.Errorf("message does not carry what the server said: %s", rep.Message)
		}
		// Deduplicated: two pods, one reply, one mention.
		if strings.Count(rep.Message, "WRONGPASS") != 1 {
			t.Errorf("the repeated reply should appear once, got %d: %s",
				strings.Count(rep.Message, "WRONGPASS"), rep.Message)
		}
	})
}

// TestFailoverRefusesWhenPodsRefuseOurCredential is the cross-mode half (rule
// §7.11). planFailover has the SAME 0-holder seed branch as Rule L and reaches it
// the same way: DataHolders() filters on Reachable.
//
// It is reachable, and the route is the failover-mode analogue of auth design
// §3.5a Path C. A rotation alone does NOT reach it — planMasterDeath correctly
// HOLDs, because no reachable replica can corroborate (the LR-017 lesson doing its
// job). But add an ordinary master-pod replacement (a drain, an eviction, a node
// recycle) and the K8s-authoritative arm declares death immediately on kubelet
// readiness, planFailover runs, sees zero data holders because every surviving
// replica is refusing our credential, and seeds the FRESH pod — which came back on
// the new password holding nothing — as master. The replicas holding the only copy
// are then repointed onto it.
func TestFailoverRefusesWhenPodsRefuseOurCredential(t *testing.T) {
	state := redisclient.NewReplicationState()
	// The replaced master: fresh pod, new password, reachable, EMPTY.
	state.ValidIPs[ipMaster] = true
	state.RedisNodes[ipMaster] = &redisclient.RedisNodeState{
		PodName: authVetoRedis0, IP: ipMaster, Reachable: true, Role: RoleMaster, Keys: 0,
	}
	// The survivors: never restarted, still on the old password, holding everything.
	for i, ip := range []string{ipReplica, ipNode3} {
		state.ValidIPs[ip] = true
		state.RedisNodes[ip] = &redisclient.RedisNodeState{
			PodName:      "redis-" + string(rune('1'+i)),
			IP:           ip,
			Reachable:    false,
			ProbeFailure: redisclient.ProbeAuthFailed,
			ProbeError:   wrongPassReply,
		}
	}

	if got := len(state.DataHolders()); got != 0 {
		t.Fatalf("fixture precondition: the operator must see 0 data holders, got %d", got)
	}

	now := time.Unix(1_000_000, 0)
	plan := planFailover(state, "", false, ipMaster, false, nil, now, authVetoCooldown)
	if plan.action != failoverRefuseUnverified {
		t.Fatalf("planFailover = %v, want failoverRefuseUnverified — seeding the fresh, "+
			"empty pod while two survivors hold the only copy is the LR-051 defect in "+
			"failover mode", plan.action)
	}
	plan = planFailover(state, "", true, ipMaster, false, nil, now, authVetoCooldown)
	if plan.action != failoverRefuseUnverified {
		t.Errorf("with allowUnsafe=true: action = %v, want failoverRefuseUnverified", plan.action)
	}
}

// TestFailoverAuthVetoDoesNotFireOnAnOrdinaryDeadPod is the failover-mode positive
// control, matching the sentinel one below: a pod that never answered must still be
// seeded over, or the veto is a blanket refusal that disables the mode's recovery.
func TestFailoverAuthVetoDoesNotFireOnAnOrdinaryDeadPod(t *testing.T) {
	state := redisclient.NewReplicationState()
	for i, ip := range []string{ipMaster, ipReplica, ipNode3} {
		state.ValidIPs[ip] = true
		state.RedisNodes[ip] = &redisclient.RedisNodeState{
			PodName: "redis-" + string(rune('0'+i)), IP: ip, Reachable: false,
			ProbeFailure: redisclient.ProbeUnroutable, ProbeError: "connection refused",
		}
	}
	now := time.Unix(1_000_000, 0)
	plan := planFailover(state, "", false, ipMaster, false, nil, now, authVetoCooldown)
	if plan.action != failoverSeed {
		t.Fatalf("planFailover = %v, want failoverSeed — a pod that never answered is the "+
			"ordinary total-restart case failover mode exists to recover", plan.action)
	}
}

// TestAuthVetoDoesNotFireOnAnOrdinaryDeadPod is the positive control, and it is what
// stops the veto being a blanket refusal. An unreachable pod that TIMED OUT or was
// unroutable is the ordinary mass-restart case Rule L exists for; only a pod that
// answered — to refuse us — is unprovably empty. Without this row an "always veto"
// implementation would pass every assertion above while disabling Rule L entirely.
func TestAuthVetoDoesNotFireOnAnOrdinaryDeadPod(t *testing.T) {
	state := redisclient.NewReplicationState()
	for i, ip := range []string{authVetoS1, authVetoS2, authVetoS3} {
		state.ValidIPs[ip] = true
		state.SentinelNodes[ip] = &redisclient.SentinelNodeState{
			PodName: "sentinel-" + string(rune('0'+i)), IP: ip, Reachable: true,
		}
	}
	for i, ip := range []string{ipMaster, ipReplica, ipNode3} {
		state.ValidIPs[ip] = true
		state.RedisNodes[ip] = &redisclient.RedisNodeState{
			PodName: "redis-" + string(rune('0'+i)), IP: ip, Reachable: false,
			ProbeFailure: redisclient.ProbeTimedOut, ProbeError: "i/o timeout",
		}
	}
	state.DetermineRealMaster()

	now := time.Unix(1_000_000, 0)
	elapsed := now.Add(-authVetoCooldown - time.Second)

	plan := planLeaderlessRecovery(state, 2, false, ipMaster, &elapsed, now, authVetoCooldown)
	if plan.action != recoverySeedNoData {
		t.Fatalf("planLeaderlessRecovery = %v, want recoverySeedNoData — a pod that never "+
			"answered is the ordinary mass-restart case and must still be reseeded; only "+
			"a pod that REFUSED us is unprovably empty", plan.action)
	}
}
