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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// Arbitrary-state safety for the sentinel- and failover-mode planners.
//
// WHAT THIS FILE IS. Every planner in this package is normally exercised against a
// state that tells a story: a capture, a leaderless deadlock, a rename. This file
// does the opposite. It constructs states DIRECTLY — as ReplicationState and
// pod-view fixtures — and asks one question of each planner: *given this state, does
// it do something destructive?* The fixtures deliberately carry no provenance in
// their names, their inputs or their assertions, because an invariant that is only
// proven against the states one particular mechanism happens to produce is worth
// nothing the moment a second mechanism produces a different one.
//
// WHERE THE STATES COME FROM. They are OBSERVED, not invented. Each is annotated
// with the entry that measured it:
//
//	(1) two pods reporting role:master at once, a third following a dead address
//	(2) a replica following an address no pod holds, link:down, holding data
//	(3) a Sentinel reporting a failover in flight for minutes on end (LR-055 measured
//	    84-86 consecutive passes, ~178s)
//	(4) two master NAMES monitored at once, one of them naming a live pod (LR-048
//	    measured 56.6s of two names naming two different live pods)
//	(5) a StatefulSet short of its Ready count with pods on two revisions
//	(6) a BARE Sentinel beside monitoring ones — the state Rule 0 exists for,
//	    arriving WITH other damage rather than alone
//	(7) a just-promoted master whose master_replid2 carries the previous lineage
//	    (LR-024's promotion chain)
//	(8) a terminating pod whose address a Sentinel still names as master (LR-050,
//	    LR-053)
//
// and, more importantly, from their COMBINATIONS. Single-fault fixtures are the easy
// half; two individually-safe states are not automatically jointly safe, and the two
// findings recorded below are both combinations rather than singles.
//
// WHAT "DESTRUCTIVE" MEANS HERE, per planner:
//
//	planForsaken            — arming a capture verdict that is not evidenced. The
//	                          verdict gates a scale-to-zero on EmptyDir storage, so a
//	                          false one deletes an instance.
//	planQuarantine          — ScaleToZero while a pod holds data the capture does not
//	                          explain, or that cannot be proven empty.
//	planLeaderlessRecovery  — a seed or an elect that discards a holder.
//	planGhostMasterRecovery — likewise.
//	planStaleMasterNames    — a REMOVE issued without an anchor, or our own pod called
//	                          foreign.
//	planFailover            — an election while a holder is unverifiable.
//	planMasterDeath         — declaring a death on the operator's dial alone.
//
// Tests that pin already-correct behaviour are CHARACTERISATION tests: no honest red
// exists for them, so each names the mutation that was applied to show it has teeth.
// Two tests are left FAILING on purpose; both say so in their own comment.

const (
	// The instance: three Redis pods and three Sentinels.
	asRedis0 = "10.0.0.10"
	asRedis1 = "10.0.0.11"
	asRedis2 = "10.0.0.12"
	asSent0  = "10.0.1.10"
	asSent1  = "10.0.1.11"
	asSent2  = "10.0.1.12"

	// An address that belongs to no pod of ours and answers with clean flags.
	asStranger = "10.9.9.9"
	// An address a pod of ours used to hold and no longer does.
	asDeparted = "10.0.0.99"

	asDesiredName = "ns.inst"
	asOtherName   = "mymaster"

	asLineageA  = "aaaaaaaaaaaa"
	asLineageB  = "bbbbbbbbbbbb"
	asLineageA2 = "cccccccccccc"
)

// asFleet is the pod set every fixture below starts from: three Redis pods and three
// Sentinels of ours, all live topology. Nothing else is ours.
func asFleet() *redisclient.ReplicationState {
	return forsakenState(
		[]string{asRedis0, asRedis1, asRedis2},
		[]string{asSent0, asSent1, asSent2},
	)
}

// asRedisNode adds one Redis pod with the replication detail the data-safety
// predicates actually read. withRedis (forsaken_plan_test.go) sets only role and
// reachability, which is not enough for a holder.
type asNode struct {
	role       string
	masterHost string
	link       string
	keys       int64
	replid     string
	replid2    string
	reachable  bool
	probe      redisclient.ProbeFailure
}

func asRedisNode(s *redisclient.ReplicationState, ip, podName string, n asNode) {
	s.RedisNodes[ip] = &redisclient.RedisNodeState{
		PodName: podName, IP: ip,
		Role: n.role, MasterHost: n.masterHost, LinkStatus: n.link,
		Keys: n.keys, Replid: n.replid, Replid2: n.replid2,
		Reachable: n.reachable, ProbeFailure: n.probe,
	}
}

// asBare is a reachable Sentinel monitoring nothing at all.
func asBare(s *redisclient.ReplicationState, ip string) {
	withSentinel(s, ip, true, false, "", "")
}

// asMonitoring is a reachable Sentinel monitoring exactly one master, under the
// desired name, at masterIP, with clean flags — i.e. that address is answering, so
// Sentinel has no reason to mark it down.
func asMonitoring(s *redisclient.ReplicationState, sentinelIP, masterIP string) {
	withMonitoredSentinel(s, sentinelIP, true, masterIP, RoleMaster,
		redisclient.MonitoredMaster{Name: asDesiredName, IP: masterIP, Flags: RoleMaster})
}

// asReplicaKnown makes a Sentinel report a known replica, which is what
// HasHealthyKnownReplica reads.
func asReplicaKnown(s *redisclient.ReplicationState, sentinelIP string, replicas ...redisclient.ReplicaInfo) {
	if sn := s.SentinelNodes[sentinelIP]; sn != nil {
		sn.Replicas = replicas
	}
}

// ---------------------------------------------------------------------------
// FINDING 1 — a capture verdict can be carried by ONE Sentinel's word.
// ---------------------------------------------------------------------------

// TestACaptureVerdictIsNeverCarriedByASingleSentinel is LEFT FAILING. It is a
// destructive verdict on a constructed state, and softening the assertion or
// choosing a kinder fixture would be exactly the wrong response.
//
// THE STATE, and it is a combination of two observed singles: (6) two Sentinels are
// BARE beside (8)/(2) one Sentinel that still names an address no pod of ours holds
// and that Sentinel has not flagged down. No pod of ours is a master.
//
// THE CLAIM THE CODE MAKES. planForsaken's clause 2 is documented as unanimity —
// "every reachable monitoring Sentinel agrees on ONE master address" — and the
// live-capture procedure that validated the verdict states the consequence
// explicitly: a one-of-three injection "reads as a transition, not a verdict".
//
// WHY THAT IS FALSE HERE. The denominator is *monitoring* Sentinels, and a bare
// Sentinel is not monitoring, so it is not counted at all. Clause 1 asks only for
// `monitoring >= 1`. With two peers bare, "unanimous among the monitoring ones" is
// one Sentinel's opinion, and there is no quorum floor anywhere in the predicate.
//
// THE CONTRAST IS INSIDE THIS REPO, one file away. Every sibling predicate that acts
// on a comparable judgement takes a majority or a quorum over ALL reachable
// Sentinels, so all three refuse on this same state — asserted below so the
// disagreement is visible rather than argued:
//
//	SentinelsMonitorGhostMaster  majority over REACHABLE (bare counted) -> false
//	planLeaderlessRecovery       reachable >= quorum, and all must be bare -> clear
//	planStaleMasterNames         G4, reachable >= quorum
//	planForsaken                 no floor at all -> CAPTURED
//
// THE CONSEQUENCE is not a wrong log line. The verdict gates the quarantine, which
// makes zero the desired replica count at build time; storage is EmptyDir, so the
// instance is deleted and the attempt is counted against a budget that latches.
// The chain is asserted below, so the test fails on the verdict and documents the
// blast radius in the same run.
//
// REACHABILITY, stated rather than assumed. Bare Sentinels beside monitoring ones is
// the ordinary state after the Sentinel StatefulSet is replaced: Sentinel storage is
// EmptyDir, so a restarted Sentinel re-learns its peer set from gossip. The
// StatefulSet-settledness gate that withholds address attribution watches the REDIS
// StatefulSet, so a Sentinel-side replacement leaves it off. That is the shape this
// fixture is: Redis settled, Sentinels part-way back.
func TestACaptureVerdictIsNeverCarriedByASingleSentinel(t *testing.T) {
	// ⚠ BLOCKED ON LR-056 — SKIPPED DELIBERATELY, DO NOT "FIX" BY INVERTING.
	//
	// Everything below is written as the assertion the operator SHOULD satisfy, and
	// it fails against this build. Do not rewrite it into a characterisation of the
	// current verdict: that would assert the defect is correct and would have to be
	// un-written again by whoever fixes it, which is how a defect acquires a test
	// defending it. The failure is real, reproducible from the fixture in this
	// function alone, and recorded in full in LR-056.
	//
	// The fix is a data-safety change to a shipped planner (`planForsaken` gates
	// ADR-016's scale-to-zero) and it predates the work this file was written for, so
	// it gets its own pass rather than a tail-end of a test milestone.
	//
	// UN-SKIP AS PART OF THE LR-056 FIX, not before.
	t.Skip("blocked on LR-056: planForsaken has no quorum floor, so with its peers bare " +
		"a single Sentinel's word arms a capture verdict that deletes the instance; " +
		"un-skip with the LR-056 fix")

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	armed := metav1.NewTime(now.Add(-2 * forsakenCooldown))

	minorityOfOne := func() *redisclient.ReplicationState {
		s := asFleet()
		asMonitoring(s, asSent0, asStranger)
		asBare(s, asSent1)
		asBare(s, asSent2)
		for i, ip := range []string{asRedis0, asRedis1, asRedis2} {
			asRedisNode(s, ip, podNameAt(i), asNode{role: roleSlave, reachable: true})
		}
		return s
	}

	// Positive control: with all three Sentinels naming the stranger, the verdict is
	// evidenced and MUST still be reached. Without this row the assertion below could
	// pass against a predicate that never arms anything.
	unanimous := minorityOfOne()
	asMonitoring(unanimous, asSent1, asStranger)
	asMonitoring(unanimous, asSent2, asStranger)
	if got := planForsaken(unanimous, &armed, now, false); !got.Forsaken {
		t.Fatalf("positive control: three Sentinels naming %s must still be a verdict, got %+v",
			asStranger, got)
	}

	s := minorityOfOne()

	// The sibling predicates, on the identical state.
	if s.SentinelsMonitorGhostMaster() {
		t.Errorf("precondition: SentinelsMonitorGhostMaster must be false — one of three is not a majority")
	}
	if got := planLeaderlessRecovery(s, 2, false, asRedis0, nil, now, leaderlessRecoveryCooldown); got.action != recoveryClearMarker {
		t.Errorf("precondition: planLeaderlessRecovery = %v, want clearMarker", got.action)
	}
	if got := planStaleMasterNames(s, asDesiredName, 2, false, false); got.Reason == staleNamesPruning {
		t.Errorf("precondition: planStaleMasterNames must not prune from a minority, got %q: %s",
			got.Reason, got.Message)
	}

	got := planForsaken(s, &armed, now, false)
	if got.Forsaken {
		t.Errorf("planForsaken.Forsaken = true (foreign_master=%s) on the word of ONE Sentinel "+
			"while its two peers are bare: clause 2's unanimity is computed over MONITORING "+
			"Sentinels only, so bare peers leave the denominator at 1 and there is no quorum "+
			"floor. Every sibling predicate refuses this same state.", got.ForeignMaster)
	}

	// And what that verdict costs, so the failure names the blast radius.
	atRisk, unverified := quarantineDataRisk(s, got.ForeignMaster, map[string]bool{
		authVetoRedis0: true, authVetoRedis1: true, authVetoRedis2: true,
	})
	q := planQuarantine(quarantineInput{
		Captured: got.Captured, Forsaken: got.Forsaken,
		DataAtRisk: atRisk, DataUnverified: unverified, Now: now,
	})
	if q.ScaleToZero {
		t.Errorf("planQuarantine.ScaleToZero = true (phase %q) off that verdict: six pods on "+
			"EmptyDir storage are removed and the attempt is counted against a budget that latches",
			q.Phase)
	}
}

func podNameAt(i int) string {
	return []string{authVetoRedis0, authVetoRedis1, authVetoRedis2}[i]
}

// ---------------------------------------------------------------------------
// FINDING 2 — the lineage gate's premise does not hold for two live masters.
// ---------------------------------------------------------------------------

// TestTwoLiveMastersAreNeverResolvedByElectingOneOfThem is LEFT FAILING, for the
// same reason as finding 1.
//
// THE STATE is observed single (1) — two pods reporting role:master at once, both
// holding keys — combined with the ghost-master signature: a majority of Sentinels
// pinned to a dead address, and no healthy replica known to them (which is what a
// whole-list Sentinel RESET leaves behind).
//
// THE PREMISE THE GATE RESTS ON. The ghost-master recovery deliberately keys its
// safety on replication LINEAGE rather than on holder count, and the reason is
// written down: "the ghost-master survivors are replicas of the *same* dead master,
// so electing the highest-offset one discards nothing (the losers resync from it)".
// That premise is about REPLICAS. It is true of the state it was written for.
//
// WHY IT FAILS HERE. Two nodes that both report role:master and share a lineage are
// not replicas of one dead master; they are two write endpoints that have both been
// accepting writes since they diverged, and a shared replid says only that they
// share a HISTORY, never that one of them is a superset of the other. So
// `holdersDiverged` is false, no opt-in is required, and the planner elects one and
// silently discards everything written on the other. The holder count that would
// have caught this is the gate Rule L uses and this planner deliberately does not.
//
// The sibling planners on the identical holder set are asserted below and split two
// ways, which is the sharpest evidence that this is a gap rather than a policy:
// Rule L refuses (holder count), failover mode elects (lineage). Only the shape of
// the gate differs.
func TestTwoLiveMastersAreNeverResolvedByElectingOneOfThem(t *testing.T) {
	// UN-SKIPPED 2026-09-03 with the LR-057 fix. It was committed skipped because it
	// asserts what the operator SHOULD do, and inverting it would have pinned an
	// election that discards acknowledged writes with no opt-in.
	//
	// The reason it was deferred rather than fixed turned out to be WRONG, and that
	// is worth keeping: the fear was that "refuse when two holders both report
	// role:master" could re-create the very wedge this planner exists to break,
	// because "a restarted pod returns as an empty master by default" (LR-004,
	// LR-014). It cannot. DataHolders is `Reachable && Keys > 0` and storage is
	// EmptyDir, so a restarted pod returns with ZERO keys and is never a holder at
	// all — the transient cannot enter the set the refusal ranges over. Pinned
	// separately by TestAnEmptyRestartedMasterIsNeverAHolder.

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	stuck := now.Add(-2 * ghostMasterRecoveryCooldown)

	// Both masters descend from one history: redis-1 was promoted at some point, so
	// it rotated its replid and kept the old one in replid2. That is one lineage by
	// the union-find, and it is also exactly what a split brain looks like.
	splitBrain := func() *redisclient.ReplicationState {
		s := asFleet()
		for _, sip := range []string{asSent0, asSent1, asSent2} {
			asMonitoring(s, sip, asDeparted)
		}
		asRedisNode(s, asRedis0, authVetoRedis0, asNode{
			role: RoleMaster, keys: 500, replid: asLineageA, reachable: true})
		asRedisNode(s, asRedis1, authVetoRedis1, asNode{
			role: RoleMaster, keys: 400, replid: asLineageA2, replid2: asLineageA, reachable: true})
		asRedisNode(s, asRedis2, authVetoRedis2, asNode{
			role: roleSlave, masterHost: asDeparted, link: linkStatusDown, reachable: true})
		return s
	}

	s := splitBrain()

	// Preconditions: this really is the ghost-master signature, and there really are
	// two holders.
	if !s.SentinelsMonitorGhostMaster() {
		t.Fatalf("precondition: the Sentinels must be pinned to a dead address")
	}
	if s.HasHealthyKnownReplica() {
		t.Fatalf("precondition: no healthy replica may be known, or the planner stands down")
	}
	if n := len(s.DataHolders()); n != 2 {
		t.Fatalf("precondition: want 2 data holders, got %d", n)
	}
	if _, diverged, _ := s.BestDataHolder(); diverged {
		t.Fatalf("precondition: the two masters must read as ONE lineage, which is the point")
	}

	// The contrast, on the identical holder set: the holder-count gate refuses.
	if got := planLeaderlessRecovery(withAllSentinelsBare(splitBrain()), 2, false, asRedis0,
		&stuck, now, leaderlessRecoveryCooldown); got.action != recoveryRefuse {
		t.Errorf("contrast: the holder-count gate = %v, want recoveryRefuse — it is the same "+
			"two holders, and only the shape of the gate differs", got.action)
	}

	got := planGhostMasterRecovery(s, 2, false, asRedis0, &stuck, now, ghostMasterRecoveryCooldown)
	switch got.action {
	case recoveryPromoteSurvivor, recoveryUnsafeElect, recoverySeedNoData:
		t.Errorf("planGhostMasterRecovery = %v (electing %s of %d holders, diverged=%v) with no "+
			"opt-in: both holders report role:master, so they are not replicas of one dead "+
			"master and a shared replid does not make either a superset of the other. The "+
			"writes on the loser are discarded silently.",
			got.action, got.masterIP, got.holders, got.diverged)
	}
}

// withAllSentinelsBare rewrites every Sentinel of a fixture to monitor nothing,
// which is the holder-count planner's own detection gate.
func withAllSentinelsBare(s *redisclient.ReplicationState) *redisclient.ReplicationState {
	for ip := range s.SentinelNodes {
		asBare(s, ip)
	}
	return s
}

// ---------------------------------------------------------------------------
// Characterisation: states that are handled correctly today.
// ---------------------------------------------------------------------------

// TestNoRecoveryActsWhileAMonitoringSentinelRemains is combination (2)+(6): a bare
// Sentinel beside monitoring ones, and a pod holding data while following an address
// nobody serves.
//
// Both recovery planners elect a master, so both must refuse a state in which they
// cannot tell a deadlock from a Sentinel quorum that is simply mid-recovery. The
// holder-count planner requires EVERY reachable Sentinel to be bare; the lineage
// planner requires a MAJORITY of reachable Sentinels to be pinned to a dead address,
// counting bare ones in the denominator. One bare Sentinel among two monitoring ones
// satisfies neither.
//
// Green from birth. Mutation applied to show teeth: changing AllSentinelsBare's
// return to `reachable > 0 && monitoring < reachable` (i.e. "some are bare") makes
// the first assertion fail with
//
//	planLeaderlessRecovery = 4, want clearMarker
//
// action 4 being recoveryPromoteSurvivor — the planner does not merely start a
// cooldown, it elects a master out from under a Sentinel quorum that still has
// opinions, because the marker is already set and the cooldown already elapsed.
func TestNoRecoveryActsWhileAMonitoringSentinelRemains(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	long := now.Add(-time.Hour)

	s := asFleet()
	asBare(s, asSent0)
	asMonitoring(s, asSent1, asDeparted)
	asMonitoring(s, asSent2, asDeparted)
	// Observed single (2): a replica following an address no pod holds, link down,
	// still holding its data.
	asRedisNode(s, asRedis0, authVetoRedis0, asNode{
		role: roleSlave, masterHost: asDeparted, link: linkStatusDown,
		keys: 1000, replid: asLineageA, reachable: true})
	asRedisNode(s, asRedis1, authVetoRedis1, asNode{
		role: roleSlave, masterHost: asDeparted, link: linkStatusDown, reachable: true})
	asRedisNode(s, asRedis2, authVetoRedis2, asNode{
		role: roleSlave, masterHost: asDeparted, link: linkStatusDown, reachable: true})
	// A healthy replica is still known, which is the discriminator from a deadlock:
	// a legitimate failover may yet happen on its own.
	asReplicaKnown(s, asSent1, redisclient.ReplicaInfo{IP: asRedis0, Flags: roleSlave})

	if got := planLeaderlessRecovery(s, 2, false, asRedis0, &long, now, leaderlessRecoveryCooldown); got.action != recoveryClearMarker {
		t.Errorf("planLeaderlessRecovery = %v, want clearMarker: one Sentinel is bare, two are "+
			"not, and an election needs the whole quorum to have lost its config", got.action)
	}
	if got := planGhostMasterRecovery(s, 2, false, asRedis0, &long, now, ghostMasterRecoveryCooldown); got.action != recoveryClearMarker {
		t.Errorf("planGhostMasterRecovery = %v, want clearMarker: a healthy replica is still "+
			"known, so a legitimate failover is still possible", got.action)
	}
}

// TestASentinelReportingAFailoverSuspendsNameScopeReconciliation is combination
// (3)+(6): a Sentinel reporting a failover in flight, beside a bare one.
//
// The measured shape is a Sentinel that reports a failover for minutes rather than
// seconds (LR-055: 84-86 consecutive passes, ~178s), so "it will be over by the next
// pass" is not an available assumption. Removing a master-name entry while a state
// machine is reconfiguring our pods under it is destructive in the plain sense: the
// entry is the thing driving the reconfiguration.
//
// Two independent signals must each hold the rule down, and both are asserted: the
// per-entry `failover_in_progress` flag, and the instance-level FailoverActive that
// comes from the desired name's own probe and covers the case where the full
// monitored-master list could not be read at all.
//
// Green from birth. Mutation applied: dropping `|| state.FailoverReported` from G3
// makes the second sub-case fail with
//
//	instance-level: Reason/Gate = "Pruning"/"", want Deferred/G3: removing stale
//	master name(s) ["mymaster"] from: sentinel-10.0.1.11=["mymaster"], ...
func TestASentinelReportingAFailoverSuspendsNameScopeReconciliation(t *testing.T) {
	base := func() *redisclient.ReplicationState {
		s := asFleet()
		asBare(s, asSent0)
		for _, sip := range []string{asSent1, asSent2} {
			withMonitoredSentinel(s, sip, true, asRedis0, RoleMaster,
				redisclient.MonitoredMaster{Name: asDesiredName, IP: asRedis0, Flags: RoleMaster},
				redisclient.MonitoredMaster{Name: asOtherName, IP: asRedis0, Flags: RoleMaster})
		}
		asRedisNode(s, asRedis0, authVetoRedis0, asNode{role: RoleMaster, keys: 10, reachable: true})
		asRedisNode(s, asRedis1, authVetoRedis1, asNode{
			role: roleSlave, masterHost: asRedis0, link: "up", keys: 10, reachable: true})
		asRedisNode(s, asRedis2, authVetoRedis2, asNode{
			role: roleSlave, masterHost: asRedis0, link: "up", keys: 10, reachable: true})
		s.RealMasterIP = asRedis0
		return s
	}

	// (a) the per-entry signal: one monitored name carries the in-progress flag.
	perEntry := base()
	perEntry.SentinelNodes[asSent1].MonitoredMasters[0].Flags = "master,failover_in_progress"
	if got := planStaleMasterNames(perEntry, asDesiredName, 2, false, false); got.Reason != staleNamesDeferred || got.Gate != "G3" {
		t.Errorf("per-entry: Reason/Gate = %q/%q, want Deferred/G3: %s", got.Reason, got.Gate, got.Message)
	} else if len(got.Prune) != 0 {
		t.Errorf("per-entry: Prune = %v, want nothing removed", got.Prune)
	}

	// (b) the instance-level signal, which is the only one left when the full
	// monitored-master list could not be read.
	instanceLevel := base()
	instanceLevel.FailoverReported = true
	if got := planStaleMasterNames(instanceLevel, asDesiredName, 2, false, false); got.Reason != staleNamesDeferred || got.Gate != "G3" {
		t.Errorf("instance-level: Reason/Gate = %q/%q, want Deferred/G3: %s", got.Reason, got.Gate, got.Message)
	} else if len(got.Prune) != 0 {
		t.Errorf("instance-level: Prune = %v, want nothing removed", got.Prune)
	}
}

// TestKeysOnAPodFollowingNobodyAreNeverDiscardable is observed single (2) fed to the
// only planner that deletes pods.
//
// The discriminator is *whose* data the keys are, never *whether* there are keys: a
// pod that is a link-up replica of the captor's master holds a copy of data that
// still exists on the captor, so discarding it loses nothing. A pod holding keys
// while following an address nobody serves is the opposite case — its keys may be
// the only copy in existence — and the link status is the whole of the difference.
//
// Green from birth. Mutation applied: dropping the `rn.LinkStatus == "up"` clause
// from `captorsCopy` makes the pointed-but-not-synced sub-case fail with
//
//	atRisk = false, want true: redis-0 points at 10.9.9.9 but its link is down,
//	so its 1000 keys were never replaced by the stranger's keyspace
//
// and the quarantine then scales to zero on a pod holding the last copy. Note that
// the first sub-case survives that mutation on its own — a pod following an address
// that is not the captor's is caught by the MasterHost clause — which is why the
// second sub-case had to exist for the link clause to be pinned at all.
func TestKeysOnAPodFollowingNobodyAreNeverDiscardable(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	ready := map[string]bool{authVetoRedis0: true, authVetoRedis1: true, authVetoRedis2: true}

	s := asFleet()
	for _, sip := range []string{asSent0, asSent1, asSent2} {
		asMonitoring(s, sip, asStranger)
	}
	// redis-0 holds keys but follows an address nobody serves: not the stranger's copy.
	asRedisNode(s, asRedis0, authVetoRedis0, asNode{
		role: roleSlave, masterHost: asDeparted, link: linkStatusDown,
		keys: 1000, replid: asLineageA, reachable: true})
	// redis-1 and redis-2 hold the stranger's keyspace, replicated in and link up.
	for i, ip := range []string{asRedis1, asRedis2} {
		asRedisNode(s, ip, podNameAt(i+1), asNode{
			role: roleSlave, masterHost: asStranger, link: "up", keys: 1000, reachable: true})
	}

	atRisk, unverified := quarantineDataRisk(s, asStranger, ready)
	if !atRisk {
		t.Errorf("quarantineDataRisk.atRisk = false, want true: redis-0 holds 1000 keys while "+
			"following %s, which nobody serves — those keys are explained by nothing", asDeparted)
	}
	if unverified {
		t.Errorf("quarantineDataRisk.unverified = true, want false: every pod answered")
	}

	q := planQuarantine(quarantineInput{
		Captured: true, Forsaken: true, DataAtRisk: atRisk, DataUnverified: unverified, Now: now})
	if q.ScaleToZero || q.Phase != quarantineHoldDataPresent {
		t.Errorf("planQuarantine = %+v, want HoldDataPresent and no scale-to-zero", q)
	}

	// The second half of the same discriminator, and the one that matters most: a pod
	// that is POINTED at the stranger but whose link is down never synced, so the keys
	// it holds are still its own. Pointing at an address is not the same as having
	// copied from it.
	pointedNotSynced := s
	pointedNotSynced.RedisNodes[asRedis0].MasterHost = asStranger
	if at, _ := quarantineDataRisk(pointedNotSynced, asStranger, ready); !at {
		t.Errorf("atRisk = false, want true: redis-0 points at %s but its link is down, so its "+
			"1000 keys were never replaced by the stranger's keyspace", asStranger)
	}

	// The control that keeps the assertion honest: with redis-0 ALSO a link-up
	// replica of the stranger, every key is explained and the quarantine proceeds.
	explained := s
	explained.RedisNodes[asRedis0].MasterHost = asStranger
	explained.RedisNodes[asRedis0].LinkStatus = "up"
	if at, _ := quarantineDataRisk(explained, asStranger, ready); at {
		t.Errorf("control: atRisk = true when every holder is a link-up replica of %s, want false",
			asStranger)
	}
}

// TestAPromotionChainIsOneLineageAndNeedsNoOptIn is observed single (7): a
// just-promoted master whose master_replid2 carries the lineage it came from.
//
// This is the state that made an earlier build refuse a perfectly safe election.
// Keying divergence on the current replid alone reads a promotion chain as two
// independent histories, and the refusal it produces is a wedge that needs a human.
// The union-find over {replid, replid2} is what makes the chain one lineage.
//
// Note the deliberate asymmetry with finding 2 above: here the holders genuinely ARE
// one master and its replicas, which is the state the lineage gate was written for,
// and electing the most complete one discards nothing.
//
// Green from birth. Mutation applied: making `holdersDiverged`'s union a no-op (so
// each holder is its own component) fails the precondition and both verdicts:
//
//	precondition: a promotion chain must read as ONE lineage
//	planGhostMasterRecovery = 5, want recoveryPromoteSurvivor with no opt-in
//	planFailover = 4, want failoverPromote with no opt-in
//
// action 5 being recoveryRefuse and action 4 failoverRefuse — the wedge that needs
// a human, on a state that is perfectly safe to elect from.
func TestAPromotionChainIsOneLineageAndNeedsNoOptIn(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	stuck := now.Add(-2 * ghostMasterRecoveryCooldown)

	chain := func() *redisclient.ReplicationState {
		s := asFleet()
		for _, sip := range []string{asSent0, asSent1, asSent2} {
			asMonitoring(s, sip, asDeparted)
		}
		// redis-1 was promoted: new replid, previous one kept in replid2.
		asRedisNode(s, asRedis1, authVetoRedis1, asNode{
			role: roleSlave, masterHost: asDeparted, link: linkStatusDown,
			keys: 100, replid: asLineageA2, replid2: asLineageA, reachable: true})
		// redis-2 never was: it still carries the original lineage.
		asRedisNode(s, asRedis2, authVetoRedis2, asNode{
			role: roleSlave, masterHost: asDeparted, link: linkStatusDown,
			keys: 90, replid: asLineageA, reachable: true})
		return s
	}

	s := chain()
	if _, diverged, _ := s.BestDataHolder(); diverged {
		// Errorf, not Fatalf: when this precondition breaks, the verdicts below are
		// the interesting part of the failure.
		t.Errorf("precondition: a promotion chain must read as ONE lineage")
	}
	if got := planGhostMasterRecovery(s, 2, false, "", &stuck, now, ghostMasterRecoveryCooldown); got.action != recoveryPromoteSurvivor {
		t.Errorf("planGhostMasterRecovery = %v, want recoveryPromoteSurvivor with no opt-in",
			got.action)
	}
	if got := planFailover(chain(), "", false, "", false, nil, now, failoverTransitionCooldown); got.action != failoverPromote {
		t.Errorf("planFailover = %v, want failoverPromote with no opt-in", got.action)
	}

	// The other direction: two genuinely independent histories must still refuse.
	independent := chain()
	independent.RedisNodes[asRedis2].Replid = asLineageB
	if got := planGhostMasterRecovery(independent, 2, false, "", &stuck, now, ghostMasterRecoveryCooldown); got.action != recoveryRefuse {
		t.Errorf("control: independent lineages = %v, want recoveryRefuse", got.action)
	}
}

// TestTwoNamesNamingTwoOfOurOwnPodsIsNotACapture is combination (1)+(4): two master
// names monitored at once, naming two different pods of ours, both of which report
// role:master.
//
// The capture verdict must stay clear on this state for two independent reasons, and
// both are load-bearing: the two names name two DIFFERENT addresses, which is a
// disagreement rather than a settled verdict; and both addresses are ours anyway.
// The name-scope reconciliation must still be able to act — the stale entry is
// attributable to a pod of ours, so refusing to prune it here would leave two
// failover state machines running over the same pods indefinitely — but it must
// anchor on a live master of ours and must never remove the desired name.
//
// Green from birth, and the mutation work is worth recording in full because it
// showed the fixture is defended in DEPTH rather than by one clause.
//
// Neutralising clause 3 (the ours/flagged-down test) AND clause 4 (no master of
// ours) together still leaves the test green: clause 2 refuses on its own, because
// the two names name two different addresses. Only when clause 2 is neutralised as
// well does the verdict flip —
//
//	planForsaken.Captured = true (foreign_master=10.0.0.10)
//
// so clause 2 is the last line here and it has teeth. For the name-scope half a
// single mutation suffices: replacing G5's `state.IsOurs(m.IP)` with
// `!state.IsGhost(m.IP)` fails it with
//
//	planStaleMasterNames.Reason = "Foreign", want Pruning
func TestTwoNamesNamingTwoOfOurOwnPodsIsNotACapture(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	armed := metav1.NewTime(now.Add(-2 * forsakenCooldown))

	s := asFleet()
	for _, sip := range []string{asSent0, asSent1, asSent2} {
		withMonitoredSentinel(s, sip, true, asRedis0, RoleMaster,
			redisclient.MonitoredMaster{Name: asDesiredName, IP: asRedis0, Flags: RoleMaster},
			redisclient.MonitoredMaster{Name: asOtherName, IP: asRedis1, Flags: RoleMaster})
	}
	asRedisNode(s, asRedis0, authVetoRedis0, asNode{role: RoleMaster, keys: 100, reachable: true})
	asRedisNode(s, asRedis1, authVetoRedis1, asNode{role: RoleMaster, keys: 40, reachable: true})
	asRedisNode(s, asRedis2, authVetoRedis2, asNode{
		role: roleSlave, masterHost: asDeparted, link: linkStatusDown, reachable: true})
	s.RealMasterIP = asRedis0

	if got := planForsaken(s, &armed, now, false); got.Captured {
		t.Errorf("planForsaken.Captured = true (foreign_master=%s): the two names name two pods "+
			"of OURS, which is a disagreement and not a capture", got.ForeignMaster)
	}

	plan := planStaleMasterNames(s, asDesiredName, 2, false, false)
	if plan.Reason != staleNamesPruning {
		t.Fatalf("planStaleMasterNames.Reason = %q, want Pruning — the stale entry names a live "+
			"pod of ours and is attributable: %s", plan.Reason, plan.Message)
	}
	for _, e := range plan.Prune {
		for _, n := range e.Names {
			if n == asDesiredName {
				t.Errorf("planStaleMasterNames.Prune contains the DESIRED name %q on %s: "+
					"re-pointing it is another rule's job and removing it is never this one's",
					n, e.SentinelPod)
			}
		}
	}
}

// TestAPodThatRefusedOurCredentialVetoesEveryDataDiscardingBranch is combination
// (1)+(6) plus a pod the operator reached and could not authenticate to.
//
// A pod that answered us, in the protocol, to refuse us, is a LIVE server whose
// keyspace is unknown rather than zero. Every branch that elects or seeds a master
// discards data by construction, so all three planners and the quarantine must
// refuse — and the refusal must not be overridable by the unsafe opt-in, because
// that opt-in authorizes discarding a set of holders the owner could SEE.
//
// The two sub-cases at the bottom are the positive controls that keep this from
// being a blanket refusal: an ordinary dead pod (timed out, unroutable) must STILL
// be recovered from, because a mass restart is the state these planners exist for.
//
// Green from birth. Mutation applied: making UnprovablyEmpty return `!n.Reachable`
// — vetoing on ANY unreachable pod rather than on an authentication failure — fails
// both controls, twice each:
//
//	control Timeout:    planLeaderlessRecovery = 7, want recoverySeedNoData
//	control Timeout:    planFailover = 6, want failoverSeed
//	control Unroutable: planLeaderlessRecovery = 7, want recoverySeedNoData
//	control Unroutable: planFailover = 6, want failoverSeed
//
// actions 7 and 6 being the two refuse-unverified verdicts swallowing the ordinary
// mass-restart recovery whole.
func TestAPodThatRefusedOurCredentialVetoesEveryDataDiscardingBranch(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	long := now.Add(-time.Hour)

	fleet := func(failure redisclient.ProbeFailure) *redisclient.ReplicationState {
		s := asFleet()
		for _, sip := range []string{asSent0, asSent1, asSent2} {
			asBare(s, sip)
		}
		for i, ip := range []string{asRedis0, asRedis1, asRedis2} {
			asRedisNode(s, ip, podNameAt(i), asNode{probe: failure})
		}
		return s
	}

	refusing := fleet(redisclient.ProbeAuthFailed)
	if got := planLeaderlessRecovery(refusing, 2, true, asRedis0, &long, now, leaderlessRecoveryCooldown); got.action != recoveryRefuseUnverified {
		t.Errorf("planLeaderlessRecovery = %v, want recoveryRefuseUnverified even with the "+
			"unsafe opt-in ON: it authorizes discarding holders the owner could see", got.action)
	}
	ghost := fleet(redisclient.ProbeAuthFailed)
	for _, sip := range []string{asSent0, asSent1, asSent2} {
		asMonitoring(ghost, sip, asDeparted)
	}
	if got := planGhostMasterRecovery(ghost, 2, true, asRedis0, &long, now, ghostMasterRecoveryCooldown); got.action != recoveryRefuseUnverified {
		t.Errorf("planGhostMasterRecovery = %v, want recoveryRefuseUnverified", got.action)
	}
	if got := planFailover(fleet(redisclient.ProbeAuthFailed), "", true, asRedis0, false, nil, now, failoverTransitionCooldown); got.action != failoverRefuseUnverified {
		t.Errorf("planFailover = %v, want failoverRefuseUnverified", got.action)
	}
	if _, unverified := quarantineDataRisk(fleet(redisclient.ProbeAuthFailed), asStranger,
		map[string]bool{authVetoRedis0: false, authVetoRedis1: false, authVetoRedis2: false}); !unverified {
		t.Errorf("quarantineDataRisk.unverified = false, want true: an authentication failure is " +
			"positive evidence of a live server and must override the kubelet's negative one")
	}

	// Positive controls: an ordinary dead fleet must still be recoverable.
	for _, failure := range []redisclient.ProbeFailure{
		redisclient.ProbeTimedOut, redisclient.ProbeUnroutable,
	} {
		if got := planLeaderlessRecovery(fleet(failure), 2, false, asRedis0, &long, now, leaderlessRecoveryCooldown); got.action != recoverySeedNoData {
			t.Errorf("control %v: planLeaderlessRecovery = %v, want recoverySeedNoData", failure, got.action)
		}
		if got := planFailover(fleet(failure), "", false, asRedis0, false, nil, now, failoverTransitionCooldown); got.action != failoverSeed {
			t.Errorf("control %v: planFailover = %v, want failoverSeed", failure, got.action)
		}
	}
}

// TestMasterDeathIsNeverDeclaredOnTheOperatorsDialAlone feeds the failover-mode
// failure detector the two states that most resemble a dead master and are not one.
//
// The operator's own dial is never sufficient evidence: an address that blackholes
// from the operator may be serving clients perfectly well, so a declaration needs
// either the kubelet's local verdict or corroboration from every replica that can
// still see the master. A single replica reporting link:up is a veto, and no
// reachable replica at all is not corroboration but the absence of it.
//
// The counter-case is asserted too: a terminating master IS declared immediately and
// deliberately, because "I am being terminated" is a Kubernetes fact rather than an
// inference, and waiting out a probe window there would spend the grace period doing
// nothing.
//
// Green from birth. Mutation applied: returning masterDeathDeclareProbe from the
// final branch — declaring on an elapsed window regardless of corroboration — fails
// exactly the two hold rows and nothing else:
//
//	.../a_replica_still_sees_the_link_up...  planMasterDeath = 5, want 3
//	.../no_reachable_replica_to_corroborate  planMasterDeath = 5, want 3
//
// action 5 being masterDeathDeclareProbe and 3 masterDeathHold.
func TestMasterDeathIsNeverDeclaredOnTheOperatorsDialAlone(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	elapsed := now.Add(-time.Minute)
	alive := masterPodView{present: true, ready: true}

	cases := []struct {
		name  string
		pod   masterPodView
		links []string
		want  masterDeathAction
	}{
		{"a replica still sees the link up — the operator is the one who is blind",
			alive, []string{"up", linkStatusDown}, masterDeathHold},
		{"no reachable replica to corroborate with",
			alive, nil, masterDeathHold},
		{"every reachable replica lost the link — corroborated",
			alive, []string{linkStatusDown, linkStatusDown}, masterDeathDeclareProbe},
		{"terminating: a Kubernetes fact, declared at once",
			masterPodView{present: true, ready: true, terminating: true}, []string{"up"}, masterDeathDeclareK8s},
		{"replaced or gone: likewise",
			masterPodView{}, []string{"up"}, masterDeathDeclareK8s},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := planMasterDeath(tc.pod, false, tc.links, &elapsed, now, 30*time.Second); got != tc.want {
				t.Errorf("planMasterDeath = %v, want %v", got, tc.want)
			}
		})
	}
}
