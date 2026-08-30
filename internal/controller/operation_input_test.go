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
	"k8s.io/apimachinery/pkg/types"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

const (
	opTestUID     = types.UID("6a1d0b6c-0000-4000-8000-000000000001")
	opTestRenamed = "ops-b.cache"
)

func opTestInstance(mode, masterName string) *littleredv1alpha1.LittleRed {
	lr := &littleredv1alpha1.LittleRed{}
	lr.Name = "inst"
	lr.Namespace = "ns"
	lr.UID = opTestUID
	lr.Spec.Mode = mode
	if masterName != "" {
		lr.Spec.Sentinel = &littleredv1alpha1.SentinelSpec{MasterName: masterName}
	}
	lr.Status.Phase = littleredv1alpha1.PhaseRunning
	return lr
}

// TestOperationCandidatesFilterByApplies pins that the registry is filtered by each
// entry's own Applies predicate and by nothing else. This is what makes an operation
// that cannot exist on an instance a NON-candidate rather than a permanently-pending
// one — and therefore what makes a Requires edge naming it vacuously satisfied instead
// of a deadlock.
func TestOperationCandidatesFilterByApplies(t *testing.T) {
	sentinel := operationCandidates(opTestInstance(ModeSentinel, "a.b"), heavyOperations)
	if len(sentinel) != 1 || sentinel[0].Name != opRename {
		t.Fatalf("sentinel candidates = %+v, want exactly the rename", sentinel)
	}
	if sentinel[0].StallAfter != 15*time.Minute {
		t.Errorf("StallAfter = %v, want the registry's 15m", sentinel[0].StallAfter)
	}

	for _, mode := range []string{ModeCluster, ModeFailover, ModeStandalone} {
		if got := operationCandidates(opTestInstance(mode, ""), heavyOperations); len(got) != 0 {
			t.Errorf("%s candidates = %+v, want none: registry v1 is sentinel-only", mode, got)
		}
	}
}

// TestOperationCandidateFingerprintsTheEffectiveName — the fingerprint identifies WHICH
// VALUE was asked for, and the value that matters is the EFFECTIVE master name: an
// instance that omits spec.sentinel is genuinely running under the legacy name, so
// fingerprinting the raw field would read its first explicit "mymaster" as a rename that
// changes nothing, and would declare an operation with no work in it.
func TestOperationCandidateFingerprintsTheEffectiveName(t *testing.T) {
	omitted := operationCandidates(opTestInstance(ModeSentinel, ""), heavyOperations)
	explicitLegacy := operationCandidates(
		opTestInstance(ModeSentinel, littleredv1alpha1.LegacySentinelMasterName), heavyOperations)
	if len(omitted) != 1 || len(explicitLegacy) != 1 {
		t.Fatalf("want one candidate each, got %d and %d", len(omitted), len(explicitLegacy))
	}
	if omitted[0].Fingerprint != explicitLegacy[0].Fingerprint {
		t.Errorf("omitted = %q, explicit %q = %q: the EFFECTIVE name is what completed",
			omitted[0].Fingerprint, littleredv1alpha1.LegacySentinelMasterName,
			explicitLegacy[0].Fingerprint)
	}

	renamed := operationCandidates(opTestInstance(ModeSentinel, opTestRenamed), heavyOperations)
	if renamed[0].Fingerprint == omitted[0].Fingerprint {
		t.Errorf("a different name must fingerprint differently, both = %q", renamed[0].Fingerprint)
	}
	if renamed[0].Fingerprint == opTestRenamed ||
		len(renamed[0].Fingerprint) != littleredv1alpha1.OperationFingerprintLen {
		t.Errorf("fingerprint = %q, want a %d-char keyed digest and never the plaintext",
			renamed[0].Fingerprint, littleredv1alpha1.OperationFingerprintLen)
	}
}

// TestBuildOperationInputProjectsTheCompletionRecord pins the ONE call site that reads
// status.acknowledgedOperations (ADR-020 trap 3), and the distinction the whole
// mechanism rests on: "no ack row" is not "a row that differs". The first is SEEDED
// (row 3, which is what stops an operator upgrade declaring an operation for every
// instance in a fleet); the second is unfinished work from a spec change.
func TestBuildOperationInputProjectsTheCompletionRecord(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	lr := opTestInstance(ModeSentinel, opTestRenamed)
	in := buildOperationInput(lr, true, now)
	if len(in.Acks) != 0 {
		t.Errorf("Acks = %v, want empty for an instance that has never been asked", in.Acks)
	}
	if in.Quarantined || in.Bootstrapping || !in.Settled || in.Active != nil {
		t.Errorf("input = %+v, want an unquarantined, initialized, settled instance with no active operation", in)
	}
	if !in.Now.Equal(now) {
		t.Errorf("Now = %v, want the caller's clock %v", in.Now, now)
	}

	lr.Status.AcknowledgedOperations = []littleredv1alpha1.OperationAck{
		{Name: opRename, Fingerprint: "deadbeefdeadbeef"},
		{Name: "SomeDeregisteredOperation", Fingerprint: "0011223344556677"},
	}
	lr.Status.QuarantinedSince = &metav1.Time{Time: now}
	lr.Status.Operation = &littleredv1alpha1.OperationStatus{Name: opRename}

	in = buildOperationInput(lr, false, now)
	if got := in.Acks[opRename]; got != "deadbeefdeadbeef" {
		t.Errorf("Acks[rename] = %q, want the persisted fingerprint", got)
	}
	if _, ok := in.Acks["SomeDeregisteredOperation"]; !ok {
		t.Error("the projection must carry every row, not only the ones that are candidates")
	}
	if !in.Quarantined {
		t.Error("Quarantined must be status.quarantinedSince != nil: a replicas:0 StatefulSet " +
			"reads SETTLED, so an unheld operation would acknowledge work no pod ever executed")
	}
	if in.Settled {
		t.Error("Settled must be the caller's measurement, not re-derived")
	}
	if in.Active == nil || in.Active.Name != opRename {
		t.Errorf("Active = %+v, want status.operation carried through for the stall clock", in.Active)
	}
}

func TestBuildOperationInputBootstrapping(t *testing.T) {
	now := time.Now()
	fresh := opTestInstance(ModeSentinel, opTestRenamed)
	fresh.Status.Phase = ""
	if !buildOperationInput(fresh, true, now).Bootstrapping {
		t.Error("an instance with no phase is bootstrapping: it is already in the state its spec asks for")
	}

	arming := opTestInstance(ModeSentinel, opTestRenamed)
	arming.Status.BootstrapRequired = true
	if !buildOperationInput(arming, true, now).Bootstrapping {
		t.Error("BootstrapRequired is bootstrapping")
	}
}

// TestOperationDriverReport pins the driver contract for registry v1's one member: the
// rename's driver is the code that already ships (Rule 0 then Rule N), and its verdict
// IS Rule N's own plan. Nothing new was written to carry it.
//
// The G6 row is the one that matters. "No Sentinel carries the desired name yet; Rule 0
// registers it next pass" fires on the FIRST pass of every ordinary rename — measured on
// t3e — and Rule 0 discharges it in the very next pass, by design. Reporting it as
// Blocked emits a Warning on the happy path, and a check that cries wolf is a check
// nobody reads. Every other gate names a state that persists until something outside
// Rule N changes, so every other gate stays Blocked.
func TestOperationDriverReport(t *testing.T) {
	cases := []struct {
		name          string
		plan          StaleMasterNamePlan
		done, blocked bool
	}{
		{
			// Rule N's desired state — every Sentinel monitors exactly the desired name
			// and nothing else — which is exactly what the rename is for.
			name: "converged is complete",
			plan: StaleMasterNamePlan{Reason: staleNamesConverged},
			done: true,
		},
		{
			// Work in flight.
			name: "pruning is progress",
			plan: StaleMasterNamePlan{Reason: staleNamesPruning},
		},
		{
			// The carve-out, and the reason this function takes a plan rather than a
			// reason string: the gate identity has to be readable without parsing prose.
			name: "G6 is progress, not blockage — Rule 0 discharges it next pass",
			plan: StaleMasterNamePlan{Reason: staleNamesDeferred, Gate: staleGateRule0Pending},
		},
		{
			name:    "G1 (empty desired name) is blocked",
			plan:    StaleMasterNamePlan{Reason: staleNamesDeferred, Gate: "G1"},
			blocked: true,
		},
		{
			name:    "G2 (no living master of ours) is blocked",
			plan:    StaleMasterNamePlan{Reason: staleNamesDeferred, Gate: "G2"},
			blocked: true,
		},
		{
			name:    "G3 (a failover is in flight) is blocked",
			plan:    StaleMasterNamePlan{Reason: staleNamesDeferred, Gate: "G3"},
			blocked: true,
		},
		{
			name:    "G4 (below quorum) is blocked",
			plan:    StaleMasterNamePlan{Reason: staleNamesDeferred, Gate: "G4"},
			blocked: true,
		},
		{
			name:    "G5 (an address we cannot attribute) is blocked",
			plan:    StaleMasterNamePlan{Reason: staleNamesDeferred, Gate: "G5"},
			blocked: true,
		},
		{
			// A capture is in evidence and ADR-016 owns the instance: the one state in
			// which a rename must never be allowed to look like progress.
			name:    "foreign is blocked",
			plan:    StaleMasterNamePlan{Reason: staleNamesForeign},
			blocked: true,
		},
		{
			// An unrecognised verdict must never read as Complete: acknowledging on a
			// value nobody defined is the acknowledge-on-sight failure by another route.
			name: "an unknown reason is neither",
			plan: StaleMasterNamePlan{Reason: "SomethingNobodyHasWrittenYet"},
		},
		{
			// A Deferred verdict whose gate a future contributor forgot to name must
			// fail SAFE — blocked, i.e. loud — rather than quietly read as progress.
			name:    "a Deferred verdict with no gate is blocked",
			plan:    StaleMasterNamePlan{Reason: staleNamesDeferred},
			blocked: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			done, blocked := operationDriverReport(tc.plan)
			if done != tc.done || blocked != tc.blocked {
				t.Errorf("operationDriverReport(%+v) = (done=%v, blocked=%v), want (%v, %v)",
					tc.plan, done, blocked, tc.done, tc.blocked)
			}
		})
	}
}

// TestPlanStaleMasterNamesCarriesTheGateIdentity — the Gate field must actually be
// populated by the planner, not merely declared. Without this the carve-out above is
// unreachable in production while its unit table passes.
//
// It asserts ONLY the new field. No verdict, message or prune decision is asserted here;
// those are TestPlanStaleMasterNames' rows and none of them moved.
func TestPlanStaleMasterNamesCarriesTheGateIdentity(t *testing.T) {
	// G1: the desired name is empty. Reached before the survey, so it needs no fixture.
	if got := planStaleMasterNames(nil, "", 2, false, false); got.Gate != "G2" {
		t.Errorf("nil ground truth: Gate = %q, want G2", got.Gate)
	}
	if got := planStaleMasterNames(&redisclient.ReplicationState{}, "", 2, false, false); got.Gate != "G1" {
		t.Errorf("empty desired name: Gate = %q, want G1", got.Gate)
	}
	// A verdict that is not a deferral carries no gate.
	forsakenPlan := planStaleMasterNames(&redisclient.ReplicationState{}, "a.b", 2, true, false)
	if forsakenPlan.Reason != staleNamesForeign || forsakenPlan.Gate != "" {
		t.Errorf("forsaken: Reason=%q Gate=%q, want Foreign with no gate",
			forsakenPlan.Reason, forsakenPlan.Gate)
	}
}

// TestOperationStatusForKeepsTheStallClock — StartedAt anchors StallAfter, so restarting
// it every pass would make a stall unreportable, and carrying ANOTHER operation's clock
// onto this one would manufacture a stall out of a stale monitoring field.
func TestOperationStatusForKeepsTheStallClock(t *testing.T) {
	started := metav1.NewTime(time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC))
	nowT := metav1.NewTime(time.Date(2026, 8, 30, 10, 5, 0, 0, time.UTC))
	current := &littleredv1alpha1.OperationStatus{Name: "A", StartedAt: started, Reason: operationReasonRunning}

	same := operationStatusFor(operationPlan{Report: "A", Reason: operationReasonStalled}, current, nowT)
	if !same.StartedAt.Equal(&started) {
		t.Errorf("StartedAt = %v, want the clock preserved at %v", same.StartedAt, started)
	}
	if same.Reason != operationReasonStalled {
		t.Errorf("Reason = %q, want the new verdict", same.Reason)
	}

	other := operationStatusFor(operationPlan{Report: "B", Reason: operationReasonRunning}, current, nowT)
	if !other.StartedAt.Equal(&nowT) {
		t.Errorf("StartedAt = %v, want a fresh clock %v for a different operation", other.StartedAt, nowT)
	}

	if operationStatusFor(operationPlan{Reason: operationReasonConverged}, current, nowT) != nil {
		t.Error("nothing to report must clear status.operation, not leave a stale one")
	}
}

// TestUpsertOperationAcksUpdatesInPlace — the list is bounded by the REGISTRY, not by
// time: one row per operation name, updated in place. It needs no expiry, and an
// age-based one would be a defect rather than hygiene (a missing row is re-seeded, never
// re-run). A row belonging to another operation must survive untouched.
func TestUpsertOperationAcksUpdatesInPlace(t *testing.T) {
	at := metav1.NewTime(time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC))
	existing := []littleredv1alpha1.OperationAck{
		{Name: "A", Fingerprint: "old"},
		{Name: "Other", Fingerprint: "keepme"},
	}
	got := upsertOperationAcks(existing, []operationCandidate{
		{Name: "A", Fingerprint: "new"},
		{Name: "B", Fingerprint: "fresh"},
	}, at)

	if len(got) != 3 {
		t.Fatalf("rows = %d (%+v), want 3: one per operation NAME", len(got), got)
	}
	byName := map[string]littleredv1alpha1.OperationAck{}
	for _, a := range got {
		byName[a.Name] = a
	}
	ackA := byName["A"]
	if ackA.Fingerprint != "new" || !ackA.AcknowledgedAt.Equal(&at) {
		t.Errorf("A = %+v, want updated in place", byName["A"])
	}
	if byName["Other"].Fingerprint != "keepme" {
		t.Errorf("Other = %+v, want untouched", byName["Other"])
	}
	if byName["B"].Fingerprint != "fresh" {
		t.Errorf("B = %+v, want appended", byName["B"])
	}
	if existing[0].Fingerprint != "old" {
		t.Error("the caller's slice must not be mutated")
	}
}

// TestOperationConditionPolarity — True means an operation is declared and unfinished:
// running, blocked, stalled, or held by a quarantine. That is a normal state, not a
// fault, and it never touches Ready. The Quarantined row is the one worth pinning: a
// held change must be REPORTED, because anything this mechanism withholds must say that
// it withheld (LR-054).
func TestOperationConditionPolarity(t *testing.T) {
	cases := []struct {
		plan operationPlan
		want metav1.ConditionStatus
	}{
		{operationPlan{Reason: operationReasonConverged}, metav1.ConditionFalse},
		{operationPlan{Reason: operationReasonSeeded}, metav1.ConditionFalse},
		{operationPlan{Report: "A", Run: "A", Reason: operationReasonRunning}, metav1.ConditionTrue},
		{operationPlan{Report: "A", Run: "A", Reason: operationReasonStalled}, metav1.ConditionTrue},
		{operationPlan{Report: "A", Run: "A", Reason: operationReasonBlocked}, metav1.ConditionTrue},
		{operationPlan{Report: "A", Reason: operationReasonQuarantined}, metav1.ConditionTrue},
	}
	for _, tc := range cases {
		got := operationConditionFor(tc.plan)
		if got.Status != tc.want {
			t.Errorf("condition for %s = %s, want %s", tc.plan.Reason, got.Status, tc.want)
		}
		if got.Reason != tc.plan.Reason {
			t.Errorf("condition reason = %q, want the plan's %q", got.Reason, tc.plan.Reason)
		}
		if got.Message == "" {
			t.Errorf("condition for %s has no message", tc.plan.Reason)
		}
	}
}
