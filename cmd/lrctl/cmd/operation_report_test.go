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
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// opNow is the fixed clock every case here renders against, so a duration is an
// assertion rather than a race.
var opNow = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

const (
	tokenOK   = "[OK]"
	tokenWarn = "[WARN]"
	opName    = "SentinelMasterNameRename"

	// phaseRunning is the CR phase, unrelated to the operation reason of the same value.
	phaseRunning = "Running"

	// controllerPlanSource is the file the drift guard reads the reason vocabulary from.
	controllerPlanSource = "../../../internal/controller/operation_plan.go"
)

func opStatus(reason string, ago time.Duration, pending ...string) *littleredv1alpha1.OperationStatus {
	return &littleredv1alpha1.OperationStatus{
		Name:      opName,
		StartedAt: metav1.NewTime(opNow.Add(-ago)),
		Reason:    reason,
		Pending:   pending,
	}
}

// TestOperationFails is the verdict, and it is the substance of the milestone.
//
// ADR-020 guarantees that Blocked and Stalled NEVER auto-resolve — there is no
// auto-exit timer, on ADR-017's lesson that a timer is the defect with a delay — so
// each needs a human and each must reach the exit code. Everything else must not:
// a running operation is a supported thing an owner asked for, and going red on it
// trains people to ignore `verify`, which is the failure this check exists to avoid.
func TestOperationFails(t *testing.T) {
	cases := []struct {
		name string
		op   *littleredv1alpha1.OperationStatus
		want bool
	}{
		{"no operation at all", nil, false},
		{"running is benign", opStatus(opReasonRunning, time.Minute), false},
		{"blocked needs a human", opStatus(opReasonBlocked, time.Minute), true},
		{"stalled needs a human", opStatus(opReasonStalled, 20*time.Minute), true},
		// The quarantine is reported, never failed on: the operation is correctly
		// waiting on ADR-016, and an instance held at zero pods already fails
		// verification on its own topology (no authority master). Failing twice for
		// one state sends the reader after the wrong thing.
		{"quarantined is held, not broken", opStatus(opReasonQuarantined, time.Minute), false},
		// A reason a future registry entry introduces is not evidence of failure.
		// The loud set is enumerated by the ADR and is exactly {Blocked, Stalled}.
		{"an unknown reason does not fail", opStatus("SomethingNew", time.Minute), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := operationFails(tc.op); got != tc.want {
				t.Errorf("operationFails() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRenderOperationVerify_Severity pins each state to the severity token verify
// already uses, and to the same boolean the exit code is derived from — a [FAIL]
// line beside an exit 0 would be worse than no check at all.
func TestRenderOperationVerify_Severity(t *testing.T) {
	cases := []struct {
		name     string
		view     operationView
		wantTok  string
		wantFail bool
		wantSub  []string
	}{
		{
			name:     "running is visible and explicitly benign",
			view:     operationView{Op: opStatus(opReasonRunning, 42*time.Second), Message: "rename is in progress"},
			wantTok:  tokenOK,
			wantFail: false,
			wantSub:  []string{opName, opReasonRunning, "42s", "rename is in progress"},
		},
		{
			name:     "blocked fails and says no timer will rescue it",
			view:     operationView{Op: opStatus(opReasonBlocked, 3*time.Minute), Message: "G2: no living master of ours"},
			wantTok:  tokenFail,
			wantFail: true,
			wantSub:  []string{opReasonBlocked, "3m0s", "G2: no living master of ours", "not auto-skipped"},
		},
		{
			name:     "stalled fails",
			view:     operationView{Op: opStatus(opReasonStalled, 16*time.Minute+2*time.Second)},
			wantTok:  tokenFail,
			wantFail: true,
			wantSub:  []string{opReasonStalled, "16m2s", "not auto-exited"},
		},
		{
			name:     "quarantined warns",
			view:     operationView{Op: opStatus(opReasonQuarantined, time.Minute)},
			wantTok:  tokenWarn,
			wantFail: false,
			wantSub:  []string{opReasonQuarantined, "no pods"},
		},
		{
			name:     "the pending queue is listed",
			view:     operationView{Op: opStatus(opReasonBlocked, time.Minute, "PasswordRotation", "AuthEnablement")},
			wantTok:  tokenFail,
			wantFail: true,
			wantSub:  []string{"Pending: PasswordRotation, AuthEnablement"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines, fail := renderOperationVerify(tc.view, opNow)
			out := strings.Join(lines, "\n")
			if fail != tc.wantFail {
				t.Errorf("fail = %v, want %v\n%s", fail, tc.wantFail, out)
			}
			if !strings.Contains(out, tc.wantTok) {
				t.Errorf("expected severity token %s, got:\n%s", tc.wantTok, out)
			}
			if !strings.Contains(out, "Declared Operation:") {
				t.Errorf("expected a Declared Operation heading, got:\n%s", out)
			}
			for _, sub := range tc.wantSub {
				if !strings.Contains(out, sub) {
					t.Errorf("expected %q in output, got:\n%s", sub, out)
				}
			}
		})
	}
}

// TestRenderOperation_AbsentIsSilent is the no-regression assertion the brief asks
// for: an instance with no operation in flight must render NOTHING, in either verb.
// Documented examples elsewhere depend on that output byte-for-byte.
func TestRenderOperation_AbsentIsSilent(t *testing.T) {
	for _, v := range []operationView{
		{},
		{Op: nil, Message: "No declared heavy operation is in progress."},
	} {
		if lines, fail := renderOperationVerify(v, opNow); len(lines) != 0 || fail {
			t.Errorf("verify rendered %v (fail=%v) for an absent operation", lines, fail)
		}
		if lines := renderOperationStatus(v, opNow); len(lines) != 0 {
			t.Errorf("status rendered %v for an absent operation", lines)
		}
	}
}

// TestRenderOperationStatus surfaces the four facts the brief names — name, reason,
// how long, and the pending queue — and marks the states that need a human with the
// same [!] marker printStatus already uses for LeaderlessRecovery and FailoverRecovery.
func TestRenderOperationStatus(t *testing.T) {
	t.Run("running", func(t *testing.T) {
		out := strings.Join(renderOperationStatus(
			operationView{Op: opStatus(opReasonRunning, 90*time.Second)}, opNow), "\n")
		for _, sub := range []string{"Operation: " + opName, opReasonRunning, "1m30s"} {
			if !strings.Contains(out, sub) {
				t.Errorf("expected %q, got:\n%s", sub, out)
			}
		}
		if strings.Contains(out, "[!]") {
			t.Errorf("a running operation must not be marked as needing action, got:\n%s", out)
		}
	})

	t.Run("stalled is marked and carries the condition message", func(t *testing.T) {
		out := strings.Join(renderOperationStatus(operationView{
			Op:      opStatus(opReasonStalled, 16*time.Minute),
			Message: "has run past its StallAfter budget",
		}, opNow), "\n")
		for _, sub := range []string{"[!]", opReasonStalled, "16m0s", "has run past its StallAfter budget"} {
			if !strings.Contains(out, sub) {
				t.Errorf("expected %q, got:\n%s", sub, out)
			}
		}
	})

	t.Run("pending queue", func(t *testing.T) {
		out := strings.Join(renderOperationStatus(
			operationView{Op: opStatus(opReasonRunning, time.Second, "PasswordRotation")}, opNow), "\n")
		if !strings.Contains(out, "Pending: PasswordRotation") {
			t.Errorf("expected the pending queue, got:\n%s", out)
		}
	})
}

// TestOperationViewOf reads the two CR surfaces the renderers consume: status.operation
// and the OperationInProgress condition message (which is where the driver says WHAT it
// is waiting on — the single most useful string an owner gets at 03:00).
func TestOperationViewOf(t *testing.T) {
	lr := &littleredv1alpha1.LittleRed{}
	if v := operationViewOf(lr); v.Op != nil || v.Message != "" {
		t.Errorf("empty CR must yield an empty view, got %+v", v)
	}
	if v := operationViewOf(nil); v.Op != nil {
		t.Errorf("nil CR must yield an empty view, got %+v", v)
	}

	lr.Status.Operation = opStatus(opReasonBlocked, time.Minute)
	lr.Status.Conditions = []metav1.Condition{{
		Type:    littleredv1alpha1.ConditionOperationInProgress,
		Status:  metav1.ConditionTrue,
		Reason:  opReasonBlocked,
		Message: "reported blocked; the queue is held rather than skipped",
	}}
	v := operationViewOf(lr)
	if v.Op == nil || v.Op.Reason != opReasonBlocked {
		t.Fatalf("expected the blocked operation, got %+v", v.Op)
	}
	if !strings.Contains(v.Message, "held rather than skipped") {
		t.Errorf("expected the condition message, got %q", v.Message)
	}
}

// TestOperationReasonsMatchController is the drift guard for the one thing lrctl
// re-implements: the reason vocabulary. The controller's constants are unexported and
// in a package the CLI does not import, so the shipped binary stays decoupled and the
// TEST reads the source of truth instead. If a reason is ever renamed there, `verify`
// would silently stop failing on it — which is precisely the failure this catches.
//
// Reported separately: the vocabulary would be better as exported constants on
// api/v1alpha1 beside ConditionOperationInProgress, which would delete this test.
func TestOperationReasonsMatchController(t *testing.T) {
	const src = controllerPlanSource
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v (has the controller's plan file moved?)", src, err)
	}
	re := regexp.MustCompile(`operationReason(\w+)\s*=\s*"([^"]+)"`)
	found := map[string]string{}
	for _, m := range re.FindAllStringSubmatch(string(b), -1) {
		found[m[1]] = m[2]
	}
	// Keyed by the controller's identifier SUFFIX, which happens to equal its value for
	// every reason. If that ever stops being true the length check below still catches a
	// reason added or removed, and the value check still catches one renamed.
	want := map[string]string{
		opReasonConverged:   opReasonConverged,
		opReasonRunning:     opReasonRunning,
		opReasonBlocked:     opReasonBlocked,
		opReasonStalled:     opReasonStalled,
		opReasonQuarantined: opReasonQuarantined,
		opReasonSeeded:      opReasonSeeded,
	}
	if len(found) != len(want) {
		t.Errorf("controller declares %d reasons, lrctl mirrors %d: %v", len(found), len(want), found)
	}
	for k, v := range want {
		if found[k] != v {
			t.Errorf("operationReason%s = %q in the controller, %q in lrctl", k, found[k], v)
		}
	}
}

// sentinelLR is a plain, healthy sentinel instance with nothing in flight.
func sentinelLR() *littleredv1alpha1.LittleRed {
	lr := &littleredv1alpha1.LittleRed{}
	lr.Name = "store-sentinel"
	lr.Namespace = "default"
	lr.Spec.Mode = modeSentinel
	lr.Status.Phase = phaseRunning
	lr.Status.Master = &littleredv1alpha1.MasterStatus{PodName: "store-sentinel-redis-0", IP: "10.0.0.1"}
	lr.Status.Sentinels = &littleredv1alpha1.SentinelStatus{Ready: 3, Total: 3}
	lr.Status.Redis = littleredv1alpha1.RedisStatus{Ready: 3, Total: 3}
	return lr
}

// TestPrintStatus_NoOperationIsUnchanged is the regression assertion the brief calls the
// steady-state diff proof: `lrctl status` for an instance with no declared operation must
// still print exactly the documented block, to the byte. Documented examples in
// docs/LRCTL.md and the runbooks depend on it.
func TestPrintStatus_NoOperationIsUnchanged(t *testing.T) {
	const want = `Cluster: store-sentinel
Namespace: default
Phase: Running
Mode: sentinel
Master: store-sentinel-redis-0 (IP: 10.0.0.1)
Sentinels: 3/3 Ready
Redis Nodes: 3/3 Ready
`
	got := captureStdout(t, func() { printStatus(sentinelLR()) })
	if got != want {
		t.Errorf("steady-state status output drifted.\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

// TestPrintStatus_Operation is the other half: when one IS in flight, the line is there,
// above every other extra, so it cannot be missed.
func TestPrintStatus_Operation(t *testing.T) {
	lr := sentinelLR()
	lr.Status.Operation = opStatus(opReasonRunning, time.Minute)
	out := captureStdout(t, func() { printStatus(lr) })
	if !strings.Contains(out, "Operation: "+opName+" — "+opReasonRunning+" for ") {
		t.Errorf("expected the operation line, got:\n%s", out)
	}
	if idx := strings.Index(out, "Operation:"); idx < strings.Index(out, "Redis Nodes:") {
		t.Errorf("the operation line must follow the identity block, got:\n%s", out)
	}
}

// TestStatusJSON_Operation pins the machine-readable path to the CRD's own key names, and
// pins the steady state: an instance with no operation emits neither key.
func TestStatusJSON_Operation(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		b, err := json.Marshal(lrToStatusJSON(sentinelLR()))
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"operation", "acknowledgedOperations"} {
			if strings.Contains(string(b), `"`+key+`"`) {
				t.Errorf("non-operating instance must not emit %q, got:\n%s", key, b)
			}
		}
	})

	t.Run("present", func(t *testing.T) {
		lr := sentinelLR()
		lr.Status.Operation = opStatus(opReasonStalled, 20*time.Minute)
		lr.Status.AcknowledgedOperations = []littleredv1alpha1.OperationAck{{
			Name:           opName,
			Fingerprint:    "9f1c2ab34de5f607",
			AcknowledgedAt: metav1.NewTime(opNow),
		}}
		b, err := json.Marshal(lrToStatusJSON(lr))
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		op, ok := got["operation"].(map[string]any)
		if !ok {
			t.Fatalf("expected an operation object, got:\n%s", b)
		}
		if op["reason"] != opReasonStalled || op["name"] != opName {
			t.Errorf("operation object = %v", op)
		}
		acks, ok := got["acknowledgedOperations"].([]any)
		if !ok || len(acks) != 1 {
			t.Fatalf("expected one acknowledgedOperations row, got:\n%s", b)
		}
		if acks[0].(map[string]any)["fingerprint"] != "9f1c2ab34de5f607" {
			t.Errorf("acknowledgedOperations row = %v", acks[0])
		}
	})
}

// TestVerifyJSON_OperationVerdict is the property that a script and the exit code cannot
// disagree: a Blocked or Stalled operation makes `verify --json` report healthy:false in
// every mode, and a Running one leaves the verdict alone.
func TestVerifyJSON_OperationVerdict(t *testing.T) {
	cases := []struct {
		name        string
		op          *littleredv1alpha1.OperationStatus
		wantHealthy bool
	}{
		{"none", nil, true},
		{"running", opStatus(opReasonRunning, time.Minute), true},
		{"quarantined", opStatus(opReasonQuarantined, time.Minute), true},
		{"blocked", opStatus(opReasonBlocked, time.Minute), false},
		{"stalled", opStatus(opReasonStalled, 20*time.Minute), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opJSON := operationJSONOf(operationView{Op: tc.op})

			sent := &sentinelVerifyJSON{Healthy: true}
			clus := &clusterVerifyJSON{Healthy: true}
			fail := &failoverVerifyJSON{Healthy: true}
			for _, r := range []verifyJSONResult{sent, clus, fail} {
				r.applyOperation(opJSON)
			}
			for name, healthy := range map[string]bool{
				"sentinel": sent.Healthy, "cluster": clus.Healthy, "failover": fail.Healthy,
			} {
				if healthy != tc.wantHealthy {
					t.Errorf("%s: healthy = %v, want %v", name, healthy, tc.wantHealthy)
				}
			}
			if (sent.Operation != nil) != (tc.op != nil) {
				t.Errorf("operation field presence = %v, want %v", sent.Operation != nil, tc.op != nil)
			}
		})
	}
}
