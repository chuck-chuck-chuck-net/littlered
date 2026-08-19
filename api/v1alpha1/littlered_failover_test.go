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

package v1alpha1

import (
	"os"
	"strings"
	"testing"
)

func TestFailoverSetDefaults(t *testing.T) {
	t.Run("failover mode with nil section gets section + defaults", func(t *testing.T) {
		lr := &LittleRed{Spec: LittleRedSpec{Mode: ModeFailover}}
		lr.SetDefaults()
		f := lr.Spec.Failover
		if f == nil {
			t.Fatal("spec.failover should be auto-created in failover mode")
		}
		if f.Replicas == nil || *f.Replicas != DefaultFailoverReplicas {
			t.Errorf("Replicas = %v, want %d", f.Replicas, DefaultFailoverReplicas)
		}
		if f.DownAfterMilliseconds != DefaultFailoverDownAfterMs {
			t.Errorf("DownAfterMilliseconds = %d, want %d", f.DownAfterMilliseconds, DefaultFailoverDownAfterMs)
		}
		if f.MinReplicasToWrite == nil || *f.MinReplicasToWrite != DefaultFailoverMinReplicasToWrite {
			t.Errorf("MinReplicasToWrite = %v, want %d (LR-038: the durable pair is on by default)",
				f.MinReplicasToWrite, DefaultFailoverMinReplicasToWrite)
		}
		if f.AllowUnsafeRebootstrapOnDeadlock {
			t.Error("AllowUnsafeRebootstrapOnDeadlock must default to false")
		}
	})

	t.Run("user values are preserved", func(t *testing.T) {
		replicas := int32(4)
		lr := &LittleRed{Spec: LittleRedSpec{Mode: ModeFailover, Failover: &FailoverSpec{
			Replicas:                         &replicas,
			DownAfterMilliseconds:            12000,
			MinReplicasToWrite:               new(1),
			AllowUnsafeRebootstrapOnDeadlock: true,
		}}}
		lr.SetDefaults()
		f := lr.Spec.Failover
		if f.Replicas == nil || *f.Replicas != 4 {
			t.Errorf("Replicas = %v, want 4 (user value preserved)", f.Replicas)
		}
		if f.DownAfterMilliseconds != 12000 {
			t.Errorf("DownAfterMilliseconds = %d, want 12000 (user value preserved)", f.DownAfterMilliseconds)
		}
		if f.MinReplicasToWrite == nil || *f.MinReplicasToWrite != 1 {
			t.Errorf("MinReplicasToWrite = %v, want 1 (user value preserved)", f.MinReplicasToWrite)
		}
		if !f.AllowUnsafeRebootstrapOnDeadlock {
			t.Error("AllowUnsafeRebootstrapOnDeadlock = false, want true (user value preserved)")
		}
	})

	t.Run("non-failover mode leaves failover section nil", func(t *testing.T) {
		lr := &LittleRed{Spec: LittleRedSpec{Mode: "sentinel"}}
		lr.SetDefaults()
		if lr.Spec.Failover != nil {
			t.Error("spec.failover must not be auto-created outside failover mode")
		}
	})
}

func TestValidateFailover(t *testing.T) {
	replicas := func(n int32) *int32 { return &n }
	tests := []struct {
		name     string
		failover *FailoverSpec
		wantErr  bool
	}{
		{"nil section is valid", nil, false},
		{"defaults are valid", &FailoverSpec{Replicas: replicas(2), DownAfterMilliseconds: 5000}, false},
		{"zero replicas rejected", &FailoverSpec{Replicas: replicas(0)}, true},
		{"negative minReplicasToWrite rejected", &FailoverSpec{Replicas: replicas(2), MinReplicasToWrite: new(-1)}, true},
		{"one replica is valid", &FailoverSpec{Replicas: replicas(1)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &LittleRed{Spec: LittleRedSpec{Mode: ModeFailover, Failover: tt.failover}}
			err := lr.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestFailoverCRDContainsModeAndCELRule asserts, via the generated CRD (the
// TestExporterDefaultsMatchDockerfile pattern), that `make manifests` picked up
// the failover mode enum value and the CEL rule gating spec.failover. The CEL
// rule's runtime behavior (apiserver admission) is envtest/e2e territory; this
// guards the generated artifact against marker/manifest drift.
func TestFailoverCRDContainsModeAndCELRule(t *testing.T) {
	const crdPath = "../../config/crd/bases/redis.chuck-chuck-chuck.net_littlereds.yaml"
	crd, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("reading generated CRD: %v", err)
	}
	crdStr := string(crd)

	for _, want := range []string{
		"- failover", // mode enum gained the value
		"rule: self.mode == 'failover' || !has(self.failover)",
		"message: spec.failover may only be set when spec.mode is 'failover'",
	} {
		if !strings.Contains(crdStr, want) {
			t.Errorf("generated CRD missing %q; run `make manifests`", want)
		}
	}
}

// TestFailoverMinReplicasToWriteExplicitZeroSurvivesDefaulting is the reason the
// field is a *int and not an int (LR-038). The default is non-zero and the
// operator Updates the whole object to add its finalizer, so with a bare int +
// omitempty an explicit 0 would be dropped on serialization and the API server
// would re-apply the default — silently turning a deliberate "off" back on, for
// exactly the replicas: 1 users the docs tell to set 0. That is LR-033's hazard.
func TestFailoverMinReplicasToWriteExplicitZeroSurvivesDefaulting(t *testing.T) {
	off := 0
	lr := &LittleRed{Spec: LittleRedSpec{Mode: ModeFailover, Failover: &FailoverSpec{
		Replicas:           new(int32(1)),
		MinReplicasToWrite: &off,
	}}}
	lr.SetDefaults()

	got := lr.Spec.Failover.MinReplicasToWrite
	if got == nil {
		t.Fatal("defaulting dropped an explicitly-set minReplicasToWrite")
	}
	if *got != 0 {
		t.Errorf("MinReplicasToWrite = %d, want 0 — an explicit opt-out must not be re-defaulted", *got)
	}
}
