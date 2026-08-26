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
	"strings"
	"testing"
)

const (
	// badMilli is the value from the field report: Kubernetes reads it as 0.375 bytes.
	badMilli = "375m"
	// goodMebi is what that CR meant.
	goodMebi = "375Mi"
	// wantMilliHint is the word the rejection must carry so the unit slip is obvious.
	wantMilliHint = "milli"
)

// TestValidateMaxmemory guards the unit-suffix collision that produced `maxmemory 1`
// in the field: Kubernetes quantity notation reads `m` as milli, so "375m" is 0.375
// bytes, and CalculateMaxmemory renders it as 1 (Value() rounds up). Redis would have
// read the same string as 375 MB, so the mistake is silent and 375-million-fold.
func TestValidateMaxmemory(t *testing.T) {
	tests := []struct {
		name       string
		maxmemory  string
		wantErr    bool
		wantErrHas string // substring the message must carry, so the user can act on it
	}{
		// The reported bug and its siblings: sub-byte SI suffixes.
		{name: "milli suffix (the field bug)", maxmemory: badMilli, wantErr: true, wantErrHas: wantMilliHint},
		{name: "milli suffix fractional", maxmemory: "1.5m", wantErr: true, wantErrHas: wantMilliHint},
		{name: "micro suffix", maxmemory: "375u", wantErr: true, wantErrHas: wantMilliHint},
		{name: "nano suffix", maxmemory: "375n", wantErr: true, wantErrHas: wantMilliHint},
		// The error must point at the two plausible intents.
		{name: "milli suffix suggests Mi", maxmemory: badMilli, wantErr: true, wantErrHas: goodMebi},

		// Implausibly small values, whatever the notation.
		{name: "raw byte count far too small", maxmemory: "1", wantErr: true, wantErrHas: "too small"},
		{name: "kilobytes below the floor", maxmemory: "64k", wantErr: true, wantErrHas: "too small"},
		{name: "negative", maxmemory: "-1Mi", wantErr: true},

		// Legitimate values must keep working.
		{name: "empty means operator default", maxmemory: ""},
		{name: "zero means unlimited in redis", maxmemory: "0"},
		{name: "binary Mi", maxmemory: goodMebi},
		{name: "decimal M", maxmemory: "375M"},
		{name: "Gi", maxmemory: "2Gi"},
		{name: "exactly the floor", maxmemory: "1Mi"},
		{name: "plain byte count above the floor", maxmemory: "393216000"},

		// Redis-native suffixes are not k8s quantities; CalculateMaxmemory passes them
		// through verbatim and redis-server parses them, so they stay valid.
		{name: "redis mb suffix", maxmemory: "375mb"},
		{name: "redis gb suffix", maxmemory: "2gb"},
		{name: "redis kb suffix uppercase", maxmemory: "512KB"},

		// Neither a quantity nor a redis suffix: redis-server would refuse to start,
		// so fail here where the message names the field.
		{name: "gibberish", maxmemory: "lots", wantErr: true},
		{name: "typo suffix", maxmemory: "375Mib", wantErr: true},
		{name: "embedded space", maxmemory: "375 Mi", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMaxmemory(tt.maxmemory)
			if tt.wantErr && err == nil {
				t.Fatalf("ValidateMaxmemory(%q) = nil, want error", tt.maxmemory)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidateMaxmemory(%q) = %v, want nil", tt.maxmemory, err)
			}
			if tt.wantErrHas != "" && !strings.Contains(err.Error(), tt.wantErrHas) {
				t.Errorf("ValidateMaxmemory(%q) error = %q, want it to mention %q",
					tt.maxmemory, err, tt.wantErrHas)
			}
		})
	}
}

// TestValidateMaxmemoryRejectsWhatCalculateMaxmemoryWouldMangle ties the guard to the
// symptom: every value the guard accepts must render to a maxmemory redis can use,
// and the field's "375m" must never reach the config file as "1".
func TestValidateMaxmemoryRejectsWhatCalculateMaxmemoryWouldMangle(t *testing.T) {
	lr := &LittleRed{}
	lr.Spec.Config.Maxmemory = badMilli

	if got := lr.CalculateMaxmemory(); got != "1" {
		t.Fatalf("precondition changed: CalculateMaxmemory(375m) = %q, want %q "+
			"(the bug this guard exists for)", got, "1")
	}
	if err := ValidateMaxmemory(lr.Spec.Config.Maxmemory); err == nil {
		t.Error("ValidateMaxmemory accepted 375m, which renders as maxmemory 1")
	}
}

// TestValidateRejectsBadMaxmemory checks the guard is wired into the CR-level Validate,
// which the reconciler calls before it renders any config.
func TestValidateRejectsBadMaxmemory(t *testing.T) {
	lr := &LittleRed{Spec: LittleRedSpec{Mode: testModeSentinel}}
	lr.Spec.Config.Maxmemory = badMilli

	err := lr.Validate()
	if err == nil {
		t.Fatal("Validate() = nil for maxmemory 375m, want error")
	}
	if !strings.Contains(err.Error(), "maxmemory") {
		t.Errorf("Validate() error = %q, want it to name the maxmemory field", err)
	}

	lr.Spec.Config.Maxmemory = goodMebi
	if err := lr.Validate(); err != nil {
		t.Errorf("Validate() = %v for maxmemory 375Mi, want nil", err)
	}
}
