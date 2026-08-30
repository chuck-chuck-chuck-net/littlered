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
	"encoding/hex"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

const (
	uidA = types.UID("2f1b9d0e-7c2a-4f61-9a4d-0d0b1f7a5c31")
	uidB = types.UID("9c6e4a72-1b58-4d3f-8e10-6a2c5b9f4d88")
)

// TestOperationFingerprintDeterministic — the same (uid, name, value) must always
// produce the same digest, across calls and across operator restarts. Without this
// the acknowledgment record is worthless: a re-derived fingerprint that does not
// equal the stored one reads as unfinished work and re-runs a completed operation
// on every pass.
func TestOperationFingerprintDeterministic(t *testing.T) {
	first := OperationFingerprint(uidA, "SentinelMasterNameRename", "prod.cache")
	for i := range 8 {
		if got := OperationFingerprint(uidA, "SentinelMasterNameRename", "prod.cache"); got != first {
			t.Fatalf("call %d = %q, want the stable %q", i, got, first)
		}
	}
	if first == "" {
		t.Fatal("fingerprint is empty; an empty digest cannot distinguish anything")
	}
	if len(first) != OperationFingerprintLen {
		t.Errorf("len = %d, want %d (house style: computeConfigHash / computePodTemplateHash)", len(first), OperationFingerprintLen)
	}
	if _, err := hex.DecodeString(first); err != nil {
		t.Errorf("fingerprint %q is not hex: %v", first, err)
	}
}

// TestOperationFingerprintIsUIDKeyed — the digest is an HMAC keyed on the instance
// UID, so the same (name, value) under a different instance is a different digest.
// This is what stops the record being a dictionary lookup the moment the
// fingerprinted value is a password (ADR-020 Rationale, reason 2).
func TestOperationFingerprintIsUIDKeyed(t *testing.T) {
	a := OperationFingerprint(uidA, "PasswordRotation", "hunter2")
	b := OperationFingerprint(uidB, "PasswordRotation", "hunter2")
	if a == b {
		t.Errorf("digest %q is identical under two instance UIDs; the fingerprint is not keyed", a)
	}
}

// TestOperationFingerprintIdentifiesValueNotField — different values of the SAME
// operation must differ. This is the property that stops "finished" matching the
// wrong edit: the same field changes repeatedly, so naming an intent by its field
// would acknowledge an operation that never ran for the value now in spec
// (ADR-020 Rationale, reason 1). It is the reason the fingerprint exists at all.
func TestOperationFingerprintIdentifiesValueNotField(t *testing.T) {
	old := OperationFingerprint(uidA, "SentinelMasterNameRename", "prod.cache")
	renamed := OperationFingerprint(uidA, "SentinelMasterNameRename", "prod.cache-2")
	if old == renamed {
		t.Errorf("two values of the same field share digest %q; a completed rename would match the next one", old)
	}

	// And the operation name is part of the input too, so two operations that
	// happen to carry the same value are still distinct rows.
	renameFP := OperationFingerprint(uidA, "SentinelMasterNameRename", "shared")
	rotateFP := OperationFingerprint(uidA, "PasswordRotation", "shared")
	if renameFP == rotateFP {
		t.Errorf("two operations with the same value share digest %q", renameFP)
	}

	// A name/value split that is not injective would collide: ("ab","c") and
	// ("a","bc") must not fingerprint alike.
	if OperationFingerprint(uidA, "ab", "c") == OperationFingerprint(uidA, "a", "bc") {
		t.Error("name|value concatenation is ambiguous: (ab,c) collides with (a,bc)")
	}
}

// TestOperationFingerprintNeverLeaksPlaintext — no fixture's plaintext may appear
// in any digest. The record is written to status, which is world-readable to
// anyone with get on the CR, and the fingerprinted value becomes a password the
// moment auth is admitted to the registry.
func TestOperationFingerprintNeverLeaksPlaintext(t *testing.T) {
	secrets := []string{"hunter2", "prod.cache", "correct-horse-battery-staple", "abc"}
	for _, uid := range []types.UID{uidA, uidB} {
		for _, name := range []string{"SentinelMasterNameRename", "PasswordRotation"} {
			for _, secret := range secrets {
				fp := OperationFingerprint(uid, name, secret)
				if fp == "" {
					t.Fatalf("empty fingerprint for (%s, %s)", name, secret)
				}
				if strings.Contains(fp, secret) {
					t.Errorf("digest %q contains the plaintext %q", fp, secret)
				}
				if strings.Contains(fp, string(uid)) {
					t.Errorf("digest %q contains the key %q", fp, uid)
				}
				if strings.Contains(fp, name) {
					t.Errorf("digest %q contains the operation name %q", fp, name)
				}
			}
		}
	}
}
