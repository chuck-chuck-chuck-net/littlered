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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"k8s.io/apimachinery/pkg/types"
)

// OperationFingerprintLen is the hex length of an operation fingerprint, matching
// the operator's other stable content hashes (computeConfigHash,
// computePodTemplateHash). The digest is truncated because it is an equality
// token, never a security boundary on its own: 64 bits of a keyed digest is far
// beyond what distinguishing a handful of spec values needs, and the key — not the
// width — is what stops the value being recovered.
const OperationFingerprintLen = 16

// OperationFingerprint identifies WHICH VALUE of a registered heavy operation the
// operator has been asked to carry out (ADR-020). It is stored in
// status.acknowledgedOperations once that work COMPLETES, and compared against a
// freshly computed one every pass: equal means converged, different means there is
// unfinished work from a spec change.
//
// It is an HMAC-SHA256 keyed on the instance UID, hex, truncated to
// OperationFingerprintLen. Three properties are load-bearing, and each has a test:
//
//  1. It identifies the VALUE, not the field. The obvious alternative — naming an
//     intent by the field it changes — fails, because the same field changes
//     repeatedly, so "finished" would match the wrong edit and an operation would
//     read as acknowledged that never ran for the value now in spec.
//  2. It never leaks the value. The moment auth joins the registry the
//     fingerprinted value is a PASSWORD, and status is readable by anyone who can
//     get the CR. A bare digest of a short secret is a dictionary lookup, so the
//     key is not optional; the instance UID is the key (the AnnotationAuthHash
//     precedent).
//  3. It structurally enforces ADR-018's refusal to remember a previous master
//     name. A keyed digest cannot be reversed, so no later contributor can quietly
//     start reading this record as "the old name" and walk that refusal back —
//     Rule N derives what to prune from evidence, and must keep doing so.
//
// The name is length-prefixed rather than merely delimited, so no (name, value)
// pair can be re-split into another: ("ab", "c") and ("a", "bc") must not collide,
// or two operations could acknowledge each other.
func OperationFingerprint(uid types.UID, name, value string) string {
	framed := strconv.Itoa(len(name)) + ":" + name + strconv.Itoa(len(value)) + ":" + value
	mac := hmac.New(sha256.New, []byte(uid))
	_, _ = mac.Write([]byte(framed)) // hash.Hash.Write never returns an error
	return hex.EncodeToString(mac.Sum(nil))[:OperationFingerprintLen]
}
