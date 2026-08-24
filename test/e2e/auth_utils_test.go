//go:build e2e
// +build e2e

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

package e2e

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	. "github.com/onsi/ginkgo/v2" //nolint:revive // dot-import is the Ginkgo convention in tests

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// =============================================================================
// Auth-on-by-default fixtures for sentinel and failover mode.
//
// WHY THE DEFAULT IS DIFFERENTIATED, and not simply "auth everywhere":
//
//   - failover mode — the password is a genuine MESH-ISOLATION control. A
//     `masterauth` mismatch aborts the replication handshake BEFORE the RDB
//     transfer, so a stale `replicaof <ip>` that lands on a foreign master after
//     an IP recycle (pillar 3.7) can never complete a sync and can never flush.
//   - sentinel mode — the password is the only thing that closes the narrow
//     ADDRESS-ADOPTION path a unique `masterName` leaves open (LR-039 "Not closed
//     by this change"; analysis §9.4): a foreign Sentinel whose recycled master
//     address is now our master reads our master's INFO directly, with no hello
//     and therefore no name check. Auth also covers the Sentinel<->Sentinel link
//     (`sentinelSendAuthIfNeeded`).
//   - cluster mode — a password does NOT protect the mesh. The cluster bus has
//     zero password authentication at every supported version
//     (`grep requirepass|masterauth src/cluster_legacy.c` -> 0 hits, LR-043 §5);
//     its protection is the LR-043 MEET confirmation/attribution guards. Making
//     auth the cluster default would assert something false about what it buys,
//     so cluster (and standalone) fixtures stay auth-free.
//
// Cluster mode loses NO auth coverage: security_test.go still loops all four
// modes for both password auth and TLS. This file governs the DEFAULT POSTURE of
// the other tiers, not what the operator supports.
//
// PER-INSTANCE SECRETS, not one shared suite Secret. LR-039's mechanism list
// records a shared password as one of the conditions under which foreign Sentinel
// gossip is accepted — a shared secret would make every e2e instance mutually
// authenticable, i.e. the suite would model the hazard rather than the mitigation.
// It costs nothing: the Secret rides in the same `kubectl apply` document stream
// as the CR, so there is no extra round trip and no extra wait. This mirrors the
// reasoning already recorded for e2eMasterName.
// =============================================================================

// e2eAuthSecretName is the per-instance Secret holding the instance's password.
func e2eAuthSecretName(crName string) string { return crName + "-auth" }

// e2eAuthPassword derives the instance's password from its CR name, so it is
// unique per instance (see the per-instance rationale above) and reproducible
// from a debug artifact without having to read the Secret back.
func e2eAuthPassword(crName string) string { return "e2e-pw-" + crName }

// e2eAuthSecretDoc renders the instance's password Secret as a YAML document,
// ready to be concatenated in front of a CR manifest in one `kubectl apply -f -`
// stream. `password` is the key the operator reads (secretKeyPassword).
func e2eAuthSecretDoc(crName string) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
type: Opaque
data:
  password: %s
---`, e2eAuthSecretName(crName), testNamespace,
		base64.StdEncoding.EncodeToString([]byte(e2eAuthPassword(crName))))
}

// e2eAuthPreamble registers the instance as auth-enabled AND returns its Secret
// document, so a fixture flips to auth-ON with two edits rather than three:
//
//	cr := e2eAuthPreamble(crName) + fmt.Sprintf(`...
//	spec:
//	  mode: sentinel
//	%s  ...`, ..., e2eAuthSpecYAML(crName), ...)
//
// Registration has to happen at RENDER time, not at apply time: the exec helpers
// look the password up by pod name and some fixtures render a manifest well
// before the pods exist.
func e2eAuthPreamble(crName string) string {
	registerE2EAuth(crName)
	return e2eAuthSecretDoc(crName)
}

// e2eAuthSpecYAML renders the `spec.auth` block for an instance, indented for
// insertion directly under `spec:`.
func e2eAuthSpecYAML(crName string) string {
	return fmt.Sprintf("  auth:\n    enabled: true\n    existingSecret: %s\n", e2eAuthSecretName(crName))
}

// -----------------------------------------------------------------------------
// The credential registry.
//
// Every helper in this suite that shells into a pod with `redis-cli` needs the
// instance's password once auth is on, and a missed one fails as an obscure
// NOAUTH far from its cause. Rather than thread a password parameter through
// ~20 call sites and four files (where an omission is silent), the deploy
// helpers register the instance here and the exec helpers look it up by POD
// NAME. An instance that was never registered gets no auth arguments, which is
// exactly the auth-free behaviour cluster/standalone fixtures require — and the
// deliberately auth-free tiers too (the two capture-staging Describes in
// sentinel_master_name_test.go / sentinel_quarantine_test.go, and failover mode's
// Minimum Topology tier; each carries its own DELIBERATELY AUTH-FREE block).
// -----------------------------------------------------------------------------

var (
	e2eAuthMu       sync.RWMutex
	e2eAuthByCRName = map[string]string{}
)

// registerE2EAuth records that crName was deployed with auth enabled. Deploy
// helpers call it; it is idempotent, so re-applying a CR is fine.
func registerE2EAuth(crName string) {
	e2eAuthMu.Lock()
	defer e2eAuthMu.Unlock()
	e2eAuthByCRName[crName] = e2eAuthPassword(crName)
}

// e2ePasswordForResource returns the password of the auth-enabled instance that
// owns the named resource — a pod (`{cr}-redis-0`, `{cr}-sentinel-2`), a Service
// (`{cr}`), or the CR itself. Empty when the instance is auth-free.
//
// Matching is by longest registered CR-name prefix, so `foo` and `foo-bar` in the
// same namespace resolve to the right instance rather than to whichever was
// registered first.
func e2ePasswordForResource(name string) string {
	e2eAuthMu.RLock()
	defer e2eAuthMu.RUnlock()
	best, pw := "", ""
	for cr, p := range e2eAuthByCRName {
		if name != cr && !strings.HasPrefix(name, cr+"-") {
			continue
		}
		if len(cr) > len(best) {
			best, pw = cr, p
		}
	}
	return pw
}

// redisCliAuthArgs returns the `redis-cli` arguments that authenticate against
// the instance owning `resourceName`, or nil when that instance is auth-free.
func redisCliAuthArgs(resourceName string) []string {
	pw := e2ePasswordForResource(resourceName)
	if pw == "" {
		return nil
	}
	return []string{"-a", pw, "--no-auth-warning"}
}

// e2eTypedAuthSpec is the typed-client sibling of e2eAuthPreamble +
// e2eAuthSpecYAML, for the fixtures that build CRs with k8sClient.Create rather
// than a `kubectl apply -f -` YAML stream — currently pdb_test.go.
// (security_test.go builds its own auth fixture by design: it is the spec that
// PROVES auth works, so it must not depend on these helpers.)
// It creates the instance's Secret, registers the instance, and returns the
// AuthSpec to embed — one call, so a fixture cannot half-flip by embedding the
// spec while forgetting the Secret.
func e2eTypedAuthSpec(ctx context.Context, c client.Client, crName string) littleredv1alpha1.AuthSpec {
	registerE2EAuth(crName)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e2eAuthSecretName(crName),
			Namespace: testNamespace,
		},
		Data: map[string][]byte{"password": []byte(e2eAuthPassword(crName))},
	}
	// AlreadyExists is fine and expected: the password is derived from the CR
	// name, so a re-created instance re-uses the identical Secret.
	if err := c.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		Fail(fmt.Sprintf("failed to create auth secret for %s: %v", crName, err))
	}
	return littleredv1alpha1.AuthSpec{Enabled: true, ExistingSecret: e2eAuthSecretName(crName)}
}
