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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
)

// inClusterNamespaceFile is where the serviceaccount admission controller
// mounts the pod's own namespace.
const inClusterNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// siteKey identifies one site client: site clients are per CR (the remote pod
// watch maps events to its owning CR) and per site.
type siteKey struct {
	owner types.NamespacedName
	site  string
}

// siteEntry is a cached site client plus the content hash of the kubeconfig it
// was built from (empty for hub-local sites), so a rotated kubeconfig Secret
// invalidates the client.
type siteEntry struct {
	client     SiteClient
	secretHash string
}

// siteClientRegistry lazily constructs and caches SiteClients per (CR, site).
// Thread-safe; lives as a field on LittleRedReconciler. Entries are invalidated
// when the site's kubeconfig Secret content changes and dropped (stopped) on CR
// deletion.
type siteClientRegistry struct {
	mu        sync.Mutex
	hubClient client.Client
	hubScheme *runtime.Scheme
	events    chan event.GenericEvent

	// hubReader is the hub's UNCACHED reader (the manager's APIReader). It answers
	// one question — "read this object from the hub, bypassing the cache" — for two
	// callers, and they are one question rather than two: the site kubeconfig
	// Secrets, and (through hubSiteClient.UncachedReader) LR-043's MEET address
	// confirmation for a hub-local site.
	//
	// It reads the site kubeconfig Secrets from the operator's OWN
	// namespace. It is the manager's APIReader — uncached, and deliberately NOT
	// hubClient: under ADR-014 namespace scoping the manager's client is
	// cache-backed for every type including Secrets, and its cache holds exactly
	// the declared watch scope, which the operator's own namespace need not be
	// part of. Reading them through hubClient therefore fails under both scoping
	// modes (allow-list: "unknown namespace for the cache"; deny-list: a silent
	// NotFound), and the failure surfaces as "site unreachable", so a scoped
	// operator never resolves any site and never says why.
	//
	// Uncached rather than cache-widened on purpose: adding the operator's own
	// namespace to the manager cache would extend it for EVERY type, including
	// the LittleRed CR the controller watches, so a scoped operator would start
	// reconciling CRs in its own namespace — a silent widening of the very scope
	// ADR-014 exists to declare. It is also not expressible in deny mode, whose
	// scope is one global metadata.namespace!=... field selector with no room for
	// a per-namespace exemption. RBAC-wise a direct Get needs only `get` on
	// secrets there (charts/littlered: the allow-mode `-site-secrets` Role),
	// where a cache would need `list`+`watch` and would hold every Secret of that
	// namespace in memory.
	hubReader client.Reader

	// operatorNamespace resolves the namespace holding the site kubeconfig
	// Secrets (the operator's own namespace). Injectable for tests.
	operatorNamespace func() (string, error)
	// newRemote constructs a remote SiteClient. Injectable for tests.
	newRemote func(siteName string, kubeconfig []byte, namespace string, owner types.NamespacedName, events chan<- event.GenericEvent) (SiteClient, error)

	entries map[siteKey]*siteEntry
}

// newSiteClientRegistry builds the production registry. hubReader must be an
// UNCACHED reader (the manager's APIReader) — see the field comment.
func newSiteClientRegistry(hubClient client.Client, hubReader client.Reader, scheme *runtime.Scheme, events chan event.GenericEvent) *siteClientRegistry {
	return &siteClientRegistry{
		hubClient:         hubClient,
		hubReader:         hubReader,
		hubScheme:         scheme,
		events:            events,
		operatorNamespace: detectOperatorNamespace,
		newRemote:         newRemoteSiteClient,
		entries:           make(map[siteKey]*siteEntry),
	}
}

// siteClient returns the SiteClient for one site of lr, constructing (or
// reconstructing, when the kubeconfig Secret content changed) it on first use.
func (reg *siteClientRegistry) siteClient(ctx context.Context, lr *littleredv1alpha1.LittleRed, site littleredv1alpha1.SiteSpec) (SiteClient, error) {
	key := siteKey{
		owner: types.NamespacedName{Namespace: lr.Namespace, Name: lr.Name},
		site:  site.Name,
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()

	// Hub-local site: the degenerate case, wraps the manager's client/cache.
	if site.KubeconfigSecretRef == nil {
		if e, ok := reg.entries[key]; ok && e.client.IsHubLocal() {
			return e.client, nil
		}
		reg.replaceLocked(key, &siteEntry{
			client: &hubSiteClient{name: site.Name, client: reg.hubClient, uncached: reg.hubReader, scheme: reg.hubScheme},
		})
		return reg.entries[key].client, nil
	}

	// Everything from here to the parsed kubeconfig is the operator reading its OWN
	// hub-local inputs, so a failure is a CONFIGURATION fault, not the site failing
	// to answer — classified with siteConfigFault so callers can tell the two apart
	// (an unreachable site may heal itself; this never will). See site_config_fault.go.
	opNS, err := reg.operatorNamespace()
	if err != nil {
		return nil, siteConfigFault(site.Name, siteFaultOperatorNamespace,
			fmt.Errorf("resolving the operator namespace (holding site kubeconfig Secrets): %w", err))
	}

	secret := &corev1.Secret{}
	if err := reg.hubReader.Get(ctx, types.NamespacedName{Namespace: opNS, Name: site.KubeconfigSecretRef.Name}, secret); err != nil {
		return nil, siteConfigFault(site.Name, siteFaultSecretUnreadable,
			fmt.Errorf("reading kubeconfig secret %s/%s: %w", opNS, site.KubeconfigSecretRef.Name, err))
	}
	kubeconfig, err := kubeconfigFromSecret(secret)
	if err != nil {
		return nil, siteConfigFault(site.Name, siteFaultSecretMalformed, err)
	}
	hash := contentHash(kubeconfig)

	if e, ok := reg.entries[key]; ok && e.secretHash == hash {
		return e.client, nil
	}

	// First use, or the kubeconfig was rotated: (re)build the site client.
	c, err := reg.newRemote(site.Name, kubeconfig, lr.Namespace, key.owner, reg.events)
	if err != nil {
		return nil, err
	}
	reg.replaceLocked(key, &siteEntry{client: c, secretHash: hash})
	return c, nil
}

// replaceLocked installs a new entry under key, stopping any previous client.
// Caller holds reg.mu.
func (reg *siteClientRegistry) replaceLocked(key siteKey, e *siteEntry) {
	if old, ok := reg.entries[key]; ok {
		old.client.Stop()
	}
	reg.entries[key] = e
}

// drop stops and removes every site client belonging to the given CR.
func (reg *siteClientRegistry) drop(owner types.NamespacedName) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	for k, e := range reg.entries {
		if k.owner == owner {
			e.client.Stop()
			delete(reg.entries, k)
		}
	}
}

// detectOperatorNamespace resolves the operator's own namespace at runtime.
func detectOperatorNamespace() (string, error) {
	return operatorNamespaceFrom(os.Getenv, os.ReadFile)
}

// operatorNamespaceFrom is the pure resolution order behind
// detectOperatorNamespace: OPERATOR_NAMESPACE env (explicit override), then
// POD_NAMESPACE (downward API convention), then the in-cluster serviceaccount
// namespace file.
func operatorNamespaceFrom(getenv func(string) string, readFile func(string) ([]byte, error)) (string, error) {
	if ns := getenv("OPERATOR_NAMESPACE"); ns != "" {
		return ns, nil
	}
	if ns := getenv("POD_NAMESPACE"); ns != "" {
		return ns, nil
	}
	if data, err := readFile(inClusterNamespaceFile); err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			return ns, nil
		}
	}
	return "", fmt.Errorf("cannot determine the operator namespace: set OPERATOR_NAMESPACE or POD_NAMESPACE (downward API), or run in-cluster")
}

// contentHash returns a stable hash of the kubeconfig bytes, used to detect
// Secret rotation.
func contentHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
