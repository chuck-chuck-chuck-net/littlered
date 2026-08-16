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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// fakeSiteClient is a stub SiteClient recording lifecycle calls.
type fakeSiteClient struct {
	name     string
	hubLocal bool
	stopped  bool
}

func (f *fakeSiteClient) Name() string     { return f.name }
func (f *fakeSiteClient) IsHubLocal() bool { return f.hubLocal }
func (f *fakeSiteClient) Stop()            { f.stopped = true }
func (f *fakeSiteClient) Apply(context.Context, *littleredv1alpha1.LittleRed, client.Object) error {
	return nil
}
func (f *fakeSiteClient) Get(context.Context, client.ObjectKey, client.Object) error { return nil }
func (f *fakeSiteClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return nil
}
func (f *fakeSiteClient) Delete(context.Context, client.Object) error { return nil }
func (f *fakeSiteClient) Gatherer(string, bool) redisclient.Gatherer  { return nil }

const testOperatorNS = "littlered-system"

// Shared fixture values for the multi-site tests: site names, the kube-context
// names of the lab sites, and recurring workload identifiers.
const (
	siteAlpha = "alpha"
	siteBeta  = "beta"
	siteCtxS1 = "scm-s1"
	siteCtxS2 = "scm-s2"

	siteName0 = "site0"
	siteName1 = "site1"
	siteName2 = "site2"

	podCacheShard00 = "cache-shard-0-0"
	keySite0Shard00 = "site0/cache-shard-0-0"
	keySite0Shard10 = "site0/cache-shard-1-0"

	// "<site>#<shard>" keys used by the per-site scale-down planner tests.
	keySite0Shard2STS = "site0#2"
	keySite2Shard1STS = "site2#1"
	keySite2Shard2STS = "site2#2"
	volDownward       = "downward"
)

// secretWithoutKubeconfigKey is a site kubeconfig Secret that exists but carries
// no "kubeconfig" key — the malformed-configuration fixture.
func secretWithoutKubeconfigKey() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: testOperatorNS},
		Data:       map[string][]byte{"value": []byte("x")},
	}
}

func kubeconfigSecret(name string, content []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testOperatorNS},
		Data:       map[string][]byte{siteKubeconfigSecretKey: content},
	}
}

// newTestRegistry builds a registry over a fake hub client pre-loaded with the
// given secrets, with a recording fake remote constructor.
func newTestRegistry(t *testing.T, objs ...client.Object) (*siteClientRegistry, *[]*fakeSiteClient) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	hub := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	// Production passes the manager's APIReader as the secret reader; the
	// (unscoped) fake stands in for it here.
	reg := newSiteClientRegistry(hub, hub, scheme, make(chan event.GenericEvent))
	reg.operatorNamespace = func() (string, error) { return testOperatorNS, nil }

	var built []*fakeSiteClient
	reg.newRemote = func(siteName string, kubeconfig []byte, namespace string, owner types.NamespacedName, events chan<- event.GenericEvent) (SiteClient, error) {
		c := &fakeSiteClient{name: siteName}
		built = append(built, c)
		return c, nil
	}
	return reg, &built
}

func multiSiteCR() *littleredv1alpha1.LittleRed {
	lr := &littleredv1alpha1.LittleRed{}
	lr.Name = "ms"
	lr.Namespace = testNamespaceDefault
	lr.Spec.Mode = ModeCluster
	return lr
}

func TestSiteRegistryHubLocalReuse(t *testing.T) {
	reg, built := newTestRegistry(t)
	lr := multiSiteCR()
	site := littleredv1alpha1.SiteSpec{Name: siteAlpha} // no secret ref = hub-local

	c1, err := reg.siteClient(context.Background(), lr, site)
	if err != nil {
		t.Fatalf("siteClient() error: %v", err)
	}
	if c1 == nil || !c1.IsHubLocal() {
		t.Fatalf("hub-local site must yield a hub-local client, got %#v", c1)
	}
	c2, err := reg.siteClient(context.Background(), lr, site)
	if err != nil {
		t.Fatalf("second siteClient() error: %v", err)
	}
	if c1 != c2 {
		t.Fatalf("hub-local client not reused: %p != %p", c1, c2)
	}
	if len(*built) != 0 {
		t.Fatalf("hub-local site must not construct a remote client, built %d", len(*built))
	}
}

func TestSiteRegistryRemoteLifecycle(t *testing.T) {
	secret := kubeconfigSecret("site-beta-kubeconfig", []byte("kubeconfig-v1"))
	reg, built := newTestRegistry(t, secret)
	lr := multiSiteCR()
	site := littleredv1alpha1.SiteSpec{
		Name:                siteBeta,
		KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: secret.Name},
	}

	c1, err := reg.siteClient(context.Background(), lr, site)
	if err != nil {
		t.Fatalf("siteClient() error: %v", err)
	}
	if c1 == nil || c1.IsHubLocal() {
		t.Fatalf("remote site must yield a non-hub-local client, got %#v", c1)
	}
	if len(*built) != 1 {
		t.Fatalf("expected 1 remote construction, got %d", len(*built))
	}

	// Unchanged secret content: the cached client is reused, nothing rebuilt.
	c2, err := reg.siteClient(context.Background(), lr, site)
	if err != nil {
		t.Fatalf("second siteClient() error: %v", err)
	}
	if c1 != c2 || len(*built) != 1 {
		t.Fatalf("client not reused on unchanged secret (built %d)", len(*built))
	}

	// Rotated kubeconfig: the client is rebuilt and the old one stopped.
	secret.Data[siteKubeconfigSecretKey] = []byte("kubeconfig-v2")
	if err := reg.hubClient.Update(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	c3, err := reg.siteClient(context.Background(), lr, site)
	if err != nil {
		t.Fatalf("post-rotation siteClient() error: %v", err)
	}
	if c3 == c1 {
		t.Fatal("client must be rebuilt when the kubeconfig secret content changes")
	}
	if len(*built) != 2 {
		t.Fatalf("expected 2 remote constructions after rotation, got %d", len(*built))
	}
	if !(*built)[0].stopped {
		t.Fatal("old client must be stopped when replaced")
	}

	// Drop on CR deletion: every client of the CR is stopped.
	reg.drop(types.NamespacedName{Namespace: lr.Namespace, Name: lr.Name})
	if !(*built)[1].stopped {
		t.Fatal("drop() must stop the CR's site clients")
	}
}

// scopedCacheClient mimics the manager's cache-backed client under ADR-014
// namespace scoping: a read whose namespace is outside the cache's scope fails
// exactly as controller-runtime's multi-namespace cache fails it
// (multi_namespace_cache.go: "unable to get: %v because of unknown namespace for
// the cache"), and is counted so a test can assert the read never went there.
type scopedCacheClient struct {
	client.Client
	scope      map[string]struct{}
	outOfScope int
}

func (c *scopedCacheClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := c.scope[key.Namespace]; !ok {
		c.outOfScope++
		return fmt.Errorf("unable to get: %v because of unknown namespace for the cache", key)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// TestSiteRegistryReadsKubeconfigOutsideTheWatchScope is the fast guard for the
// namespace-scoping defect: the site kubeconfig Secrets live in the OPERATOR's
// own namespace, which is not part of its declared watch scope, so they must not
// be read through the scoped, cache-backed manager client. The envtest sibling
// (site_registry_scope_envtest_test.go) proves the same against a real scoped
// cache; this one keeps the guard cheap and always-runnable.
func TestSiteRegistryReadsKubeconfigOutsideTheWatchScope(t *testing.T) {
	secret := kubeconfigSecret("site-beta-kubeconfig", []byte("kubeconfig-v1"))
	reg, built := newTestRegistry(t, secret)

	// The operator watches only the CR's namespace; testOperatorNS (holding the
	// site kubeconfig Secrets) is deliberately absent from the scope.
	scoped := &scopedCacheClient{
		Client: reg.hubClient,
		scope:  map[string]struct{}{testNamespaceDefault: {}},
	}
	reg.hubClient = scoped

	lr := multiSiteCR()
	c, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
		Name:                siteBeta,
		KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: secret.Name},
	})
	if err != nil {
		t.Fatalf("siteClient() under a scoped cache: %v; the operator must read its own "+
			"namespace's kubeconfig Secret regardless of the watch scope", err)
	}
	if c == nil || len(*built) != 1 {
		t.Fatalf("expected one remote site client, got %v / %d built", c, len(*built))
	}
	if scoped.outOfScope != 0 {
		t.Errorf("the site kubeconfig Secret was read through the scoped cache client "+
			"(%d out-of-scope Gets); it must go through the uncached reader", scoped.outOfScope)
	}
}

func TestSiteRegistryErrors(t *testing.T) {
	t.Run("missing secret", func(t *testing.T) {
		reg, _ := newTestRegistry(t)
		lr := multiSiteCR()
		_, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
			Name:                siteBeta,
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: "nope"},
		})
		if err == nil {
			t.Fatal("expected error for missing kubeconfig secret")
		}
	})

	t.Run("secret without kubeconfig key", func(t *testing.T) {
		secret := secretWithoutKubeconfigKey()
		reg, _ := newTestRegistry(t, secret)
		lr := multiSiteCR()
		_, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
			Name:                siteBeta,
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: "bad"},
		})
		if err == nil {
			t.Fatal("expected error for secret without kubeconfig key")
		}
	})

	t.Run("operator namespace unresolvable", func(t *testing.T) {
		reg, _ := newTestRegistry(t)
		reg.operatorNamespace = func() (string, error) { return "", errors.New("no namespace") }
		lr := multiSiteCR()
		_, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
			Name:                siteBeta,
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: "whatever"},
		})
		if err == nil {
			t.Fatal("expected error when the operator namespace cannot be resolved")
		}
	})
}

func TestOperatorNamespaceFrom(t *testing.T) {
	noFile := func(string) ([]byte, error) { return nil, errors.New("no file") }

	tests := []struct {
		name    string
		env     map[string]string
		read    func(string) ([]byte, error)
		want    string
		wantErr bool
	}{
		{
			name: "OPERATOR_NAMESPACE wins",
			env:  map[string]string{"OPERATOR_NAMESPACE": "explicit", "POD_NAMESPACE": volDownward},
			read: noFile,
			want: "explicit",
		},
		{
			name: "POD_NAMESPACE fallback",
			env:  map[string]string{"POD_NAMESPACE": volDownward},
			read: noFile,
			want: volDownward,
		},
		{
			name: "in-cluster namespace file, trimmed",
			env:  map[string]string{},
			read: func(path string) ([]byte, error) {
				if path != inClusterNamespaceFile {
					return nil, fmt.Errorf("unexpected path %q", path)
				}
				return []byte("from-file\n"), nil
			},
			want: "from-file",
		},
		{
			name:    "nothing resolvable",
			env:     map[string]string{},
			read:    noFile,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(k string) string { return tc.env[k] }
			got, err := operatorNamespaceFrom(getenv, tc.read)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("operatorNamespaceFrom() = %q, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("operatorNamespaceFrom() error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("operatorNamespaceFrom() = %q, want %q", got, tc.want)
			}
		})
	}
}
