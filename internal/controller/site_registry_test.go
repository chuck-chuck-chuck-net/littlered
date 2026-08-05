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
func (f *fakeSiteClient) NodeReady(context.Context, string) (bool, bool, error) {
	return false, false, nil
}
func (f *fakeSiteClient) Gatherer(string, bool) redisclient.Gatherer { return nil }

const testOperatorNS = "littlered-system"

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

	reg := newSiteClientRegistry(hub, scheme, make(chan event.GenericEvent))
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
	lr.Namespace = "default"
	lr.Spec.Mode = ModeCluster
	return lr
}

func TestSiteRegistryHubLocalReuse(t *testing.T) {
	reg, built := newTestRegistry(t)
	lr := multiSiteCR()
	site := littleredv1alpha1.SiteSpec{Name: "alpha"} // no secret ref = hub-local

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
		Name:                "beta",
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

func TestSiteRegistryErrors(t *testing.T) {
	t.Run("missing secret", func(t *testing.T) {
		reg, _ := newTestRegistry(t)
		lr := multiSiteCR()
		_, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
			Name:                "beta",
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: "nope"},
		})
		if err == nil {
			t.Fatal("expected error for missing kubeconfig secret")
		}
	})

	t.Run("secret without kubeconfig key", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: testOperatorNS},
			Data:       map[string][]byte{"value": []byte("x")},
		}
		reg, _ := newTestRegistry(t, secret)
		lr := multiSiteCR()
		_, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
			Name:                "beta",
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
			Name:                "beta",
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
			env:  map[string]string{"OPERATOR_NAMESPACE": "explicit", "POD_NAMESPACE": "downward"},
			read: noFile,
			want: "explicit",
		},
		{
			name: "POD_NAMESPACE fallback",
			env:  map[string]string{"POD_NAMESPACE": "downward"},
			read: noFile,
			want: "downward",
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
