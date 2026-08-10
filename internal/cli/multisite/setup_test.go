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

package multisite

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"
)

const ctxS1 = "scm-s1"

func TestBuildSiteKubeconfig(t *testing.T) {
	ca := []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
	kc, err := BuildSiteKubeconfig(SiteKubeconfigParams{
		ContextName: ctxS1,
		Server:      "https://10.2.1.10:6443",
		CAData:      ca,
		Token:       "tok-abc",
		Namespace:   "ms-smoke",
	})
	if err != nil {
		t.Fatalf("BuildSiteKubeconfig: %v", err)
	}

	// It must parse as a real kubeconfig, and carry the repl-net endpoint (NOT any
	// workstation admin endpoint), the token, the CA, and the namespace default.
	cfg, err := clientcmd.Load(kc)
	if err != nil {
		t.Fatalf("generated kubeconfig does not parse: %v\n%s", err, kc)
	}
	if cfg.CurrentContext != ctxS1 {
		t.Errorf("current-context = %q, want scm-s1", cfg.CurrentContext)
	}
	cluster, ok := cfg.Clusters[ctxS1]
	if !ok {
		t.Fatalf("cluster scm-s1 missing")
	}
	if cluster.Server != "https://10.2.1.10:6443" {
		t.Errorf("server = %q, want the repl-net endpoint", cluster.Server)
	}
	if !reflect.DeepEqual(cluster.CertificateAuthorityData, ca) {
		t.Errorf("CA data not round-tripped")
	}
	authInfo, ok := cfg.AuthInfos["littlered-site"]
	if !ok || authInfo.Token != "tok-abc" {
		t.Errorf("token not set correctly: %+v", authInfo)
	}
	kctx, ok := cfg.Contexts[ctxS1]
	if !ok || kctx.Namespace != "ms-smoke" {
		t.Errorf("context namespace = %q, want ms-smoke", kctx.Namespace)
	}
}

func TestBuildSiteKubeconfigRequiresServerAndToken(t *testing.T) {
	// The repl-net server cannot be derived from the workstation kubeconfig, so it
	// is mandatory (the §8 wrinkle). Missing server or token must error.
	if _, err := BuildSiteKubeconfig(SiteKubeconfigParams{ContextName: "x", Token: "t"}); err == nil {
		t.Errorf("missing Server: want error")
	}
	if _, err := BuildSiteKubeconfig(SiteKubeconfigParams{ContextName: "x", Server: "https://h:6443"}); err == nil {
		t.Errorf("missing Token: want error")
	}
}

func TestBuildHubSiteSecretKeyIsKubeconfig(t *testing.T) {
	// The data key MUST be "kubeconfig" — the operator's siteKubeconfigSecretKey.
	s := BuildHubSiteSecret("littlered-system", "site-scm-s1", []byte("data"))
	if s.Namespace != "littlered-system" || s.Name != "site-scm-s1" {
		t.Fatalf("secret meta = %s/%s", s.Namespace, s.Name)
	}
	if _, ok := s.Data["kubeconfig"]; !ok {
		t.Fatalf("secret must hold the kubeconfig under key %q, got keys %v", "kubeconfig", keysOf(s.Data))
	}
	if s.Type != corev1.SecretTypeOpaque {
		t.Errorf("secret type = %q, want Opaque", s.Type)
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestSiteRoleRulesMatchOperatorWorkloadVerbs(t *testing.T) {
	// Drift guard: the site Role must grant exactly the operator's cluster-mode
	// workload verbs (config/rbac/role.yaml), scoped namespaced. If the operator's
	// role gains/loses a workload resource, this must be updated in lockstep.
	want := map[string][]string{
		"/configmaps":                 {"create", "delete", "get", "list", "patch", "update", "watch"},
		"/services":                   {"create", "delete", "get", "list", "patch", "update", "watch"},
		"/events":                     {"create", "patch"},
		"/pods":                       {"delete", "get", "list", "patch", "update", "watch"},
		"/secrets":                    {"get", "list", "watch"},
		"apps/statefulsets":           {"create", "delete", "get", "list", "patch", "update", "watch"},
		"policy/poddisruptionbudgets": {"create", "delete", "get", "list", "patch", "update", "watch"},
	}
	got := map[string][]string{}
	for _, r := range siteRoleRules() {
		for _, g := range r.APIGroups {
			for _, res := range r.Resources {
				got[g+"/"+res] = r.Verbs
			}
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("siteRoleRules drift:\n got = %v\nwant = %v", got, want)
	}
}
