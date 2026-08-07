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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientcmdapiv1 "k8s.io/client-go/tools/clientcmd/api/v1"
	"sigs.k8s.io/yaml"
)

// SiteSetupNames are the object names the site-Secret generator creates in a
// site (SA/Role/RoleBinding/ClusterRole/ClusterRoleBinding/token Secret) plus
// the resulting hub Secret name.
type SiteSetupNames struct {
	Namespace      string // instance namespace (on the site AND the hub)
	ServiceAccount string
	Role           string
	RoleBinding    string
	ClusterRole    string // nodes get;list (ADR-012 witness) — must not collide across instances
	TokenSecret    string
	HubSecret      string // Secret created in the operator namespace on the hub
}

// DefaultSiteSetupNames returns conventional names for a site's setup objects.
// The ClusterRole/ClusterRoleBinding (cluster-scoped, ADR-012 node read) are
// suffixed with the namespace so multiple instances in different namespaces do
// not share — or clobber — one cluster-scoped RBAC object.
func DefaultSiteSetupNames(namespace, hubSecret string) SiteSetupNames {
	return SiteSetupNames{
		Namespace:      namespace,
		ServiceAccount: "littlered-site",
		Role:           "littlered-site",
		RoleBinding:    "littlered-site",
		ClusterRole:    "littlered-site-nodes-" + namespace,
		TokenSecret:    "littlered-site-token",
		HubSecret:      hubSecret,
	}
}

// siteRoleRules is the namespaced workload CRUD the hub operator needs in a site
// (docs/MULTI_SITE_DESIGN.md §7: workload CRUD + pod read in the instance
// namespace, NOT cluster-admin). It mirrors the operator's cluster-mode verbs in
// config/rbac/role.yaml, scoped to the site's instance namespace. The
// setup_test.go drift-guard asserts this set stays in step with the operator's.
func siteRoleRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"configmaps", "services"},
			Verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: []string{"create", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"pods"},
			Verbs: []string{"delete", "get", "list", "patch", "update", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{"apps"}, Resources: []string{"statefulsets"},
			Verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{APIGroups: []string{"policy"}, Resources: []string{"poddisruptionbudgets"},
			Verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
	}
}

func objectMeta(name, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
		Labels:    map[string]string{"app.kubernetes.io/managed-by": "lrctl-multisite-setup"},
	}
}

// BuildSiteNamespace builds the instance Namespace object (created on the site if
// absent).
func BuildSiteNamespace(names SiteSetupNames) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: names.Namespace},
	}
}

// BuildSiteServiceAccount builds the site ServiceAccount the operator authenticates as.
func BuildSiteServiceAccount(names SiteSetupNames) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: objectMeta(names.ServiceAccount, names.Namespace),
	}
}

// BuildSiteRole builds the namespaced workload-CRUD Role.
func BuildSiteRole(names SiteSetupNames) *rbacv1.Role {
	return &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: objectMeta(names.Role, names.Namespace),
		Rules:      siteRoleRules(),
	}
}

// BuildSiteRoleBinding binds the site ServiceAccount to the workload-CRUD Role.
func BuildSiteRoleBinding(names SiteSetupNames) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: objectMeta(names.RoleBinding, names.Namespace),
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: names.Role},
		Subjects: []rbacv1.Subject{
			{Kind: "ServiceAccount", Name: names.ServiceAccount, Namespace: names.Namespace},
		},
	}
}

// BuildSiteNodesClusterRole builds the ONE cluster-scoped grant multi-site needs:
// nodes get;list, for the ADR-012 site-loss witness (§7). No other cluster-scoped
// access is granted.
func BuildSiteNodesClusterRole(names SiteSetupNames) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{
			Name:   names.ClusterRole,
			Labels: map[string]string{"app.kubernetes.io/managed-by": "lrctl-multisite-setup"},
		},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get", "list"}},
		},
	}
}

// BuildSiteNodesClusterRoleBinding binds the site ServiceAccount to the node-read ClusterRole.
func BuildSiteNodesClusterRoleBinding(names SiteSetupNames) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{
			Name:   names.ClusterRole,
			Labels: map[string]string{"app.kubernetes.io/managed-by": "lrctl-multisite-setup"},
		},
		RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: names.ClusterRole},
		Subjects: []rbacv1.Subject{
			{Kind: "ServiceAccount", Name: names.ServiceAccount, Namespace: names.Namespace},
		},
	}
}

// BuildSiteTokenSecret builds a long-lived ServiceAccount-token Secret (the
// kubelet populates .data.token and .data['ca.crt']).
func BuildSiteTokenSecret(names SiteSetupNames) *corev1.Secret {
	meta := objectMeta(names.TokenSecret, names.Namespace)
	meta.Annotations = map[string]string{"kubernetes.io/service-account.name": names.ServiceAccount}
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: meta,
		Type:       corev1.SecretTypeServiceAccountToken,
	}
}

// SiteKubeconfigParams are the inputs to BuildSiteKubeconfig.
type SiteKubeconfigParams struct {
	// ContextName names the cluster/context/current-context in the generated
	// kubeconfig (conventionally the site name).
	ContextName string
	// Server is the site API endpoint the OPERATOR (running in hub pods) will
	// use — the replication-net endpoint. It cannot be derived from the
	// workstation kubeconfig (which holds the admin endpoint), so it is a
	// required parameter (docs/MULTI_SITE_DESIGN.md §8, the two-network wrinkle).
	Server string
	// CAData is the site API server's CA certificate (raw PEM bytes).
	CAData []byte
	// Token is the ServiceAccount token.
	Token string
	// Namespace is the instance namespace the context defaults to.
	Namespace string
}

// BuildSiteKubeconfig assembles the kubeconfig the operator stores as the site
// Secret: server = the repl-net endpoint (a PARAMETER — see SiteKubeconfigParams.Server),
// the site CA, and the ServiceAccount token. Returns YAML bytes ready to be the
// Secret's "kubeconfig" data value.
func BuildSiteKubeconfig(p SiteKubeconfigParams) ([]byte, error) {
	if p.ContextName == "" || p.Server == "" || p.Token == "" {
		return nil, fmt.Errorf("kubeconfig requires a context name, server endpoint, and token")
	}
	const user = "littlered-site"
	cfg := clientcmdapiv1.Config{
		APIVersion: "v1",
		Kind:       "Config",
		Clusters: []clientcmdapiv1.NamedCluster{{
			Name: p.ContextName,
			Cluster: clientcmdapiv1.Cluster{
				Server:                   p.Server,
				CertificateAuthorityData: p.CAData,
			},
		}},
		AuthInfos: []clientcmdapiv1.NamedAuthInfo{{
			Name:     user,
			AuthInfo: clientcmdapiv1.AuthInfo{Token: p.Token},
		}},
		Contexts: []clientcmdapiv1.NamedContext{{
			Name: p.ContextName,
			Context: clientcmdapiv1.Context{
				Cluster:   p.ContextName,
				AuthInfo:  user,
				Namespace: p.Namespace,
			},
		}},
		CurrentContext: p.ContextName,
	}
	return yaml.Marshal(&cfg)
}

// BuildHubSiteSecret builds the opaque Secret stored in the operator namespace on
// the hub, holding the assembled kubeconfig under the "kubeconfig" data key
// (matching the operator's siteKubeconfigSecretKey).
func BuildHubSiteSecret(operatorNamespace, secretName string, kubeconfig []byte) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: operatorNamespace,
			Labels:    map[string]string{"app.kubernetes.io/managed-by": "lrctl-multisite-setup"},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"kubeconfig": kubeconfig},
	}
}
