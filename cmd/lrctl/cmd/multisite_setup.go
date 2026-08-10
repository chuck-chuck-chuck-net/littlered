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

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/littlered-operator/littlered-operator/internal/cli/multisite"
)

var (
	setupSite          string
	setupSiteAdminCtx  string
	setupHubContext    string
	setupOperatorNS    string
	setupReplEndpoint  string
	setupHubSecretName string
	setupDryRun        bool
	setupSAName        string
	setupTokenWaitSecs int
)

var multisiteSetupSiteCmd = &cobra.Command{
	Use:   "setup-site",
	Short: "Provision a site's ServiceAccount/RBAC + token and create the site-kubeconfig Secret on the hub",
	Long: `Automates the manual multi-site site onboarding (docs/MULTI_SITE_DESIGN.md §7).

In the SITE (via --site-admin-context) it creates, in the instance namespace: the namespace
(if absent), a ServiceAccount, a namespaced Role (workload CRUD the operator needs — no
cluster-scoped access; the ADR-012 site-loss takeover is decided by pure quorum arithmetic),
and a long-lived ServiceAccount token. It then assembles a kubeconfig
whose server is the site's REPLICATION-NET endpoint (--repl-endpoint, a required parameter:
it is reachable only from hub pods and cannot be derived from your workstation kubeconfig,
which holds the admin endpoint — design §8) and creates it as a Secret (data key "kubeconfig")
in the operator namespace on the HUB (via --hub-context).

Idempotent (re-running updates in place). Use --dry-run to print the objects without applying.`,
	Args: cobra.NoArgs,
	RunE: runSetupSite,
}

func runSetupSite(_ *cobra.Command, _ []string) error {
	if setupSite == "" {
		return fmt.Errorf("--site is required (the site name; also the generated context name)")
	}
	if setupReplEndpoint == "" {
		return fmt.Errorf("--repl-endpoint is required (the site's replication-net API endpoint, reachable from hub pods)")
	}
	ns := namespace
	if ns == "" {
		return fmt.Errorf("--namespace (-n) is required (the instance namespace)")
	}
	adminCtx := setupSiteAdminCtx
	if adminCtx == "" {
		adminCtx = setupSite // convention: site name == admin context name
	}
	hubCtx := setupHubContext
	if hubCtx == "" {
		hubCtx = kubeContext
	}
	hubSecret := setupHubSecretName
	if hubSecret == "" {
		hubSecret = "site-" + setupSite
	}

	names := multisite.DefaultSiteSetupNames(ns, hubSecret)
	if setupSAName != "" {
		names.ServiceAccount = setupSAName
	}

	// The objects to create in the site (order matters: ns first).
	siteObjs := []client.Object{
		multisite.BuildSiteNamespace(names),
		multisite.BuildSiteServiceAccount(names),
		multisite.BuildSiteRole(names),
		multisite.BuildSiteRoleBinding(names),
		multisite.BuildSiteTokenSecret(names),
	}

	if setupDryRun {
		return dryRunSetupSite(names, siteObjs, hubCtx, hubSecret, ns)
	}

	ctx := context.Background()

	// --- Site side ---
	siteClient, siteCore, _, _, err := k8s.NewClientForContext(kubeconfig, adminCtx)
	if err != nil {
		return fmt.Errorf("building site client for context %q: %w", adminCtx, err)
	}
	fmt.Printf("Provisioning site %q (admin context %q), namespace %q...\n", setupSite, adminCtx, ns)
	for _, obj := range siteObjs {
		if err := applyObject(ctx, siteClient, obj); err != nil {
			return fmt.Errorf("applying %T %q in site: %w", obj, obj.GetName(), err)
		}
		fmt.Printf("  applied %T %s\n", obj, obj.GetName())
	}

	// Wait for the token controller to populate the SA-token Secret.
	fmt.Printf("Waiting for the ServiceAccount token (up to %ds)...\n", setupTokenWaitSecs)
	token, ca, err := waitForToken(ctx, siteCore, ns, names.TokenSecret, time.Duration(setupTokenWaitSecs)*time.Second)
	if err != nil {
		return err
	}

	// --- Assemble kubeconfig (server = the repl-net endpoint PARAM) ---
	kc, err := multisite.BuildSiteKubeconfig(multisite.SiteKubeconfigParams{
		ContextName: setupSite,
		Server:      setupReplEndpoint,
		CAData:      ca,
		Token:       token,
		Namespace:   ns,
	})
	if err != nil {
		return err
	}

	// --- Hub side: create the site Secret in the operator namespace ---
	hubClient, _, _, _, err := k8s.NewClientForContext(kubeconfig, hubCtx)
	if err != nil {
		return fmt.Errorf("building hub client for context %q: %w", hubCtx, err)
	}
	hubSec := multisite.BuildHubSiteSecret(setupOperatorNS, hubSecret, kc)
	if err := applyObject(ctx, hubClient, hubSec); err != nil {
		return fmt.Errorf("creating hub Secret %s/%s: %w", setupOperatorNS, hubSecret, err)
	}
	fmt.Printf("Created site-kubeconfig Secret %s/%s on the hub (context %q).\n", setupOperatorNS, hubSecret, hubCtx)
	fmt.Printf("Reference it from the CR: spec.multiSite.sites[].kubeconfigSecretRef.name: %s\n", hubSecret)
	return nil
}

// applyObject idempotently creates or updates obj through c.
func applyObject(ctx context.Context, c client.Client, obj client.Object) error {
	err := c.Create(ctx, obj)
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	// Already exists: fetch the current resourceVersion and update in place.
	existing := obj.DeepCopyObject().(client.Object)
	if getErr := c.Get(ctx, client.ObjectKeyFromObject(obj), existing); getErr != nil {
		return getErr
	}
	obj.SetResourceVersion(existing.GetResourceVersion())
	return c.Update(ctx, obj)
}

// waitForToken polls the SA-token Secret until its token (and ca.crt) are
// populated by the token controller.
func waitForToken(
	ctx context.Context, core *kubernetes.Clientset, ns, secretName string, timeout time.Duration,
) (token string, ca []byte, err error) {
	deadline := time.Now().Add(timeout)
	for {
		sec, getErr := core.CoreV1().Secrets(ns).Get(ctx, secretName, metav1.GetOptions{})
		if getErr == nil {
			if t := sec.Data["token"]; len(t) > 0 {
				return string(t), sec.Data["ca.crt"], nil
			}
		}
		if time.Now().After(deadline) {
			return "", nil, fmt.Errorf("timed out waiting for token in Secret %s/%s to be populated", ns, secretName)
		}
		time.Sleep(time.Second)
	}
}

func dryRunSetupSite(names multisite.SiteSetupNames, siteObjs []client.Object, hubCtx, hubSecret, ns string) error {
	fmt.Printf("# DRY RUN — site %q, namespace %q, hub context %q\n", setupSite, ns, hubCtx)
	fmt.Printf("# Would apply to the site (admin context %q):\n", firstNonEmpty(setupSiteAdminCtx, setupSite))
	for _, obj := range siteObjs {
		b, _ := yaml.Marshal(obj)
		fmt.Printf("---\n%s", b)
	}
	// Show the hub Secret with a placeholder kubeconfig (the real token/CA come from
	// the live token controller, unavailable in a dry run).
	placeholder, _ := multisite.BuildSiteKubeconfig(multisite.SiteKubeconfigParams{
		ContextName: setupSite, Server: setupReplEndpoint, CAData: []byte("<SITE-CA>"),
		Token: "<SA-TOKEN>", Namespace: ns,
	})
	hubSec := multisite.BuildHubSiteSecret(setupOperatorNS, hubSecret, placeholder)
	b, _ := yaml.Marshal(hubSec)
	fmt.Printf("# Would create on the hub (context %q):\n---\n%s", hubCtx, b)
	_ = names
	return nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func init() {
	multisiteCmd.AddCommand(multisiteSetupSiteCmd)
	f := multisiteSetupSiteCmd.Flags()
	f.StringVar(&setupSite, "site", "", "Site name (required; also the generated kube-context name)")
	f.StringVar(&setupSiteAdminCtx, "site-admin-context", "",
		"Workstation kube-context for the site's admin API (default: the --site value)")
	f.StringVar(&setupHubContext, "hub-context", "", "Hub kube-context (default: --context / current context)")
	f.StringVar(&setupOperatorNS, "operator-namespace", "littlered-system",
		"Operator namespace on the hub where the site Secret is created")
	f.StringVar(&setupReplEndpoint, "repl-endpoint", "",
		"REQUIRED: the site's replication-net API endpoint (reachable from hub pods), e.g. https://10.2.1.10:6443")
	f.StringVar(&setupHubSecretName, "secret-name", "", "Hub Secret name (default: site-<site>)")
	f.StringVar(&setupSAName, "service-account", "", "ServiceAccount name to create in the site (default: littlered-site)")
	f.BoolVar(&setupDryRun, "dry-run", false, "Print the objects that would be created without applying them")
	f.IntVar(&setupTokenWaitSecs, "token-timeout", 15, "Seconds to wait for the ServiceAccount token to be populated")
}
