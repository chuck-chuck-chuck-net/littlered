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

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/littlered-operator/littlered-operator/internal/cli/multisite"
)

var preflightOperatorNS string

var multisitePreflightCmd = &cobra.Command{
	Use:   "preflight <name>",
	Short: "Validate a multi-site instance's operator connectivity prerequisites",
	Long: `Checks the setup a multi-site instance depends on (docs/MULTI_SITE_DESIGN.md §8, which
lists TWO DISTINCT network paths the hub operator needs):

  (a) each site's kubeconfigSecretRef Secret exists in the operator namespace on the hub
      and is well-formed (parses, has the "kubeconfig" key, names an endpoint);
  (b) each site's Kubernetes API is reachable over the WORKSTATION path (your own
      kube-context) — this is the path lrctl itself uses, NOT the operator's;
  (c) the hub->site-API path the OPERATOR uses (the replication-net endpoint embedded in
      the Secret) is reported but NOT tested — it is reachable only from hub pods;
  (d) the hub-pod -> spoke-pod-IP DATA path (Redis dials) is reported as REQUIRING an
      in-cluster probe — it cannot be truthfully verified from a workstation.

This is honest about which path each check used; it never claims a pass it did not observe.`,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: completeLittleRedNames,
	RunE:              runMultisitePreflight,
}

func runMultisitePreflight(_ *cobra.Command, args []string) error {
	ctx := context.Background()
	hubClient, _, _, defaultNS, err := k8s.NewClientForContext(kubeconfig, kubeContext)
	if err != nil {
		return err
	}
	ns := namespace
	if ns == "" {
		ns = defaultNS
	}

	lr, err := getLittleRed(ctx, hubClient, client.ObjectKey{Name: args[0], Namespace: ns})
	if err != nil {
		return err
	}
	if !isMultiSite(lr) {
		return fmt.Errorf("%s/%s is not a multi-site instance (spec.multiSite is unset)", ns, args[0])
	}

	fmt.Printf("Multi-Site Preflight: %s/%s (%d sites, operator namespace %q)\n",
		ns, lr.Name, len(lr.Spec.MultiSite.Sites), preflightOperatorNS)

	// Resolve site name -> workstation context (path b).
	siteNames := make([]string, len(lr.Spec.MultiSite.Sites))
	for i, s := range lr.Spec.MultiSite.Sites {
		siteNames[i] = s.Name
	}
	explicit, err := multisite.ParseSiteContextFlag(siteContext)
	if err != nil {
		return err
	}
	available, err := k8s.AvailableContexts(kubeconfig)
	if err != nil {
		return err
	}
	resolution := multisite.ResolveSiteContexts(siteNames, explicit, available)

	problems := 0
	for _, site := range lr.Spec.MultiSite.Sites {
		fmt.Printf("\nSite %q:\n", site.Name)

		// (a) Secret existence + well-formedness on the hub.
		endpoint, secretOK := preflightSecret(ctx, hubClient, site)
		if !secretOK {
			problems++
		}

		// (c) operator path (repl-net endpoint) — reported, not tested.
		if endpoint != "" {
			fmt.Printf("  [INFO] operator path (hub-pod -> site-API): %s — "+
				"NOT tested (reachable only from hub pods)\n", endpoint)
		}

		// (b) workstation path — the site's own kube-context.
		if !preflightWorkstationAPI(ctx, site.Name, ns, resolution) {
			problems++
		}
	}

	// (d) pod-IP data path — cannot be verified from a workstation.
	fmt.Printf("\n[INFO] hub-pod -> spoke-pod-IP DATA path (Redis dials over the routed pod network):\n")
	fmt.Printf("       REQUIRES an in-cluster probe (a pod on the hub dialing a spoke pod IP); NOT verified here.\n")
	fmt.Printf("       This is a DIFFERENT network path from the site-API checks above (design §8).\n")

	fmt.Printf("\nSummary:\n")
	if problems > 0 {
		fmt.Printf("  [FAIL] %d preflight check(s) failed — see above.\n", problems)
		return fmt.Errorf("multi-site preflight found %d problem(s) for %s/%s", problems, ns, lr.Name)
	}
	fmt.Printf("  [OK] Secrets well-formed and every site's Kubernetes API reachable over the workstation path.\n")
	fmt.Printf("       (The operator's repl-net paths — site-API and pod-IP — still require in-cluster verification.)\n")
	return nil
}

// preflightSecret checks a site's kubeconfig Secret on the hub and returns the
// embedded server endpoint (the operator's repl-net path) and whether it is
// well-formed. A hub-local site (no kubeconfigSecretRef) needs no Secret.
func preflightSecret(
	ctx context.Context, hubClient client.Client, site littleredv1alpha1.SiteSpec,
) (endpoint string, ok bool) {
	if site.KubeconfigSecretRef == nil {
		fmt.Printf("  [OK] hub-local site (no kubeconfigSecretRef; its workloads live on the hub)\n")
		return "", true
	}
	sn := site.KubeconfigSecretRef.Name
	secret := &corev1.Secret{}
	if err := hubClient.Get(ctx, client.ObjectKey{Namespace: preflightOperatorNS, Name: sn}, secret); err != nil {
		fmt.Printf("  [FAIL] kubeconfig Secret %s/%s: %v\n", preflightOperatorNS, sn, err)
		return "", false
	}
	data, keyOK := secret.Data["kubeconfig"]
	if !keyOK || len(data) == 0 {
		fmt.Printf("  [FAIL] Secret %s/%s has no non-empty \"kubeconfig\" key\n", preflightOperatorNS, sn)
		return "", false
	}
	cfg, err := clientcmd.Load(data)
	if err != nil {
		fmt.Printf("  [FAIL] Secret %s/%s: kubeconfig does not parse: %v\n", preflightOperatorNS, sn, err)
		return "", false
	}
	for _, c := range cfg.Clusters {
		if c.Server != "" {
			endpoint = c.Server
			break
		}
	}
	if endpoint == "" {
		fmt.Printf("  [FAIL] Secret %s/%s: kubeconfig names no server endpoint\n", preflightOperatorNS, sn)
		return "", false
	}
	fmt.Printf("  [OK] kubeconfig Secret %s/%s exists and is well-formed\n", preflightOperatorNS, sn)
	return endpoint, true
}

// preflightWorkstationAPI dials the site's Kubernetes API over the user's own
// kube-context (the workstation path lrctl uses) with a lightweight list.
func preflightWorkstationAPI(
	ctx context.Context, siteName, ns string, resolution multisite.SiteContextResolution,
) bool {
	ctxName, resolved := resolution.Resolved[siteName]
	if !resolved {
		reason := "no kube-context resolved"
		for _, u := range resolution.Unresolved {
			if u.Site == siteName {
				reason = u.Reason
			}
		}
		fmt.Printf("  [FAIL] workstation path: %s\n", reason)
		return false
	}
	_, core, _, _, err := k8s.NewClientForContext(kubeconfig, ctxName)
	if err != nil {
		fmt.Printf("  [FAIL] workstation path (context %q): %v\n", ctxName, err)
		return false
	}
	// A lightweight namespace Get proves the API is reachable and the credentials work.
	if _, err := core.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{}); err != nil {
		fmt.Printf("  [FAIL] workstation path (context %q): site API unreachable or namespace %q missing: %v\n",
			ctxName, ns, err)
		return false
	}
	fmt.Printf("  [OK] workstation path: site Kubernetes API reachable via context %q\n", ctxName)
	return true
}

func init() {
	multisiteCmd.AddCommand(multisitePreflightCmd)
	multisitePreflightCmd.Flags().StringVar(&preflightOperatorNS, "operator-namespace", "littlered-system",
		"Operator namespace on the hub holding the site kubeconfig Secrets")
}
