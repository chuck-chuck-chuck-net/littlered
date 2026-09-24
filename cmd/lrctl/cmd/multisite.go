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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	"github.com/chuck-chuck-chuck-net/littlered/internal/cli/k8s"
	"github.com/chuck-chuck-chuck-net/littlered/internal/cli/multisite"
	"github.com/chuck-chuck-chuck-net/littlered/internal/cli/types"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// multisiteCmd groups the multi-site helper verbs (preflight, setup-site). The
// site-aware behaviour of the READ verbs (status/verify/inspect/debug-dump) is
// auto-detected from spec.multiSite, not gated behind this group.
var multisiteCmd = &cobra.Command{
	Use:   "multisite",
	Short: "Multi-site cluster helpers (connectivity preflight, site-Secret setup)",
	Long: `Helpers for multi-site (stretched) cluster instances managed hub-and-spokes by one
operator (docs/MULTI_SITE_DESIGN.md). The read verbs (status/verify/inspect/debug-dump)
already auto-detect spec.multiSite; this group adds a connectivity preflight and a
site-kubeconfig-Secret generator.`,
}

// isMultiSite reports whether the CR is a multi-site cluster instance.
func isMultiSite(lr *littleredv1alpha1.LittleRed) bool {
	return lr.Spec.MultiSite != nil
}

// getLittleRed fetches a CR by key (hub context).
func getLittleRed(
	ctx context.Context, k8sClient client.Client, key client.ObjectKey,
) (*littleredv1alpha1.LittleRed, error) {
	lr := &littleredv1alpha1.LittleRed{}
	if err := k8sClient.Get(ctx, key, lr); err != nil {
		return nil, fmt.Errorf("failed to get LittleRed %s/%s: %w", key.Namespace, key.Name, err)
	}
	return lr, nil
}

// siteGather holds one site's workstation-routable clients and observed cluster
// pods for the shared exec-based gather.
type siteGather struct {
	name       string
	context    string
	coreClient *kubernetes.Clientset
	config     *rest.Config
	cCtx       *types.ClusterContext
	pods       []multisite.PodRef
	reachable  bool
	err        error // context resolution or connectivity failure
}

// resolveMultiSiteGathers builds a per-site gather for every site of a multi-site
// instance, through the user's own kube-contexts (NOT the operator's site
// Secrets, whose replication-net endpoint is hub-pod-only — design §8). Site
// names are resolved to contexts by --site-context and/or the name==context
// convention. An unresolved or unreachable site is returned with err set and
// reachable=false; it contributes no pods but never aborts the others (design
// §3.2: an unreachable site is unmanaged, not broken).
func resolveMultiSiteGathers(
	ctx context.Context, lr *littleredv1alpha1.LittleRed,
) ([]siteGather, multisite.SiteContextResolution, error) {
	siteNames := make([]string, len(lr.Spec.MultiSite.Sites))
	for i, s := range lr.Spec.MultiSite.Sites {
		siteNames[i] = s.Name
	}

	explicit, err := multisite.ParseSiteContextFlag(siteContext)
	if err != nil {
		return nil, multisite.SiteContextResolution{}, err
	}
	available, err := k8s.AvailableContexts(kubeconfig)
	if err != nil {
		return nil, multisite.SiteContextResolution{}, err
	}
	resolution := multisite.ResolveSiteContexts(siteNames, explicit, available)

	unresolvedReason := make(map[string]string, len(resolution.Unresolved))
	for _, u := range resolution.Unresolved {
		unresolvedReason[u.Site] = u.Reason
	}

	gathers := make([]siteGather, 0, len(siteNames))
	for _, name := range siteNames {
		sg := siteGather{name: name}
		ctxName, ok := resolution.Resolved[name]
		if !ok {
			sg.err = fmt.Errorf("%s", unresolvedReason[name])
			gathers = append(gathers, sg)
			continue
		}
		sg.context = ctxName

		_, coreClient, config, _, err := k8s.NewClientForContext(kubeconfig, ctxName)
		if err != nil {
			sg.err = fmt.Errorf("building client for context %q: %w", ctxName, err)
			gathers = append(gathers, sg)
			continue
		}
		sg.coreClient = coreClient
		sg.config = config

		pods, err := listSiteClusterPods(ctx, coreClient, lr.Namespace, lr.Name)
		if err != nil {
			sg.err = fmt.Errorf("listing pods via context %q: %w", ctxName, err)
			gathers = append(gathers, sg)
			continue
		}
		sg.reachable = true
		sg.cCtx = &types.ClusterContext{
			Name:           lr.Name,
			Namespace:      lr.Namespace,
			Mode:           modeCluster,
			RedisPods:      pods,
			RedisContainer: containerRedis,
		}
		for _, p := range pods {
			sg.pods = append(sg.pods, multisite.PodRef{ObjName: p.Name, IP: p.Status.PodIP})
		}
		gathers = append(gathers, sg)
	}
	return gathers, resolution, nil
}

// listSiteClusterPods lists this instance's cluster pods in one site via the
// core clientset (exec path needs the clientset, not the controller-runtime
// client). Selects the shared cluster component label set.
func listSiteClusterPods(
	ctx context.Context, coreClient *kubernetes.Clientset, namespace, name string,
) ([]corev1.Pod, error) {
	selector := labels.SelectorFromSet(labels.Set{
		labelAppInstance:              name,
		"app.kubernetes.io/component": modeCluster,
	}).String()
	list, err := coreClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// multiSiteGatherer implements redisclient.Gatherer by routing each probe to the
// owning site's exec-based cliGatherer, keyed by the site prefix of the
// site-qualified ground-truth key. It is the CLI analogue of the operator's
// single union gather (operatorGatherer over unionClusterPods); the difference
// is transport — the operator dials pod IPs directly, the CLI must exec into the
// owning site's Kubernetes API, so it fans by site.
type multiSiteGatherer struct {
	bySite map[string]*cliGatherer
}

func newMultiSiteGatherer(gathers []siteGather) *multiSiteGatherer {
	g := &multiSiteGatherer{bySite: make(map[string]*cliGatherer)}
	for i := range gathers {
		sg := &gathers[i]
		if !sg.reachable {
			continue
		}
		g.bySite[sg.name] = &cliGatherer{coreClient: sg.coreClient, config: sg.config, cCtx: sg.cCtx}
	}
	return g
}

// route resolves a site-qualified key to its site's gatherer and the bare object
// name to exec against.
func (g *multiSiteGatherer) route(key string) (*cliGatherer, string, error) {
	site, objName := multisite.SplitSiteKey(key)
	cg, ok := g.bySite[site]
	if !ok {
		return nil, "", fmt.Errorf("site %q not reachable", site)
	}
	return cg, objName, nil
}

func (g *multiSiteGatherer) GetRedisState(ctx context.Context, key, ip string) (*redisclient.RedisNodeState, error) {
	cg, obj, err := g.route(key)
	if err != nil {
		return nil, err
	}
	rs, err := cg.GetRedisState(ctx, obj, ip)
	if rs != nil {
		rs.PodName = key // keep the site-qualified key in the ground truth
	}
	return rs, err
}

func (g *multiSiteGatherer) GetSentinelState(
	ctx context.Context, key, ip, masterName string,
) (*redisclient.SentinelNodeState, error) {
	cg, obj, err := g.route(key)
	if err != nil {
		return nil, err
	}
	return cg.GetSentinelState(ctx, obj, ip, masterName)
}

func (g *multiSiteGatherer) GetClusterID(ctx context.Context, key, ip string) (string, error) {
	cg, obj, err := g.route(key)
	if err != nil {
		return "", err
	}
	return cg.GetClusterID(ctx, obj, ip)
}

func (g *multiSiteGatherer) GetClusterInfo(ctx context.Context, key, ip string) (*redisclient.ClusterInfo, error) {
	cg, obj, err := g.route(key)
	if err != nil {
		return nil, err
	}
	return cg.GetClusterInfo(ctx, obj, ip)
}

func (g *multiSiteGatherer) GetClusterNodes(
	ctx context.Context, key, ip string,
) ([]redisclient.ClusterNodeInfo, error) {
	cg, obj, err := g.route(key)
	if err != nil {
		return nil, err
	}
	return cg.GetClusterNodes(ctx, obj, ip)
}

// unionSitePods projects the resolved gathers onto the pure UnionClusterPods
// input.
func unionSitePods(gathers []siteGather) map[string]string {
	sites := make([]multisite.SitePods, 0, len(gathers))
	for _, sg := range gathers {
		if !sg.reachable {
			continue
		}
		sites = append(sites, multisite.SitePods{Site: sg.name, Pods: sg.pods})
	}
	return multisite.UnionClusterPods(sites)
}

func init() {
	rootCmd.AddCommand(multisiteCmd)
}
