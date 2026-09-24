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
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	"github.com/chuck-chuck-chuck-net/littlered/internal/multisitegather"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

const (
	// gatherOneShotBudget bounds how long the one-shot gather waits for its
	// freshly-built per-site informer caches to sync enough to observe the
	// expected pods. A fresh process must give the site caches a moment; this is
	// the only place freshness-at-startup matters (the running operator's caches
	// are long-synced). Comfortably exceeds siteCacheSyncTimeout headroom while
	// staying inside lrctl's Job wait.
	gatherOneShotBudget = 25 * time.Second
	// gatherOneShotPoll is the re-observe cadence while waiting for caches.
	gatherOneShotPoll = 1 * time.Second
)

// RunOneShotGather is the operator binary's `--gather <ns>/<name>` mode — tier 2
// of the lrctl multi-site model. It constructs the SAME per-site SiteClients the
// running operator uses (from the CR's kubeconfig Secrets, read in the operator
// namespace), runs the EXISTING multi-site gather once, and writes the
// site-attributed ground truth as JSON to out. It runs on the hub, so it reads
// the Secrets and dials every site exactly as the reconcile loop does — lrctl
// then relays this over the Kubernetes API control path (the Job's pod logs),
// needing no direct site access of its own. Nothing is persisted; the process
// exits after emitting.
func RunOneShotGather(ctx context.Context, cfg *rest.Config, scheme *runtime.Scheme, target string, out io.Writer) error {
	ns, name, ok := strings.Cut(target, "/")
	if !ok || ns == "" || name == "" {
		return fmt.Errorf("invalid --gather target %q: want <namespace>/<name>", target)
	}

	hubClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("building hub client: %w", err)
	}

	// hubClient is already uncached (no manager, no cache), so it doubles as the
	// site-kubeconfig Secret reader — the one-shot gather has no watch scope to
	// read outside of, and building a second client would be pure waste.
	reg := newSiteClientRegistry(hubClient, hubClient, scheme, nil)
	r := &LittleRedReconciler{Client: hubClient, Scheme: scheme, sites: reg}

	lr := &littleredv1alpha1.LittleRed{}
	if err := hubClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, lr); err != nil {
		return fmt.Errorf("getting LittleRed %s: %w", target, err)
	}
	if !isMultiSite(lr) {
		return fmt.Errorf("LittleRed %s is not a multi-site instance (spec.multiSite is unset)", target)
	}

	// Poll the gather until the per-site caches have synced enough to observe the
	// expected pods, or the budget expires (then emit best-effort — mirroring the
	// operator's "an unreachable site contributes nothing" tolerance).
	expected := multiSiteTotalMembers(lr)
	deadline := time.Now().Add(gatherOneShotBudget)
	var gt *redisclient.ClusterGroundTruth
	var obs []siteObservation
	for {
		gt, obs = r.gatherMultiSiteWithObs(ctx, lr)
		if observedReachablePods(obs) >= expected || time.Now().After(deadline) {
			break
		}
		time.Sleep(gatherOneShotPoll)
	}

	result := multisitegather.Result{
		Instance:    name,
		Namespace:   ns,
		GatheredAt:  time.Now().UTC(),
		Sites:       siteObservationsToWire(obs),
		GroundTruth: gt,
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// observedReachablePods counts the pods observed across all reachable sites this
// pass — the readiness signal the one-shot poll waits on.
func observedReachablePods(obs []siteObservation) int {
	n := 0
	for _, o := range obs {
		if o.reachable {
			n += len(o.pods)
		}
	}
	return n
}

// siteObservationsToWire projects the operator's per-site observations onto the
// JSON wire summary.
func siteObservationsToWire(obs []siteObservation) []multisitegather.SiteObservation {
	out := make([]multisitegather.SiteObservation, 0, len(obs))
	for _, o := range obs {
		so := multisitegather.SiteObservation{
			Name:      o.site.name,
			Reachable: o.reachable,
			Pods:      len(o.pods),
		}
		if !o.reachable && o.site.err != nil {
			so.Error = o.site.err.Error()
		}
		out = append(out, so)
	}
	return out
}
