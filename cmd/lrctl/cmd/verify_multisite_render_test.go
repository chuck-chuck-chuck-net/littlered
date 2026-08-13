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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// multiSiteStatusLR builds a LittleRed whose published multi-site status has the given
// per-shard master sites and per-site master counts — enough for the tier-1 verify render.
func multiSiteStatusLR(
	sites, shards int, masterCounts map[string]int32, shardMasterSites []string,
) *littleredv1alpha1.LittleRed {
	lr := &littleredv1alpha1.LittleRed{}
	lr.Name = "ms"
	lr.Namespace = "default"
	lr.Spec.Mode = "cluster"
	now := metav1.Now()
	ms := &littleredv1alpha1.MultiSiteStatus{
		ClusterState:      "ok",
		SlotsAssigned:     16384,
		CrossSiteOK:       true,
		LastReconcileTime: &now,
	}
	// The Sites list length is the resilience denominator (numSites) for the tier-1 render.
	for i := range sites {
		ms.Sites = append(ms.Sites, littleredv1alpha1.SiteStatus{Name: shardMasterSites[i], Reachable: true})
	}
	for i := range shards {
		mSite := ""
		if i < len(shardMasterSites) {
			mSite = shardMasterSites[i]
		}
		ms.Shards = append(ms.Shards, littleredv1alpha1.ShardSiteView{
			Shard: i, MasterSite: mSite, ReplicaSites: []string{"other"}, CrossSite: true,
		})
	}
	for s, c := range masterCounts {
		ms.MasterCounts = append(ms.MasterCounts, littleredv1alpha1.SiteMasterCount{Site: s, Masters: c})
	}
	lr.Status.MultiSite = ms
	return lr
}

// TestVerifyMultiSiteFromStatus_masterDistribution asserts the tier-1 verify render surfaces
// the ADR-012 D2 master-per-site distribution with BOTH thresholds, and distinguishes the two
// kinds of drift: a site over the native target (a recovery downgrade — its loss recovers by
// operator takeover instead of a Redis election) from a site over the takeover floor (an
// actionable availability exposure — its loss cannot be fenced at all).
//
// LR-031: K=4 over 3 sites at 2+1+1 sits AT the floor, so it renders as recoverable and no
// reduced-resilience WARN fires. The pre-correction render flagged it [IMBALANCED] + [WARN].
func TestVerifyMultiSiteFromStatus_masterDistribution(t *testing.T) {
	// 3 sites, 4 shards; s1 holds 2 — over the native target 1, at the floor 2.
	lr := multiSiteStatusLR(3, 4,
		map[string]int32{"s1": 2, "s2": 1, "s3": 1},
		[]string{"s1", "s1", "s2", "s3"})

	out := captureStdout(t, func() { _ = verifyMultiSiteFromStatus(lr) })

	for _, want := range []string{
		"Master Distribution (4 shards; native target 1, takeover floor 2 master(s)/site",
		"site s1: 2 master(s) [OVER TARGET]",
		"site s2: 1 master(s) [OK]",
		"[OK] recoverable, but not natively",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("verify output missing %q\n---\n%s", want, out)
		}
	}
	if strings.Contains(out, "[WARN] reduced site-loss resilience") {
		t.Errorf("K=4 over 3 sites must NOT warn — the floor is reachable\n---\n%s", out)
	}
}

// TestVerifyMultiSiteFromStatus_overFloor asserts a site past the takeover FLOOR renders as the
// actionable exposure it is: that site's loss leaves the lost side holding a master majority,
// so the takeover cannot be fenced and recovery needs a human or allowUnsafeSiteTakeover.
func TestVerifyMultiSiteFromStatus_overFloor(t *testing.T) {
	// 2 sites, 4 shards drifted to 3+1: floor is 2, so s1 is over it.
	lr := multiSiteStatusLR(2, 4,
		map[string]int32{"s1": 3, "s2": 1},
		[]string{"s1", "s1", "s1", "s2"})

	out := captureStdout(t, func() { _ = verifyMultiSiteFromStatus(lr) })

	for _, want := range []string{
		"site s1: 3 master(s) [OVER FLOOR]",
		"[WARN] master drift past the takeover floor",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("verify output missing %q\n---\n%s", want, out)
		}
	}
	// The topology itself is fine (2+2 is reachable) — the drift is live, not structural.
	if strings.Contains(out, "[WARN] reduced site-loss resilience") {
		t.Errorf("K=4 over 2 sites is structurally resilient; drift must not read as structural\n---\n%s", out)
	}
}

// TestVerifyMultiSiteFromStatus_balancedResilient asserts the balanced+resilient render: no
// [IMBALANCED], no reduced-resilience WARN, and the [OK] balanced line.
func TestVerifyMultiSiteFromStatus_balancedResilient(t *testing.T) {
	// 3 sites, 3 shards, one master each: target 1, balanced, resilient (3*1 >= 3).
	lr := multiSiteStatusLR(3, 3,
		map[string]int32{"s1": 1, "s2": 1, "s3": 1},
		[]string{"s1", "s2", "s3"})

	out := captureStdout(t, func() { _ = verifyMultiSiteFromStatus(lr) })

	if strings.Contains(out, "[IMBALANCED]") {
		t.Errorf("balanced topology must not show [IMBALANCED]\n---\n%s", out)
	}
	if strings.Contains(out, "reduced site-loss resilience") {
		t.Errorf("resilient topology must not show the reduced-resilience WARN\n---\n%s", out)
	}
	if !strings.Contains(out, "[OK] balanced") {
		t.Errorf("expected the [OK] balanced line\n---\n%s", out)
	}
}
