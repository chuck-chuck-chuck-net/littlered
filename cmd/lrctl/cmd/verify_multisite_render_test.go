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
// the ADR-012 D2 master-per-site distribution: the balance target, an [IMBALANCED] marker for
// a site over target, and the structural reduced-resilience [WARN] for a K-over-S topology
// that cannot auto-survive an arbitrary single-site loss (K=4 over 3 sites).
func TestVerifyMultiSiteFromStatus_masterDistribution(t *testing.T) {
	// 3 sites, 4 shards; s1 currently holds 2 masters (imbalanced, target 1). Not resilient.
	lr := multiSiteStatusLR(3, 4,
		map[string]int32{"s1": 2, "s2": 1, "s3": 1},
		[]string{"s1", "s1", "s2", "s3"})

	out := captureStdout(t, func() { _ = verifyMultiSiteFromStatus(lr) })

	for _, want := range []string{
		"Master Distribution (balance target 1 master(s)/site for 4 shards",
		"site s1: 2 master(s) [IMBALANCED]",
		"site s2: 1 master(s) [OK]",
		"[WARN] reduced site-loss resilience",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("verify output missing %q\n---\n%s", want, out)
		}
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
