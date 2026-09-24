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
	"reflect"
	"testing"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
)

// TestRepairMayForceTakeover asserts the generic repair loop may force a takeover ONLY in
// single-site mode; in multi-site the fence-gated recoverSiteLoss is the sole authority
// (Finding #1 hardening).
func TestRepairMayForceTakeover(t *testing.T) {
	single := &littleredv1alpha1.LittleRed{
		Spec: littleredv1alpha1.LittleRedSpec{
			Mode:    ComponentCluster,
			Cluster: &littleredv1alpha1.ClusterSpec{Shards: 3},
		},
	}
	multi := &littleredv1alpha1.LittleRed{
		Spec: littleredv1alpha1.LittleRedSpec{
			Mode:    ComponentCluster,
			Cluster: &littleredv1alpha1.ClusterSpec{Shards: 4},
			MultiSite: &littleredv1alpha1.MultiSiteSpec{
				SitesPerShard: 2,
				Sites: []littleredv1alpha1.SiteSpec{
					{Name: siteCtxS1}, {Name: siteCtxS2},
				},
			},
		},
	}
	if got := repairMayForceTakeover(single); !got {
		t.Errorf("single-site: repairMayForceTakeover = %v, want true (repair owns single-site quorum recovery)", got)
	}
	if got := repairMayForceTakeover(multi); got {
		t.Errorf("multi-site: repairMayForceTakeover = %v, want false (recoverSiteLoss is the sole takeover authority)", got)
	}
}

// TestGhostForgetPlan asserts that a gossip-known ghost is forgotten only when every hosting
// site is observable; when a site is unobservable, no ghost is forgotten (an unreachable
// site's node is "unknown, not gone" — pillar 3.4). This guards BOTH repair Step-2 and
// recoverSiteLoss action-2 FORGET.
func TestGhostForgetPlan(t *testing.T) {
	ghosts := []string{"nodeA", "nodeB"}
	tests := []struct {
		name       string
		ghosts     []string
		forgetSafe bool
		want       []string
	}{
		{
			name:       "all sites observable: a no-backing-pod ghost is confirmed gone and IS forgotten (routine cleanup)",
			ghosts:     ghosts,
			forgetSafe: true,
			want:       ghosts,
		},
		{
			name:       "a site unobservable: nodes are unknown-not-gone and are NOT forgotten",
			ghosts:     ghosts,
			forgetSafe: false,
			want:       nil,
		},
		{
			name:       "no ghosts, observable: nothing to forget",
			ghosts:     nil,
			forgetSafe: true,
			want:       nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ghostForgetPlan(tc.ghosts, tc.forgetSafe)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ghostForgetPlan(%v, %v) = %v, want %v", tc.ghosts, tc.forgetSafe, got, tc.want)
			}
		})
	}
}

// TestAllSitesObservable asserts the FORGET-safety signal: true iff every observed site's API
// answered; a single unreachable site makes it false (so ghost FORGET is suppressed).
func TestAllSitesObservable(t *testing.T) {
	tests := []struct {
		name string
		obs  []siteObservation
		want bool
	}{
		{
			name: "all reachable",
			obs:  []siteObservation{{reachable: true}, {reachable: true}},
			want: true,
		},
		{
			name: "one unreachable (site loss / management-path cut)",
			obs:  []siteObservation{{reachable: true}, {reachable: false}},
			want: false,
		},
		{
			name: "empty",
			obs:  nil,
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := allSitesObservable(tc.obs); got != tc.want {
				t.Errorf("allSitesObservable(%+v) = %v, want %v", tc.obs, got, tc.want)
			}
		})
	}
}
