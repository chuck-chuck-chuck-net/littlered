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

package v1alpha1

import (
	"strings"
	"testing"
)

// multiSiteLR builds a cluster-mode LittleRed with the given multiSite spec.
func multiSiteLR(mode string, ms *MultiSiteSpec) *LittleRed {
	lr := &LittleRed{}
	lr.Spec.Mode = mode
	lr.Spec.MultiSite = ms
	return lr
}

func remoteSite(name, secret string) SiteSpec {
	return SiteSpec{Name: name, KubeconfigSecretRef: &KubeconfigSecretRef{Name: secret}}
}

func hubLocalSite(name string) SiteSpec {
	return SiteSpec{Name: name}
}

func TestValidateMultiSite(t *testing.T) {
	tests := []struct {
		name    string
		lr      *LittleRed
		wantErr string // substring; empty = valid
	}{
		{
			name:    "no multiSite is always valid",
			lr:      multiSiteLR("standalone", nil),
			wantErr: "",
		},
		{
			name: "valid dual-site (one hub-local, one remote)",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites: []SiteSpec{hubLocalSite("alpha"), remoteSite("beta", "site-beta-kubeconfig")},
			}),
			wantErr: "",
		},
		{
			name: "valid tri-site all remote with sitesPerShard 2",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites: []SiteSpec{
					remoteSite("alpha", "a"), remoteSite("beta", "b"), remoteSite("gamma", "c"),
				},
				SitesPerShard: 2,
			}),
			wantErr: "",
		},
		{
			name: "multiSite requires cluster mode",
			lr: multiSiteLR("sentinel", &MultiSiteSpec{
				Sites: []SiteSpec{remoteSite("alpha", "a"), remoteSite("beta", "b")},
			}),
			wantErr: "cluster mode",
		},
		{
			name: "fewer than 2 sites",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites: []SiteSpec{remoteSite("alpha", "a")},
			}),
			wantErr: "at least 2 sites",
		},
		{
			name: "duplicate site names",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites: []SiteSpec{remoteSite("alpha", "a"), remoteSite("alpha", "b")},
			}),
			wantErr: "duplicate site name",
		},
		{
			name: "empty site name",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites: []SiteSpec{remoteSite("", "a"), remoteSite("beta", "b")},
			}),
			wantErr: "name must not be empty",
		},
		{
			name: "empty kubeconfigSecretRef name",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites: []SiteSpec{remoteSite("alpha", ""), remoteSite("beta", "b")},
			}),
			wantErr: "kubeconfigSecretRef.name",
		},
		{
			name: "two hub-local sites are meaningless",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites: []SiteSpec{hubLocalSite("alpha"), hubLocalSite("beta")},
			}),
			wantErr: "at most one site may be hub-local",
		},
		{
			name: "sitesPerShard below 2",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites:         []SiteSpec{remoteSite("alpha", "a"), remoteSite("beta", "b")},
				SitesPerShard: 1,
			}),
			wantErr: "sitesPerShard",
		},
		{
			name: "sitesPerShard above site count",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites:         []SiteSpec{remoteSite("alpha", "a"), remoteSite("beta", "b")},
				SitesPerShard: 3,
			}),
			wantErr: "sitesPerShard",
		},
		{
			name: "future network mode nodeport is not accepted",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites:   []SiteSpec{remoteSite("alpha", "a"), remoteSite("beta", "b")},
				Network: MultiSiteNetworkSpec{Mode: "nodeport"},
			}),
			wantErr: "network.mode",
		},
		{
			name: "explicit pod-routed network mode is accepted",
			lr: multiSiteLR("cluster", &MultiSiteSpec{
				Sites:   []SiteSpec{remoteSite("alpha", "a"), remoteSite("beta", "b")},
				Network: MultiSiteNetworkSpec{Mode: MultiSiteNetworkModePodRouted},
			}),
			wantErr: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Cluster-mode CRs need the cluster block to survive the shards check.
			if tc.lr.Spec.Mode == "cluster" {
				tc.lr.Spec.Cluster = &ClusterSpec{Shards: 3}
			}
			err := tc.lr.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestMultiSiteSetDefaults(t *testing.T) {
	t.Run("sitesPerShard defaults to the number of sites (dynamic)", func(t *testing.T) {
		lr := multiSiteLR("cluster", &MultiSiteSpec{
			Sites: []SiteSpec{
				remoteSite("alpha", "a"), remoteSite("beta", "b"), remoteSite("gamma", "c"),
			},
		})
		lr.SetDefaults()
		if got := lr.Spec.MultiSite.SitesPerShard; got != 3 {
			t.Fatalf("SitesPerShard = %d, want 3 (number of sites)", got)
		}
	})

	t.Run("explicit sitesPerShard is preserved", func(t *testing.T) {
		lr := multiSiteLR("cluster", &MultiSiteSpec{
			Sites: []SiteSpec{
				remoteSite("alpha", "a"), remoteSite("beta", "b"), remoteSite("gamma", "c"),
			},
			SitesPerShard: 2,
		})
		lr.SetDefaults()
		if got := lr.Spec.MultiSite.SitesPerShard; got != 2 {
			t.Fatalf("SitesPerShard = %d, want 2 (explicit)", got)
		}
	})

	t.Run("network mode defaults to pod-routed", func(t *testing.T) {
		lr := multiSiteLR("cluster", &MultiSiteSpec{
			Sites: []SiteSpec{remoteSite("alpha", "a"), remoteSite("beta", "b")},
		})
		lr.SetDefaults()
		if got := lr.Spec.MultiSite.Network.Mode; got != MultiSiteNetworkModePodRouted {
			t.Fatalf("Network.Mode = %q, want %q", got, MultiSiteNetworkModePodRouted)
		}
	})

	t.Run("absent multiSite stays absent", func(t *testing.T) {
		lr := multiSiteLR("cluster", nil)
		lr.SetDefaults()
		if lr.Spec.MultiSite != nil {
			t.Fatalf("MultiSite = %+v, want nil (never auto-created)", lr.Spec.MultiSite)
		}
	})
}
