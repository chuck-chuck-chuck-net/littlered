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

import "testing"

func TestMultiSiteReady(t *testing.T) {
	tests := []struct {
		name             string
		sites            []siteReadiness
		wantReady        bool
		wantAnyReachable bool
	}{
		{
			name: "all reachable and ready -> ready",
			sites: []siteReadiness{
				{Name: "a", Reachable: true, Ready: 6, Total: 6},
				{Name: "b", Reachable: true, Ready: 6, Total: 6},
			},
			wantReady:        true,
			wantAnyReachable: true,
		},
		{
			name: "one site unreachable, rest ready -> act on the rest",
			sites: []siteReadiness{
				{Name: "a", Reachable: true, Ready: 6, Total: 6},
				{Name: "b", Reachable: false, Ready: 0, Total: 6},
			},
			wantReady:        true,
			wantAnyReachable: true,
		},
		{
			name: "a reachable site not fully ready -> not ready",
			sites: []siteReadiness{
				{Name: "a", Reachable: true, Ready: 4, Total: 6},
				{Name: "b", Reachable: true, Ready: 6, Total: 6},
			},
			wantReady:        false,
			wantAnyReachable: true,
		},
		{
			name: "all unreachable -> not ready, nothing to do",
			sites: []siteReadiness{
				{Name: "a", Reachable: false, Ready: 0, Total: 6},
				{Name: "b", Reachable: false, Ready: 0, Total: 6},
			},
			wantReady:        false,
			wantAnyReachable: false,
		},
		{
			name: "unreachable site's readiness is irrelevant",
			sites: []siteReadiness{
				{Name: "a", Reachable: true, Ready: 6, Total: 6},
				{Name: "b", Reachable: false, Ready: 6, Total: 6},
			},
			wantReady:        true,
			wantAnyReachable: true,
		},
		{
			name: "single reachable ready site (parity) -> ready",
			sites: []siteReadiness{
				{Name: "a", Reachable: true, Ready: 3, Total: 3},
			},
			wantReady:        true,
			wantAnyReachable: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready, any := multiSiteReady(tc.sites)
			if ready != tc.wantReady || any != tc.wantAnyReachable {
				t.Fatalf("multiSiteReady() = (%v,%v), want (%v,%v)", ready, any, tc.wantReady, tc.wantAnyReachable)
			}
		})
	}
}
