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

// TestClassifySiteWitness is the red-first table for the ADR-012 condition-3 witness
// classifier — one case per grade, including the load-bearing node-controller downgrade.
func TestClassifySiteWitness(t *testing.T) {
	// Shorthands for the per-pod facts.
	gone := witnessPod{present: false}
	ready := witnessPod{present: true, ready: true}
	downKubeletAttested := witnessPod{present: true, ready: false, nodeKnown: true, nodeReady: true}
	downNodeNotReady := witnessPod{present: true, ready: false, nodeKnown: true, nodeReady: false}
	downNodeUnknown := witnessPod{present: true, ready: false, nodeKnown: false}

	cases := []struct {
		name         string
		apiReachable bool
		pods         []witnessPod
		want         siteWitnessGrade
	}{
		{
			name:         "pod gone, api reachable => ConfirmedDown",
			apiReachable: true,
			pods:         []witnessPod{gone, gone},
			want:         witnessConfirmedDown,
		},
		{
			name:         "not-Ready pod on a Ready node => ConfirmedDown (kubelet-attested)",
			apiReachable: true,
			pods:         []witnessPod{downKubeletAttested, gone},
			want:         witnessConfirmedDown,
		},
		{
			name:         "not-Ready pod on a NotReady node => Unobservable (node-controller downgrade)",
			apiReachable: true,
			pods:         []witnessPod{downNodeNotReady, gone},
			want:         witnessUnobservable,
		},
		{
			name:         "not-Ready pod on an Unknown/unreadable node => Unobservable (downgrade)",
			apiReachable: true,
			pods:         []witnessPod{downNodeUnknown},
			want:         witnessUnobservable,
		},
		{
			name:         "api unreachable => Unobservable",
			apiReachable: false,
			pods:         []witnessPod{downKubeletAttested, gone}, // pod facts irrelevant when the API did not answer
			want:         witnessUnobservable,
		},
		{
			name:         "api reachable, a pod Ready => Contradiction (site alive)",
			apiReachable: true,
			pods:         []witnessPod{ready, gone},
			want:         witnessContradiction,
		},
		{
			name:         "a Ready pod dominates a node-controller-inferred not-Ready pod => Contradiction",
			apiReachable: true,
			pods:         []witnessPod{ready, downNodeNotReady},
			want:         witnessContradiction,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifySiteWitness(tc.apiReachable, tc.pods)
			if got != tc.want {
				t.Fatalf("classifySiteWitness(%v, ...) = %s, want %s", tc.apiReachable, got, tc.want)
			}
		})
	}
}
