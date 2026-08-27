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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClusterShardRolloutSettled(t *testing.T) {
	sts := func(gen, observed int64, replicas int32, updateRev, currentRev string, updated, ready, total int32) *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Generation: gen},
			Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
			Status: appsv1.StatefulSetStatus{
				ObservedGeneration: observed,
				UpdateRevision:     updateRev,
				CurrentRevision:    currentRev,
				UpdatedReplicas:    updated,
				ReadyReplicas:      ready,
				Replicas:           total,
			},
		}
	}

	tests := []struct {
		name string
		sts  *appsv1.StatefulSet
		want bool
	}{
		{"fully settled", sts(3, 3, 2, "rev2", "rev2", 2, 2, 2), true},
		{"generation not yet observed (just applied)", sts(4, 3, 2, "rev2", "rev2", 2, 2, 2), false},
		{"mid rollout: update != current revision", sts(3, 3, 2, "rev3", "rev2", 1, 2, 2), false},
		{"not all updated to new revision", sts(3, 3, 2, "rev3", "rev3", 1, 2, 2), false},
		{"a pod not ready", sts(3, 3, 2, "rev2", "rev2", 2, 1, 2), false},
		{"replica count not yet reached", sts(3, 3, 2, "rev2", "rev2", 2, 2, 1), false},
		{"fresh STS, empty status", sts(1, 0, 2, "", "", 0, 0, 0), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := statefulSetRolloutSettled(tt.sts); got != tt.want {
				t.Errorf("statefulSetRolloutSettled(%s) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}

	if statefulSetRolloutSettled(&appsv1.StatefulSet{}) {
		t.Error("STS with nil Replicas must not be settled")
	}
}
