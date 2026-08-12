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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

func TestKubeconfigFromSecret(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string][]byte
		want    string
		wantErr bool
	}{
		{
			name: "kubeconfig key present",
			data: map[string][]byte{siteKubeconfigSecretKey: []byte("apiVersion: v1")},
			want: "apiVersion: v1",
		},
		{
			name:    "kubeconfig key missing",
			data:    map[string][]byte{"value": []byte("x")},
			wantErr: true,
		},
		{
			name:    "kubeconfig key empty",
			data:    map[string][]byte{siteKubeconfigSecretKey: {}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
				Data:       tc.data,
			}
			got, err := kubeconfigFromSecret(secret)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("kubeconfigFromSecret() = %q, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("kubeconfigFromSecret() error: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("kubeconfigFromSecret() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStampInstanceLabels(t *testing.T) {
	lr := &littleredv1alpha1.LittleRed{}
	lr.Name = "ms"
	lr.Namespace = testNamespaceDefault
	lr.Spec.Mode = ModeCluster
	lr.SetDefaults()

	t.Run("nil labels get the instance label set", func(t *testing.T) {
		obj := &appsv1.StatefulSet{}
		stampInstanceLabels(lr, obj)
		labels := obj.GetLabels()
		if labels[labelAppInstance] != "ms" {
			t.Fatalf("label %s = %q, want %q (the teardown sweep selects on it)", labelAppInstance, labels[labelAppInstance], "ms")
		}
		if labels[labelAppName] != appName {
			t.Fatalf("label %s = %q, want %q", labelAppName, labels[labelAppName], appName)
		}
	})

	t.Run("builder-set labels are not overridden", func(t *testing.T) {
		obj := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				LabelMode:       "custom",
				"builder-extra": "kept",
			}},
		}
		stampInstanceLabels(lr, obj)
		labels := obj.GetLabels()
		if labels[LabelMode] != "custom" {
			t.Fatalf("builder label overridden: mode = %q", labels[LabelMode])
		}
		if labels["builder-extra"] != "kept" {
			t.Fatal("unrelated label lost")
		}
		if labels[labelAppInstance] != "ms" {
			t.Fatalf("missing instance label not filled in: %v", labels)
		}
	})
}
