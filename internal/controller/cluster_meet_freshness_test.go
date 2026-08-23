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
	"net/http"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// TestConfirmPodIP is the LR-043 primary guard: before CLUSTER MEET, confirm against the
// API server (uncached) that the address we are about to introduce is STILL the IP of the
// pod we think it is. Kubernetes holds at most one live pod per IP, so a confirmed IP
// makes attribution-by-inference unnecessary — and a recycled IP is by definition no
// longer our pod's IP.
func TestConfirmPodIP(t *testing.T) {
	const (
		ipMeetTarget  = "10.0.0.202"
		podMeetTarget = "lr-shard-1-0"
	)

	pod := func(name, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Status:     corev1.PodStatus{PodIP: ip},
		}
	}
	// A pod under deletion. The fake client requires a finalizer for the object to
	// persist with a deletionTimestamp rather than being removed outright.
	terminating := func(name, ip string) *corev1.Pod {
		p := pod(name, ip)
		p.Finalizers = []string{"littlered.test/hold"}
		now := metav1.Now()
		p.DeletionTimestamp = &now
		return p
	}

	tests := []struct {
		name    string
		objects []*corev1.Pod
		podName string
		ip      string
		wantOK  bool
		wantWhy string
	}{
		{
			name:    "pod still holds the address",
			objects: []*corev1.Pod{pod(podMeetTarget, ipMeetTarget)},
			podName: podMeetTarget, ip: ipMeetTarget,
			wantOK: true,
		},
		{
			// The recycled-IP case: our pod came back on a new address, so the cached IP
			// we were about to MEET now belongs to somebody else.
			name:    "pod moved to a different address",
			objects: []*corev1.Pod{pod(podMeetTarget, "10.0.0.203")},
			podName: podMeetTarget, ip: ipMeetTarget,
			wantOK: false, wantWhy: podIPChanged,
		},
		{
			name:    "pod object is gone",
			objects: nil,
			podName: podMeetTarget, ip: ipMeetTarget,
			wantOK: false, wantWhy: podIPGone,
		},
		{
			// The one residual LR-043 left open, now closed. The kubelet writes
			// Status.PodIP, so a TERMINATING pod's object can still report an address the
			// CNI has already released and handed to somebody else — the only window in
			// which "our pod object claims this IP" is not the same as "this IP is ours".
			// Closing it here is what earns the demotion of attribution to a warning:
			// Kubernetes is allowed to be the sole authority on ownership only if its
			// answer is about a pod that still holds its address.
			name:    "pod is terminating (its address may already be handed on)",
			objects: []*corev1.Pod{terminating(podMeetTarget, ipMeetTarget)},
			podName: podMeetTarget, ip: ipMeetTarget,
			wantOK: false, wantWhy: podIPTerminating,
		},
		{
			name:    "pod exists but has no address yet",
			objects: []*corev1.Pod{pod(podMeetTarget, "")},
			podName: podMeetTarget, ip: ipMeetTarget,
			wantOK: false, wantWhy: podIPChanged,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.NewClientBuilder()
			for _, o := range tc.objects {
				b = b.WithObjects(o)
			}
			r := &LittleRedReconciler{APIReader: b.Build()}

			ok, why := r.confirmPodIP(context.Background(), "ns", tc.podName, tc.ip)
			if ok != tc.wantOK {
				t.Errorf("confirmPodIP ok = %v (%q), want %v", ok, why, tc.wantOK)
			}
			if !tc.wantOK && why != tc.wantWhy {
				t.Errorf("confirmPodIP reason = %q, want %q", why, tc.wantWhy)
			}
		})
	}
}

// TestSetupWithManagerDefaultsAPIReader guards the enforcement, not the wiring: a
// production reconciler must not be able to run with a nil APIReader, because the MEET
// guard would then silently degrade to the cached read — the very defect (LR-043), with
// every other test still green. SetupWithManager defaults it because that is the one place
// the manager is in hand; if a future refactor removes that default, this goes red.
//
// The manager is built against an unreachable config with a stub RESTMapper, so the test
// needs no control plane: nothing here connects (the cache is lazy until Start).
func TestSetupWithManagerDefaultsAPIReader(t *testing.T) {
	sch := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sch); err != nil {
		t.Fatalf("AddToScheme(client-go): %v", err)
	}
	if err := littleredv1alpha1.AddToScheme(sch); err != nil {
		t.Fatalf("AddToScheme(littlered): %v", err)
	}
	mgr, err := ctrl.NewManager(&rest.Config{Host: "127.0.0.1:1"}, ctrl.Options{
		Scheme: sch,
		MapperProvider: func(*rest.Config, *http.Client) (meta.RESTMapper, error) {
			return meta.NewDefaultRESTMapper(nil), nil
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	r := &LittleRedReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
	if r.APIReader == nil {
		t.Fatal("APIReader is nil after SetupWithManager: the CLUSTER MEET guard would fall back to the cached read")
	}
	if r.apiReader() != r.APIReader {
		t.Error("apiReader() did not return the defaulted uncached reader")
	}
}
