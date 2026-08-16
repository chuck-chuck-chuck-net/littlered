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
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/event"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// "Site unreachable" and "I cannot read my own configuration" are different
// states: the first may heal itself (pillar 3.2 treats it as normal), the second
// never will without a human. resolveSites used to report both as unreachable,
// which makes the alert an operator actually wants impossible to write. These
// tests pin the classification, its per-site status surface, the warning
// condition, and the bootstrap refusal.

// TestSiteConfigFaultClassification: every HUB-LOCAL failure of site resolution —
// the operator could not read or parse its own inputs — must classify as a config
// fault carrying its reason; anything that touches the site must not.
func TestSiteConfigFaultClassification(t *testing.T) {
	lr := multiSiteCR()

	t.Run("operator namespace unresolvable", func(t *testing.T) {
		reg, _ := newTestRegistry(t)
		reg.operatorNamespace = func() (string, error) { return "", errors.New("no downward API") }
		_, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
			Name:                siteBeta,
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: "kc"},
		})
		assertSiteConfigFault(t, err, siteFaultOperatorNamespace)
	})

	t.Run("kubeconfig secret missing", func(t *testing.T) {
		reg, _ := newTestRegistry(t)
		_, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
			Name:                siteBeta,
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: "nope"},
		})
		assertSiteConfigFault(t, err, siteFaultSecretUnreadable)
	})

	t.Run("kubeconfig secret without the kubeconfig key", func(t *testing.T) {
		secret := secretWithoutKubeconfigKey()
		reg, _ := newTestRegistry(t, secret)
		_, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
			Name:                siteBeta,
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: secretWithoutKubeconfigKey().Name},
		})
		assertSiteConfigFault(t, err, siteFaultSecretMalformed)
	})

	// The kubeconfig BYTES are present but are not a kubeconfig. The parse is
	// hub-local — it never touches the site — so it belongs on the config side of
	// the line even though it happens inside newRemoteSiteClient.
	t.Run("kubeconfig unparseable", func(t *testing.T) {
		_, err := newRemoteSiteClient(siteBeta, []byte("}{ not a kubeconfig"), testNamespaceDefault,
			types.NamespacedName{Namespace: testNamespaceDefault, Name: "ms"}, nil)
		assertSiteConfigFault(t, err, siteFaultSecretMalformed)
	})

	// Anything past the kubeconfig is site-side: it must NOT be laundered into a
	// config fault, or an ordinary unreachable site would demand human action.
	t.Run("site-side failure is not a config fault", func(t *testing.T) {
		secret := kubeconfigSecret("kc", []byte("kubeconfig-v1"))
		reg, _ := newTestRegistry(t, secret)
		reg.newRemote = func(string, []byte, string, types.NamespacedName, chan<- event.GenericEvent) (SiteClient, error) {
			return nil, errors.New("dial tcp 10.0.0.1:6443: i/o timeout")
		}
		_, err := reg.siteClient(context.Background(), lr, littleredv1alpha1.SiteSpec{
			Name:                siteBeta,
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: "kc"},
		})
		if err == nil {
			t.Fatal("expected an error")
		}
		if sce, ok := siteConfigFaultOf(err); ok {
			t.Fatalf("site-side failure misclassified as config fault (reason %q): %v", sce.reason, err)
		}
	})
}

func assertSiteConfigFault(t *testing.T, err error, wantReason string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error classifying as a config fault with reason %q", wantReason)
	}
	sce, ok := siteConfigFaultOf(err)
	if !ok {
		t.Fatalf("error is not a config fault (a human must act, so it must not read as "+
			"'site unreachable'): %v", err)
	}
	if sce.reason != wantReason {
		t.Fatalf("config fault reason = %q, want %q (err: %v)", sce.reason, wantReason, err)
	}
	if sce.site == "" {
		t.Errorf("config fault must name the site, got %q", sce.site)
	}
	if !errors.Is(err, err) || errors.Unwrap(sce) == nil {
		t.Errorf("config fault must wrap the cause so operators see it: %v", err)
	}
}

// TestSummarizeSiteConfigFaults: the summary must let a reader tell a FULLY
// unmanaged instance (every remote site faulted) from a partially unmanaged one,
// and must never claim "all sites" when a hub-local site is still managed — a
// hub-local site has no kubeconfig Secret and cannot config-fault at all.
func TestSummarizeSiteConfigFaults(t *testing.T) {
	remoteOK := func(n string) siteConfigObservation {
		return siteConfigObservation{name: n, remote: true}
	}
	remoteBad := func(n, reason string) siteConfigObservation {
		return siteConfigObservation{name: n, remote: true, fault: true, reason: reason, message: reason + " detail"}
	}
	hubLocal := func(n string) siteConfigObservation {
		return siteConfigObservation{name: n, remote: false}
	}

	tests := []struct {
		name             string
		in               []siteConfigObservation
		wantFaulted      []string
		wantRemoteTotal  int
		wantHubLocal     int
		wantAllRemoteBad bool
	}{
		{
			name:            "no faults",
			in:              []siteConfigObservation{remoteOK(siteName0), remoteOK(siteName1)},
			wantFaulted:     nil,
			wantRemoteTotal: 2,
		},
		{
			name:            "one of three faulted (partially unmanaged)",
			in:              []siteConfigObservation{remoteOK(siteName0), remoteBad(siteName1, siteFaultSecretUnreadable), remoteOK(siteName2)},
			wantFaulted:     []string{siteName1},
			wantRemoteTotal: 3,
		},
		{
			name: "every remote site faulted (fully unmanaged)",
			in: []siteConfigObservation{
				remoteBad(siteName0, siteFaultOperatorNamespace),
				remoteBad(siteName1, siteFaultOperatorNamespace),
			},
			wantFaulted:      []string{siteName0, siteName1},
			wantRemoteTotal:  2,
			wantAllRemoteBad: true,
		},
		{
			// The asymmetry at site_registry.go: a hub-local site never touches
			// operator-namespace detection, so it keeps working when every remote
			// site is broken. Claiming "all sites" here would be a lie.
			name: "every remote site faulted but a hub-local site survives",
			in: []siteConfigObservation{
				hubLocal("hub"),
				remoteBad(siteName1, siteFaultOperatorNamespace),
			},
			wantFaulted:      []string{siteName1},
			wantRemoteTotal:  1,
			wantHubLocal:     1,
			wantAllRemoteBad: true,
		},
		{
			name:            "no remote sites at all",
			in:              []siteConfigObservation{hubLocal("hub")},
			wantFaulted:     nil,
			wantRemoteTotal: 0,
			wantHubLocal:    1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := summarizeSiteConfigFaults(tc.in)
			if strings.Join(got.Faulted, ",") != strings.Join(tc.wantFaulted, ",") {
				t.Errorf("Faulted = %v, want %v", got.Faulted, tc.wantFaulted)
			}
			if got.RemoteTotal != tc.wantRemoteTotal {
				t.Errorf("RemoteTotal = %d, want %d", got.RemoteTotal, tc.wantRemoteTotal)
			}
			if got.HubLocalTotal != tc.wantHubLocal {
				t.Errorf("HubLocalTotal = %d, want %d", got.HubLocalTotal, tc.wantHubLocal)
			}
			if got.AllRemoteFaulted != tc.wantAllRemoteBad {
				t.Errorf("AllRemoteFaulted = %v, want %v", got.AllRemoteFaulted, tc.wantAllRemoteBad)
			}
			if got.Any() != (len(tc.wantFaulted) > 0) {
				t.Errorf("Any() = %v, want %v", got.Any(), len(tc.wantFaulted) > 0)
			}
			if len(got.Details) != len(tc.wantFaulted) {
				t.Errorf("Details = %v, want one entry per faulted site (%d)", got.Details, len(tc.wantFaulted))
			}
			// The message must state the fully-vs-partially-unmanaged distinction and
			// never claim more sites than are actually faulted.
			if got.Any() {
				msg := describeSiteConfigFaults(got)
				if msg == "" {
					t.Fatal("describeSiteConfigFaults returned an empty message for a faulted summary")
				}
				for _, n := range tc.wantFaulted {
					if !strings.Contains(msg, n) {
						t.Errorf("message must name faulted site %q: %s", n, msg)
					}
				}
				if tc.wantHubLocal > 0 && !strings.Contains(msg, "hub-local") {
					t.Errorf("message must not imply the hub-local site is unmanaged: %s", msg)
				}
			}
		})
	}
}

// TestSiteConfigCondition mirrors the ReducedSiteLossResilience precedent: a
// warning condition on a still-Running instance, with a ONE-SHOT event on the
// transition into the state, cleared to False when the configuration is fixed.
func TestSiteConfigCondition(t *testing.T) {
	countEvents := func(evs []string) int {
		n := 0
		for _, e := range evs {
			if strings.Contains(e, reasonMultiSiteConfigInvalid) {
				n++
			}
		}
		return n
	}
	faulted := []siteConfigObservation{
		{name: siteName0, remote: true},
		{name: siteName1, remote: true, fault: true, reason: siteFaultSecretUnreadable, message: "secret \"kc1\" not found"},
	}
	healthy := []siteConfigObservation{
		{name: siteName0, remote: true},
		{name: siteName1, remote: true},
	}

	t.Run("faulted config warns once and keeps warning without re-eventing", func(t *testing.T) {
		lr := multiSiteClusterCR(2, 2, 4, 1)
		lr.Status.Phase = littleredv1alpha1.PhaseRunning
		rec := events.NewFakeRecorder(16)
		r := &LittleRedReconciler{Recorder: rec}

		r.reconcileSiteConfigCondition(lr, faulted)
		c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionMultiSiteConfigInvalid)
		if c == nil || c.Status != metav1.ConditionTrue || c.Reason != reasonMultiSiteConfigInvalid {
			t.Fatalf("condition = %+v, want True/%s", c, reasonMultiSiteConfigInvalid)
		}
		if !strings.Contains(c.Message, siteName1) {
			t.Errorf("condition message must name the faulted site: %s", c.Message)
		}
		if got := countEvents(drainRecorderEvents(rec)); got != 1 {
			t.Fatalf("warning events = %d, want 1", got)
		}
		// A config fault must NOT take the instance out of Running: the data plane
		// is healthy and phase cannot carry two orthogonal facts.
		if lr.Status.Phase != littleredv1alpha1.PhaseRunning {
			t.Fatalf("phase = %q, want it left at Running", lr.Status.Phase)
		}

		r.reconcileSiteConfigCondition(lr, faulted)
		if got := countEvents(drainRecorderEvents(rec)); got != 0 {
			t.Fatalf("re-emitted warning events = %d, want 0 (one-shot)", got)
		}
	})

	t.Run("fixed config clears the condition", func(t *testing.T) {
		lr := multiSiteClusterCR(2, 2, 4, 1)
		rec := events.NewFakeRecorder(16)
		r := &LittleRedReconciler{Recorder: rec}

		r.reconcileSiteConfigCondition(lr, faulted)
		drainRecorderEvents(rec)
		r.reconcileSiteConfigCondition(lr, healthy)

		c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionMultiSiteConfigInvalid)
		if c == nil || c.Status != metav1.ConditionFalse {
			t.Fatalf("condition = %+v, want False once the configuration is readable", c)
		}
		if got := countEvents(drainRecorderEvents(rec)); got != 0 {
			t.Fatalf("clearing must not emit a warning, got %d", got)
		}
	})
}

// TestSiteConfigBootstrapGuard: at bootstrap a config fault is refused loudly —
// nothing is running to protect and half-building an undesigned topology is the
// worse outcome. Once the instance exists, the same fault must NOT halt
// reconciliation of the healthy sites. And the refusal is a STATE, not a latch.
func TestSiteConfigBootstrapGuard(t *testing.T) {
	faultedSites := func() []resolvedSite {
		return []resolvedSite{
			{idx: 0, name: siteName0, client: &fakeSiteClient{name: siteName0}},
			{idx: 1, name: siteName1, err: siteConfigFault(siteName1, siteFaultSecretUnreadable, errors.New("not found"))},
		}
	}

	t.Run("refuses at bootstrap", func(t *testing.T) {
		lr := multiSiteClusterCR(2, 2, 4, 1)
		rec := events.NewFakeRecorder(16)
		r := &LittleRedReconciler{Recorder: rec}

		refused, res := r.enforceSiteConfigGuard(lr, faultedSites())
		if !refused {
			t.Fatal("a config fault before anything is built must refuse, not half-build a topology")
		}
		if res.RequeueAfter == 0 {
			t.Error("a refusal must requeue so the fix is picked up without an external trigger")
		}
		if lr.Status.Phase != littleredv1alpha1.PhaseFailed {
			t.Errorf("phase = %q, want Failed at bootstrap refusal", lr.Status.Phase)
		}
		c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionMultiSiteConfigInvalid)
		if c == nil || c.Status != metav1.ConditionTrue {
			t.Errorf("condition = %+v, want True", c)
		}
		ready := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonMultiSiteConfigInvalid {
			t.Errorf("Ready = %+v, want False/%s", ready, reasonMultiSiteConfigInvalid)
		}
	})

	t.Run("does not refuse once the instance has been built", func(t *testing.T) {
		lr := multiSiteClusterCR(2, 2, 4, 1)
		lr.Status.Phase = littleredv1alpha1.PhaseRunning
		lr.Status.MultiSite = &littleredv1alpha1.MultiSiteStatus{
			Sites: []littleredv1alpha1.SiteStatus{{Name: siteName0, Reachable: true}, {Name: siteName1}},
		}
		r := &LittleRedReconciler{Recorder: events.NewFakeRecorder(16)}

		refused, _ := r.enforceSiteConfigGuard(lr, faultedSites())
		if refused {
			t.Fatal("one site's unreadable Secret must not halt management of the healthy sites")
		}
		if lr.Status.Phase != littleredv1alpha1.PhaseRunning {
			t.Errorf("phase = %q, want Running left untouched", lr.Status.Phase)
		}
	})

	t.Run("is a state, not a latch", func(t *testing.T) {
		lr := multiSiteClusterCR(2, 2, 4, 1)
		r := &LittleRedReconciler{Recorder: events.NewFakeRecorder(16)}

		refused, _ := r.enforceSiteConfigGuard(lr, faultedSites())
		if !refused {
			t.Fatal("expected the bootstrap refusal")
		}
		// The Secret is fixed. Phase is still Failed from the refusal, so a latch
		// would keep refusing forever (the LR-033 deadlock shape).
		healthy := []resolvedSite{
			{idx: 0, name: siteName0, client: &fakeSiteClient{name: siteName0}},
			{idx: 1, name: siteName1, client: &fakeSiteClient{name: siteName1}},
		}
		refused, _ = r.enforceSiteConfigGuard(lr, healthy)
		if refused {
			t.Fatal("the refusal must clear as soon as the configuration is readable")
		}
	})
}

// TestSiteConfigObservationsFrom pins the projection that feeds the pure seam:
// spec order, remote-vs-hub-local from the kubeconfigSecretRef, and the fault
// classification taken from the resolve error.
func TestSiteConfigObservationsFrom(t *testing.T) {
	lr := multiSiteClusterCR(2, 2, 4, 1)
	lr.Spec.MultiSite.Sites[0].KubeconfigSecretRef = nil // hub-local

	got := siteConfigObservationsFrom(lr, []resolvedSite{
		{idx: 0, name: siteName0, client: &fakeSiteClient{name: siteName0}},
		{idx: 1, name: siteName1, err: siteConfigFault(siteName1, siteFaultSecretMalformed, errors.New("no kubeconfig key"))},
	})

	if len(got) != 2 {
		t.Fatalf("got %d observations, want 2", len(got))
	}
	if got[0].remote {
		t.Error("a site without a kubeconfigSecretRef is hub-local")
	}
	if !got[1].remote || !got[1].fault || got[1].reason != siteFaultSecretMalformed {
		t.Errorf("site1 = %+v, want a remote config fault with reason %s", got[1], siteFaultSecretMalformed)
	}
	if got[1].message == "" {
		t.Error("the observation must carry the cause for the status surface")
	}
}
