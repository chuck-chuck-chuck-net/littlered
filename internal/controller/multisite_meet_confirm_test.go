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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// LR-063. LR-043's primary CLUSTER MEET guard confirms an address against the API
// server, uncached, before introducing it. In multi-site the ground truth is keyed by
// siteQualifiedKey ("<site>/<objName>") because the bare object name names a DIFFERENT
// pod in every hosting site, while confirmPodIP addressed the HUB's API server with that
// whole key as a resource name. The hub has never heard of the pod, the Get can never
// succeed, and the guard denies forever — which on the seed returns the whole pass and so
// also skips repairCluster's TotalSlots==0 bootstrap. Measured on t3e: 129 consecutive
// denials, partitions=8, masters=0, zero bootstrap lines.
//
// The invariant LR-043 rests on is NOT weakened by multi-site: pod-routed is the only
// supported transport (MultiSiteNetworkSpec.Mode is an enum of one), so pod CIDRs are
// mutually routed and pod IPs are globally unique across the hub and every site. What is
// per-cluster is the ORACLE, not the fact — only the owning site's API server can answer
// about its own pods. So the fix routes the question; it does not settle for less evidence.

// siteReaders is a fake of the per-site uncached readers the confirmer routes over.
type siteReaders map[string]client.Reader

func (s siteReaders) readerFor(site string) (client.Reader, bool) {
	r, ok := s[site]
	return r, ok
}

func podAt(name, ns, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Status:     corev1.PodStatus{PodIP: ip},
	}
}

func terminatingPodAt(name, ns, ip string) *corev1.Pod {
	p := podAt(name, ns, ip)
	p.Finalizers = []string{"littlered.test/hold"}
	now := metav1.Now()
	p.DeletionTimestamp = &now
	return p
}

// TestMultiSiteConfirmPodIPRoutesToTheOwningSite is the headline red. The fixture is the
// t3e shape: two sites that BOTH host a pod called "ms-b-shard-3-0", holding different
// addresses, which is exactly why the key is qualified.
func TestMultiSiteConfirmPodIPRoutesToTheOwningSite(t *testing.T) {
	const (
		ns      = "ms-e2e-b"
		objName = "ms-b-shard-3-0"
		ipS1    = "10.233.2.176"
		ipS2    = "10.233.65.21" // the address the field log names
	)

	readers := siteReaders{
		"scm-s1": fake.NewClientBuilder().WithObjects(podAt(objName, ns, ipS1)).Build(),
		"scm-s2": fake.NewClientBuilder().WithObjects(podAt(objName, ns, ipS2)).Build(),
		"scm-s3": fake.NewClientBuilder().Build(), // hosts no pod of this shard
	}

	tests := []struct {
		name    string
		key     string
		ip      string
		wantOK  bool
		wantWhy string
	}{
		{
			// The field case. Pre-fix this is confirm-failed and the cluster never forms.
			name: "the owning site confirms its own pod's address",
			key:  siteQualifiedKey("scm-s2", objName), ip: ipS2,
			wantOK: true,
		},
		{
			// The reason the key is qualified: the same object name in the other site
			// holds a different address, and confirming against the wrong site would
			// answer about the wrong pod.
			name: "a same-named pod in another site does not confirm this address",
			key:  siteQualifiedKey("scm-s1", objName), ip: ipS2,
			wantOK: false, wantWhy: podIPChanged,
		},
		{
			name: "the owning site reports the pod gone",
			key:  siteQualifiedKey("scm-s3", objName), ip: ipS2,
			wantOK: false, wantWhy: podIPGone,
		},
		{
			// Never fall back to the hub for an address we cannot attribute to a site:
			// a bare name would confirm against whichever API server we happen to hold,
			// which is the cross-instance merge LR-043 exists to prevent.
			name: "an unqualified key names no site and is refused",
			key:  objName, ip: ipS2,
			wantOK: false, wantWhy: podIPUnknownSite,
		},
		{
			name: "a key naming a site we could not resolve is refused",
			key:  siteQualifiedKey("scm-s9", objName), ip: ipS2,
			wantOK: false, wantWhy: podIPUnknownSite,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			confirm := multiSiteConfirmPodIP(readers.readerFor)
			ok, why := confirm(context.Background(), ns, tc.key, tc.ip)
			if ok != tc.wantOK {
				t.Errorf("confirm(%q, %q) ok = %v (%q), want %v", tc.key, tc.ip, ok, why, tc.wantOK)
			}
			if !tc.wantOK && why != tc.wantWhy {
				t.Errorf("confirm(%q, %q) reason = %q, want %q", tc.key, tc.ip, why, tc.wantWhy)
			}
		})
	}
}

// TestMultiSiteConfirmPodIPRefusesATerminatingPod pins that the multi-site confirmer
// applies LR-043's regression-section clause too: the kubelet writes Status.PodIP, so a
// terminating pod's object can keep naming an address the CNI has handed on. The two
// confirmers share one predicate precisely so this cannot drift (LR-045).
func TestMultiSiteConfirmPodIPRefusesATerminatingPod(t *testing.T) {
	const (
		ns      = "ms-e2e-b"
		objName = "ms-b-shard-0-0"
		ip      = "10.233.65.206"
	)
	readers := siteReaders{
		"scm-s2": fake.NewClientBuilder().WithObjects(terminatingPodAt(objName, ns, ip)).Build(),
	}

	confirm := multiSiteConfirmPodIP(readers.readerFor)
	ok, why := confirm(context.Background(), ns, siteQualifiedKey("scm-s2", objName), ip)
	if ok || why != podIPTerminating {
		t.Errorf("confirm = %v (%q), want false (%q)", ok, why, podIPTerminating)
	}
}

// TestSingleSiteConfirmPodIPIsUnchanged is the parity control: the single-site confirmer
// must keep addressing the hub with a bare object name, so the fix cannot be earned by
// changing what single-site does. Its own table lives in TestConfirmPodIP; this pins that
// the shared predicate did not move it.
func TestSingleSiteConfirmPodIPIsUnchanged(t *testing.T) {
	const (
		ns      = "default"
		objName = "lr-shard-1-0"
		ip      = "10.0.0.202"
	)
	r := &LittleRedReconciler{
		APIReader: fake.NewClientBuilder().WithObjects(podAt(objName, ns, ip)).Build(),
	}

	if ok, why := r.confirmPodIP(context.Background(), ns, objName, ip); !ok {
		t.Errorf("single-site confirmPodIP = false (%q), want true", why)
	}
	// A site-qualified key must NOT resolve on the single-site path — there is no site to
	// route to and the hub holds no such object.
	if ok, _ := r.confirmPodIP(context.Background(), ns, siteQualifiedKey("scm-s2", objName), ip); ok {
		t.Error("single-site confirmPodIP accepted a site-qualified key, want refusal")
	}
}

// TestSiteReaderLookupMapsEveryResolvedSite guards the wiring between the resolved sites
// and the confirmer. An empty or nil-valued map makes EVERY key unattributable, which
// reproduces LR-063's stall exactly — the guard would look present and refuse everything,
// which is how the defect presented in the first place.
func TestSiteReaderLookupMapsEveryResolvedSite(t *testing.T) {
	reader := fake.NewClientBuilder().Build()
	sites := []resolvedSite{
		{idx: 0, name: siteAlpha, client: &readerSiteClient{name: siteAlpha, reader: reader}},
		{idx: 1, name: siteBeta, client: &readerSiteClient{name: siteBeta, reader: reader}},
		{idx: 2, name: "gamma", client: nil}, // did not resolve this pass
	}

	lookup := siteReaderLookup(sites)

	for _, want := range []string{siteAlpha, siteBeta} {
		if rd, ok := lookup(want); !ok || rd == nil {
			t.Errorf("siteReaderLookup(%q) = (%v, %v), want a reader", want, rd, ok)
		}
	}
	// A site that did not resolve must be ABSENT, so the confirmer refuses rather than
	// reaching for another site's API server.
	if rd, ok := lookup("gamma"); ok || rd != nil {
		t.Errorf("siteReaderLookup(unresolved) = (%v, %v), want absent", rd, ok)
	}
	if rd, ok := lookup("nosuchsite"); ok || rd != nil {
		t.Errorf("siteReaderLookup(unknown) = (%v, %v), want absent", rd, ok)
	}
}

// TestRemoteSiteClientUncachedReaderIsNotTheCachedClient pins the one property that makes
// the multi-site guard real rather than decorative: the reader it routes over must not be
// the cache-backed client. LR-012 documented that cache returning a stale Status.PodIP
// during churn, and that staleness IS the hazard LR-043 exists to close — so a confirmer
// reading through it is LR-041/LR-052's class, a guard that reads as present and is not.
// The structural check is the only one available without a live API server, and it has
// teeth against the tempting "just return c.client" simplification.
func TestRemoteSiteClientUncachedReaderIsNotTheCachedClient(t *testing.T) {
	cached := fake.NewClientBuilder().Build()
	direct := fake.NewClientBuilder().Build()
	c := &remoteSiteClient{name: "alpha", client: cached, uncached: direct}

	got := c.UncachedReader()
	if got == nil {
		t.Fatal("remoteSiteClient.UncachedReader() = nil, want the direct reader")
	}
	if got == client.Reader(cached) {
		t.Error("remoteSiteClient.UncachedReader() returned the CACHE-BACKED client; " +
			"the MEET confirmation must not read through the cache (LR-012/LR-043)")
	}
	if got != client.Reader(direct) {
		t.Error("remoteSiteClient.UncachedReader() did not return the direct reader it was built with")
	}
}

// readerSiteClient is a SiteClient that only answers UncachedReader — the single method
// the MEET confirmer uses.
type readerSiteClient struct {
	name   string
	reader client.Reader
}

func (s *readerSiteClient) Name() string                                { return s.name }
func (s *readerSiteClient) IsHubLocal() bool                            { return false }
func (s *readerSiteClient) Stop()                                       {}
func (s *readerSiteClient) UncachedReader() client.Reader               { return s.reader }
func (s *readerSiteClient) Delete(context.Context, client.Object) error { return nil }
func (s *readerSiteClient) Get(context.Context, client.ObjectKey, client.Object) error {
	return nil
}
func (s *readerSiteClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return nil
}
func (s *readerSiteClient) Apply(context.Context, *littleredv1alpha1.LittleRed, client.Object) error {
	return nil
}
func (s *readerSiteClient) Gatherer(string, bool) redisclient.Gatherer { return nil }
