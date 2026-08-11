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
	"fmt"
	"sort"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// newStatusFakeClient builds a fake client.Client that supports Status().Update on the CR.
func newStatusFakeClient(t *testing.T, lr *littleredv1alpha1.LittleRed) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := littleredv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(lr).WithStatusSubresource(lr).Build()
}

func containsKind(applied, kind string) bool { return strings.Contains(applied, "."+kind+"/") }
func endsWith(applied, name string) bool     { return strings.HasSuffix(applied, "/"+name) }
func shardOfName(applied string) int {
	// applied is "<type>/<name>"; name is {instance}-shard-K[-pdb]. Reuse the redis parser
	// where possible; PDBs are {name}-shard-K-pdb which the pod-name regex does not match.
	idx := strings.LastIndex(applied, "/")
	name := applied[idx+1:]
	name = strings.TrimSuffix(name, "-pdb")
	// name is now {instance}-shard-K; extract trailing integer after "-shard-".
	m := strings.LastIndex(name, "-shard-")
	if m < 0 {
		return -1
	}
	var k int
	if _, err := fmt.Sscanf(name[m+len("-shard-"):], "%d", &k); err != nil {
		return -1
	}
	return k
}
func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// recordingSiteClient is a test SiteClient that records applies/deletes and serves a fixed
// set of pods and StatefulSets, or errors on every call when unreachable.
type recordingSiteClient struct {
	name        string
	unreachable bool
	// applyErrOnly models a site whose informer-cache READS still succeed (Get/List from a
	// synced-then-partitioned cache) while the live WRITE path (Apply/Patch) times out — the
	// exact shape of an unreachable site's API in the ensure path (item 8). Distinct from
	// `unreachable`, which errors on every call.
	applyErrOnly bool
	pods         []corev1.Pod
	sts          map[string]*appsv1.StatefulSet
	applied      []string // "<Kind>/<name>"
	deleted      []string // pod names deleted
}

func (f *recordingSiteClient) Name() string                               { return f.name }
func (f *recordingSiteClient) IsHubLocal() bool                           { return false }
func (f *recordingSiteClient) Stop()                                      {}
func (f *recordingSiteClient) Gatherer(string, bool) redisclient.Gatherer { return nil }

func (f *recordingSiteClient) errIfDown() error {
	if f.unreachable {
		return errors.New("site unreachable")
	}
	return nil
}

func (f *recordingSiteClient) Apply(_ context.Context, _ *littleredv1alpha1.LittleRed, obj client.Object) error {
	if err := f.errIfDown(); err != nil {
		return err
	}
	if f.applyErrOnly {
		return errors.New("apply timeout (site API unreachable)")
	}
	f.applied = append(f.applied, fmt.Sprintf("%T/%s", obj, obj.GetName()))
	return nil
}

func (f *recordingSiteClient) Get(_ context.Context, key client.ObjectKey, obj client.Object) error {
	if err := f.errIfDown(); err != nil {
		return err
	}
	if s, ok := obj.(*appsv1.StatefulSet); ok {
		if got, ok := f.sts[key.Name]; ok {
			*s = *got
			return nil
		}
	}
	return apierrorsNotFound(key.Name)
}

func (f *recordingSiteClient) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	if err := f.errIfDown(); err != nil {
		return err
	}
	if pl, ok := list.(*corev1.PodList); ok {
		pl.Items = append([]corev1.Pod(nil), f.pods...)
	}
	if sl, ok := list.(*appsv1.StatefulSetList); ok {
		for _, sts := range f.sts {
			sl.Items = append(sl.Items, *sts)
		}
	}
	return nil
}

func (f *recordingSiteClient) Delete(_ context.Context, obj client.Object) error {
	if err := f.errIfDown(); err != nil {
		return err
	}
	f.deleted = append(f.deleted, obj.GetName())
	return nil
}

// apierrorsNotFound builds a NotFound error without importing the apierrors package name
// collision into the test's readability.
func apierrorsNotFound(name string) error {
	return &notFoundErr{name: name}
}

type notFoundErr struct{ name string }

func (e *notFoundErr) Error() string { return fmt.Sprintf("%s not found", e.name) }
func (e *notFoundErr) Status() metav1.Status {
	return metav1.Status{Reason: metav1.StatusReasonNotFound}
}

// multiSiteClusterCR builds a multi-site cluster CR. The shape params are explicit for
// readability even where the current tests exercise one value each.
//
//nolint:unparam
func multiSiteClusterCR(sites, sitesPerShard, shards, replicasPerShard int) *littleredv1alpha1.LittleRed {
	lr := &littleredv1alpha1.LittleRed{}
	lr.Name = "cache"
	lr.Namespace = testNamespaceDefault
	lr.Spec.Mode = ModeCluster
	rp := replicasPerShard
	lr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{Shards: shards, ReplicasPerShard: &rp}
	ms := &littleredv1alpha1.MultiSiteSpec{SitesPerShard: sitesPerShard}
	for i := range sites {
		ms.Sites = append(ms.Sites, littleredv1alpha1.SiteSpec{
			Name:                fmt.Sprintf("site%d", i),
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: fmt.Sprintf("kc%d", i)},
		})
	}
	lr.Spec.MultiSite = ms
	return lr
}

func resolvedFromFakes(fakes []*recordingSiteClient) []resolvedSite {
	out := make([]resolvedSite, len(fakes))
	for i, f := range fakes {
		out[i] = resolvedSite{idx: i, name: f.name, client: f}
		if f.unreachable {
			out[i].client = f // still set; observeSites/List will error
		}
	}
	return out
}

// TestEnsureMultiSiteClusterResourcesPlacement asserts the round-robin placement: each site
// hosts exactly the shards (K+j)%S, gets the shared ConfigMap+Services, and a PDB per hosted
// shard — applied through that site's own SiteClient.
func TestEnsureMultiSiteClusterResourcesPlacement(t *testing.T) {
	// 3 sites, sitesPerShard 2, 3 shards, replicasPerShard 1.
	lr := multiSiteClusterCR(3, 2, 3, 1)
	fakes := []*recordingSiteClient{{name: "site0"}, {name: "site1"}, {name: "site2"}}
	sites := resolvedFromFakes(fakes)

	r := &LittleRedReconciler{}
	if err := r.ensureMultiSiteClusterResources(context.Background(), lr, sites); err != nil {
		t.Fatalf("ensureMultiSiteClusterResources: %v", err)
	}

	// hosting: shard0->{0,1}, shard1->{1,2}, shard2->{2,0}
	wantShardsBySite := map[string][]int{
		"site0": {0, 2},
		"site1": {0, 1},
		"site2": {1, 2},
	}
	for i, f := range fakes {
		// Each hosting site applies CM + headless Service + client Service + one STS and one
		// PDB per hosted shard.
		var stsShards, pdbShards []int
		var haveCM, haveHeadless, haveClient bool
		for _, a := range f.applied {
			switch {
			case containsKind(a, "ConfigMap"):
				haveCM = true
			case containsKind(a, "StatefulSet"):
				stsShards = append(stsShards, shardOfName(a))
			case containsKind(a, "PodDisruptionBudget"):
				pdbShards = append(pdbShards, shardOfName(a))
			case containsKind(a, "Service"):
				// headless is {name}-cluster, client is {name}
				if endsWith(a, clusterHeadlessServiceName(lr)) {
					haveHeadless = true
				} else if endsWith(a, serviceName(lr)) {
					haveClient = true
				}
			}
		}
		sort.Ints(stsShards)
		sort.Ints(pdbShards)
		want := wantShardsBySite[f.name]
		if !equalInts(stsShards, want) {
			t.Fatalf("site%d STS shards = %v, want %v (applied=%v)", i, stsShards, want, f.applied)
		}
		if !equalInts(pdbShards, want) {
			t.Fatalf("site%d PDB shards = %v, want %v", i, pdbShards, want)
		}
		if !haveCM || !haveHeadless || !haveClient {
			t.Fatalf("site%d missing shared object: cm=%v headless=%v client=%v", i, haveCM, haveHeadless, haveClient)
		}
	}
}

// TestEnsureMultiSiteResourcesSkipsUnreachable asserts one unreachable site does not block
// applying to the reachable rest.
func TestEnsureMultiSiteResourcesSkipsUnreachable(t *testing.T) {
	lr := multiSiteClusterCR(2, 2, 3, 1)
	fakes := []*recordingSiteClient{{name: "site0", unreachable: true}, {name: "site1"}}
	sites := resolvedFromFakes(fakes)

	r := &LittleRedReconciler{}
	if err := r.ensureMultiSiteClusterResources(context.Background(), lr, sites); err != nil {
		t.Fatalf("ensureMultiSiteClusterResources returned error on unreachable site: %v", err)
	}
	if len(fakes[0].applied) != 0 {
		t.Fatalf("unreachable site0 must not record applies, got %v", fakes[0].applied)
	}
	if len(fakes[1].applied) == 0 {
		t.Fatalf("reachable site1 must still be reconciled")
	}
}

// TestReconcileMultiSiteSTSTolerantOfApplyFailure is the item-8 red-first repro: a site whose
// reads (Get/List) succeed from the informer cache but whose live Apply times out must NOT abort
// the whole reconcile. The pre-populated per-site STSs carry no pod-spec-hash annotation, so they
// differ from the freshly built desired STS — forcing the template-update Apply path (the two
// `return err` sites in reconcileMultiSiteClusterStatefulSets). RED against current code: the
// site0 Apply error is returned and the reconcile aborts before the reachable site1 is reconciled.
func TestReconcileMultiSiteSTSTolerantOfApplyFailure(t *testing.T) {
	lr := multiSiteClusterCR(2, 2, 2, 1)
	r := &LittleRedReconciler{}

	// Both sites host shards 0,1 and answer reads; site0's Apply/Patch fails (unreachable API).
	site0 := fakeWithShardSTSs("site0", lr.Name, 0, 1)
	site0.applyErrOnly = true
	site1 := fakeWithShardSTSs("site1", lr.Name, 0, 1)
	sites := []resolvedSite{
		{idx: 0, name: "site0", client: site0},
		{idx: 1, name: "site1", client: site1},
	}
	shardsBySite := map[int][]int{0: {0, 1}, 1: {0, 1}}

	if err := r.reconcileMultiSiteClusterStatefulSets(context.Background(), lr, sites, shardsBySite); err != nil {
		t.Fatalf("reconcileMultiSiteClusterStatefulSets aborted on an unreachable site's Apply: %v", err)
	}
	// The reachable site must still have been reconciled (at least one STS applied).
	if len(site1.applied) == 0 {
		t.Fatalf("reachable site1 must still be reconciled when a sibling site's Apply fails")
	}
}

func TestUnionClusterPodsSkipsUnreachable(t *testing.T) {
	obs := []siteObservation{
		{
			site:      resolvedSite{name: "a"},
			reachable: true,
			pods: []sitePodObservation{
				{objName: "cache-shard-0-0", key: "a/cache-shard-0-0", ip: "10.0.0.1"},
				{objName: "cache-shard-0-1", key: "a/cache-shard-0-1", ip: ""}, // no IP -> skipped
			},
		},
		{
			site:      resolvedSite{name: "b"},
			reachable: false,
			pods:      []sitePodObservation{{objName: "cache-shard-0-0", key: "b/cache-shard-0-0", ip: "10.1.0.1"}},
		},
	}
	got := unionClusterPods(obs)
	if len(got) != 1 || got["10.0.0.1"] != "a/cache-shard-0-0" {
		t.Fatalf("unionClusterPods = %v, want only the reachable site's IP-bearing pod", got)
	}
}

// TestRecoverMultiSiteWipePerSite asserts the wipe recovery recycles stuck pods only in
// reachable sites, deleting each through its owning site's client, past the cooldown.
func TestRecoverMultiSiteWipePerSite(t *testing.T) {
	lr := multiSiteClusterCR(2, 2, 3, 1)
	site0 := &recordingSiteClient{name: "site0"}
	site1 := &recordingSiteClient{name: "site1", unreachable: true}
	// Arm the cooldown well in the past so the plan reaches wipeRecycle.
	past := metav1.NewTime(metav1.Now().Add(-10 * clusterWipeRecoveryCooldown))
	lr.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{WipeDeadlockSince: &past}

	obs := []siteObservation{
		{
			site:      resolvedSite{name: "site0", client: site0},
			reachable: true,
			pods: []sitePodObservation{
				{objName: "cache-shard-0-0", key: "site0/cache-shard-0-0", ready: false, restarted: true},
				{objName: "cache-shard-1-0", key: "site0/cache-shard-1-0", ready: true, restarted: true}, // ready -> never recycled
			},
		},
		{
			// Unreachable: its pods are invisible, never recycled here.
			site:      resolvedSite{name: "site1", client: site1},
			reachable: false,
			pods:      nil,
		},
	}

	r := &LittleRedReconciler{}
	if err := r.recoverMultiSiteClusterWipeDeadlock(context.Background(), lr, obs); err != nil {
		t.Fatalf("recoverMultiSiteClusterWipeDeadlock: %v", err)
	}
	if len(site0.deleted) != 1 || site0.deleted[0] != "cache-shard-0-0" {
		t.Fatalf("site0 deleted = %v, want [cache-shard-0-0]", site0.deleted)
	}
	if len(site1.deleted) != 0 {
		t.Fatalf("unreachable site1 must not delete anything, got %v", site1.deleted)
	}
}

// shardSTSForSite builds an observed shard StatefulSet as it would exist in a site: named
// {instance}-shard-K and carrying the stable LabelShard identity label the guard reads back.
func shardSTSForSite(instance string, shard int) *appsv1.StatefulSet {
	sts := &appsv1.StatefulSet{}
	sts.Name = fmt.Sprintf("%s-shard-%d", instance, shard)
	sts.Namespace = testNamespaceDefault
	sts.Labels = map[string]string{LabelShard: fmt.Sprintf("%d", shard)}
	return sts
}

// fakeWithShardSTSs builds a recording site client pre-populated with the shard StatefulSets it
// hosts (each keyed by object name, as SiteClient.Get/List expect).
func fakeWithShardSTSs(name, instance string, shards ...int) *recordingSiteClient {
	f := &recordingSiteClient{name: name, sts: map[string]*appsv1.StatefulSet{}}
	for _, k := range shards {
		s := shardSTSForSite(instance, k)
		f.sts[s.Name] = s
	}
	return f
}

// TestEnforceMultiSiteScaleDownGuardRefusesAndNeverDeletes is the refusal WIRING test and the
// load-bearing data-safety proof: a multi-site CR whose desired topology no longer includes some
// existing per-site shard StatefulSets must be REFUSED (condition set, phase Failed) and NO
// StatefulSet may be deleted. Models a shards decrease 3->2 against sites still holding shard-2.
func TestEnforceMultiSiteScaleDownGuardRefusesAndNeverDeletes(t *testing.T) {
	// New/desired topology: 2 sites, sitesPerShard 2, shards 2. Old cluster hosted shard 2 as
	// well (in site0 and site1 under the round-robin), so those STSs are now orphaned.
	lr := multiSiteClusterCR(2, 2, 2, 1)
	r := &LittleRedReconciler{}
	r.Client = newStatusFakeClient(t, lr)

	// Both sites host shards 0,1 (desired) AND a lingering shard 2 (orphan from the old shards=3).
	site0 := fakeWithShardSTSs("site0", lr.Name, 0, 1, 2)
	site1 := fakeWithShardSTSs("site1", lr.Name, 0, 1, 2)
	sites := []resolvedSite{
		{idx: 0, name: "site0", client: site0},
		{idx: 1, name: "site1", client: site1},
	}

	refused, _, err := r.enforceMultiSiteScaleDownGuard(context.Background(), lr, sites)
	if err != nil {
		t.Fatalf("enforceMultiSiteScaleDownGuard: %v", err)
	}
	if !refused {
		t.Fatalf("expected refusal for orphaned shard-2 StatefulSets, got refused=false")
	}

	// Refusal surfaced.
	if lr.Status.Status != reasonMultiSiteScaleDownRefused {
		t.Fatalf("Status.Status = %q, want MultiSiteScaleDownRefused", lr.Status.Status)
	}
	if lr.Status.Phase != littleredv1alpha1.PhaseFailed {
		t.Fatalf("Status.Phase = %q, want Failed", lr.Status.Phase)
	}
	var readyCond *metav1.Condition
	for i := range lr.Status.Conditions {
		if lr.Status.Conditions[i].Type == littleredv1alpha1.ConditionReady {
			readyCond = &lr.Status.Conditions[i]
		}
	}
	if readyCond == nil || readyCond.Status != metav1.ConditionFalse || readyCond.Reason != reasonMultiSiteScaleDownRefused {
		t.Fatalf("Ready condition = %+v, want False/MultiSiteScaleDownRefused", readyCond)
	}

	// THE INVARIANT: no StatefulSet was deleted, in either site, and the observed STSs remain.
	for _, f := range []*recordingSiteClient{site0, site1} {
		if len(f.deleted) != 0 {
			t.Fatalf("%s: guard deleted %v — it must NEVER delete data", f.name, f.deleted)
		}
		if _, ok := f.sts[fmt.Sprintf("%s-shard-2", lr.Name)]; !ok {
			t.Fatalf("%s: orphaned shard-2 STS must still exist after refusal", f.name)
		}
	}
}

// TestEnforceMultiSiteScaleDownGuardNoOpWhenSteady asserts the guard does nothing (refused=false,
// no condition) when the observed StatefulSets exactly match the desired placement.
func TestEnforceMultiSiteScaleDownGuardNoOpWhenSteady(t *testing.T) {
	// 2 sites, sitesPerShard 2, shards 2 -> every shard hosts in both sites: site0/site1 each host {0,1}.
	lr := multiSiteClusterCR(2, 2, 2, 1)
	r := &LittleRedReconciler{}
	r.Client = newStatusFakeClient(t, lr)

	site0 := fakeWithShardSTSs("site0", lr.Name, 0, 1)
	site1 := fakeWithShardSTSs("site1", lr.Name, 0, 1)
	sites := []resolvedSite{
		{idx: 0, name: "site0", client: site0},
		{idx: 1, name: "site1", client: site1},
	}

	refused, _, err := r.enforceMultiSiteScaleDownGuard(context.Background(), lr, sites)
	if err != nil {
		t.Fatalf("enforceMultiSiteScaleDownGuard: %v", err)
	}
	if refused {
		t.Fatalf("steady topology must not be refused")
	}
	if lr.Status.Status == reasonMultiSiteScaleDownRefused {
		t.Fatalf("steady topology must not set the refusal status")
	}
}

// TestUpdateMultiSiteClusterStatusSites asserts per-site observed state is populated.
func TestUpdateMultiSiteClusterStatusSites(t *testing.T) {
	lr := multiSiteClusterCR(2, 2, 3, 1)
	obs := []siteObservation{
		{
			site:      resolvedSite{name: "site0"},
			reachable: true,
			pods: []sitePodObservation{
				{key: "site0/cache-shard-0-0", ready: true},
				{key: "site0/cache-shard-0-1", ready: true},
			},
		},
		{site: resolvedSite{name: "site1"}, reachable: false},
	}
	// Pass a non-nil (empty) gt so status update does not dial pod IPs.
	gt := redisclient.NewClusterGroundTruth()

	r := &LittleRedReconciler{}
	// No client wired: Status().Update would fail, but oldStatus==newStatus? It changes, so
	// updateMultiSiteClusterStatus will try Status().Update. Provide a fake client.
	r.Client = newStatusFakeClient(t, lr)

	if _, err := r.updateMultiSiteClusterStatus(context.Background(), lr, gt, obs); err != nil {
		t.Fatalf("updateMultiSiteClusterStatus: %v", err)
	}
	if lr.Status.MultiSite == nil || len(lr.Status.MultiSite.Sites) != 2 {
		t.Fatalf("expected 2 site statuses, got %+v", lr.Status.MultiSite)
	}
	bySite := map[string]littleredv1alpha1.SiteStatus{}
	for _, s := range lr.Status.MultiSite.Sites {
		bySite[s.Name] = s
	}
	if !bySite["site0"].Reachable || bySite["site0"].Pods == nil || bySite["site0"].Pods.Ready != 2 {
		t.Fatalf("site0 status wrong: %+v", bySite["site0"])
	}
	if bySite["site1"].Reachable {
		t.Fatalf("site1 must be reported unreachable")
	}
	// Total expected members = shards(3) * sitesPerShard(2) * (rp(1)+1) = 12.
	if lr.Status.Redis.Total != 12 {
		t.Fatalf("Redis.Total = %d, want 12", lr.Status.Redis.Total)
	}
}
