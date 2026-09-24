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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

const (
	// siteKubeconfigSecretKey is the Secret data key holding a site's kubeconfig.
	// The Secret lives in the operator namespace; its credentials are bound to a
	// namespaced Role in the target site (workload CRUD + pod read in the
	// instance namespace).
	siteKubeconfigSecretKey = "kubeconfig"

	// siteAPICallTimeout bounds every Kubernetes API call to a remote site.
	// An unreachable site is unmanaged, not broken (design §3.2): it must fail
	// fast and be reported unreachable, never stall the reconcile that has to
	// keep managing the other sites (the LR-017 lesson, at the K8s-API layer).
	siteAPICallTimeout = 5 * time.Second

	// siteCacheSyncTimeout bounds the background informer registration and the
	// initial cache sync of a remote site. Nothing in the reconcile path waits
	// on it; it only bounds the background bring-up goroutine.
	siteCacheSyncTimeout = 30 * time.Second

	// multiSiteFinalizerName is the finalizer driving spoke-side garbage
	// collection: owner references do not work across Kubernetes clusters, so
	// on CR deletion the operator sweeps the instance-labeled workload objects
	// through every site's SiteClient before releasing the CR. The documented
	// escape hatch for a permanently unreachable site is removing this
	// finalizer manually.
	multiSiteFinalizerName = "redis.chuck-chuck-chuck.net/multisite-teardown"
)

// SiteClient is the one narrow per-site seam of multi-site cluster mode
// (docs/MULTI_SITE_DESIGN.md §3.1/§8): every access to a site — workload CRUD,
// pod list/watch, Redis dial — goes through it, so the reconciliation logic
// never knows whether a site is the hub's own Kubernetes cluster or a remote
// one (and a future tiered agent could implement it without touching any
// reconcile logic).
//
// The pod-watch duty is not a method: a remote implementation bridges its pod
// informer events to the reconciler's GenericEvent channel at construction
// time; the hub-local implementation relies on the manager's existing watches.
type SiteClient interface {
	// Name returns the site name from spec.multiSite.sites.
	Name() string
	// IsHubLocal reports whether the site is the hub's own Kubernetes cluster.
	IsHubLocal() bool

	// Apply server-side-applies a workload object in the site, stamping the
	// instance's standard label set. On the hub-local site it also sets the
	// controller owner reference; spoke-side objects get none (owner references
	// do not work across Kubernetes clusters — the finalizer-driven teardown
	// sweep is their garbage collection).
	Apply(ctx context.Context, owner *littleredv1alpha1.LittleRed, obj client.Object) error
	// Get reads an object from the site.
	Get(ctx context.Context, key client.ObjectKey, obj client.Object) error
	// UncachedReader reads directly from this site's API server, bypassing the
	// informer cache. It exists for exactly one caller: LR-043's CLUSTER MEET
	// address confirmation, whose entire point is that the cache is where the
	// hazard originates (LR-012 documented it returning a stale Status.PodIP
	// during churn). Routing that check through the ordinary cached Get above
	// would leave a guard that reads as present and is not — LR-041/LR-052's
	// class. Costs one `get` on pods, which the site Role already grants, so no
	// new RBAC. Never use it for the steady loop: LR-043 declined an uncached
	// gather on per-pass cost and that reasoning is unchanged.
	UncachedReader() client.Reader
	// List lists objects in the site.
	List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
	// Delete deletes an object in the site.
	Delete(ctx context.Context, obj client.Object) error

	// Gatherer returns the Redis dial seam for this site's pods. With the
	// pod-routed transport the hub dials pod IPs directly, so all sites share
	// the direct-TCP gatherer; a future transport (nodeport/multus) would
	// return a site-specific dialer here.
	Gatherer(password string, tlsEnabled bool) redisclient.Gatherer

	// Stop releases the site's watch/cache resources (no-op for hub-local).
	Stop()
}

// kubeconfigFromSecret extracts the kubeconfig bytes from a site kubeconfig
// Secret (data key "kubeconfig").
func kubeconfigFromSecret(secret *corev1.Secret) ([]byte, error) {
	data := secret.Data[siteKubeconfigSecretKey]
	if len(data) == 0 {
		return nil, fmt.Errorf("secret %s/%s does not hold a non-empty %q key",
			secret.Namespace, secret.Name, siteKubeconfigSecretKey)
	}
	return data, nil
}

// stampInstanceLabels ensures obj carries the instance's standard label set
// (commonLabels) without overriding anything the builder already set. Spoke-side
// objects carry no owner references, so these labels are what the multi-site
// teardown sweep (and any cross-site listing) selects on.
func stampInstanceLabels(owner *littleredv1alpha1.LittleRed, obj client.Object) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	for k, v := range commonLabels(owner) {
		if _, ok := labels[k]; !ok {
			labels[k] = v
		}
	}
	obj.SetLabels(labels)
}

// ssaApply server-side-applies obj through c, mirroring LittleRedReconciler.apply
// minus the owner reference (the caller decides ownership — hub-local yes,
// spoke no).
func ssaApply(ctx context.Context, c client.Client, scheme *runtime.Scheme, obj client.Object) error {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		return err
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: raw}
	return c.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(fieldManager), client.ForceOwnership)
}

// hubSiteClient is the degenerate SiteClient: the site is the hub's own
// Kubernetes cluster, so it wraps the manager's client/cache — zero new
// connections, and children keep the ordinary controller owner reference.
type hubSiteClient struct {
	name string
	// client is the manager's cache-backed client; uncached is its APIReader —
	// the same reader single-site's confirmPodIP uses, so a hub-local site is
	// byte-identical to single-site on the MEET path.
	client   client.Client
	uncached client.Reader
	scheme   *runtime.Scheme
}

func (h *hubSiteClient) Name() string     { return h.name }
func (h *hubSiteClient) IsHubLocal() bool { return true }
func (h *hubSiteClient) Stop()            {}

func (h *hubSiteClient) Apply(ctx context.Context, owner *littleredv1alpha1.LittleRed, obj client.Object) error {
	stampInstanceLabels(owner, obj)
	if err := controllerutil.SetControllerReference(owner, obj, h.scheme); err != nil {
		return err
	}
	return ssaApply(ctx, h.client, h.scheme, obj)
}

func (h *hubSiteClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	return h.client.Get(ctx, key, obj)
}

func (h *hubSiteClient) UncachedReader() client.Reader {
	if h.uncached != nil {
		return h.uncached
	}
	// Unit/envtest reconcilers built without a manager: their client is itself a
	// direct, uncached client, so this is not a silent downgrade (LR-043's own
	// argument for apiReader()'s fallback).
	return h.client
}

func (h *hubSiteClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return h.client.List(ctx, list, opts...)
}

func (h *hubSiteClient) Delete(ctx context.Context, obj client.Object) error {
	return h.client.Delete(ctx, obj)
}

func (h *hubSiteClient) Gatherer(password string, tlsEnabled bool) redisclient.Gatherer {
	return &operatorGatherer{password: password, tlsEnabled: tlsEnabled}
}

// remoteSiteClient is the SiteClient for a spoke: its own client + informer
// cache built from a kubeconfig Secret, scoped to the instance namespace. Every
// call is deadline-bounded so an unreachable site can never block a reconcile;
// its pod informer bridges events onto the reconciler's GenericEvent channel so
// spoke pod churn triggers reconciles of the owning CR, like local watches do.
type remoteSiteClient struct {
	name      string
	namespace string
	owner     types.NamespacedName
	client    client.Client
	uncached  client.Reader
	cache     cache.Cache
	scheme    *runtime.Scheme
	cancel    context.CancelFunc
}

// newRemoteSiteClient constructs a SiteClient for a remote site from kubeconfig
// bytes. Construction never dials the site: the cache is started (and the pod
// informer registered) in a background goroutine with its own bounded bring-up,
// so an unreachable site surfaces as bounded per-call errors, not as a stuck
// constructor.
func newRemoteSiteClient(siteName string, kubeconfig []byte, namespace string, owner types.NamespacedName, events chan<- event.GenericEvent) (SiteClient, error) {
	// EVERYTHING in this function is hub-local: it reads the Secret's bytes and
	// builds objects, and no call leaves the process — controller-runtime's REST
	// mapper is lazy, so discovery happens on first use, not here. So every failure
	// below is a CONFIGURATION fault, never an unreachable site
	// (site_config_fault.go). The first site-side moment is c.run() starting the
	// cache, which is deliberately fire-and-forget.
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, siteConfigFault(siteName, siteFaultSecretMalformed,
			fmt.Errorf("parsing kubeconfig: %w", err))
	}
	// Bound every direct HTTP call to the site's API server.
	cfg.Timeout = siteAPICallTimeout

	// A separate reason from siteFaultSecretMalformed: the kubeconfig DID parse, so
	// telling the reader it is unparseable would send them looking for the wrong
	// thing. What fails here is using it — unusable TLS material (a CA bundle that
	// is valid base64 but not PEM) or a server that is not a parseable URL.
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, siteConfigFault(siteName, siteFaultClientUnbuildable,
			fmt.Errorf("building scheme: %w", err))
	}

	ca, err := cache.New(cfg, cache.Options{
		Scheme:            scheme,
		DefaultNamespaces: map[string]cache.Config{namespace: {}},
	})
	if err != nil {
		return nil, siteConfigFault(siteName, siteFaultClientUnbuildable,
			fmt.Errorf("building cache: %w", err))
	}

	cl, err := client.New(cfg, client.Options{
		Scheme: scheme,
		Cache:  &client.CacheOptions{Reader: ca},
	})
	if err != nil {
		return nil, siteConfigFault(siteName, siteFaultClientUnbuildable,
			fmt.Errorf("building client: %w", err))
	}

	// A second, DIRECT client for the MEET confirmation only (see UncachedReader).
	// It registers no informer and holds no objects, so it adds no watch and no RAM;
	// it needs only `get`, which the per-site Role already grants. Hub-local, like
	// everything else in this constructor — client.New does not dial (the REST mapper
	// is lazy), so a failure here is still siteFaultClientUnbuildable (LR-036).
	uncached, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, siteConfigFault(siteName, siteFaultClientUnbuildable,
			fmt.Errorf("building uncached client: %w", err))
	}

	bctx, cancel := context.WithCancel(context.Background())
	c := &remoteSiteClient{
		name:      siteName,
		namespace: namespace,
		owner:     owner,
		client:    cl,
		uncached:  uncached,
		cache:     ca,
		scheme:    scheme,
		cancel:    cancel,
	}
	go c.run(bctx, events)
	return c, nil
}

// run starts the site cache and bridges its pod events to the reconciler.
// It is deliberately fire-and-forget: reads through the client are correct
// (bounded, erroring while unsynced) whether or not this bring-up succeeded,
// and a site that won't sync is reported unreachable, never waited on forever.
func (c *remoteSiteClient) run(ctx context.Context, events chan<- event.GenericEvent) {
	log := logf.Log.WithName("site-client").WithValues("site", c.name, "instance", c.owner.String())

	go func() {
		if err := c.cache.Start(ctx); err != nil {
			log.Error(err, "site cache stopped with error")
		}
	}()

	// Register the pod informer without blocking on sync: an unreachable site
	// must never block anything, and the informer keeps retrying on its own
	// once registered.
	regCtx, regCancel := context.WithTimeout(ctx, siteCacheSyncTimeout)
	defer regCancel()
	informer, err := c.cache.GetInformer(regCtx, &corev1.Pod{}, cache.BlockUntilSynced(false))
	if err != nil {
		log.Error(err, "failed to register site pod informer; spoke pod events will not trigger reconciles until the site client is rebuilt")
		return
	}

	if events == nil {
		// Reconciler built without SetupWithManager (unit/envtest): the
		// monitor channel does not exist, reconcile-cadence detection still
		// works (parity with ensureSentinelMonitor).
		return
	}

	notify := func(obj any) {
		pod := podFromEvent(obj)
		if pod == nil || pod.Labels[labelAppInstance] != c.owner.Name || pod.Labels[labelAppName] != appName {
			return
		}
		select {
		case events <- event.GenericEvent{
			Object: &littleredv1alpha1.LittleRed{
				ObjectMeta: metav1.ObjectMeta{
					Name:      c.owner.Name,
					Namespace: c.owner.Namespace,
				},
			},
		}:
		case <-ctx.Done():
		}
	}
	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    notify,
		UpdateFunc: func(_, newObj any) { notify(newObj) },
		DeleteFunc: notify,
	}); err != nil {
		log.Error(err, "failed to add site pod event handler")
		return
	}

	// Visibility only — nothing waits on this.
	if !c.cache.WaitForCacheSync(regCtx) {
		log.Info("site cache did not sync within bound; treating the site as unreachable until it syncs")
	}
}

// podFromEvent unwraps an informer event object (including the DeleteFunc
// tombstone case) into a Pod, or nil.
func podFromEvent(obj any) *corev1.Pod {
	if pod, ok := obj.(*corev1.Pod); ok {
		return pod
	}
	if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		if pod, ok := tombstone.Obj.(*corev1.Pod); ok {
			return pod
		}
	}
	return nil
}

func (c *remoteSiteClient) Name() string     { return c.name }
func (c *remoteSiteClient) IsHubLocal() bool { return false }

func (c *remoteSiteClient) Stop() {
	c.cancel()
}

// bounded derives a per-call deadline so a dead or blackholing site API fails
// fast (cache reads wait for informer sync up to the ctx deadline; direct calls
// are additionally bounded by the rest.Config timeout).
func bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, siteAPICallTimeout)
}

func (c *remoteSiteClient) Apply(ctx context.Context, owner *littleredv1alpha1.LittleRed, obj client.Object) error {
	// No SetControllerReference: a spoke-side object referencing the
	// hub-resident CR UID would dangle and the spoke's GC controller would
	// delete it out from under us. The stamped instance labels are the
	// teardown sweep's handle instead.
	stampInstanceLabels(owner, obj)
	ctx, cancel := bounded(ctx)
	defer cancel()
	return ssaApply(ctx, c.client, c.scheme, obj)
}

func (c *remoteSiteClient) UncachedReader() client.Reader { return c.uncached }

func (c *remoteSiteClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	ctx, cancel := bounded(ctx)
	defer cancel()
	return c.client.Get(ctx, key, obj)
}

func (c *remoteSiteClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	ctx, cancel := bounded(ctx)
	defer cancel()
	return c.client.List(ctx, list, opts...)
}

func (c *remoteSiteClient) Delete(ctx context.Context, obj client.Object) error {
	ctx, cancel := bounded(ctx)
	defer cancel()
	return c.client.Delete(ctx, obj)
}

func (c *remoteSiteClient) Gatherer(password string, tlsEnabled bool) redisclient.Gatherer {
	// pod-routed transport: the hub dials pod IPs directly, same as local.
	return &operatorGatherer{password: password, tlsEnabled: tlsEnabled}
}
