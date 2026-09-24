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

// site_cluster_reconcile.go is the site-aware EXECUTOR for multi-site cluster mode
// (docs/MULTI_SITE_DESIGN.md §4/§8). It is a thin I/O layer over the pure placement and
// gating seams (site_placement.go, site_gating.go): it fans resource CRUD, pod
// observation, gather, bootstrap and wipe recovery out across every site's SiteClient,
// while the topology DECISIONS stay in the shared, single-site-tested code (repairCluster,
// GatherClusterGroundTruth, IsHealthy, planClusterWipeRecovery). Single-site mode never
// enters any of this — reconcileCluster branches here only when isMultiSite(lr).

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// multiSiteSiteNames returns the site names in spec order (the ordering the round-robin
// placement is defined over).
func multiSiteSiteNames(lr *littleredv1alpha1.LittleRed) []string {
	if lr.Spec.MultiSite == nil {
		return nil
	}
	names := make([]string, len(lr.Spec.MultiSite.Sites))
	for i, s := range lr.Spec.MultiSite.Sites {
		names[i] = s.Name
	}
	return names
}

// multiSiteParams resolves the shard/replica/sitesPerShard triplet. shards and
// replicasPerShard carry CRD defaults; sitesPerShard is required (LR-033) and is read
// as given, clamped only to the site count.
func multiSiteParams(lr *littleredv1alpha1.LittleRed) (shards, replicasPerShard, sitesPerShard int) {
	shards = clusterShardCount(lr)
	replicasPerShard = clusterReplicasPerShard(lr.Spec.Cluster)
	// sitesPerShard is required and never defaulted (LR-033) — read it, do not
	// invent one. A zero here means a spec that never passed validation, and
	// silently substituting len(sites) would be exactly the hidden copy-factor
	// decision the field is required in order to avoid; the placement then yields
	// no pods, which surfaces as an obvious failure rather than a silent topology.
	if lr.Spec.MultiSite != nil {
		sitesPerShard = lr.Spec.MultiSite.SitesPerShard
	}
	if sitesPerShard > len(multiSiteSiteNames(lr)) {
		sitesPerShard = len(multiSiteSiteNames(lr))
	}
	return shards, replicasPerShard, sitesPerShard
}

// multiSiteTotalMembers is the expected total pod count of the stretched cluster:
// shards x sitesPerShard x (replicasPerShard + 1) (design §4).
func multiSiteTotalMembers(lr *littleredv1alpha1.LittleRed) int {
	shards, rps, sps := multiSiteParams(lr)
	return shards * sps * (rps + 1)
}

// resolvedSite pairs a spec site with its (possibly unavailable) SiteClient. A site whose
// client cannot be constructed is returned with err set and client nil; callers treat it as
// unreachable and never fatal (design §3.2: an unreachable site is unmanaged, not broken).
//
// err carries TWO distinguishable states, because they call for opposite responses: a site
// whose Kubernetes API did not answer (self-healing, no action) and a hub-local CONFIGURATION
// fault the operator cannot recover from without a human. Classify with configFault (an
// errors.As on siteConfigError) rather than treating every err the same — that conflation is
// the bug site_config_fault.go exists to fix.
type resolvedSite struct {
	idx    int
	name   string
	client SiteClient
	err    error
}

// configFault reports whether this site failed to resolve because the operator could not read
// its own configuration (unresolvable namespace, missing/unreadable/malformed kubeconfig
// Secret), as opposed to the site not answering.
func (rs resolvedSite) configFault() (*siteConfigError, bool) { return siteConfigFaultOf(rs.err) }

// resolveSites obtains a SiteClient for every spec site, in spec order. Never fails: a
// site whose client cannot be built is returned unreachable (err set, classified — see
// resolvedSite). Returns nil when the registry is absent (unit/envtest without a manager).
func (r *LittleRedReconciler) resolveSites(ctx context.Context, lr *littleredv1alpha1.LittleRed) []resolvedSite {
	if r.sites == nil || lr.Spec.MultiSite == nil {
		return nil
	}
	out := make([]resolvedSite, 0, len(lr.Spec.MultiSite.Sites))
	for i, s := range lr.Spec.MultiSite.Sites {
		rs := resolvedSite{idx: i, name: s.Name}
		if sc, err := r.sites.siteClient(ctx, lr, s); err != nil {
			rs.err = err
		} else {
			rs.client = sc
		}
		out = append(out, rs)
	}
	return out
}

// sitePodObservation is one observed cluster pod in a site, from the Kubernetes API only
// (no Redis dial) — the same blackhole-proof, kubelet-authoritative view the single-site
// wipe recovery relies on (ADR-008).
type sitePodObservation struct {
	objName   string
	key       string // site-qualified ground-truth key
	ip        string
	ready     bool
	restarted bool
	oomKilled bool
	// terminating is whether the pod is being deleted (DeletionTimestamp set) — the
	// site-loss quiet-cluster gate excludes a terminating survivor (ADR-012 condition 5).
	terminating bool
	// restartCount is the redis container's restart count (0 = never restarted); the
	// fenced-alive rejoin recycle only recycles pods that returned WITHOUT a restart.
	restartCount int32
}

// siteObservation is one site's observed state this pass.
type siteObservation struct {
	site      resolvedSite
	reachable bool
	pods      []sitePodObservation
}

// observeSites lists this instance's cluster pods in every site through its SiteClient. An
// unreachable site (client absent or List error) contributes no pods and is marked
// unreachable — never stalling the pass (bounded per-call deadlines live in the M1
// SiteClient). Pod health is read from the redis container status.
func (r *LittleRedReconciler) observeSites(ctx context.Context, lr *littleredv1alpha1.LittleRed, sites []resolvedSite) []siteObservation {
	out := make([]siteObservation, 0, len(sites))
	sel := client.MatchingLabels(clusterSelectorLabels(lr))
	inNS := client.InNamespace(lr.Namespace)
	for _, s := range sites {
		o := siteObservation{site: s}
		if s.client == nil {
			out = append(out, o)
			continue
		}
		list := &corev1.PodList{}
		if err := s.client.List(ctx, list, inNS, sel); err != nil {
			out = append(out, o) // unreachable
			continue
		}
		o.reachable = true
		for i := range list.Items {
			pod := &list.Items[i]
			obsPod := sitePodObservation{
				objName:     pod.Name,
				key:         siteQualifiedKey(s.name, pod.Name),
				ip:          pod.Status.PodIP,
				terminating: pod.DeletionTimestamp != nil,
			}
			for j := range pod.Status.ContainerStatuses {
				cs := &pod.Status.ContainerStatuses[j]
				if cs.Name != ComponentRedis {
					continue
				}
				obsPod.ready = cs.Ready
				obsPod.restarted = cs.RestartCount >= 1
				obsPod.restartCount = cs.RestartCount
				if cs.LastTerminationState.Terminated != nil &&
					cs.LastTerminationState.Terminated.Reason == containerReasonOOMKilled {
					obsPod.oomKilled = true
				}
			}
			o.pods = append(o.pods, obsPod)
		}
		out = append(out, o)
	}
	return out
}

// expectedPodsPerSite counts the pods each site should host under the placement SSOT.
func expectedPodsPerSite(lr *littleredv1alpha1.LittleRed) map[string]int32 {
	shards, rps, sps := multiSiteParams(lr)
	counts := map[string]int32{}
	for _, ref := range multiSiteClusterPodRefs(lr.Name, multiSiteSiteNames(lr), shards, rps, sps) {
		counts[ref.SiteName]++
	}
	return counts
}

// readinessFromObs projects the per-site observations onto the pure gating input.
func readinessFromObs(lr *littleredv1alpha1.LittleRed, obs []siteObservation) []siteReadiness {
	expected := expectedPodsPerSite(lr)
	out := make([]siteReadiness, 0, len(obs))
	for _, o := range obs {
		sr := siteReadiness{Name: o.site.name, Reachable: o.reachable, Total: expected[o.site.name]}
		for _, p := range o.pods {
			if p.ready {
				sr.Ready++
			}
		}
		out = append(out, sr)
	}
	return out
}

// reconcileMultiSiteCluster is the multi-site cluster spine: place resources across sites,
// gate on per-site readiness, gather the union ground truth, and route to the shared
// repair loop or status update. It reuses the single-site-tested topology decisions
// wholesale — only the I/O fans out across sites.
func (r *LittleRedReconciler) reconcileMultiSiteCluster(ctx context.Context, lr *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	fast, _ := lr.GetRequeueIntervals()

	if r.sites == nil {
		// No site registry (reconciler built without a manager). Multi-site needs
		// per-site clients; nothing to do but requeue and surface it.
		log.Info("Multi-site cluster reconcile requested but the site registry is not initialized; requeueing")
		return ctrl.Result{RequeueAfter: fast}, nil
	}

	sites := r.resolveSites(ctx, lr)

	// 0a. Unreadable-configuration guard. A site the operator cannot CONFIGURE (its own
	// namespace unresolvable, or the kubeconfig Secret missing/unreadable/malformed) is not
	// an unreachable site: it never heals on its own. Before anything is built, refuse rather
	// than half-build a topology across only the sites that resolved. On an existing instance
	// this never fires — the warning condition carries it and the healthy sites keep being
	// reconciled (one deleted Secret must not become an instance-wide management outage).
	if refused, res := r.enforceSiteConfigGuard(lr, sites); refused {
		if err := r.Status().Update(ctx, lr); err != nil && !apierrors.IsConflict(err) {
			return ctrl.Result{}, err
		}
		log.Info("Refusing multi-site bringup: site configuration unreadable",
			"sites", summarizeSiteConfigFaults(siteConfigObservationsFrom(lr, sites)).Faulted)
		return res, nil
	}

	// 0b. Scale-down / topology-shrink refusal guard (LR-028, MULTI_SITE_DESIGN.md §10 item 7).
	// Before creating the new topology beside orphaned data, detect per-site shard StatefulSets
	// the desired placement no longer includes — reducing shards, lowering sitesPerShard, or
	// removing a still-observable site — and REFUSE (surface a condition, wait), never delete
	// (pillar 3.13; cluster storage is EmptyDir). This is the multi-site analogue of the
	// single-site detectOrphanedShardStatefulSets / reportShardScaleDown guard. A site fully
	// removed from the spec is no longer resolved, so its StatefulSets are unobservable and
	// cannot be refused — refusal requires observability (documented in §10).
	if refused, res, err := r.enforceMultiSiteScaleDownGuard(ctx, lr, sites); err != nil {
		return ctrl.Result{}, err
	} else if refused {
		return res, nil
	}

	// 1. Ensure resources across every hosting site (ConfigMap, Services, per-shard STS, PDB).
	if err := r.ensureMultiSiteClusterResources(ctx, lr, sites); err != nil {
		log.Error(err, "Failed to ensure multi-site cluster resources")
		return ctrl.Result{}, err
	}

	// 2. Observe every site.
	obs := r.observeSites(ctx, lr, sites)

	// 2a. Site-loss takeover (ADR-012). This is the SOLE fence-gated authority for a
	// quorum-loss TAKEOVER in multi-site. It runs ahead of the readiness gate and repair, and
	// — as a belt-and-braces guarantee — the generic repairCluster is itself made INCAPABLE of
	// the unguarded acts here: repairMayForceTakeover is false in multi-site (so repair's Step-0
	// quorum recovery and Step-1 stuck-orphan promotion never issue CLUSTER FAILOVER TAKEOVER),
	// and ghost FORGET is passed forgetSafe=allSitesObservable(obs) (so a node on an unreachable
	// site is never forgotten — pillar 3.4). Safety therefore no longer rests on recoverSiteLoss
	// merely *acting* to bypass repair; repair simply cannot take over or forget-on-unreachability.
	// It runs before the readiness gate because a Kubernetes-confirmed dead site also FAILS that
	// gate. When it acts (arm/hold/wait/refuse/fire/recycle) we requeue without running repair;
	// only "nothing to do" / "survivors keep quorum" fall through.
	if acted, err := r.recoverSiteLoss(ctx, lr, obs); err != nil {
		return ctrl.Result{}, err
	} else if acted {
		return ctrl.Result{RequeueAfter: fast}, nil
	}

	// 2b. Gate on per-site readiness.
	ready, anyReachable := multiSiteReady(readinessFromObs(lr, obs))

	if !ready {
		log.Info("Multi-site: waiting for reachable sites' pods to be ready", "anyReachable", anyReachable)
		// Per-site wipe-deadlock recovery: recycle stuck redis-down pods in the reachable
		// sites (an unreachable site's pods are simply not seen, so it never blinds the rest).
		if err := r.recoverMultiSiteClusterWipeDeadlock(ctx, lr, obs); err != nil {
			return ctrl.Result{}, err
		}
		// Pass an empty ground truth so status reflects readiness without dialing pods that
		// are not up yet (mirrors single-site, which sets Initializing from counts only).
		if _, err := r.updateMultiSiteClusterStatus(ctx, lr, redisclient.NewClusterGroundTruth(), obs); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: fast}, nil
	}

	// 3. Gather the union ground truth (pod IPs are routed, so one gather over the union
	//    is correct — the gatherer dials IPs directly regardless of site).
	gt := r.gatherGroundTruth(ctx, lr)

	shards, _, _ := multiSiteParams(lr)
	expectedMembers := int32(multiSiteTotalMembers(lr))
	isHealthy := gt.IsHealthy(expectedMembers, int32(shards))

	if !isHealthy || gt.HasPartitions() || gt.HasGhostNodes() || gt.HasOrphanedSlots() || gt.HasEmptyMasters() {
		r.getLogger(ctx, lr, LogCategoryState).Info("Multi-site cluster not healthy or topology issues detected, running repair",
			"partitions", len(gt.Partitions), "ghosts", len(gt.GhostNodes),
			"orphanedSlots", gt.HasOrphanedSlots(), "emptyMasters", gt.HasEmptyMasters(),
			"masters", gt.CountMasters(), "allNodesView", len(gt.AllNodeIDs))
		// forgetSafe only when every hosting site answered this pass; otherwise an unreachable
		// site's nodes are "unknown, not gone" and repair must not FORGET them (pillar 3.4).
		// The MEET confirmer is built from the sites resolved THIS pass, so a key naming
		// a site that did not resolve is refused rather than confirmed elsewhere (LR-063).
		return r.repairCluster(ctx, lr, gt, allSitesObservable(obs), multiSiteConfirmPodIP(siteReaderLookup(sites)))
	}

	// 3b. Master-balance drift correction (ADR-012 D2). Steady-state MAINTENANCE, healthy path
	// only: keep the slot-serving masters spread so no single site holds a Redis master majority
	// (the placement property that keeps a whole-site loss inside planSiteTakeover's fence-safe
	// even split rather than the drift-refusal). At most one graceful CLUSTER FAILOVER per
	// reconcile; when it acts, requeue without updating status this pass. Multi-site only —
	// ADR-009 declines master balancing in single-site mode.
	if acted, err := r.rebalanceMasters(ctx, lr, gt, obs); err != nil {
		return ctrl.Result{}, err
	} else if acted {
		return ctrl.Result{RequeueAfter: fast}, nil
	}

	// 4. Healthy — update status (also refreshes per-site observed state).
	return r.updateMultiSiteClusterStatus(ctx, lr, gt, obs)
}

// ensureMultiSiteClusterResources applies the ConfigMap, shared headless + client Services,
// per-shard StatefulSets and per-shard PDBs into each site that hosts at least one shard,
// through that site's SiteClient. An unreachable site's apply errors are logged and skipped
// (retried next pass), never fatal — the reachable sites must still be reconciled.
func (r *LittleRedReconciler) ensureMultiSiteClusterResources(ctx context.Context, lr *littleredv1alpha1.LittleRed, sites []resolvedSite) error {
	shards, _, sps := multiSiteParams(lr)
	numSites := len(sites)
	if numSites == 0 {
		return nil
	}
	hosting := multiSiteShardHostingSites(numSites, shards, sps)
	shardsBySite := map[int][]int{}
	for k, siteIdxs := range hosting {
		for _, si := range siteIdxs {
			shardsBySite[si] = append(shardsBySite[si], k)
		}
	}

	log := r.getLogger(ctx, lr, LogCategoryRecon)
	createPDB := r.pdbEnabled(lr) && clusterHasReplicas(lr)

	for _, s := range sites {
		hostedShards := shardsBySite[s.idx]
		if len(hostedShards) == 0 {
			continue // this site hosts no shard under the current placement
		}
		if s.client == nil {
			log.Info("Multi-site: site unreachable, deferring resource apply", "site", s.name, "err", errText(s.err))
			continue
		}
		// Site-shared objects: a fresh build per site (Apply mutates the object — stamps
		// labels, sets the owner ref only on the hub-local site).
		if err := s.client.Apply(ctx, lr, buildClusterConfigMap(lr)); err != nil {
			log.Info("Multi-site: ConfigMap apply failed, will retry", "site", s.name, "err", err.Error())
			continue
		}
		if err := s.client.Apply(ctx, lr, buildClusterHeadlessService(lr)); err != nil {
			log.Info("Multi-site: headless Service apply failed, will retry", "site", s.name, "err", err.Error())
			continue
		}
		if err := s.client.Apply(ctx, lr, buildClusterClientService(lr)); err != nil {
			log.Info("Multi-site: client Service apply failed, will retry", "site", s.name, "err", err.Error())
			continue
		}
		for _, k := range hostedShards {
			if createPDB {
				if err := s.client.Apply(ctx, lr, buildClusterShardPDB(lr, k)); err != nil {
					log.Info("Multi-site: PDB apply failed, will retry", "site", s.name, "shard", k, "err", err.Error())
				}
			}
		}
	}

	// StatefulSets are applied with cross-site + cross-shard rollout serialization (below).
	return r.reconcileMultiSiteClusterStatefulSets(ctx, lr, sites, shardsBySite)
}

// reconcileMultiSiteClusterStatefulSets creates/updates the per-(site, shard) StatefulSets.
// Missing ones are created immediately and in parallel (bootstrap has no data to protect).
// TEMPLATE UPDATES are serialized to at most one StatefulSet across the whole instance at a
// time (design §10.4): iterating shard-major then site, the first (site, shard) whose applied
// pod-template hash differs is rolled and the pass returns, deferring every later STS until it
// settles (statefulSetRolloutSettled). This strictly guarantees a config change never
// restarts a shard's master in more than one site at once — the cross-site extension of the
// single-site LR-021 serialization. Governs only operator-triggered rollouts.
func (r *LittleRedReconciler) reconcileMultiSiteClusterStatefulSets(ctx context.Context, lr *littleredv1alpha1.LittleRed, sites []resolvedSite, shardsBySite map[int][]int) error {
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	shards, _, _ := multiSiteParams(lr)

	for k := range shards {
		for _, s := range sites {
			if !hostsShard(shardsBySite, s.idx, k) {
				continue
			}
			if s.client == nil {
				continue // unreachable; retried next pass
			}
			// partition=nil: the multi-site path is NOT state-gated by ADR-017/LR-047.
			// The single-site path passes planShardRolloutPartition's cursor here, which
			// needs that shard's pods and cluster ground truth; this loop holds neither —
			// it has a site client and a StatefulSet. Passing nil is byte-identical to the
			// pre-ADR-017 shape this code was written against, so the cross-site LR-021
			// serialization below still holds, but WITHIN one shard the handover is back
			// on the StatefulSet controller's redundancy-blind gates. That is the state
			// LR-047 measured losing a shard's dataset. Tracked, not fixed here — see
			// BACKLOG.md and the LR-026 note in the changelog.
			desired := buildClusterShardStatefulSet(lr, k, nil)
			existing := &appsv1.StatefulSet{}
			err := s.client.Get(ctx, types.NamespacedName{
				Name:      clusterShardStatefulSetName(lr, k),
				Namespace: lr.Namespace,
			}, existing)
			if apierrors.IsNotFound(err) {
				if err := s.client.Apply(ctx, lr, buildClusterShardStatefulSet(lr, k, nil)); err != nil {
					log.Info("Multi-site: shard STS create failed, will retry", "site", s.name, "shard", k, "err", err.Error())
				}
				continue
			}
			if err != nil {
				log.Info("Multi-site: shard STS get failed, will retry", "site", s.name, "shard", k, "err", err.Error())
				continue
			}

			desiredHash := desired.Spec.Template.Annotations[AnnotationPodSpecHash]
			appliedHash := existing.Spec.Template.Annotations[AnnotationPodSpecHash]
			if appliedHash != desiredHash {
				if err := s.client.Apply(ctx, lr, buildClusterShardStatefulSet(lr, k, nil)); err != nil {
					// An unreachable site's live Apply/Patch times out even though its cached Get
					// succeeded — skip it (retried next pass), never abort the whole reconcile
					// (pillar 3.2: unreachable = unmanaged, not broken). Mirrors the CM/Service
					// applies above. The deferred-rollout return below does not run, so a
					// reachable sibling can still be rolled this pass.
					log.Info("Multi-site serialized rollout: shard STS update apply failed, will retry",
						"site", s.name, "shard", k, "err", err.Error())
					continue
				}
				log.Info("Multi-site serialized rollout: rolling one shard STS, deferring the rest until it settles",
					"site", s.name, "shard", k)
				return nil
			}
			if !statefulSetRolloutSettled(existing) {
				log.Info("Multi-site serialized rollout: waiting for shard STS to settle before rolling the next",
					"site", s.name, "shard", k)
				return nil
			}
			// Settled at desired: keep non-template fields in sync and advance.
			if err := s.client.Apply(ctx, lr, buildClusterShardStatefulSet(lr, k, nil)); err != nil {
				log.Info("Multi-site: settled shard STS sync apply failed, will retry",
					"site", s.name, "shard", k, "err", err.Error())
				continue
			}
		}
	}
	return nil
}

// hostsShard reports whether site si hosts shard k under the computed placement.
func hostsShard(shardsBySite map[int][]int, si, k int) bool {
	return slices.Contains(shardsBySite[si], k)
}

// allSitesObservable reports whether the operator obtained every site's observed state this
// pass — "answered, and we were able to ask". A site the operator could not CONFIGURE (see
// site_config_fault.go) counts as unobservable exactly like one that did not answer, and that
// is deliberate: in both cases its nodes are unknown.
//
// It is the multi-site FORGET-safety signal (ghostForgetPlan): only when every hosting site is
// observable can a gossip-known node with no backing pod be treated as confirmed gone; if any
// site is unobservable, its nodes are "unknown, not gone" (pillar 3.4) and no ghost is
// forgotten this pass. Empty obs (no sites) is not observable.
//
// So an unreadable site configuration SUPPRESSES ghost FORGET, which is the correct direction
// and worth stating rather than leaving to accident: FORGET is cosmetic cleanup, safe to defer
// until the state is healed and stable, and never doing it costs nothing. (The name keeps its
// broad meaning on purpose — renaming would churn call sites for no gain.)
func allSitesObservable(obs []siteObservation) bool {
	if len(obs) == 0 {
		return false
	}
	for _, o := range obs {
		if !o.reachable {
			return false
		}
	}
	return true
}

// gatherMultiSiteGroundTruth builds the union IP->qualified-key map across every reachable
// site and runs the shared GatherClusterGroundTruth over it. Pod IPs are routed (pod-routed
// transport), so one direct-TCP gatherer dials every site's pods; an unreachable site
// contributes nothing and never stalls the gather (the shared gather already bounds every
// dial with ProbeTimeout — LR-017).
func (r *LittleRedReconciler) gatherMultiSiteGroundTruth(ctx context.Context, lr *littleredv1alpha1.LittleRed) *redisclient.ClusterGroundTruth {
	gt, _ := r.gatherMultiSiteWithObs(ctx, lr)
	return gt
}

// gatherMultiSiteWithObs is gatherMultiSiteGroundTruth plus the per-site
// observations it derived (reachability + observed pods). The one-shot
// `--gather` mode (tier 2) needs both: the union ground truth and the per-site
// summary, so lrctl can tell "site unreachable" from "site hosts no pods".
func (r *LittleRedReconciler) gatherMultiSiteWithObs(ctx context.Context, lr *littleredv1alpha1.LittleRed) (*redisclient.ClusterGroundTruth, []siteObservation) {
	if r.sites == nil {
		return redisclient.NewClusterGroundTruth(), nil
	}
	sites := r.resolveSites(ctx, lr)
	obs := r.observeSites(ctx, lr, sites)
	clusterPods := unionClusterPods(obs)
	password := r.getRedisPassword(ctx, lr)
	g := &operatorGatherer{password: password, tlsEnabled: lr.Spec.TLS.Enabled}
	return redisclient.GatherClusterGroundTruth(ctx, g, clusterPods), obs
}

// unionClusterPods builds the gather's IP->qualified-key map from the union of every
// REACHABLE site's observed pods (with an assigned IP). An unreachable site contributes
// nothing (design §3.2/§8), so it never blinds the gather of the rest.
func unionClusterPods(obs []siteObservation) map[string]string {
	clusterPods := make(map[string]string)
	for _, o := range obs {
		if !o.reachable {
			continue
		}
		for _, p := range o.pods {
			if p.ip != "" {
				clusterPods[p.ip] = p.key
			}
		}
	}
	return clusterPods
}

// bootstrapMultiSiteCluster initializes a stretched Redis Cluster: MEET every pod across
// every site to a single seed, assign each shard's slots to its initial master (evenly
// spread across sites), and wire every other member as a replica of its shard master — so
// each shard has members in >= 2 sites. Mirrors the single-site bootstrapCluster, resolving
// each pod through its owning site's SiteClient and keying ground truth by site-qualified key.
func (r *LittleRedReconciler) bootstrapMultiSiteCluster(ctx context.Context, lr *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	auditLog.Info("Bootstrapping/Healing multi-site cluster")

	shards, rps, sps := multiSiteParams(lr)
	siteNames := multiSiteSiteNames(lr)
	refs := multiSiteClusterPodRefs(lr.Name, siteNames, shards, rps, sps)
	plan := planMultiSiteBootstrap(refs)

	sites := r.resolveSites(ctx, lr)
	siteByIdx := map[int]resolvedSite{}
	for _, s := range sites {
		siteByIdx[s.idx] = s
	}

	password := r.getRedisPassword(ctx, lr)
	clusterClient := redisclient.NewClusterClient(password, lr.Spec.TLS.Enabled)

	// Fetch each hosting site's shard STS once for the revision gate.
	type siteShard struct {
		site  int
		shard int
	}
	stsByKey := map[siteShard]*appsv1.StatefulSet{}

	podIPs := make(map[string]string, len(refs))  // key -> IP
	nodeIDs := make(map[string]string, len(refs)) // key -> NodeID

	for _, ref := range refs {
		s := siteByIdx[ref.SiteIdx]
		if s.client == nil {
			log.Info("Multi-site bootstrap: site unreachable, deferring", "site", ref.SiteName)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		sk := siteShard{site: ref.SiteIdx, shard: ref.ShardIdx}
		sts, ok := stsByKey[sk]
		if !ok {
			sts = &appsv1.StatefulSet{}
			if err := s.client.Get(ctx, types.NamespacedName{Name: clusterShardStatefulSetName(lr, ref.ShardIdx), Namespace: lr.Namespace}, sts); err != nil {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			stsByKey[sk] = sts
		}

		pod := &corev1.Pod{}
		if err := s.client.Get(ctx, types.NamespacedName{Name: ref.ObjName, Namespace: lr.Namespace}, pod); err != nil {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		currentRevision := sts.Status.CurrentRevision
		podRevision := pod.Labels["controller-revision-hash"]
		if pod.Status.PodIP == "" || currentRevision == "" || podRevision != currentRevision {
			log.Info("Multi-site bootstrap: pod not ready (no IP or stale revision)",
				"site", ref.SiteName, "pod", ref.ObjName, "podRevision", podRevision, "stsRevision", currentRevision)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		podIPs[ref.Key] = pod.Status.PodIP

		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, littleredv1alpha1.RedisPort)
		id, err := clusterClient.GetMyID(ctx, addr)
		if err != nil {
			auditLog.Error(err, "Multi-site bootstrap: failed to get Node ID", "site", ref.SiteName, "pod", ref.ObjName)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		nodeIDs[ref.Key] = id
	}

	// 1. MEET everyone to shard 0's initial master (the seed).
	seedKey := plan.MastersByShard[0]
	seedAddr := fmt.Sprintf("%s:%d", podIPs[seedKey], littleredv1alpha1.RedisPort)
	for _, ref := range refs {
		if ref.Key == seedKey {
			continue
		}
		auditLog.Info("Multi-site: meeting node", "node", ref.Key, "seed", seedKey)
		if err := clusterClient.ClusterMeet(ctx, seedAddr, podIPs[ref.Key], littleredv1alpha1.RedisPort); err != nil {
			auditLog.Error(err, "Multi-site: failed to meet node", "node", ref.Key)
		}
	}

	// Wait for gossip to propagate slightly.
	time.Sleep(2 * time.Second)

	// 2. Assign slots to each shard's initial master.
	if lr.Annotations[AnnotationDebugSkipSlotAssignment] == annotationValueTrue {
		auditLog.Info("DEBUG: Skipping slot assignment due to annotation")
	} else {
		slotRanges := redisclient.GenerateSlotRanges(shards)
		for k := range shards {
			masterKey := plan.MastersByShard[k]
			masterAddr := fmt.Sprintf("%s:%d", podIPs[masterKey], littleredv1alpha1.RedisPort)
			masterID := nodeIDs[masterKey]

			nodes, err := clusterClient.GetClusterNodes(ctx, masterAddr)
			if err == nil {
				hasSlots := false
				for _, n := range nodes {
					if n.NodeID == masterID && len(n.Slots) > 0 {
						hasSlots = true
						break
					}
				}
				if hasSlots {
					log.Info("Multi-site: node already has slots, skipping", "shard", k, "master", masterKey)
					continue
				}
			}

			auditLog.Info("Multi-site: assigning slots to master", "shard", k, "master", masterKey,
				"slots", fmt.Sprintf("%d-%d", slotRanges[k].Start, slotRanges[k].End))
			slots, _ := redisclient.ExpandSlotRange(redisclient.FormatSlotRange(slotRanges[k].Start, slotRanges[k].End))
			if err := clusterClient.ClusterAddSlots(ctx, masterAddr, slots...); err != nil {
				auditLog.Error(err, "Multi-site: failed to add slots", "shard", k, "master", masterKey)
			}
		}
	}

	// 3. Wire replicas: each non-master member replicates its shard's initial master.
	for _, ref := range refs {
		masterKey, isReplica := plan.ReplicaToMaster[ref.Key]
		if !isReplica {
			continue
		}
		masterID := nodeIDs[masterKey]
		replicaAddr := fmt.Sprintf("%s:%d", podIPs[ref.Key], littleredv1alpha1.RedisPort)

		nodes, err := clusterClient.GetClusterNodes(ctx, replicaAddr)
		alreadyCorrect := false
		if err == nil {
			for _, n := range nodes {
				if n.NodeID == nodeIDs[ref.Key] && n.MasterID == masterID {
					alreadyCorrect = true
					break
				}
			}
		}
		if !alreadyCorrect {
			auditLog.Info("Multi-site: assigning replica to master", "replica", ref.Key, "master", masterKey)
			if err := clusterClient.ClusterReplicate(ctx, replicaAddr, masterID); err != nil {
				auditLog.Error(err, "Multi-site: failed to replicate", "replica", ref.Key, "master", masterKey)
			}
		}
	}

	return r.updateClusterStatus(ctx, lr, nil)
}

// recoverMultiSiteClusterWipeDeadlock is the per-site analogue of recoverClusterWipeDeadlock
// (ADR-008). It gathers the recyclable-pod signature from the union of REACHABLE sites' pods
// (an unreachable site's pods are simply invisible, so it never blinds recovery of the rest),
// runs the same pure planClusterWipeRecovery gate/cooldown, and recycles each stuck pod via
// its OWNING site's SiteClient. Never touches a Ready pod (a possible data holder).
func (r *LittleRedReconciler) recoverMultiSiteClusterWipeDeadlock(ctx context.Context, lr *littleredv1alpha1.LittleRed, obs []siteObservation) error {
	// Build the health list plus a key->(siteClient, objName) map for targeted deletion.
	type target struct {
		client  SiteClient
		objName string
	}
	targets := map[string]target{}
	health := make([]clusterPodHealth, 0)
	for _, o := range obs {
		if !o.reachable || o.site.client == nil {
			continue
		}
		for _, p := range o.pods {
			health = append(health, clusterPodHealth{
				Name:       p.key,
				RedisReady: p.ready,
				Restarted:  p.restarted,
				OOMKilled:  p.oomKilled,
			})
			targets[p.key] = target{client: o.site.client, objName: p.objName}
		}
	}

	if lr.Status.Cluster == nil {
		lr.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{}
	}
	var since *time.Time
	if lr.Status.Cluster.WipeDeadlockSince != nil {
		t := lr.Status.Cluster.WipeDeadlockSince.Time
		since = &t
	}
	now := time.Now()
	plan := planClusterWipeRecovery(health, since, now, clusterWipeRecoveryCooldown)

	stateLog := r.getLogger(ctx, lr, LogCategoryState)
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)

	switch plan.action {
	case wipeClearMarker:
		lr.Status.Cluster.WipeDeadlockSince = nil
	case wipeStartCooldown:
		stateLog.Info("Multi-site cluster wipe-deadlock signature observed; arming recovery cooldown",
			"stuckPods", recyclableNames(health), "cooldown", clusterWipeRecoveryCooldown.String())
		t := metav1.NewTime(now)
		lr.Status.Cluster.WipeDeadlockSince = &t
	case wipeWait:
		stateLog.Info("Multi-site cluster wipe-deadlock persists; waiting out recovery cooldown",
			"stuckPods", recyclableNames(health))
	case wipeRecycle:
		auditLog.Info("Breaking multi-site cluster wipe deadlock: recycling stuck redis-down pods in their sites",
			"pods", plan.recycle)
		for _, key := range plan.recycle {
			tg, ok := targets[key]
			if !ok {
				continue
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: tg.objName, Namespace: lr.Namespace}}
			if err := tg.client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
				auditLog.Error(err, "Multi-site: failed to recycle stuck pod", "pod", key)
				return err
			}
		}
		lr.Status.Cluster.WipeDeadlockSince = nil
	}
	return nil
}

// updateMultiSiteClusterStatus updates the LittleRed status for a multi-site cluster:
// aggregate readiness across sites, per-site observed state (status.multiSite.sites), the
// cluster node view from the union ground truth, and the high-level phase. gt may be passed
// through (happy path / bootstrap tail); nil gathers fresh. obs may be passed through to
// avoid re-listing; nil re-observes.
func (r *LittleRedReconciler) updateMultiSiteClusterStatus(ctx context.Context, lr *littleredv1alpha1.LittleRed, gt *redisclient.ClusterGroundTruth, obs []siteObservation) (ctrl.Result, error) {
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	oldStatus := lr.Status.DeepCopy()

	if obs == nil {
		obs = r.observeSites(ctx, lr, r.resolveSites(ctx, lr))
	}

	// Aggregate readiness and build per-site status.
	var ready int32
	now := metav1.Now()
	siteStatuses := make([]littleredv1alpha1.SiteStatus, 0, len(obs))
	expected := expectedPodsPerSite(lr)
	for _, o := range obs {
		var siteReady int32
		for _, p := range o.pods {
			if p.ready {
				siteReady++
			}
		}
		ready += siteReady
		// Reachable stays a single broad boolean ("did not answer, or we could not ask")
		// because the observability arithmetic depends on it; Reason/Message carry the
		// distinction a human needs — only a configuration reason demands action.
		reason, message := siteStatusReason(o)
		ss := littleredv1alpha1.SiteStatus{
			Name:          o.site.name,
			Reachable:     o.reachable,
			Reason:        reason,
			Message:       message,
			LastProbeTime: &now,
			Pods: &littleredv1alpha1.RedisStatus{
				Ready: siteReady,
				Total: expected[o.site.name],
			},
		}
		siteStatuses = append(siteStatuses, ss)
	}
	lr.Status.Redis.Ready = ready
	lr.Status.Redis.Total = int32(multiSiteTotalMembers(lr))
	if lr.Status.MultiSite == nil {
		lr.Status.MultiSite = &littleredv1alpha1.MultiSiteStatus{}
	}
	lr.Status.MultiSite.Sites = siteStatuses

	shards, rps, _ := multiSiteParams(lr)
	if gt == nil {
		gt = r.gatherMultiSiteGroundTruth(ctx, lr)
	}
	clusterOK := false
	if gt != nil {
		if lr.Status.Cluster == nil {
			lr.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{}
		}
		lr.Status.Cluster.State = gt.ClusterState
		clusterOK = gt.IsHealthy(lr.Status.Redis.Total, int32(shards))

		nodeStates := make([]littleredv1alpha1.ClusterNodeState, 0)
		for _, ref := range multiSiteClusterPodRefs(lr.Name, multiSiteSiteNames(lr), shards, rps, multiSiteSitesPerShard(lr)) {
			if node, ok := gt.Nodes[ref.Key]; ok {
				nodeStates = append(nodeStates, littleredv1alpha1.ClusterNodeState{
					PodName:      ref.Key,
					NodeID:       node.NodeID,
					Role:         node.Role,
					MasterNodeID: node.MasterNodeID,
					SlotRanges:   strings.Join(node.Slots, ","),
				})
			}
		}
		lr.Status.Cluster.Nodes = nodeStates

		// Publish the compact, site-attributed cross-site view for tier-1
		// (read-only-hub) consumers (lrctl status / verify --from-status): they
		// render this without any site access. Freshness = reconcile interval.
		state, slots, crossSiteOK, shardViews, masterCounts := buildCrossSiteView(gt)
		lr.Status.MultiSite.ClusterState = state
		lr.Status.MultiSite.SlotsAssigned = slots
		lr.Status.MultiSite.CrossSiteOK = crossSiteOK
		lr.Status.MultiSite.Shards = shardViews
		lr.Status.MultiSite.MasterCounts = masterCounts
		reconTime := metav1.Now()
		lr.Status.MultiSite.LastReconcileTime = &reconTime
	}

	// Reduced-site-loss-resilience warning (ADR-012 D2 "Reduced-resilience warning"):
	// a STRUCTURAL, spec-level condition — set independent of the gather — for a topology
	// that cannot auto-survive an arbitrary single-site loss (S sites x (ceil(K/2)-1) < K).
	// It is a surfaced warning + one-shot event, never a refusal (the opt-in-not-block norm);
	// transient master drift within a resilient topology is the rebalancer's concern, not this.
	r.reconcileReducedResilienceCondition(lr)

	// Unreadable-configuration warning: a site the operator cannot configure is unmanaged
	// until a human acts, which an unreachable site is not. Condition + one-shot event only —
	// the phase is deliberately left alone (the data plane is healthy, and phase cannot carry
	// two orthogonal facts); the bootstrap case is refused earlier, in enforceSiteConfigGuard.
	r.reconcileSiteConfigCondition(lr, siteConfigObservationsFrom(lr, resolvedFromObs(obs)))

	if lr.Status.Redis.Ready == lr.Status.Redis.Total && clusterOK {
		lr.Status.Phase = littleredv1alpha1.PhaseRunning
		lr.Status.Status = littleredv1alpha1.ConditionReady
		meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
			Type:               littleredv1alpha1.ConditionReady,
			Status:             metav1.ConditionTrue,
			Reason:             "ClusterHealthy",
			Message:            "All pods ready and stretched cluster state is ok across sites",
			LastTransitionTime: metav1.Now(),
		})
	} else {
		if lr.Status.Phase != littleredv1alpha1.PhasePending {
			lr.Status.Phase = littleredv1alpha1.PhaseInitializing
		}
		lr.Status.Status = "Initializing"
		log.Info("Multi-site not yet Running, requeueing",
			"redis", fmt.Sprintf("%d/%d", lr.Status.Redis.Ready, lr.Status.Redis.Total), "clusterHealthy", clusterOK)
		meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
			Type:               littleredv1alpha1.ConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             "ClusterNotReady",
			Message:            "Waiting for the stretched cluster to be healthy across sites",
			LastTransitionTime: metav1.Now(),
		})
	}

	if !reflect.DeepEqual(oldStatus, &lr.Status) {
		if err := r.Status().Update(ctx, lr); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
	}

	fast, steady := lr.GetRequeueIntervals()
	if lr.Status.Phase == littleredv1alpha1.PhaseRunning {
		return ctrl.Result{RequeueAfter: steady}, nil
	}
	return ctrl.Result{RequeueAfter: fast}, nil
}

// reconcileReducedResilienceCondition sets or clears the ReducedSiteLossResilience warning
// condition (ADR-012 D2) from the STRUCTURAL, spec-level resilience predicate
// siteLossResilient(shards, numSites) — it depends only on the topology (K shards over S
// sites), never on the live master distribution. When the invariant is unsatisfiable the
// condition is True with an explanatory message, plus a one-shot Warning event fired only on
// the transition into the warning state (idempotent — subsequent passes re-affirm the True
// condition without re-emitting). When satisfiable it is False (a resilient topology). It is
// a surfaced warning, never a refusal (the opt-in-not-block norm): the reconcile continues
// regardless.
func (r *LittleRedReconciler) reconcileReducedResilienceCondition(lr *littleredv1alpha1.LittleRed) {
	shards := clusterShardCount(lr)
	numSites := len(multiSiteSiteNames(lr))
	floor := takeoverSurvivalFloor(shards)

	if siteLossResilient(shards, numSites) {
		meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
			Type:   littleredv1alpha1.ConditionReducedSiteLossResilience,
			Status: metav1.ConditionFalse,
			Reason: reasonSiteLossResilient,
			Message: fmt.Sprintf("Topology can auto-survive any single-site loss: %d sites x %d master(s)/site "+
				"(takeover floor floor(%d/2)) >= %d shards.", numSites, floor, shards, shards),
			LastTransitionTime: metav1.Now(),
		})
		return
	}

	msg := fmt.Sprintf("Topology cannot auto-survive an arbitrary single-site loss: %d sites x %d master(s)/site "+
		"(takeover floor floor(%d/2)) < %d shards, so pigeonhole forces some site over the floor; losing that "+
		"heavier-master site leaves the lost side holding a master majority, so the takeover cannot be fenced "+
		"safely. Use an even shard count for two sites, add a site, or rely on manual / "+
		"multiSite.allowUnsafeSiteTakeover recovery for the heavier site's loss.", numSites, floor, shards, shards)

	// One-shot Warning event: only on the transition INTO the warning state (the condition
	// was absent or not True). meta.SetStatusCondition itself is idempotent for the condition.
	if existing := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionReducedSiteLossResilience); existing == nil || existing.Status != metav1.ConditionTrue {
		r.event(lr, corev1.EventTypeWarning, reasonReducedSiteLossResilience, msg)
	}
	meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionReducedSiteLossResilience,
		Status:             metav1.ConditionTrue,
		Reason:             reasonReducedSiteLossResilience,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
}

// multiSiteSitesPerShard is the spec's sitesPerShard, clamped to the site count.
// It is required and never defaulted (LR-033).
func multiSiteSitesPerShard(lr *littleredv1alpha1.LittleRed) int {
	_, _, sps := multiSiteParams(lr)
	return sps
}

// intendedShardMasterKey returns the ground-truth key of shard shardIdx's intended master:
// the {name}-shard-K-0 pod name in single-site, and the site-qualified initial master (its
// primary hosting site) in multi-site (pod names collide across sites). Used by the
// missing-shard repair step to know which node a shard's slots belong to.
func (r *LittleRedReconciler) intendedShardMasterKey(lr *littleredv1alpha1.LittleRed, shardIdx int) string {
	if isMultiSite(lr) {
		return multiSiteInitialMasterKey(lr.Name, multiSiteSiteNames(lr), shardIdx)
	}
	return shardMasterPodName(lr.Name, shardIdx)
}

// observeShardStatefulSets lists this instance's per-shard cluster StatefulSets in every
// REACHABLE site through its SiteClient, recovering each shard index from the stable LabelShard
// identity label. An unreachable site (client absent or List error) contributes nothing — it
// cannot be observed this pass, so its StatefulSets are never treated as orphans (refusal
// requires observability; a site fully removed from the spec is likewise never resolved and so
// never appears here — MULTI_SITE_DESIGN.md §10 item 7).
func (r *LittleRedReconciler) observeShardStatefulSets(ctx context.Context, lr *littleredv1alpha1.LittleRed, sites []resolvedSite) []observedShardSTS {
	var out []observedShardSTS
	sel := client.MatchingLabels(clusterSelectorLabels(lr))
	inNS := client.InNamespace(lr.Namespace)
	for _, s := range sites {
		if s.client == nil {
			continue
		}
		list := &appsv1.StatefulSetList{}
		if err := s.client.List(ctx, list, inNS, sel); err != nil {
			continue // unreachable this pass
		}
		for i := range list.Items {
			sts := &list.Items[i]
			val, ok := sts.Labels[LabelShard]
			if !ok {
				continue
			}
			shardIdx, err := strconv.Atoi(val)
			if err != nil {
				continue
			}
			out = append(out, observedShardSTS{Site: s.name, Shard: shardIdx, Name: sts.Name})
		}
	}
	return out
}

// enforceMultiSiteScaleDownGuard observes the per-site shard StatefulSets across sites, runs the
// pure planMultiSiteScaleDown diff against the desired placement, and refuses (reportMultiSite-
// ScaleDown) if any are orphaned. Returns refused=true with the requeue Result when it refused;
// refused=false lets the caller proceed to resource reconciliation. It NEVER deletes anything.
func (r *LittleRedReconciler) enforceMultiSiteScaleDownGuard(ctx context.Context, lr *littleredv1alpha1.LittleRed, sites []resolvedSite) (bool, ctrl.Result, error) {
	shards, _, sps := multiSiteParams(lr)
	desired := desiredMultiSiteShardSet(multiSiteSiteNames(lr), shards, sps)
	observed := r.observeShardStatefulSets(ctx, lr, sites)
	orphans := planMultiSiteScaleDown(desired, observed)
	if len(orphans) == 0 {
		return false, ctrl.Result{}, nil
	}
	res, err := r.reportMultiSiteScaleDown(ctx, lr, orphans)
	return true, res, err
}

// reportMultiSiteScaleDown surfaces a refused multi-site topology shrink and waits. Like the
// single-site reportShardScaleDown (and the legacy guard), it does NOT delete the orphaned
// StatefulSets — that would destroy their EmptyDir data, and LittleRed never deletes data by
// default (pillar 3.13). An operator restores the topology or migrates+removes the data manually;
// reconciliation resumes automatically once no orphan remains.
func (r *LittleRedReconciler) reportMultiSiteScaleDown(ctx context.Context, lr *littleredv1alpha1.LittleRed, orphans []multiSiteOrphanSTS) (ctrl.Result, error) {
	names := make([]string, 0, len(orphans))
	for _, o := range orphans {
		names = append(names, fmt.Sprintf("%s/%s", o.Site, o.Name))
	}
	sort.Strings(names) // stable message/event regardless of per-site List order
	shards, _, sps := multiSiteParams(lr)
	msg := fmt.Sprintf("Refusing multi-site topology change (shards=%d, sitesPerShard=%d): per-site "+
		"shard StatefulSet(s) %s are not part of the desired placement and would be orphaned, losing "+
		"their slots (and data). Cluster storage is EmptyDir and there is no member-relocation path; "+
		"LittleRed never deletes data by default. The multi-site topology is fixed for an instance's "+
		"lifetime (see docs/MULTI_SITE_DESIGN.md §2), so this is drift the CRD immutability rules did "+
		"not catch: restore spec.cluster.shards / spec.multiSite.sites / spec.multiSite.sitesPerShard "+
		"to the topology these StatefulSets belong to, or migrate the data and delete them manually.",
		shards, sps, strings.Join(names, ", "))

	log := r.getLogger(ctx, lr, LogCategoryRecon)
	log.Info("Refusing multi-site scale-down", "orphans", names)
	r.event(lr, corev1.EventTypeWarning, reasonMultiSiteScaleDownRefused, msg)

	lr.Status.Phase = littleredv1alpha1.PhaseFailed
	lr.Status.Status = reasonMultiSiteScaleDownRefused
	meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reasonMultiSiteScaleDownRefused,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
	if err := r.Status().Update(ctx, lr); err != nil && !apierrors.IsConflict(err) {
		return ctrl.Result{}, err
	}
	_, steady := lr.GetRequeueIntervals()
	return ctrl.Result{RequeueAfter: steady}, nil
}

// errText renders an error for logging, tolerating nil.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// multiSiteConfirmPodIP builds the LR-043 MEET-address confirmer for multi-site: it
// routes each question to the API server that actually owns the pod.
//
// The ground truth is keyed by siteQualifiedKey ("<site>/<objName>") because the bare
// object name names a DIFFERENT pod in every hosting site — shard K has a StatefulSet in
// each of its sitesPerShard sites, so "{name}-shard-K-0" exists in all of them. Handing
// that whole key to the hub's API server as a resource name is what LR-063 was: the Get
// can never succeed, the guard denies forever, and because a denied SEED returns the
// whole repair pass it also skips repairCluster's TotalSlots==0 bootstrap, so a stretched
// cluster can never form.
//
// The evidence is NOT weaker here than in single-site, and the fix is not an
// approximation. Pod-routed is the only transport spec.multiSite.network.mode accepts, so
// pod CIDRs are mutually routed and pod IPs are globally unique across the hub and every
// site. LR-043's premise — Kubernetes holds at most one live pod per IP, so a confirmed
// address IS our pod — therefore holds across the whole federation. What is per-cluster
// is the ORACLE, not the fact: only the owning site's API server can answer about its own
// pods. Routing the question restores the guard at full strength.
//
// An address whose key names no resolvable site is REFUSED (podIPUnknownSite), never
// confirmed against whichever reader is at hand. Falling back to the hub would ask the
// wrong cluster about a colliding name, and on a hub-local site it would even succeed —
// confirming a foreign pod's address, which is the cross-instance merge LR-043 exists to
// prevent.
func multiSiteConfirmPodIP(readerFor func(site string) (client.Reader, bool)) podIPConfirmer {
	return func(ctx context.Context, namespace, podKey, ip string) (bool, string) {
		if podKey == "" || ip == "" {
			return false, podIPUnknown
		}
		site, objName := splitSiteQualifiedKey(podKey)
		if site == "" || objName == "" {
			return false, podIPUnknownSite
		}
		reader, ok := readerFor(site)
		if !ok || reader == nil {
			// The site is not resolved this pass (unreachable API, a configuration
			// fault, or a key naming a site no longer in the spec). Deny, per LR-043's
			// direction. This has no domain of effect on the ordinary unreachable-site
			// path: unionClusterPods drops an unreachable site's pods from the gather
			// entirely, so they are in no partition and can be neither seed nor target.
			return false, podIPUnknownSite
		}
		pod := &corev1.Pod{}
		err := reader.Get(ctx, types.NamespacedName{Name: objName, Namespace: namespace}, pod)
		return podIPVerdict(pod, err, ip)
	}
}

// siteReaderLookup turns the sites resolved this pass into the site->uncached-reader
// lookup multiSiteConfirmPodIP routes over. A site that did not resolve (unreachable API,
// a configuration fault, or a name no longer in the spec) is simply absent, which the
// confirmer reads as "refuse" — never as "ask somebody else".
func siteReaderLookup(sites []resolvedSite) func(string) (client.Reader, bool) {
	readers := make(map[string]client.Reader, len(sites))
	for _, s := range sites {
		if s.client == nil {
			continue
		}
		readers[s.name] = s.client.UncachedReader()
	}
	return func(site string) (client.Reader, bool) {
		rd, ok := readers[site]
		return rd, ok
	}
}
