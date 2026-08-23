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
	"net"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

const (
	// AnnotationMigrateLegacySTS pins the ADR-013 legacy→per-shard migration. The only
	// recognized value is "hold", which parks a non-mutating holding state for a maintenance
	// window / change-control sign-off. Absence (or any other value) ⇒ proceed.
	AnnotationMigrateLegacySTS = "redis.chuck-chuck-chuck.net/migrate-legacy-sts"
	// migrateLegacyHoldValue is the sole recognized value of AnnotationMigrateLegacySTS.
	migrateLegacyHoldValue = "hold"

	// statusMigrating is the status.status string while a migration is in flight (Phase stays
	// a non-failed value — Initializing — so the instance never reads as terminally Failed).
	statusMigrating = "Migrating"
)

// migPodFact is the minimal, pure view of one cluster-member pod that the migration
// driver's pure helpers need: its name, dial IP, and kubelet readiness of the redis
// container. It is assembled by the driver from the pod list (component=cluster) and
// carries no k8s types so the helpers stay unit-testable.
type migPodFact struct {
	Name       string
	IP         string // pod IP ("" when not yet assigned)
	RedisReady bool   // kubelet readiness of the redis container
}

// isLegacyClusterPod reports whether podName is a pre-0.3 single-STS cluster pod
// ({instanceName}-cluster-<N>) and NOT a per-shard pod ({instanceName}-shard-K-M).
func isLegacyClusterPod(podName, instanceName string) bool {
	rest, ok := strings.CutPrefix(podName, instanceName+"-cluster-")
	if !ok || rest == "" {
		return false
	}
	_, err := strconv.Atoi(rest) // a single integer ordinal (no further "-M" segment)
	return err == nil
}

// isNewShardPod reports whether podName is a 0.3 per-shard cluster pod
// ({instanceName}-shard-<K>-<M>), with K and M both non-negative integers.
func isNewShardPod(podName, instanceName string) bool {
	rest, ok := strings.CutPrefix(podName, instanceName+"-shard-")
	if !ok {
		return false
	}
	parts := strings.Split(rest, "-")
	if len(parts) != 2 {
		return false
	}
	if _, err := strconv.Atoi(parts[0]); err != nil {
		return false
	}
	_, err := strconv.Atoi(parts[1])
	return err == nil
}

// allLegacyPodsReady reports whether every legacy pod in the slice has its redis
// container Ready (kubelet-authoritative). Returns false if there are no legacy pods
// (a legacy STS with no Ready pods is not a safe cluster to start rewriting).
func allLegacyPodsReady(pods []migPodFact, instanceName string) bool {
	seen := 0
	for _, p := range pods {
		if !isLegacyClusterPod(p.Name, instanceName) {
			continue
		}
		seen++
		if !p.RedisReady {
			return false
		}
	}
	return seen > 0
}

// buildLegacyFactsFromPods assembles the pure LegacyFacts the migration plan needs from
// the cluster pod list + live ground truth, plus two driver flags: whether all legacy
// pods are Ready (health gate) and whether any new-shard pod exists yet (entry-gate guard).
//
//   - LegacyNodeIDs: NodeIDs of the legacy pods that are members of the mesh (present in gt).
//   - NewPodAddrs: {new pod name → IP:port} for every new-shard pod that has an IP (is up).
//   - SeedAddrs: IP:port of every reachable legacy pod, usable as a MEET/FORGET seed.
func buildLegacyFactsFromPods(pods []migPodFact, gt *redisclient.ClusterGroundTruth, instanceName string) (redisclient.LegacyFacts, bool, bool) {
	facts := redisclient.LegacyFacts{NewPodAddrs: map[string]string{}}
	newPodsExist := false
	for _, p := range pods {
		switch {
		case isLegacyClusterPod(p.Name, instanceName):
			if n := gt.Nodes[p.Name]; n != nil {
				facts.LegacyNodeIDs = append(facts.LegacyNodeIDs, n.NodeID)
				if n.Reachable && p.IP != "" {
					facts.SeedAddrs = append(facts.SeedAddrs, addrOf(p.IP))
				}
			}
		case isNewShardPod(p.Name, instanceName):
			newPodsExist = true
			if p.IP != "" {
				facts.NewPodAddrs[p.Name] = addrOf(p.IP)
			}
		}
	}
	return facts, allLegacyPodsReady(pods, instanceName), newPodsExist
}

// restrictToLegacyMesh removes from gt.Nodes every new-shard pod that is NOT yet a member
// of the legacy-rooted cluster mesh (a fresh, un-MET pod is its own single-node cluster in
// a separate partition). This reconstructs the migration plan's contract that gt.Nodes
// reflects mesh membership: an un-MET new pod is absent, a MET one is present with state.
// Legacy pods are never removed. It is a no-op when there are no new pods, and (for safety)
// when no partition containing a legacy node can be identified.
func restrictToLegacyMesh(gt *redisclient.ClusterGroundTruth, legacyNodeIDs []string, instanceName string) {
	legacySet := make(map[string]bool, len(legacyNodeIDs))
	for _, id := range legacyNodeIDs {
		legacySet[id] = true
	}
	// Find the partition that contains a legacy node — the real cluster mesh.
	var meshSet map[string]bool
	for _, part := range gt.Partitions {
		isLegacyPartition := false
		for _, id := range part {
			if legacySet[id] {
				isLegacyPartition = true
				break
			}
		}
		if isLegacyPartition {
			meshSet = make(map[string]bool, len(part))
			for _, id := range part {
				meshSet[id] = true
			}
			break
		}
	}
	if meshSet == nil {
		return // cannot identify the legacy mesh; do not filter (safe default)
	}
	for podName, n := range gt.Nodes {
		if isNewShardPod(podName, instanceName) && !meshSet[n.NodeID] {
			delete(gt.Nodes, podName)
		}
	}
}

// addrOf formats a pod IP as the redis dial address (IP:RedisPort).
func addrOf(ip string) string {
	return fmt.Sprintf("%s:%d", ip, littleredv1alpha1.RedisPort)
}

// redisContainerReady reports the kubelet readiness of a pod's redis container.
func redisContainerReady(pod *corev1.Pod) bool {
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.Name == ComponentRedis {
			return cs.Ready
		}
	}
	return false
}

// migrateLegacyCluster runs one step of the ADR-013 in-place legacy→per-shard cluster
// migration state machine. It is called at the top of reconcileCluster, before the rest of
// the cluster reconcile: when it returns handled=true the caller returns res immediately, so
// the steady-state repair loop is fully suspended while a migration is in flight (ADR-013 §6).
//
// It is a fenced unit (this file): the migration decision is the pure PlanClusterMigration
// seam; this driver only performs the I/O (gather, MEET/move/REPLICATE/FORGET, STS delete)
// and re-derives the phase from live state every pass — nothing load-bearing is persisted
// (status.cluster.migration is a monitoring surface only, ADR-006/§7).
//
// Returns (res, handled, err): handled=false means "not migrating — run the normal reconcile".
func (r *LittleRedReconciler) migrateLegacyCluster(
	ctx context.Context, lr *littleredv1alpha1.LittleRed,
) (ctrl.Result, bool, error) {
	fast, _ := lr.GetRequeueIntervals()
	log := r.getLogger(ctx, lr, LogCategoryRecon)

	legacy, err := r.detectLegacyClusterStatefulSet(ctx, lr)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if !legacy {
		// No legacy STS. If we were migrating, this is the Complete transition: clear the
		// monitoring status and let the steady-state reconcile resume next pass.
		if lr.Status.Cluster != nil && lr.Status.Cluster.Migration != nil {
			log.Info("Legacy cluster migration complete: legacy StatefulSet gone; resuming steady-state reconcile")
			r.event(lr, corev1.EventTypeNormal, "MigrationComplete",
				"Legacy single-StatefulSet cluster migrated to the per-shard layout; legacy workload removed")
			lr.Status.Cluster.Migration = nil
			if uerr := r.persistStatus(ctx, lr); uerr != nil {
				return ctrl.Result{}, true, uerr
			}
			return ctrl.Result{RequeueAfter: fast}, true, nil
		}
		return ctrl.Result{}, false, nil
	}

	// hold escape hatch (ADR-013 §3): non-mutating.
	if lr.Annotations[AnnotationMigrateLegacySTS] == migrateLegacyHoldValue {
		log.Info("Legacy cluster migration held by annotation; no changes will be made")
		r.ensureMigrationStarted(lr, lr.Spec.Cluster)
		r.setMigrationCondition(lr, "MigrationHeld",
			fmt.Sprintf("Migration held by %s=%s; no changes will be made until the annotation is removed",
				AnnotationMigrateLegacySTS, migrateLegacyHoldValue))
		if uerr := r.persistStatus(ctx, lr); uerr != nil {
			return ctrl.Result{}, true, uerr
		}
		return ctrl.Result{RequeueAfter: fast}, true, nil
	}

	cluster := lr.Spec.Cluster
	if cluster == nil {
		// Cluster mode with no cluster spec should not happen; let the normal reconcile handle it.
		return ctrl.Result{}, false, nil
	}
	shards := cluster.Shards
	rps := clusterReplicasPerShard(cluster)

	// Gather ground truth from the legacy cluster (component=cluster selects legacy + new).
	gt, pods, err := r.gatherFromLegacy(ctx, lr)
	if err != nil {
		return ctrl.Result{}, true, err
	}

	facts, allLegacyReady, newPodsExist := buildLegacyFactsFromPods(pods, gt, lr.Name)

	r.ensureMigrationStarted(lr, cluster)

	if len(facts.SeedAddrs) == 0 {
		log.Info("Legacy cluster migration: no reachable legacy seed node yet; waiting")
		r.setMigrationCondition(lr, "MigrationWaitingSeed",
			"No reachable legacy cluster node to use as a migration seed; waiting")
		if uerr := r.persistStatus(ctx, lr); uerr != nil {
			return ctrl.Result{}, true, uerr
		}
		return ctrl.Result{RequeueAfter: fast}, true, nil
	}

	// Entry gates (ADR-013 §2/§5). Enforced ONLY before any new-shard pod exists — i.e. on
	// the intact legacy cluster. Once migration is underway, a drained legacy master would
	// false-fail the shape check (it owns zero slots), so we do not re-gate.
	if !newPodsExist {
		if !redisclient.LegacyMigrationReady(gt, allLegacyReady) {
			log.Info("Legacy cluster not yet healthy enough to begin migration; waiting")
			r.setMigrationCondition(lr, "MigrationWaitingHealthy",
				"Legacy cluster is not currently healthy (needs cluster_state=ok, all 16384 slots assigned, "+
					"all legacy pods Ready, and a reachable master quorum) — waiting before starting migration")
			if uerr := r.persistStatus(ctx, lr); uerr != nil {
				return ctrl.Result{}, true, uerr
			}
			return ctrl.Result{RequeueAfter: fast}, true, nil
		}
		if !redisclient.LegacyShapePreserved(gt, shards, rps) {
			return r.refuseUnsupportedMigration(ctx, lr, shards, rps)
		}
	}

	// Restrict gt to the legacy-rooted mesh so a fresh, un-MET new pod (its own single-node
	// cluster in a separate partition) does not read as MET (the plan's gt.Nodes contract).
	restrictToLegacyMesh(gt, facts.LegacyNodeIDs, lr.Name)

	plan := redisclient.PlanClusterMigration(gt, shards, rps, lr.Name, facts)
	r.setMigrationStatus(lr, plan)

	return r.executeMigrationPhase(ctx, lr, gt, facts, plan)
}

// executeMigrationMeets joins the not-yet-met new per-shard pods into the legacy cluster
// mesh via a reachable legacy seed.
//
// Attribution before introduction (LR-043): the plan's MEET targets are addresses of pods
// that are not yet mesh members, taken from the cache-backed pod list — so, exactly as in
// the repair loop's Step 1, a stale IP recycled onto another instance's Redis pod would be
// MEETed, and MEET validates nothing about membership on either side. The pure plan cannot
// make this call: restrictToLegacyMesh removes both "unidentified" and "identified but not
// yet in the mesh" pods from gt.Nodes, so the evidence is gone by the time it runs. Hence
// the check lives here, where the address can be probed directly — one CLUSTER NODES per
// un-met pod, in the Meet phase only.
//
// The seed is NOT put through the same two guards: it is a legacy {name}-cluster-N pod of
// an already self-consistent cluster, identified this pass and gated by the
// LegacyShapePreserved facts, and it is the node the migration's whole ground truth is
// anchored on (restrictToLegacyMesh keys the mesh off the legacy partition). Note the
// original reason for this exemption — that the attribution predicate's slot-alignment
// clause would refuse a legitimate single-node legacy cluster — has since dissolved with
// that clause's removal; gating the seed too is now cheap and safe, and is left as a
// follow-up rather than folded in here unreasoned.
func (r *LittleRedReconciler) executeMigrationMeets(
	ctx context.Context,
	lr *littleredv1alpha1.LittleRed,
	gt *redisclient.ClusterGroundTruth,
	facts redisclient.LegacyFacts,
	plan redisclient.MigrationPlan,
	clusterClient *redisclient.ClusterClient,
) {
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	seed := facts.SeedAddrs[0]

	ourIDs := make(map[string]bool, len(gt.Nodes))
	for _, n := range gt.Nodes {
		if n.Reachable && n.NodeID != "" {
			ourIDs[n.NodeID] = true
		}
	}
	podNameOfAddr := make(map[string]string, len(facts.NewPodAddrs))
	for podName, a := range facts.NewPodAddrs {
		podNameOfAddr[a] = podName
	}

	for _, addr := range plan.Meets {
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			host = addr
		}
		podName := podNameOfAddr[addr]
		// Primary guard (LR-043): confirm the address against the API server (uncached)
		// before introducing it. The addresses come from a cached pod List, and a new pod
		// crashing/rescheduling mid-migration (the LR-025 chaos shape) is exactly how a
		// stale one gets here. Meet phase only, one GET per not-yet-met pod.
		if ok, why := r.confirmPodIP(ctx, lr.Namespace, podName, host); !ok {
			auditLog.Info("Migration MEET: address is no longer confirmed as this pod's; skipping",
				"target", host, "pod", podName, "reason", why)
			continue
		}
		cand := redisclient.MeetCandidate{PodName: podName, PodIP: host}
		if view, viewErr := clusterClient.GetClusterNodes(ctx, addr); viewErr == nil {
			cand = redisclient.MeetCandidateFromNodes(podName, host, "", view)
		} else {
			auditLog.Info("Migration MEET: could not read the target's own cluster view; not meeting it this pass",
				"target", host, "error", viewErr)
		}
		if v := redisclient.AttributeMeetTarget(cand, ourIDs); !v.Allowed() {
			auditLog.Info("Migration MEET: skipping target not attributable to this instance",
				"target", host, "pod", cand.PodName, "nodeID", cand.NodeID, "verdict", v)
			continue
		}
		auditLog.Info("Migration MEET: joining new pod into the legacy cluster", "seed", seed, "target", host)
		if err := clusterClient.ClusterMeet(ctx, seed, host, littleredv1alpha1.RedisPort); err != nil {
			auditLog.Error(err, "Migration MEET failed; will retry", "target", host)
		}
	}
}

// executeMigrationPhase performs the idempotent I/O for the planned phase, sets the
// in-progress condition, persists the monitoring status, and returns a fast requeue with
// handled=true. Every action is safe to repeat: the next gather re-derives the phase.
func (r *LittleRedReconciler) executeMigrationPhase(
	ctx context.Context,
	lr *littleredv1alpha1.LittleRed,
	gt *redisclient.ClusterGroundTruth,
	facts redisclient.LegacyFacts,
	plan redisclient.MigrationPlan,
) (ctrl.Result, bool, error) {
	fast, _ := lr.GetRequeueIntervals()
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)

	password := r.getRedisPassword(ctx, lr)
	clusterClient := redisclient.NewClusterClient(password, lr.Spec.TLS.Enabled)

	log.Info("Legacy cluster migration step", "phase", plan.Phase,
		"shardsMoved", plan.ShardsMoved, "totalShards", plan.TotalShards, "reason", plan.Reason)

	switch plan.Phase {
	case redisclient.MigrationStandup:
		// Create the new per-shard StatefulSets (+ shared Services/ConfigMap). Deliberately
		// does NOT delete the legacy PDB (kept until Decommission) and never runs the repair loop.
		if err := r.ensureMigrationResources(ctx, lr); err != nil {
			return ctrl.Result{}, true, err
		}

	case redisclient.MigrationMeet:
		r.executeMigrationMeets(ctx, lr, gt, facts, plan, clusterClient)

	case redisclient.MigrationReplicate:
		// Attach each new pod as a slot-less replica of the node currently owning its shard's
		// range (legacy master pre-failover, {name}-shard-K-0 post-failover). It full-syncs; the
		// pure plan only emits an attach the executing node already knows via gossip (else defers).
		//
		// Defensive (belt-and-suspenders, MIGRATION_CHAOS_SELF_REPLICATE_DEADLOCK): a REPLICATE
		// whose target is the pod's OWN NodeID is rejected by Redis (ERR Can't replicate myself)
		// and, retried every pass, would wedge the migration forever. The pure planner already
		// refuses to emit one; this guard keeps a future plan regression from deadlocking.
		selfID := map[string]string{} // dial addr -> that node's own NodeID
		for _, n := range gt.Nodes {
			if n.PodIP != "" {
				selfID[addrOf(n.PodIP)] = n.NodeID
			}
		}
		for _, ra := range plan.Replicates {
			if ra.ReplicaAddr == "" {
				continue
			}
			if selfID[ra.ReplicaAddr] == ra.MasterID {
				auditLog.Info("Migration Replicate: skipping REPLICATE self (node already owns its shard's range)",
					"replica", ra.ReplicaAddr, "owner", ra.MasterID)
				continue
			}
			auditLog.Info("Migration Replicate: attaching new pod as a slot-less replica of its range owner",
				"replica", ra.ReplicaAddr, "owner", ra.MasterID)
			if err := clusterClient.ClusterReplicate(ctx, ra.ReplicaAddr, ra.MasterID); err != nil {
				auditLog.Error(err, "Migration replicate failed; will retry", "replica", ra.ReplicaAddr)
			}
		}

	case redisclient.MigrationFailover:
		// Promote a synced {name}-shard-K-0 to own its range: a coordinated CLUSTER FAILOVER
		// (atomic ownership flip; the legacy master demotes to a live replica), or a forced
		// TAKEOVER only on the §7 edge (range owner unreachable + this replica confirmed synced).
		for _, fo := range plan.Failovers {
			if fo.Addr == "" {
				continue
			}
			if fo.Force {
				auditLog.Info("Migration Failover: forced TAKEOVER of synced new master (range owner unreachable)", "master", fo.Addr)
				if err := clusterClient.ClusterFailoverTakeover(ctx, fo.Addr); err != nil {
					auditLog.Error(err, "Migration forced failover (TAKEOVER) failed; will retry", "master", fo.Addr)
				}
				continue
			}
			auditLog.Info("Migration Failover: coordinated CLUSTER FAILOVER promoting synced new master", "master", fo.Addr)
			if err := clusterClient.ClusterFailover(ctx, fo.Addr); err != nil {
				auditLog.Error(err, "Migration coordinated failover failed; will retry", "master", fo.Addr)
			}
		}

	case redisclient.MigrationDecommission:
		r.forgetLegacyNodes(ctx, lr, gt, clusterClient, plan.Forgets)
		if plan.DeleteLegacy {
			auditLog.Info("Migration Decommission: deleting legacy StatefulSet + PDB (all legacy nodes drained, zero slots)")
			if err := r.deleteIfExists(ctx, lr, &appsv1.StatefulSet{}, clusterStatefulSetName(lr)); err != nil {
				return ctrl.Result{}, true, err
			}
			if err := r.deleteIfExists(ctx, lr, &policyv1.PodDisruptionBudget{}, clusterPodDisruptionBudgetName(lr)); err != nil {
				return ctrl.Result{}, true, err
			}
		}

	case redisclient.MigrationComplete:
		// No legacy nodes remain in the mesh but the STS lingered this pass; next pass (once
		// the STS is gone) hits the Complete transition at the top. Nothing to do here.
	}

	r.setMigrationCondition(lr, "MigrationInProgress",
		fmt.Sprintf("Migrating legacy cluster to per-shard layout: phase %s (%d/%d shards moved)",
			plan.Phase, plan.ShardsMoved, plan.TotalShards))
	if err := r.persistStatus(ctx, lr); err != nil {
		return ctrl.Result{}, true, err
	}
	return ctrl.Result{RequeueAfter: fast}, true, nil
}

// forgetLegacyNodes broadcasts CLUSTER FORGET for every drained legacy node ID to every
// reachable mesh node, skipping the node that would be forgetting itself. Mirrors the repair
// loop's ghost FORGET broadcast (only reachable targets, to avoid blocking on dead dials —
// LR-012). Failures are logged and retried next pass (the node may already be gone).
func (r *LittleRedReconciler) forgetLegacyNodes(
	ctx context.Context,
	lr *littleredv1alpha1.LittleRed,
	gt *redisclient.ClusterGroundTruth,
	clusterClient *redisclient.ClusterClient,
	forgets []string,
) {
	if len(forgets) == 0 {
		return
	}
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	for _, id := range forgets {
		for _, node := range gt.Nodes {
			if !node.Reachable || node.NodeID == id {
				continue // skip dead dials and self-forget
			}
			addr := addrOf(node.PodIP)
			if err := clusterClient.ClusterForget(ctx, addr, id); err != nil {
				auditLog.Info("Migration FORGET failed (node may already be gone)", "node", addr, "legacy", id, "error", err)
			}
		}
	}
}

// gatherFromLegacy lists the ACTUAL cluster-member pods (component=cluster selector — selects
// both legacy {name}-cluster-N and new {name}-shard-K-M pods), builds the pure per-pod facts,
// and gathers full ground truth over every pod that has an IP. Everything downstream is by
// IP/ID, so the returned gt covers legacy and new nodes uniformly (ADR-013 §2.3). Unlike the
// steady-state gatherGroundTruth (which enumerates only the NEW expected pods via
// ClusterPodRefs), this can see legacy pods that exist before the new StatefulSets do.
func (r *LittleRedReconciler) gatherFromLegacy(
	ctx context.Context, lr *littleredv1alpha1.LittleRed,
) (*redisclient.ClusterGroundTruth, []migPodFact, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(lr.Namespace),
		client.MatchingLabels(clusterSelectorLabels(lr)),
	); err != nil {
		return nil, nil, err
	}

	clusterPods := make(map[string]string)
	facts := make([]migPodFact, 0, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		facts = append(facts, migPodFact{
			Name:       pod.Name,
			IP:         pod.Status.PodIP,
			RedisReady: redisContainerReady(pod),
		})
		if pod.Status.PodIP != "" {
			clusterPods[pod.Status.PodIP] = pod.Name
		}
	}

	password := r.getRedisPassword(ctx, lr)
	g := &operatorGatherer{password: password, tlsEnabled: lr.Spec.TLS.Enabled}
	gt := redisclient.GatherClusterGroundTruth(ctx, g, clusterPods)
	return gt, facts, nil
}

// ensureMigrationResources creates/updates the resources the new per-shard layout needs
// (ConfigMap, shared headless + client Services, per-shard StatefulSets) WITHOUT touching
// the legacy PDB — that is deleted only at Decommission, so legacy pods keep their disruption
// protection while they still hold slots. It never runs the repair loop or per-shard PDB
// reconcile (which would delete the legacy {name}-cluster-pdb early).
func (r *LittleRedReconciler) ensureMigrationResources(ctx context.Context, lr *littleredv1alpha1.LittleRed) error {
	if err := r.reconcileClusterConfigMap(ctx, lr); err != nil {
		return err
	}
	if err := r.reconcileClusterHeadlessService(ctx, lr); err != nil {
		return err
	}
	if err := r.reconcileClusterStatefulSet(ctx, lr); err != nil {
		return err
	}
	return r.reconcileClusterClientService(ctx, lr)
}

// ensureMigrationStarted stamps status.cluster.migration.StartedAt (and TotalShards) on the
// first migration pass. StartedAt is the "when did we enter migration mode" monitoring marker,
// mirroring wipeDeadlockSince (LR-023).
func (r *LittleRedReconciler) ensureMigrationStarted(lr *littleredv1alpha1.LittleRed, cluster *littleredv1alpha1.ClusterSpec) {
	if lr.Status.Cluster == nil {
		lr.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{}
	}
	if lr.Status.Cluster.Migration == nil {
		now := metav1.Now()
		total := 0
		if cluster != nil {
			total = cluster.Shards
		}
		lr.Status.Cluster.Migration = &littleredv1alpha1.ClusterMigrationStatus{
			StartedAt:   &now,
			TotalShards: total,
		}
	}
}

// setMigrationStatus updates the monitoring surface from the current plan (phase re-derived
// live every pass, never read back — ADR-006). StartedAt is preserved once stamped.
func (r *LittleRedReconciler) setMigrationStatus(lr *littleredv1alpha1.LittleRed, plan redisclient.MigrationPlan) {
	if lr.Status.Cluster == nil {
		lr.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{}
	}
	if lr.Status.Cluster.Migration == nil {
		lr.Status.Cluster.Migration = &littleredv1alpha1.ClusterMigrationStatus{}
	}
	m := lr.Status.Cluster.Migration
	m.Phase = string(plan.Phase)
	m.ShardsMoved = plan.ShardsMoved
	m.TotalShards = plan.TotalShards
}

// setMigrationCondition sets the in-progress LegacyClusterTopology condition (repurposed from
// ADR-007's terminal Failed into ADR-013's phase-carrying in-progress condition). Phase stays
// Initializing (a non-failed value) throughout migration.
func (r *LittleRedReconciler) setMigrationCondition(lr *littleredv1alpha1.LittleRed, reason, msg string) {
	if lr.Status.Phase != littleredv1alpha1.PhasePending {
		lr.Status.Phase = littleredv1alpha1.PhaseInitializing
	}
	lr.Status.Status = statusMigrating
	meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
}

// refuseUnsupportedMigration is the terminal refuse for a non-shape-preserving legacy topology
// (ADR-013 §5): the 1:1 range mapping only holds for the identical shape, so we refuse rather
// than guess. Delegates to reportLegacyMigrationRefused (the repurposed reportLegacyClusterTopology).
func (r *LittleRedReconciler) refuseUnsupportedMigration(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, shards, rps int,
) (ctrl.Result, bool, error) {
	msg := fmt.Sprintf("Legacy cluster topology is not shape-preserving (expected exactly %d slot-owning "+
		"masters each owning one aligned range, and %d members). Migration supports only an identically-"+
		"shaped in-place move; change shard/replica counts with a reshard AFTER migration. Refusing.",
		shards, shards*(1+rps))
	res, err := r.reportLegacyMigrationRefused(ctx, lr, "MigrationUnsupportedTopology", msg)
	return res, true, err
}

// persistStatus writes status; a conflict is swallowed (the caller requeues anyway).
func (r *LittleRedReconciler) persistStatus(ctx context.Context, lr *littleredv1alpha1.LittleRed) error {
	if err := r.Status().Update(ctx, lr); err != nil && !apierrors.IsConflict(err) {
		return err
	}
	return nil
}
