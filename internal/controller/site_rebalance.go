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
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// site_rebalance.go holds the PURE, I/O-free master-BALANCE planner for multi-site cluster
// mode (ADR-012 decision D2), sibling of planSiteTakeover / planClusterWipeRecovery /
// planFencedRejoinRecycle. It is a *prevention* seam for the drift-refusal that the
// site-loss takeover (D1) surfaces: by keeping the slot-serving masters spread so that no
// single site can hold a Redis master MAJORITY, it narrows the odd/drifted split that would
// otherwise refuse (or need the allowUnsafeSiteTakeover opt-in) on a whole-site loss.
//
// The pure planner (planMasterRebalance) is wired into the multi-site reconcile spine on the
// steady-state HEALTHY path only (reconcileMultiSiteCluster): buildMasterRebalanceInput projects
// the gather onto the planner input, and rebalanceMasters executes at most ONE graceful
// CLUSTER FAILOVER per reconcile (LR-021-style serialization — a graceful failover is a per-shard
// write pause), behind a quiet gate and a post-move settle/anti-thrash cooldown
// (status.multiSite.rebalancePendingSince). It is maintenance, not recovery: it never runs during
// site-loss takeover, repair, or bringup. Cluster-mode multi-site only — ADR-009 declines master
// balancing in single-site mode, so rebalanceMasters is never called there.

// masterBalanceTarget is the D2 invariant threshold: the maximum number of slot-serving
// masters any single site may hold so that NO site reaches a Redis master majority
// (majority of the `shards` masters is (shards/2)+1). It is ceil(shards/2)-1 — one below the
// count that would give a site the majority on the OTHER side of an even split.
//
//	shards: 2 => 0 (dual-site is unsatisfiable — two masters must live somewhere)
//	shards: 3 => 1   shards: 4 => 1   shards: 5 => 2   shards: 6 => 2
//
// A site holding more than this owns enough masters that losing the complementary site(s)
// can strand the survivors below quorum with the lost side potentially still serving — the
// exact drift the takeover fence refuses on.
func masterBalanceTarget(shards int) int { return (shards+1)/2 - 1 }

// masterRebalanceInput is the pure view the balancer decides on: where each shard's
// slot-serving master currently lives, and which sites hold a PROMOTABLE replica of each
// shard (a graceful CLUSTER FAILOVER target). Indexed by shard.
type masterRebalanceInput struct {
	// masterSite[shard] is the site currently hosting shard's slot-serving master. Its
	// length is the shard count K (the denominator of the invariant).
	masterSite []string
	// replicaSites[shard] lists the sites that hold a promotable replica of shard — the only
	// sites its mastership may be moved to. The master's own site need not be listed.
	replicaSites [][]string
}

// masterMove is one graceful promotion the balancer recommends: promote shard's replica in
// toSite to be its slot-serving master (the current master demotes to a replica).
type masterMove struct {
	shard  int
	toSite string
}

// planMasterRebalance returns the MINIMAL, deterministic set of graceful promotions that
// restores the D2 invariant max_i m_i <= masterBalanceTarget(K) — one move per unit of
// excess, so the returned length equals the total excess and is minimal. It returns nil when
//   - the placement already satisfies the invariant, OR
//   - no set of moves can restore it under the replica-placement constraints (unsatisfiable:
//     e.g. K=2, or K=4 with only 3 sites, or a donor whose over-target shards have no
//     promotable replica in a site that can still receive) — the balancer never emits a
//     partial, non-restoring churn.
//
// A move is only ever emitted from a site currently AT the maximum-over-target load onto a
// site that has a promotable replica of that shard AND stays <= target after receiving it, so
// every move strictly reduces the excess and none creates a new violation. Selection is fully
// deterministic (donors by count desc then name asc; recipients by count asc then name asc;
// shards by ascending index) so the same input always yields the same plan. This is a greedy
// solver, complete whenever a straightforward (chain-free) rebalancing exists — which it does
// for the multi-site drift cases it targets, since every shard has cross-site replicas.
func planMasterRebalance(in masterRebalanceInput) []masterMove {
	k := len(in.masterSite)
	if k == 0 {
		return nil
	}
	target := masterBalanceTarget(k)

	counts := map[string]int{}
	for _, s := range in.masterSite {
		counts[s]++
	}
	if maxMasterCount(counts) <= target {
		return nil
	}

	moved := make([]bool, k)
	var moves []masterMove
	for maxMasterCount(counts) > target {
		mv, ok := bestRebalanceMove(in, counts, target, moved)
		if !ok {
			return nil // stuck: the invariant cannot be fully restored — no partial churn
		}
		moves = append(moves, mv)
		counts[in.masterSite[mv.shard]]--
		counts[mv.toSite]++
		moved[mv.shard] = true
	}
	return moves
}

// bestRebalanceMove finds the next deterministic improving move, or ok=false when none of the
// over-target donors can shed a shard to a site that can still receive it.
func bestRebalanceMove(in masterRebalanceInput, counts map[string]int, target int, moved []bool) (masterMove, bool) {
	// Donors: sites over target, drained heaviest-first (tie-broken by name) so the same
	// input is deterministic. We try each in turn — a heavier donor with no receivable
	// replica must not mask a lighter donor that can move.
	donors := make([]string, 0, len(counts))
	for site, c := range counts {
		if c > target {
			donors = append(donors, site)
		}
	}
	sort.Slice(donors, func(i, j int) bool {
		if counts[donors[i]] != counts[donors[j]] {
			return counts[donors[i]] > counts[donors[j]]
		}
		return donors[i] < donors[j]
	})

	for _, donor := range donors {
		// Ascending shard index for determinism; first shard mastered at donor, not yet
		// moved, with a receivable recipient wins.
		for shard := 0; shard < len(in.masterSite); shard++ {
			if moved[shard] || in.masterSite[shard] != donor {
				continue
			}
			if recipient, ok := bestRecipient(in.replicaSites[shard], donor, counts, target); ok {
				return masterMove{shard: shard, toSite: recipient}, true
			}
		}
	}
	return masterMove{}, false
}

// bestRecipient picks the deterministic recipient for a shard's mastership among the sites
// that hold a promotable replica of it: a site other than the donor whose current master
// count is strictly below target (so it stays <= target after receiving), chosen by lowest
// count then lowest name. ok=false when no replica site can receive.
func bestRecipient(replicaSites []string, donor string, counts map[string]int, target int) (string, bool) {
	best := ""
	found := false
	for _, site := range replicaSites {
		if site == donor || counts[site] >= target {
			continue
		}
		if !found || counts[site] < counts[best] || (counts[site] == counts[best] && site < best) {
			best = site
			found = true
		}
	}
	return best, found
}

// maxMasterCount returns the largest per-site master count in counts (0 for an empty map).
func maxMasterCount(counts map[string]int) int {
	max := 0
	for _, c := range counts {
		if c > max {
			max = c
		}
	}
	return max
}

// --- Input builder (pure projection of the gather) ---------------------------------------

// buildMasterRebalanceInput projects the gathered stretched-cluster ground truth onto the pure
// planMasterRebalance input (ADR-012 D2). Per shard K it records:
//   - masterSite[K]: the site of the reachable slot-serving master of shard K (empty if none —
//     which does not happen on the steady-state healthy path the rebalancer runs on, since every
//     shard then has a reachable master; an empty entry there is inert, it merely counts as a
//     phantom "" site the planner cannot move onto).
//   - replicaSites[K]: the sites holding a reachable, IN-SYNC (link:up) promotable replica of
//     shard K — the only sites its mastership may be gracefully failed over to (CLUSTER FAILOVER
//     coordinates the handoff with the replica, so a lagging/disconnected replica is not a valid
//     target). Deduplicated and sorted for determinism.
//
// Shard membership is read from the pod name (redisclient.ShardIndexFromPodName), the site from
// the site-qualified ground-truth key (siteFromKey) — mirroring buildSiteTakeoverInput.
func buildMasterRebalanceInput(gt *redisclient.ClusterGroundTruth, shards int) masterRebalanceInput {
	in := masterRebalanceInput{
		masterSite:   make([]string, shards),
		replicaSites: make([][]string, shards),
	}
	if gt == nil || shards <= 0 {
		return in
	}

	replicaSet := make([]map[string]bool, shards)
	for k := range shards {
		replicaSet[k] = map[string]bool{}
	}

	for _, n := range gt.Nodes {
		if !n.Reachable {
			continue
		}
		k := redisclient.ShardIndexFromPodName(n.PodName)
		if k < 0 || k >= shards {
			continue
		}
		site := siteFromKey(n.PodName)
		if n.Role == RoleMaster && len(n.Slots) > 0 {
			in.masterSite[k] = site
			continue
		}
		// A reachable member that is not a slot-serving master: a promotable graceful-failover
		// target only if it is a replica whose replication link to its master is up (in-sync).
		if n.Role == RoleReplica && n.LinkStatus == "up" {
			replicaSet[k][site] = true
		}
	}

	for k := range shards {
		sites := make([]string, 0, len(replicaSet[k]))
		for s := range replicaSet[k] {
			sites = append(sites, s)
		}
		sort.Strings(sites)
		in.replicaSites[k] = sites
	}
	return in
}

// --- Executor + gating (steady-state healthy-path master-balance drift correction) -------

// masterRebalanceCooldown is the post-move settle window: after issuing one graceful
// CLUSTER FAILOVER, the rebalancer neither re-evaluates nor re-issues for at least this long, so a
// failover in flight is never restarted (anti-thrash) and the next gather reflects the new master
// distribution before another move is considered. Convergence of a multi-move drift is therefore
// one move per cooldown — deliberately slow (pillar 3.5: bounded churn, maintenance not hot-path).
const masterRebalanceCooldown = 30 * time.Second

// rebalanceMasters corrects master-distribution drift (ADR-012 D2) on the steady-state HEALTHY
// path of the multi-site reconcile. It maintains the balance invariant max_i m_i <=
// ceil(K/2)-1 so that no single site holds a Redis master majority — the placement property that
// keeps a whole-site loss inside planSiteTakeover's fence-safe even-split (D1), rather than the
// drift-refusal. It issues at most ONE graceful CLUSTER FAILOVER per reconcile.
//
// Returns acted=true only when it issued a failover (caller requeues fast, skipping the status
// update this pass); false lets the caller fall through to the normal status update — including
// while waiting out the post-move cooldown, so status stays fresh during convergence.
//
// Multi-site cluster mode only. Single-site cluster mode never calls this (ADR-009 stands there).
func (r *LittleRedReconciler) rebalanceMasters(ctx context.Context, lr *littleredv1alpha1.LittleRed, gt *redisclient.ClusterGroundTruth, obs []siteObservation) (bool, error) {
	if !isMultiSite(lr) || gt == nil {
		return false, nil
	}
	stateLog := r.getLogger(ctx, lr, LogCategoryState)

	// Quiet gate (Rule-A analog, mirrors the takeover): no terminating survivor pod in any
	// reachable site. On the healthy path an in-flight failover/reshard/rollout would already
	// have failed the health check upstream, so terminating pods are the residual signal.
	for _, o := range obs {
		if !o.reachable {
			continue
		}
		for _, p := range o.pods {
			if p.terminating {
				return false, nil
			}
		}
	}

	shards, _, _ := multiSiteParams(lr)
	in := buildMasterRebalanceInput(gt, shards)
	moves := planMasterRebalance(in)

	if len(moves) == 0 {
		// Invariant holds (or is unsatisfiable — no partial churn). Clear any settle marker.
		return false, r.clearRebalancePendingSince(ctx, lr)
	}

	// Drift exists. Honour the post-move cooldown: while a prior graceful failover is still
	// settling, neither re-evaluate nor re-issue — fall through to status (acted=false).
	if lr.Status.MultiSite != nil && lr.Status.MultiSite.RebalancePendingSince != nil {
		if time.Since(lr.Status.MultiSite.RebalancePendingSince.Time) < masterRebalanceCooldown {
			stateLog.Info("Master-balance drift persists; waiting out the post-move settle cooldown before the next graceful failover",
				"pendingMoves", len(moves))
			return false, nil
		}
	}

	// Execute exactly ONE move this pass (LR-021-style serialization).
	mv := moves[0]
	ip, key, ok := r.pickRebalanceReplica(gt, mv.shard, mv.toSite)
	if !ok {
		// The planner selected a move whose in-sync replica is not currently resolvable to a
		// dial address (raced away between gather and now). Skip this pass; re-derived next time.
		stateLog.Info("Master-balance: selected move has no resolvable in-sync replica this pass; retrying",
			"shard", mv.shard, "toSite", mv.toSite)
		return false, nil
	}

	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	auditLog.Info("Master-balance: correcting drift with a graceful CLUSTER FAILOVER on an under-mastered site's replica",
		"shard", mv.shard, "toSite", mv.toSite, "replica", key)

	password := r.getRedisPassword(ctx, lr)
	cc := redisclient.NewClusterClient(password, lr.Spec.TLS.Enabled)
	addr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.RedisPort)
	if err := cc.ClusterFailover(ctx, addr); err != nil {
		// A failed graceful failover is not fatal — retry next reconcile. Do NOT stamp the
		// marker (nothing is in flight to settle), so the next pass re-attempts promptly.
		auditLog.Error(err, "Master-balance: graceful CLUSTER FAILOVER failed (will retry)", "replica", key)
		return false, nil
	}

	// Stamp the settle/anti-thrash marker and requeue.
	return true, r.setRebalancePendingSince(ctx, lr, metav1.Now())
}

// pickRebalanceReplica resolves the reachable, in-sync (link:up) replica of shard `shard` in
// site `toSite` to promote, returning its pod IP and ground-truth key. Deterministic (lowest key)
// so retries hit the same target. ok=false when none is currently resolvable (raced away).
func (r *LittleRedReconciler) pickRebalanceReplica(gt *redisclient.ClusterGroundTruth, shard int, toSite string) (ip, key string, ok bool) {
	for _, n := range gt.Nodes {
		if !n.Reachable || n.PodIP == "" {
			continue
		}
		if n.Role != RoleReplica || n.LinkStatus != "up" {
			continue
		}
		if redisclient.ShardIndexFromPodName(n.PodName) != shard || siteFromKey(n.PodName) != toSite {
			continue
		}
		if !ok || n.PodName < key {
			ip, key, ok = n.PodIP, n.PodName, true
		}
	}
	return ip, key, ok
}

// setRebalancePendingSince stamps Status.MultiSite.RebalancePendingSince to t (retry on
// conflict). It always (re-)stamps: each fired move restarts the settle cooldown, so a multi-move
// convergence advances at exactly one graceful failover per cooldown window.
func (r *LittleRedReconciler) setRebalancePendingSince(ctx context.Context, lr *littleredv1alpha1.LittleRed, t metav1.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.MultiSite == nil {
			latest.Status.MultiSite = &littleredv1alpha1.MultiSiteStatus{}
		}
		latest.Status.MultiSite.RebalancePendingSince = &t
		if lr.Status.MultiSite == nil {
			lr.Status.MultiSite = &littleredv1alpha1.MultiSiteStatus{}
		}
		lr.Status.MultiSite.RebalancePendingSince = &t
		return r.Status().Update(ctx, latest)
	})
}

// clearRebalancePendingSince resets Status.MultiSite.RebalancePendingSince once the balance
// invariant holds again (retry on conflict). No-op if never stamped.
func (r *LittleRedReconciler) clearRebalancePendingSince(ctx context.Context, lr *littleredv1alpha1.LittleRed) error {
	if lr.Status.MultiSite == nil || lr.Status.MultiSite.RebalancePendingSince == nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.MultiSite == nil || latest.Status.MultiSite.RebalancePendingSince == nil {
			return nil
		}
		latest.Status.MultiSite.RebalancePendingSince = nil
		lr.Status.MultiSite.RebalancePendingSince = nil
		return r.Status().Update(ctx, latest)
	})
}
