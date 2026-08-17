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

	corev1 "k8s.io/api/core/v1"
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
// single site can hold a Redis master MAJORITY, it keeps a whole-site loss inside D1's
// fence-safe window instead of the drifted split that refuses (or needs the
// allowUnsafeSiteTakeover opt-in). Where the topology allows, it aims one step further and
// keeps the loss natively electable — see the two thresholds below.
//
// The pure planner (planMasterRebalance) is wired into the multi-site reconcile spine on the
// steady-state HEALTHY path only (reconcileMultiSiteCluster): buildMasterRebalanceInput projects
// the gather onto the planner input, and rebalanceMasters executes at most ONE graceful
// CLUSTER FAILOVER per reconcile (LR-021-style serialization — a graceful failover is a per-shard
// write pause), behind a quiet gate and a post-move settle/anti-thrash cooldown
// (status.multiSite.rebalancePendingSince). It is maintenance, not recovery: it never runs during
// site-loss takeover, repair, or bringup. Cluster-mode multi-site only — ADR-009 declines master
// balancing in single-site mode, so rebalanceMasters is never called there.

// The two ADR-012 D2 balance thresholds. With K = shards = slot-serving masters and
// q = (K/2)+1 the Redis master majority, losing a site that holds `l` masters survives by
// either of two paths, and they impose DIFFERENT conditions:
//
//   - NATIVE — the survivors keep quorum (K−l >= q), so Redis elects on its own within
//     cluster-node-timeout with the operator uninvolved. Bound: l <= K−q = ceil(K/2)-1.
//   - TAKEOVER — the LOST side lacks quorum (l < q), so the survivors hold a "blocking
//     minority" (too few to elect, enough to deny the other side an election), D1's fence
//     holds, and planSiteTakeover fires provably split-brain-free. Bound: l <= q-1 = floor(K/2).
//
// Since K−q <= q−1 always, the takeover condition is the WEAKER one. The two coincide for odd
// K and differ by exactly one for even K — the placement-side reading of D1's blocking-minority
// window (empty for odd K, one value wide for even K).
//
// Conflating them is LR-031: the native bound alone was used to gate BOTH paths, which
// declared the D3-recommended dual-site K=4 "2+2" placement unsurvivable (planSiteTakeover
// fires on it with no opt-in) and stopped the rebalancer from ever maintaining it.

// nativeSurvivalTarget is the STANDARD: the maximum slot-serving masters a site may hold for
// its loss to leave the survivors holding quorum, so Redis recovers natively — faster,
// seamless, and independent of the operator. Exceeding it only downgrades recovery to a
// takeover (the cooldown, plus a live hub); it is not an outage.
//
//	shards: 3 => 1   shards: 4 => 1   shards: 5 => 2   shards: 6 => 2
func nativeSurvivalTarget(shards int) int { return (shards+1)/2 - 1 }

// takeoverSurvivalFloor is the HARD constraint: the maximum slot-serving masters a site may
// hold for its loss to leave the lost side short of quorum, so the survivors hold a blocking
// minority and planSiteTakeover can recover unattended. Exceeding it is an availability
// exposure — that site's loss hits the drift-refusal and needs a human or
// multiSite.allowUnsafeSiteTakeover.
//
//	shards: 3 => 1   shards: 4 => 2   shards: 5 => 2   shards: 6 => 3
func takeoverSurvivalFloor(shards int) int { return shards / 2 }

// siteLossResilient reports whether the D2 FLOOR is *structurally reachable* for a topology of
// `shards` (K) over `numSites` (S): whether K slot-serving masters can be spread so no single
// site exceeds takeoverSurvivalFloor(K). Total capacity under the floor is S·floor(K/2); below
// K, the pigeonhole principle forces some site over it, and losing that site refuses — the
// topology cannot auto-survive an arbitrary single-site loss (ADR-012 Consequence
// "Reduced-resilience warning": unreachable iff `S·⌊K/2⌋ < K`).
//
// This is a spec-level (structural) predicate — it says nothing about the *current* master
// distribution (drift, owned by the rebalancer); it only decides whether a survivable placement
// exists at all. Given Redis Cluster's three-master minimum (K >= 3) it is false in exactly one
// regime: TWO SITES WITH AN ODD K. For S >= 3 the floor is always reachable (3·⌊K/2⌋ >= K for
// every K >= 3), so the warning never fires there.
//
// Superseded (LR-031): the former "K=4 over 3 sites" headline trap was an artefact of deriving
// this from the native bound. A 2+1+1 split loses at most 2 = ⌊4/2⌋ masters, which is fence-safe,
// so that topology does auto-survive any single-site loss — by takeover for the 2-master site,
// natively for the others.
func siteLossResilient(shards, numSites int) bool {
	return numSites*takeoverSurvivalFloor(shards) >= shards
}

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
// restores the best ADR-012 D2 threshold currently reachable — the native target if a move set
// attains it, else the takeover floor. One move per unit of excess, so the returned length
// equals the total excess and is minimal. It returns nil when
//   - the placement already satisfies the native target, OR
//   - no set of moves attains EITHER threshold under the replica-placement constraints (e.g. a
//     donor whose over-threshold shards have no promotable replica in a site that can still
//     receive) — the balancer never emits a partial, non-restoring churn.
//
// A move is only ever emitted from a site currently AT the maximum-over-threshold load onto a
// site that has a promotable replica of that shard AND stays <= threshold after receiving it, so
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
	// Two tiers (ADR-012 D2). Aim for the native target — a natively-survivable placement is
	// strictly better — but fall back to the takeover floor when the target is out of reach,
	// because the floor is what keeps a site loss recoverable AT ALL. Progress that reaches
	// only the floor is real progress, not partial churn; refusing to move because the target
	// is unreachable is how a topology comes to sit permanently below the floor (LR-031: for
	// K=4 over 2 sites the target is unreachable, so a 3+1 drift was never corrected to 2+2).
	//
	// A placement already satisfying the target satisfies the floor too (target <= floor), so
	// the "no moves needed" and "unsatisfiable" nil returns cannot be confused here.
	if moves := planToThreshold(in, nativeSurvivalTarget(k)); moves != nil {
		return moves
	}
	return planToThreshold(in, takeoverSurvivalFloor(k))
}

// planToThreshold is planMasterRebalance's solver for ONE threshold: the minimal deterministic
// move set restoring max_i m_i <= threshold, nil when already satisfied or unreachable.
func planToThreshold(in masterRebalanceInput, target int) []masterMove {
	k := len(in.masterSite)

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
// path of the multi-site reconcile. It maintains the best threshold the topology allows: the
// takeoverSurvivalFloor at minimum — no site holds a Redis master majority, so a whole-site loss
// stays inside planSiteTakeover's fence-safe window rather than the drift-refusal — and the
// tighter nativeSurvivalTarget where reachable, so the loss is electable natively. It issues at
// most ONE graceful CLUSTER FAILOVER per reconcile.
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

	addr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.RedisPort)
	if err := r.issueGracefulFailover(ctx, lr, addr); err != nil {
		// A failed graceful failover is not fatal — retry next reconcile. Do NOT stamp the
		// marker (nothing is in flight to settle), so the next pass re-attempts promptly.
		auditLog.Error(err, "Master-balance: graceful CLUSTER FAILOVER failed (will retry)", "replica", key)
		return false, nil
	}

	// Report the move. Emitted only AFTER the failover is accepted: announcing a move that
	// did not happen would tell an operator mastership changed when it did not, which is
	// worse than saying nothing. This is the rebalancer's only machine-readable "mastership
	// moved" signal — before it, the actuation LR-031 switched on for the recommended
	// dual-site topology was visible only as an after-the-fact master-count change or by
	// grepping this file's audit prose.
	r.event(lr, corev1.EventTypeNormal, reasonMasterRebalanced, fmt.Sprintf(
		"Master-balance: moved shard %d's mastership to site %q via a graceful CLUSTER FAILOVER "+
			"on replica %s, correcting master-per-site drift (ADR-012 D2). One move per reconcile, "+
			"no data loss.", mv.shard, mv.toSite, key))

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
