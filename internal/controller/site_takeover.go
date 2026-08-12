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
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// site_takeover.go holds the PURE, I/O-free site-loss TAKEOVER planner for multi-site
// cluster mode (ADR-012 decision D1), sibling of planClusterWipeRecovery /
// planGhostMasterRecovery / planLeaderlessRecovery. It decides — behind a strict gate —
// whether the hub operator may issue CLUSTER FAILOVER TAKEOVER on surviving replicas to
// recover a whole site's loss that native Redis failover cannot (a below-quorum survivor set).
//
// A wrong takeover means a LIVE split-brain (two serving masters per shard), so every
// condition is a veto: the planner fires ONLY when all hold, and otherwise waits, holds, or
// refuses. The dangerous ambiguity — "site dead" vs "site alive but partitioned from the
// hub" — is resolved by pure Domain-M quorum ARITHMETIC (D1): the survivors' own
// cluster_state:fail plus the reachable/unreachable slot-serving-master counts. An
// alive-but-isolated site with a master minority has itself fail-stopped (Redis's
// unconditional minority check) within cluster-node-timeout, so the fence — unreachable
// masters short of a majority — is a mechanical split-brain guarantee, decided from the
// survivors' first-hand evidence, never from the hub's dial reachability alone (LR-017).
// (The earlier Kubernetes "witness" failure-detector is deleted; see ADR-012 D1.)

// siteTakeoverAction is the planner's verdict.
type siteTakeoverAction int

const (
	// siteTakeoverNone: not a recoverable site loss — a healthy cluster, or a total loss
	// with no surviving replica to promote (descoped, manual re-bootstrap). Clear the
	// marker; do nothing. Condition 1 fails.
	siteTakeoverNone siteTakeoverAction = iota
	// siteTakeoverWaitNative: the survivors still hold (or could regain) a master majority,
	// so Redis will elect on its own (pillar 3.5). Clear the marker; wait. Condition 2 fails.
	siteTakeoverWaitNative
	// siteTakeoverHold: the cluster is not quiet (condition 6) — a terminating survivor or an
	// in-flight failover/reshard/rollout means Redis may still be resolving this itself.
	// Clear the marker; do nothing this pass.
	siteTakeoverHold
	// siteTakeoverArm: the fire-able signature was just observed. Arm the cooldown
	// (status.multiSite.takeoverPendingSince) and wait (condition 5).
	siteTakeoverArm
	// siteTakeoverWait: the signature persists but the cooldown has not elapsed. Wait.
	siteTakeoverWait
	// siteTakeoverRefuse: the fence is unsafe — the unreachable masters could hold a serving
	// majority (drift), so an isolated-but-alive site might not have fenced itself and a
	// takeover might create a live dual writer. Refuse, surface a condition, keep the marker.
	// Needs the allowUnsafeSiteTakeover opt-in or a human (the fence gate).
	siteTakeoverRefuse
	// siteTakeover: fire — safe. The fence holds: the unreachable slot-serving masters are
	// short of a majority, so an isolated-alive lost side has itself fail-stopped and no
	// split-brain is possible. Issue CLUSTER FAILOVER TAKEOVER on each orphan target.
	siteTakeover
	// siteTakeoverUnsafe: fire despite the drift-unsafe fence, because
	// allowUnsafeSiteTakeover authorizes it. Same actions as siteTakeover, surfaced loudly.
	siteTakeoverUnsafe
)

func (a siteTakeoverAction) String() string {
	switch a {
	case siteTakeoverNone:
		return "None"
	case siteTakeoverWaitNative:
		return "WaitNative"
	case siteTakeoverHold:
		return "Hold"
	case siteTakeoverArm:
		return "Arm"
	case siteTakeoverWait:
		return "Wait"
	case siteTakeoverRefuse:
		return "Refuse"
	case siteTakeover:
		return "Takeover"
	case siteTakeoverUnsafe:
		return "TakeoverUnsafe"
	default:
		return "Unknown"
	}
}

// orphanTarget is the surviving replica chosen to TAKEOVER for one orphaned shard: the
// most-complete survivor (highest replication offset within one lineage — LR-024). Keys
// and IP are ground-truth identifiers the executor resolves to a dial address.
type orphanTarget struct {
	shard   int
	key     string // site-qualified ground-truth key of the replica to promote
	ip      string
	site    string
	offset  int64
	replid  string
	replid2 string
}

// siteTakeoverInput is the pure, I/O-free view the planner decides on. The executor
// (recoverSiteLoss) builds it from the gather alone (no Kubernetes witness — ADR-012 D1).
type siteTakeoverInput struct {
	// clusterFormed reports whether the stretched cluster has ever been bootstrapped —
	// i.e. ANY node in the gathered view (reachable survivor OR a gossip-known dead master)
	// owns at least one slot. It is the discriminator between a genuine site loss and a
	// cluster that has simply not bootstrapped yet: a formed cluster that lost every master
	// still shows its dead masters owning slots in the survivors' gossip (ownership is kept
	// until FORGET/TAKEOVER), whereas a never-bootstrapped cluster has ZERO slots assigned
	// anywhere. Without this, initial bringup — fresh nodes reporting role:master, 0 slots,
	// cluster_state:fail (uncovered) — is indistinguishable from "all masters lost" and the
	// planner would arm/fire a takeover across pods that never formed a cluster, preempting
	// bootstrap (a hard bringup deadlock — LR-026). false ⇒ siteTakeoverNone unconditionally.
	clusterFormed bool

	// size is the total number of slot-serving masters in the stretched cluster. Every
	// shard's slots are owned by exactly one master (owned even while that master is dead
	// and not-yet-failed-over — gossip keeps the ownership until FORGET/TAKEOVER), so this
	// equals the shard count. It is the denominator of the unconditional minority check
	// (ADR-012: "size counts only masters serving at least one slot").
	size int
	// reachableMasters is the number of slot-serving masters PRESENT IN THE GATHERED VIEW
	// (gt.CountMasters()). NOTE the name is a slight misnomer: CountMasters counts every
	// gossip-known slot-serving master in gt.Nodes, not strictly the ones the hub could dial.
	// This is nonetheless the correct quantity for the ADR-012 arithmetic, because gt.Nodes is
	// built from the OBSERVED pods across REACHABLE sites only (unionClusterPods): a genuinely
	// lost site's pods are never observed, so its masters are absent from gt.Nodes and NOT
	// counted here — exactly what condition 2 needs. (An artificial partition where the hub can
	// still observe/reach the "lost" site keeps its masters in gt.Nodes and correctly makes the
	// planner see no loss — see M4b Finding #2.) unreachableMasters := size - this.
	reachableMasters int

	// orphanTargets is one entry per orphaned shard (no reachable master) that HAS a
	// reachable surviving replica to promote. Empty ⇒ condition 1 fails (healthy, or a
	// total loss with nothing to promote).
	orphanTargets []orphanTarget

	// survivorsClusterFail is true iff ≥1 reachable survivor reports its OWN
	// cluster_state:fail — the first-hand minority-fail-stop signal (ADR-012 condition 2
	// vantage). Required corroboration for the arithmetic; the hub's dial is transport.
	survivorsClusterFail bool

	// quiet is the Rule-A analog (condition 6): no terminating survivor pods and no
	// in-flight failover/reshard/serialized rollout. Decided on a fresh gather.
	quiet bool

	// allowUnsafe mirrors spec.multiSite.allowUnsafeSiteTakeover (condition 4 escape hatch).
	allowUnsafe bool
}

// siteTakeoverPlan is the planner's decision, with the arithmetic and targets the executor
// and the log/condition messages need.
type siteTakeoverPlan struct {
	action  siteTakeoverAction
	targets []orphanTarget // shards to TAKEOVER (siteTakeover / siteTakeoverUnsafe)

	// Surfaced for logging / the refusal condition message.
	size               int
	reachableMasters   int
	unreachableMasters int
	fenceSafe          bool
}

// majorityOf returns the strict majority threshold (size/2)+1 of a set of `size`
// slot-serving masters — the exact quantity Redis's unconditional minority check uses
// (ADR-012; Redis 8.4 cluster_legacy.c clusterUpdateState()).
func majorityOf(size int) int { return size/2 + 1 }

// planSiteTakeover is the pure decision for the multi-site site-loss takeover (ADR-012 D1).
// It performs no I/O. Its SAFETY rests entirely on Domain-M quorum ARITHMETIC — there is no
// Kubernetes witness. Two facts about the slot-serving masters carry the whole case:
//
//   - condition 2 (native exhausted): the reachable masters are BELOW a majority
//     (reachable < (size/2)+1) AND the survivors report their own cluster_state:fail. So
//     Redis cannot elect on its own and has fail-stopped — the operator must act.
//   - the fence (last gate): the UNREACHABLE masters are themselves below a majority
//     (unreachable < (size/2)+1). An alive-but-isolated site can reach only its own masters,
//     so by the same unconditional minority check it too has fail-stopped within
//     cluster-node-timeout of the cut — it is fenced, and a takeover is provably
//     split-brain-free. When the unreachable masters COULD hold a majority (drift), an
//     isolated site might still be serving: refuse unless allowUnsafeSiteTakeover.
//
// Arithmetic identity (ADR-012 condition 4): condition 2 (reachable ≤ size/2) and the fence
// (unreachable ≤ size/2) hold together only when the masters split EXACTLY in half — the
// fence-safe automated path IS the even-split case. Any odd/drifted split makes a loss either
// native (condition 2 fails ⇒ WaitNative) or the drift refusal.
//
// Evaluation order (cheap dispositive vetoes first; the fence — the split-brain safety case —
// last, immediately before firing):
//
//	0 (formed) → 1 (orphans+survivors) → 2 (native exhausted) → 6 (quiet) → 5 (cooldown) →
//	fence → fire.
func planSiteTakeover(in siteTakeoverInput, since *time.Time, now time.Time, cooldown time.Duration) siteTakeoverPlan {
	unreachable := in.size - in.reachableMasters
	majority := majorityOf(in.size)
	fenceSafe := unreachable < majority
	base := siteTakeoverPlan{
		size:               in.size,
		reachableMasters:   in.reachableMasters,
		unreachableMasters: unreachable,
		fenceSafe:          fenceSafe,
	}
	plan := func(a siteTakeoverAction) siteTakeoverPlan { p := base; p.action = a; return p }

	// Condition 0 (LR-026): the cluster must have been bootstrapped for a site-loss takeover
	// to be meaningful. A never-formed cluster (no slot owned anywhere) presents the SAME
	// surface as "all masters lost" — zero slot-serving masters, fresh nodes classifiable as
	// promotable survivors, survivors reporting their own cluster_state:fail (uncovered) — so
	// without this veto initial bringup would arm and eventually FIRE a takeover across pods
	// that never joined a cluster, and (because recoverSiteLoss short-circuits repair whenever
	// it acts) bootstrap would never run. This is not the total-wipe case (a formed cluster
	// whose gossip memory is gone) — that is owned by the wipe-recovery/bootstrap path.
	if !in.clusterFormed {
		return plan(siteTakeoverNone)
	}

	// Condition 1: orphaned shards WITH a surviving replica. None ⇒ healthy cluster, or a
	// total loss with nothing to promote (descoped). Nothing to do — clear the marker.
	if len(in.orphanTargets) == 0 {
		return plan(siteTakeoverNone)
	}

	// Condition 2: native path exhausted, evaluated from the SURVIVORS' own vantage. The
	// survivors must be BOTH below the master majority AND reporting their own
	// cluster_state:fail (the first-hand minority fail-stop). If either fails, Redis can
	// still elect on its own (or has not fail-stopped) — wait, do not intervene (pillar 3.5).
	nativeExhausted := in.size > 0 && in.reachableMasters < majority && in.survivorsClusterFail
	if !nativeExhausted {
		return plan(siteTakeoverWaitNative)
	}

	// Condition 6: quiet cluster (Rule-A analog). A terminating survivor or an in-flight
	// failover/reshard/rollout means Redis may still be resolving this itself — hold.
	if !in.quiet {
		return plan(siteTakeoverHold)
	}

	// Condition 5: cooldown. First observation arms; then wait out max(60s, 2×node-timeout)
	// so the isolated side's fence has certainly engaged and stale Kubernetes views settle.
	if since == nil {
		return plan(siteTakeoverArm)
	}
	if now.Sub(*since) < cooldown {
		return plan(siteTakeoverWait)
	}

	// Fence (last): the takeover is provably split-brain-free iff the UNREACHABLE slot-serving
	// masters are themselves short of a majority — an isolated site with a master minority has
	// fail-stopped within cluster-node-timeout. If they could hold a majority (drift), an alive
	// isolated site might still be serving: refuse unless allowUnsafeSiteTakeover authorises it.
	if !fenceSafe {
		if !in.allowUnsafe {
			return plan(siteTakeoverRefuse)
		}
		p := plan(siteTakeoverUnsafe)
		p.targets = in.orphanTargets
		return p
	}
	p := plan(siteTakeover)
	p.targets = in.orphanTargets
	return p
}

// pickShardSurvivor selects the most-complete surviving replica of one orphaned shard to
// promote: highest replication offset, tie-broken by key/IP determinism, chosen WITHIN the
// dominant replication lineage (LR-024 — a post-failover promotion chain rotates replid and
// shifts the old one into replid2, so lineage is the connected component over
// {replid, replid2}, never keyed on replid alone). Returns (target, true) when a survivor
// exists; (zero, false) when the shard has no reachable survivor (its data is lost — a
// total-loss shard, not a takeover target).
func pickShardSurvivor(shard int, candidates []orphanCandidate) (orphanTarget, bool) {
	if len(candidates) == 0 {
		return orphanTarget{}, false
	}

	// Group candidates by replication lineage via union-find over {replid, replid2}, so a
	// promotion chain (replid rotated, old shifted into replid2 — LR-024) is ONE lineage.
	parent := map[string]string{}
	var find func(string) string
	find = func(x string) string {
		p, ok := parent[x]
		if !ok {
			parent[x] = x
			return x
		}
		if p != x {
			r := find(p)
			parent[x] = r
			return r
		}
		return x
	}
	union := func(a, b string) { parent[find(a)] = find(b) }
	valid := func(r string) bool { return r != "" }
	root := func(c orphanCandidate) string {
		switch {
		case valid(c.replid):
			return find(c.replid)
		case valid(c.replid2):
			return find(c.replid2)
		default:
			return "__nolineage_" + c.key // a candidate with no lineage info is its own lineage
		}
	}

	for _, c := range candidates {
		switch {
		case valid(c.replid) && valid(c.replid2):
			union(c.replid, c.replid2)
		case valid(c.replid):
			find(c.replid)
		case valid(c.replid2):
			find(c.replid2)
		}
	}

	// Find the dominant lineage (most members; tie-broken by the higher best-offset in it),
	// then the most-complete candidate WITHIN it. Offsets are only comparable within one
	// lineage, so we never compare a divergent outlier's offset against the dominant set.
	byLineage := map[string][]orphanCandidate{}
	for _, c := range candidates {
		r := root(c)
		byLineage[r] = append(byLineage[r], c)
	}

	var dominant []orphanCandidate
	for _, group := range byLineage {
		if betterLineage(group, dominant) {
			dominant = group
		}
	}

	best := dominant[0]
	for _, c := range dominant {
		if moreComplete(c, best) {
			best = c
		}
	}
	return orphanTarget{
		shard:   shard,
		key:     best.key,
		ip:      best.ip,
		site:    best.site,
		offset:  best.offset,
		replid:  best.replid,
		replid2: best.replid2,
	}, true
}

// betterLineage reports whether group should replace the current dominant lineage: more
// members wins; on a tie, the group whose most-complete member has the higher offset;
// final tie-break by that member's key for determinism.
func betterLineage(group, current []orphanCandidate) bool {
	if len(current) == 0 {
		return true
	}
	if len(group) != len(current) {
		return len(group) > len(current)
	}
	gb, cb := group[0], current[0]
	for _, c := range group {
		if moreComplete(c, gb) {
			gb = c
		}
	}
	for _, c := range current {
		if moreComplete(c, cb) {
			cb = c
		}
	}
	if gb.offset != cb.offset {
		return gb.offset > cb.offset
	}
	return gb.key < cb.key
}

// moreComplete reports whether a is a better takeover target than b: higher replication
// offset, tie-broken by lower key for determinism (mirrors BestDataHolder's offset rule).
func moreComplete(a, b orphanCandidate) bool {
	if a.offset != b.offset {
		return a.offset > b.offset
	}
	return a.key < b.key
}

// orphanCandidate is one reachable, data-bearing surviving replica of an orphaned shard,
// as seen in the gather. It is the input to pickShardSurvivor.
type orphanCandidate struct {
	key     string
	ip      string
	site    string
	offset  int64
	replid  string
	replid2 string
}

// --- Fenced-alive rejoin recycle (ADR-012 action 3) — pure decision ---------------------

// recycleCandidate is one reachable multi-site pod evaluated for fenced-alive rejoin
// recycling: a pod that returned WITHOUT a restart in a healed site carries an old node ID
// and a stale config epoch that the survivors already FORGOT, so it cannot gossip back in.
type recycleCandidate struct {
	key          string
	restartCount int32
	// isolated is true when the pod is NOT in the serving cluster's main partition — the
	// gossip-level signal that it is fenced out (survivors dropped it and reject its
	// packets). A fresh (recycled) pod re-joins via MEET and is not isolated for long.
	isolated bool
}

// planFencedRejoinRecycle selects the pods to recycle (pod-delete → StatefulSet reschedules
// fresh, ADR-008 machinery) so a fenced-alive returnee re-joins through the ordinary
// empty-pod MEET path instead of re-MEETing a stale ex-master (ADR-012 action 3).
//
// It fires ONLY when the serving cluster already holds a master majority
// (clusterHasMajority) — so it never churns pods mid-bootstrap (no majority yet) — and only
// for pods that are BOTH isolated from that majority AND never restarted (a restarted pod is
// the crash/reschedule case the wipe-recovery path owns, not a fenced-alive returnee).
// Recycling an isolated, data-empty (EmptyDir) pod is data-safe either way: a genuine
// returnee gets its clean rejoin, and a briefly-isolated fresh pod is only re-created.
func planFencedRejoinRecycle(cands []recycleCandidate, clusterHasMajority bool) []string {
	if !clusterHasMajority {
		return nil
	}
	var out []string
	for _, c := range cands {
		if c.isolated && c.restartCount == 0 {
			out = append(out, c.key)
		}
	}
	return out
}

// --- Executor -----------------------------------------------------------------------------

// siteFromKey returns the site name from a site-qualified ground-truth key (siteName/objName).
func siteFromKey(key string) string {
	if before, _, ok := strings.Cut(key, "/"); ok {
		return before
	}
	return ""
}

// siteTakeoverCooldownFor returns the site-loss takeover cooldown: max(60s, 2 ×
// cluster-node-timeout) (ADR-012 condition 5) — long enough that the isolated side's own
// minority fail-stop (the fence) has certainly engaged and stale Kubernetes views settle.
func siteTakeoverCooldownFor(lr *littleredv1alpha1.LittleRed) time.Duration {
	nodeTimeoutMs := littleredv1alpha1.DefaultClusterNodeTimeout
	if lr.Spec.Cluster != nil && lr.Spec.Cluster.ClusterNodeTimeout > 0 {
		nodeTimeoutMs = lr.Spec.Cluster.ClusterNodeTimeout
	}
	twice := 2 * time.Duration(nodeTimeoutMs) * time.Millisecond
	if twice > 60*time.Second {
		return twice
	}
	return 60 * time.Second
}

// recoverSiteLoss is the multi-site site-loss takeover EXECUTOR (ADR-012). It runs in the
// reconcile spine BEFORE the readiness gate and repair, so it — not the generic
// repairCluster Step-0 quorum-loss TAKEOVER — owns the takeover decision in multi-site,
// where a naive quorum-loss takeover across a possibly-alive (link-cut) site would create a
// live split-brain.
//
// It returns acted=true for every verdict except the two that are safe to fall through to
// the normal readiness/gather/repair flow: siteTakeoverNone (healthy, or a descoped total
// loss) and siteTakeoverWaitNative (survivors still hold a majority, Redis self-heals). For
// every other verdict — Hold (cluster not quiet), Arm, Wait, Refuse, and the fire actions —
// it returns acted=true and the caller requeues without running repair, so the unguarded
// Step-0 takeover can never fire during a quorum loss.
func (r *LittleRedReconciler) recoverSiteLoss(ctx context.Context, lr *littleredv1alpha1.LittleRed, obs []siteObservation) (bool, error) {
	if r.sites == nil || !isMultiSite(lr) {
		return false, nil
	}
	stateLog := r.getLogger(ctx, lr, LogCategoryState)
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)

	shards, _, _ := multiSiteParams(lr)

	// Gather the survivors' own view (bounded per-probe dials, LR-017). Everything below is
	// decided from the survivors' first-hand evidence (their cluster_state and master counts),
	// never from the hub's dial reachability alone.
	gt := r.gatherMultiSiteGroundTruth(ctx, lr)

	in := r.buildSiteTakeoverInput(lr, gt, obs, shards)

	// Fenced-alive rejoin recycle runs regardless of the takeover verdict: once the serving
	// cluster has regained a majority, an isolated never-restarted pod is a fenced-out
	// returnee that must be recycled before repair re-MEETs its stale ex-master.
	if r.recycleFencedReturnees(ctx, lr, gt, obs, in) {
		return true, nil
	}

	cooldown := siteTakeoverCooldownFor(lr)
	var since *time.Time
	if lr.Status.MultiSite != nil && lr.Status.MultiSite.TakeoverPendingSince != nil {
		t := lr.Status.MultiSite.TakeoverPendingSince.Time
		since = &t
	}
	plan := planSiteTakeover(in, since, time.Now(), cooldown)

	switch plan.action {
	case siteTakeoverNone, siteTakeoverWaitNative:
		return false, r.clearTakeoverPendingSince(ctx, lr, reasonRecovered, "No site-loss takeover needed.")

	case siteTakeoverHold:
		stateLog.Info("Site-loss takeover held: the cluster is not quiet (terminating survivor or in-flight failover/reshard/rollout); taking no action",
			"reachableMasters", plan.reachableMasters, "size", plan.size)
		return true, r.clearTakeoverPendingSince(ctx, lr, reasonRecovered, "Site loss not actionable (cluster not quiet).")

	case siteTakeoverArm:
		stateLog.Info("Site-loss takeover: native failover is exhausted; arming cooldown",
			"reachableMasters", plan.reachableMasters, "size", plan.size, "cooldown", cooldown.String())
		return true, r.setTakeoverPendingSince(ctx, lr, metav1.Now())

	case siteTakeoverWait:
		stateLog.Info("Site-loss takeover: waiting out cooldown before intervening",
			"reachableMasters", plan.reachableMasters, "size", plan.size, "cooldown", cooldown.String())
		return true, nil

	case siteTakeoverRefuse:
		msg := fmt.Sprintf("Refusing automatic site-loss takeover: %d of %d slot-serving masters are unreachable — the lost "+
			"side could still hold a serving majority (drift), so an isolated-but-alive site might not have fenced itself and a "+
			"takeover risks a live split-brain. Set multiSite.allowUnsafeSiteTakeover=true to authorise, or intervene manually.",
			plan.unreachableMasters, plan.size)
		stateLog.Info(msg)
		r.event(lr, corev1.EventTypeWarning, reasonRefusedSiteTakeover, msg)
		return true, r.setTakeoverCondition(ctx, lr, metav1.ConditionTrue, reasonRefusedSiteTakeover, msg)

	case siteTakeover, siteTakeoverUnsafe:
		r.executeSiteTakeover(ctx, lr, gt, plan, allSitesObservable(obs), auditLog)
		msg := fmt.Sprintf("Site-loss takeover executed: promoted %d surviving replica(s) via CLUSTER FAILOVER TAKEOVER "+
			"(%d/%d masters were unreachable)", len(plan.targets), plan.unreachableMasters, plan.size)
		if plan.action == siteTakeoverUnsafe {
			msg = "UNSAFE " + msg + " — the fence was unavailable and allowUnsafeSiteTakeover authorised it; a divergent write history on the lost side, if any, is discarded"
			r.event(lr, corev1.EventTypeWarning, reasonUnsafeRebootstrap, msg)
		} else {
			r.event(lr, corev1.EventTypeNormal, reasonSiteTakeoverExecuted, msg)
		}
		return true, r.clearTakeoverPendingSince(ctx, lr, reasonSiteTakeoverExecuted, msg)
	}
	return true, nil
}

// buildSiteTakeoverInput projects the gather onto the pure planner input (ADR-012 D1). size
// is the shard count (every shard's slots are owned by exactly one master, even a dead
// not-yet-failed-over one); reachableMasters is CountMasters(); the orphan targets are the
// best surviving replica of each shard with no reachable master. The takeover safety no
// longer consults a Kubernetes witness (D1): it rests entirely on the Domain-M quorum
// arithmetic (reachable-master majority + the unreachable-master minority self-fence),
// corroborated by the survivors' own cluster_state:fail.
func (r *LittleRedReconciler) buildSiteTakeoverInput(lr *littleredv1alpha1.LittleRed, gt *redisclient.ClusterGroundTruth, obs []siteObservation, shards int) siteTakeoverInput {
	in := siteTakeoverInput{
		size: shards,
		// CountMasters counts gossip-known slot-serving masters in gt.Nodes; since gt.Nodes only
		// holds pods observed across reachable sites, a lost site's masters are absent and not
		// counted (see the field doc — the name is a mild misnomer, the value is correct here).
		reachableMasters: gt.CountMasters(),
		allowUnsafe:      lr.Spec.MultiSite != nil && lr.Spec.MultiSite.AllowUnsafeSiteTakeover,
	}

	// clusterFormed (LR-026): has the cluster ever been bootstrapped? ANY node in the gathered
	// view owning a slot proves it — including a gossip-known DEAD master (Reachable=false)
	// whose slot ownership the survivors still carry until FORGET/TAKEOVER. Zero slots anywhere
	// means the cluster has not formed yet (initial bringup), so the site-loss takeover must
	// stay quiet and let bootstrap run.
	for _, n := range gt.Nodes {
		if len(n.Slots) > 0 {
			in.clusterFormed = true
			break
		}
	}

	// Survivor corroboration: any reachable node reporting its OWN cluster_state:fail.
	for _, n := range gt.Nodes {
		if n.Reachable && n.ClusterState == redisclient.ClusterStateFail {
			in.survivorsClusterFail = true
			break
		}
	}

	// Reachable slot-serving master per shard, and the reachable survivors per shard.
	shardHasMaster := make([]bool, shards)
	shardCandidates := make([][]orphanCandidate, shards)
	for _, n := range gt.Nodes {
		if !n.Reachable {
			continue
		}
		k := redisclient.ShardIndexFromPodName(n.PodName)
		if k < 0 || k >= shards {
			continue
		}
		if n.Role == RoleMaster && len(n.Slots) > 0 {
			shardHasMaster[k] = true
			continue
		}
		// A reachable non-slot-serving member of the shard: a survivor we could promote.
		shardCandidates[k] = append(shardCandidates[k], orphanCandidate{
			key:     n.PodName,
			ip:      n.PodIP,
			site:    siteFromKey(n.PodName),
			offset:  n.ReplOffset,
			replid:  n.MasterReplid,
			replid2: n.MasterReplid2,
		})
	}
	for k := range shards {
		if shardHasMaster[k] {
			continue
		}
		if tgt, ok := pickShardSurvivor(k, shardCandidates[k]); ok {
			in.orphanTargets = append(in.orphanTargets, tgt)
		}
	}

	// Quiet cluster (condition 6): no terminating survivor pod in any reachable site.
	in.quiet = true
	for _, o := range obs {
		if !o.reachable {
			continue
		}
		for _, p := range o.pods {
			if p.terminating {
				in.quiet = false
			}
		}
	}
	return in
}

// executeSiteTakeover issues CLUSTER FAILOVER TAKEOVER on each orphan target (action 1) then
// FORGETs the lost site's node IDs on every reachable node (action 2). TAKEOVER→FORGET
// ordering is REQUIRED: a shard's other surviving replica still points at the dead master
// until it follows the slot claim, and FORGET refuses ("Can't forget my master") on such a
// node — so FORGET trails idempotently across reconciles and a partial pass is NORMAL, not
// an error. Concurrent no-consensus epoch bumps across shards converge via Redis's
// config-epoch collision resolution (named in ADR-012; asserted in the multi-shard e2e).
func (r *LittleRedReconciler) executeSiteTakeover(ctx context.Context, lr *littleredv1alpha1.LittleRed, gt *redisclient.ClusterGroundTruth, plan siteTakeoverPlan, forgetSafe bool, auditLog logr.Logger) {
	password := r.getRedisPassword(ctx, lr)
	cc := redisclient.NewClusterClient(password, lr.Spec.TLS.Enabled)

	// Action 1: TAKEOVER on the best surviving replica of each orphaned shard.
	for _, t := range plan.targets {
		addr := fmt.Sprintf("%s:%d", t.ip, littleredv1alpha1.RedisPort)
		auditLog.Info("Site-loss takeover: promoting surviving replica via CLUSTER FAILOVER TAKEOVER",
			"shard", t.shard, "pod", t.key, "site", t.site, "offset", t.offset)
		if err := cc.ClusterFailoverTakeover(ctx, addr); err != nil {
			// A single failed TAKEOVER is not fatal: retry on the next reconcile. Returning
			// here would abandon the other shards' promotions for this pass.
			auditLog.Error(err, "Site-loss takeover: CLUSTER FAILOVER TAKEOVER failed (will retry)", "pod", t.key)
		}
	}

	// Action 2: FORGET the lost site's node IDs (the ghosts) on every reachable node — but only
	// when every hosting site's Kubernetes API answered this pass (forgetSafe =
	// allSitesObservable). While a hosting site's API is unreachable, a gossip-known node with
	// no backing pod is "unknown, not gone" — indistinguishable by NodeID from a node on a
	// merely-unreachable site (pillar 3.4 / ADR-012: "forgotten only when its own site's own
	// view supports it, never merely because the hub cannot reach it"), so FORGET is deferred.
	// It trails idempotently to a later pass once every site is observable again (a fenced-alive
	// returnee's pods are then recycled and their stale IDs become confirmed-gone ghosts). NOTE:
	// forgetSafe is REACHABILITY, not the deleted Kubernetes witness — the takeover firing
	// decision itself (D1) is pure quorum arithmetic.
	if !forgetSafe {
		auditLog.Info("Site-loss takeover: deferring CLUSTER FORGET — a hosting site's API is unreachable, so its nodes are unknown-not-gone (pillar 3.4); FORGET will trail once observable")
		return
	}
	// Trails idempotently — "Can't forget my master" until the sibling replica follows the slot
	// claim is expected, not an error.
	for _, ghostID := range gt.GhostNodes {
		for _, n := range gt.Nodes {
			if !n.Reachable {
				continue
			}
			addr := fmt.Sprintf("%s:%d", n.PodIP, littleredv1alpha1.RedisPort)
			if err := cc.ClusterForget(ctx, addr, ghostID); err != nil {
				auditLog.Info("Site-loss takeover: FORGET trailing (expected until the slot claim propagates)",
					"ghost", ghostID, "on", n.PodName, "err", err.Error())
			}
		}
	}
}

// recycleFencedReturnees recycles fenced-alive returnee pods (ADR-012 action 3) via their
// owning site's SiteClient. See planFencedRejoinRecycle for the (majority-gated,
// isolated + never-restarted) signal. Returns acted=true when it deleted at least one pod.
func (r *LittleRedReconciler) recycleFencedReturnees(ctx context.Context, lr *littleredv1alpha1.LittleRed, gt *redisclient.ClusterGroundTruth, obs []siteObservation, in siteTakeoverInput) bool {
	// The serving cluster must already hold a master majority AND every shard must be served
	// (no orphaned shards this pass), so we never churn a pod mid-bootstrap or mid-takeover.
	clusterHasMajority := in.reachableMasters >= majorityOf(in.size) && len(in.orphanTargets) == 0
	if !clusterHasMajority {
		return false
	}

	// The main serving partition is the largest connected component of the survivor gossip.
	mainPartition := map[string]bool{}
	if seed := gt.GetLargestPartitionSeed(); seed != nil {
		for _, part := range gt.Partitions {
			for _, id := range part {
				if id == seed.NodeID {
					for _, m := range part {
						mainPartition[m] = true
					}
				}
			}
		}
	}
	if len(gt.Partitions) <= 1 {
		return false // a whole cluster: nothing is fenced out
	}

	// restart counts by ground-truth key.
	restartByKey := map[string]int32{}
	targetByKey := map[string]struct {
		client  SiteClient
		objName string
	}{}
	for _, o := range obs {
		if !o.reachable || o.site.client == nil {
			continue
		}
		for _, p := range o.pods {
			restartByKey[p.key] = p.restartCount
			targetByKey[p.key] = struct {
				client  SiteClient
				objName string
			}{client: o.site.client, objName: p.objName}
		}
	}

	cands := make([]recycleCandidate, 0, len(gt.Nodes))
	for _, n := range gt.Nodes {
		if !n.Reachable {
			continue
		}
		cands = append(cands, recycleCandidate{
			key:          n.PodName,
			restartCount: restartByKey[n.PodName],
			isolated:     !mainPartition[n.NodeID],
		})
	}

	recycle := planFencedRejoinRecycle(cands, clusterHasMajority)
	if len(recycle) == 0 {
		return false
	}

	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	acted := false
	for _, key := range recycle {
		tg, ok := targetByKey[key]
		if !ok || tg.client == nil {
			continue
		}
		auditLog.Info("Site-loss recovery: recycling fenced-alive returnee so it rejoins fresh (new node ID) instead of re-MEETing a stale ex-master",
			"pod", key)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: tg.objName, Namespace: lr.Namespace}}
		if err := tg.client.Delete(ctx, pod); err != nil {
			auditLog.Info("Site-loss recovery: recycle delete failed (will retry)", "pod", key, "err", err.Error())
			continue
		}
		acted = true
	}
	return acted
}

// --- Cooldown marker + condition helpers (mirror setGhostMasterStuckSince) ----------------

// setTakeoverPendingSince stamps Status.MultiSite.TakeoverPendingSince and sets the
// MultiSiteTakeover condition True/SiteLossDetected (retry on conflict). No-op if already stamped.
func (r *LittleRedReconciler) setTakeoverPendingSince(ctx context.Context, lr *littleredv1alpha1.LittleRed, t metav1.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.MultiSite == nil {
			latest.Status.MultiSite = &littleredv1alpha1.MultiSiteStatus{}
		}
		if latest.Status.MultiSite.TakeoverPendingSince != nil {
			return nil
		}
		latest.Status.MultiSite.TakeoverPendingSince = &t
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    littleredv1alpha1.ConditionMultiSiteTakeover,
			Status:  metav1.ConditionTrue,
			Reason:  reasonSiteLossDetected,
			Message: "A site loss has exhausted native failover; waiting out the takeover cooldown.",
		})
		if lr.Status.MultiSite == nil {
			lr.Status.MultiSite = &littleredv1alpha1.MultiSiteStatus{}
		}
		lr.Status.MultiSite.TakeoverPendingSince = &t
		return r.Status().Update(ctx, latest)
	})
}

// setTakeoverCondition updates the MultiSiteTakeover condition without touching the marker
// (the refuse-and-wait state). Retry on conflict.
func (r *LittleRedReconciler) setTakeoverCondition(ctx context.Context, lr *littleredv1alpha1.LittleRed, status metav1.ConditionStatus, reason, message string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionMultiSiteTakeover, Status: status, Reason: reason, Message: message,
		})
		return r.Status().Update(ctx, latest)
	})
}

// clearTakeoverPendingSince resets Status.MultiSite.TakeoverPendingSince once no site-loss
// takeover is needed, recording the outcome on the MultiSiteTakeover condition (False).
// No-op if never stamped and the condition is already settled.
func (r *LittleRedReconciler) clearTakeoverPendingSince(ctx context.Context, lr *littleredv1alpha1.LittleRed, reason, message string) error {
	if lr.Status.MultiSite == nil || lr.Status.MultiSite.TakeoverPendingSince == nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.MultiSite == nil || latest.Status.MultiSite.TakeoverPendingSince == nil {
			return nil
		}
		latest.Status.MultiSite.TakeoverPendingSince = nil
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionMultiSiteTakeover, Status: metav1.ConditionFalse, Reason: reason, Message: message,
		})
		lr.Status.MultiSite.TakeoverPendingSince = nil
		return r.Status().Update(ctx, latest)
	})
}
