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

import "sort"

// site_rebalance.go holds the PURE, I/O-free master-BALANCE planner for multi-site cluster
// mode (ADR-012 decision D2), sibling of planSiteTakeover / planClusterWipeRecovery /
// planFencedRejoinRecycle. It is a *prevention* seam for the drift-refusal that the
// site-loss takeover (D1) surfaces: by keeping the slot-serving masters spread so that no
// single site can hold a Redis master MAJORITY, it narrows the odd/drifted split that would
// otherwise refuse (or need the allowUnsafeSiteTakeover opt-in) on a whole-site loss.
//
// This milestone provides the decision only; it is NOT yet wired into any reconcile path
// (its sole caller is the unit test). Execution — issuing graceful CLUSTER FAILOVER on the
// promoted replicas — is a later milestone.

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
