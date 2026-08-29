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

package redis

import (
	"fmt"
	"sort"
	"strings"
)

// RedisNodeState represents the replication state of a Redis/Valkey pod
type RedisNodeState struct {
	PodName    string
	IP         string
	Role       string
	MasterHost string
	LinkStatus string
	Offset     int64
	// Keys is the total number of keys the node currently holds (INFO keyspace).
	// Zero means empty. Used to decide whether leaderless recovery is data-safe.
	Keys int64
	// Replid is the node's master_replid — its current replication lineage identity.
	Replid string
	// Replid2 is the node's master_replid2 — the lineage it descended from before its
	// last promotion/resync rotated the replid. Two nodes are the SAME lineage when their
	// replid histories connect through this (a promotion chain), even though their current
	// Replids differ. Ignoring it makes a normal post-failover survivor look divergent.
	Replid2   string
	Reachable bool

	// ProbeFailure says WHY this node is unreachable, and ProbeError carries what
	// the server actually said (bounded, see DescribeProbeError). Both are ProbeOK/""
	// when Reachable.
	//
	// Before LR-051 the probe error was discarded on the spot, so a credential
	// mismatch and a dial timeout were byte-identical to every rule in the operator:
	// Role:"", Keys:0, LinkStatus:"", Reachable:false. That is not a cosmetic loss.
	// DataHolders() filters on Reachable, so an AuthFailed pod reads as "holds no
	// data" while it may be holding ALL of it — which voids Rule L's >=2-holder
	// REFUSE, the single gate whose purpose is to stop the operator discarding data.
	// See ProbeFailure, LR-051, and the auth design §3.5a Path C.
	ProbeFailure ProbeFailure
	ProbeError   string
}

// UnprovablyEmpty reports whether this node CANNOT be shown to hold no data.
//
// A node the operator could not authenticate to answered us — in the protocol, to
// refuse us — so a live server is running there and its keyspace is unknown rather
// than zero. Every decision that discards data must treat it as a potential sole
// copy. A node that did not answer AT ALL is a different case and is deliberately
// NOT covered here: that judgement belongs to the kubelet's readiness probe
// (authoritative and blackhole-proof, LR-023), never to the operator's own dial
// (LR-017).
func (n *RedisNodeState) UnprovablyEmpty() bool {
	return n != nil && !n.Reachable && n.ProbeFailure == ProbeAuthFailed
}

// SentinelNodeState represents the monitoring state of a Sentinel pod
type SentinelNodeState struct {
	PodName        string
	IP             string
	Monitoring     bool
	MasterIP       string
	FailoverStatus string
	Reachable      bool
	Replicas       []ReplicaInfo

	// MasterFlags is the monitored master's `flags` field as this Sentinel reports it
	// (e.g. "master", "s_down,master"). It is the discriminator between a master that
	// is merely DEAD and one that is alive but not ours: a captured instance reports a
	// foreign master with clean flags, because from Sentinel's vantage it is healthy —
	// which is exactly why no failover ever fires. Already on the wire; previously
	// discarded by the gatherers.
	MasterFlags string

	// NumOtherSentinels / NumSlaves are the peer and replica counts this Sentinel
	// reports for the monitored master. Exceeding what we deployed is the loudest
	// available sign that another Sentinel deployment has joined our quorum.
	NumOtherSentinels int
	NumSlaves         int

	// MonitoredMasters is EVERY master name this Sentinel monitors, not only the
	// one we asked about (`SENTINEL MASTERS`).
	//
	// The rest of this struct is the answer to a single-name question, which can
	// confirm or deny the name we passed and nothing else. A Sentinel carrying a
	// leftover name alongside the desired one answers `Monitoring: true` either
	// way, so the leftover is invisible to every field above — and that is the
	// state a half-finished master-name change leaves behind, which is why this is
	// gathered unconditionally rather than only when a Sentinel reads bare.
	//
	// An EMPTY list means "we could not read it", not "this Sentinel monitors
	// nothing": the extra round trip degrades to empty rather than to
	// Reachable:false, because a Sentinel that cannot answer one added question is
	// not a dead Sentinel (LR-041's class of mistake). Callers must therefore not
	// read emptiness as evidence of absence.
	MonitoredMasters []MonitoredMaster

	// ProbeFailure / ProbeError: why this Sentinel is unreachable and what it said.
	// Same rationale as RedisNodeState's (LR-051). A Sentinel we cannot authenticate
	// to is not a dead Sentinel — it is one whose monitoring view we simply cannot
	// read, which is exactly the state that made every Monitoring-gated rule go
	// quietly inert in LR-041.
	ProbeFailure ProbeFailure
	ProbeError   string
}

// ReplicationState represents the combined "Ground Truth" of a replication-based
// instance — every data pod's replication view plus (in sentinel mode) every
// Sentinel's monitoring view. It is the shared state container for the
// sentinel- and failover-mode reconciliation paths; the Sentinel-specific
// predicates on it are no-ops when SentinelNodes is empty.
type ReplicationState struct {
	RedisNodes    map[string]*RedisNodeState
	SentinelNodes map[string]*SentinelNodeState
	ValidIPs      map[string]bool

	// Derived Truth
	RealMasterIP   string
	FailoverActive bool
}

// NewReplicationState initializes an empty ReplicationState
func NewReplicationState() *ReplicationState {
	return &ReplicationState{
		RedisNodes:    make(map[string]*RedisNodeState),
		SentinelNodes: make(map[string]*SentinelNodeState),
		ValidIPs:      make(map[string]bool),
	}
}

// DetermineRealMaster uses the gathered information to decide who the authoritative master is.
func (s *ReplicationState) DetermineRealMaster() {
	// 1. Check for active failover
	for _, sn := range s.SentinelNodes {
		if sn.Reachable && sn.Monitoring && sn.FailoverStatus != "" &&
			sn.FailoverStatus != failoverStateNone && sn.FailoverStatus != "no-failover" {
			s.FailoverActive = true
			break
		}
	}

	// 2. Count what Sentinels think
	masterCounts := make(map[string]int)
	reachableSentinels := 0
	for _, sn := range s.SentinelNodes {
		if sn.Reachable {
			reachableSentinels++
			if sn.Monitoring && sn.MasterIP != "" {
				masterCounts[sn.MasterIP]++
			}
		}
	}

	// 3. Majority of Sentinels wins (if IP is still valid)
	ghostMasterCount := 0
	for ip, count := range masterCounts {
		if s.IsGhost(ip) {
			ghostMasterCount += count
		}
		if count >= (reachableSentinels/2)+1 && s.ValidIPs[ip] {
			s.RealMasterIP = ip
			return
		}
	}

	// 4. If Sentinels are idle/split, fallback to identifying the one Redis master.
	// Safety: We ONLY fallback to the Redis-only view if Sentinels are NOT
	// unanimous (majority) about a ghost master. If a majority of Sentinels
	// see a master but that IP is a ghost, it strongly implies a recent pod
	// death and we MUST wait for Sentinel's down-after-milliseconds timeout
	// and subsequent failover. Falling back here would cause us to identify
	// a "stale" or "default" master (like a restarting pod) and potentially
	// issue RESETs that wipe Sentinel's failover state.
	if !s.FailoverActive && ghostMasterCount < (reachableSentinels/2)+1 {
		for _, rn := range s.RedisNodes {
			if rn.Reachable && rn.Role == roleMaster {
				s.RealMasterIP = rn.IP
				return
			}
		}
	}
}

// AllSentinelsBare reports whether at least one Sentinel is reachable and NONE of
// the reachable Sentinels is monitoring a master. This is the signature of a
// bootstrap deadlock (as opposed to a recent master death, where the Sentinels
// still monitor the now-dead master and know its replicas). It returns the count
// of reachable Sentinels so callers can gate on quorum.
func (s *ReplicationState) AllSentinelsBare() (bare bool, reachable int) {
	monitoring := 0
	for _, sn := range s.SentinelNodes {
		if !sn.Reachable {
			continue
		}
		reachable++
		if sn.Monitoring {
			monitoring++
		}
	}
	return reachable > 0 && monitoring == 0, reachable
}

// AuthFailedRedisNodes returns the Redis pods the operator reached but could not
// authenticate to, sorted by pod name for a deterministic message.
//
// These are NOT unreachable pods in any useful sense: something answered on that
// address and spoke the protocol well enough to refuse us. They are the nodes
// UnprovablyEmpty is about, and the ones that must veto any action that discards
// data (Rule L, the ghost-master recovery, the quarantine).
func (s *ReplicationState) AuthFailedRedisNodes() []*RedisNodeState {
	var out []*RedisNodeState
	for _, rn := range s.RedisNodes {
		if rn.UnprovablyEmpty() {
			out = append(out, rn)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PodName < out[j].PodName })
	return out
}

// AuthFailedSentinelNodes is AuthFailedRedisNodes' twin for Sentinel pods. It
// changes no decision — no rule discards data on the strength of a Sentinel's
// answer — but it is reported, because a quorum the operator cannot read is the
// LR-041 shape and an owner needs to see it named rather than inferred from
// silence.
func (s *ReplicationState) AuthFailedSentinelNodes() []*SentinelNodeState {
	var out []*SentinelNodeState
	for _, sn := range s.SentinelNodes {
		if sn != nil && !sn.Reachable && sn.ProbeFailure == ProbeAuthFailed {
			out = append(out, sn)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PodName < out[j].PodName })
	return out
}

// HasAuthFailure reports whether ANY pod of this instance refused the operator's
// credential — the predicate the data-discarding planners veto on.
func (s *ReplicationState) HasAuthFailure() bool {
	return len(s.AuthFailedRedisNodes()) > 0 || len(s.AuthFailedSentinelNodes()) > 0
}

// DataHolders returns the reachable Redis nodes that currently hold keys.
func (s *ReplicationState) DataHolders() []*RedisNodeState {
	var holders []*RedisNodeState
	for _, rn := range s.RedisNodes {
		if rn.Reachable && rn.Keys > 0 {
			holders = append(holders, rn)
		}
	}
	return holders
}

// BestDataHolder picks the most-complete data-holding node to elect as master when
// an unsafe rebootstrap is authorized. Selection is: highest replication offset,
// tie-broken by key count, then by IP for determinism. The rationale (see ADR-005)
// is that the usual failure is a lost replication link leaving one node with newer,
// more-complete data and the other with an older snapshot — the higher offset is
// the newer node, so full-syncs should flow outward from it.
//
// diverged reports whether the holders span more than one INDEPENDENT replication
// lineage. When true, the offsets are NOT comparable and electing any single master
// discards genuinely independent writes — callers should surface this loudly.
//
// Divergence is computed over each node's replid AND replid2 (see holdersDiverged), not
// its current replid alone: a promotion or resync rotates the current replid to a new
// value and shifts the old one into replid2, so a normal post-failover survivor has a
// different current replid yet is the SAME lineage. Comparing only replid would flag such
// a promotion chain as divergent and wrongly refuse a safe election.
//
// Returns (nil, false) when no node holds data.
func (s *ReplicationState) BestDataHolder() (best *RedisNodeState, diverged bool) {
	holders := s.DataHolders()
	if len(holders) == 0 {
		return nil, false
	}
	best = holders[0]
	for _, h := range holders {
		switch {
		case h.Offset != best.Offset:
			if h.Offset > best.Offset {
				best = h
			}
		case h.Keys != best.Keys:
			if h.Keys > best.Keys {
				best = h
			}
		case h.IP < best.IP:
			best = h
		}
	}
	return best, holdersDiverged(holders)
}

// isZeroReplid reports whether a replid is empty or the all-zero sentinel Redis reports
// for an unset master_replid2.
func isZeroReplid(r string) bool {
	if r == "" {
		return true
	}
	for _, c := range r {
		if c != '0' {
			return false
		}
	}
	return true
}

// holdersDiverged reports whether the data holders span more than one independent
// replication lineage. Each holder's replid and replid2 are treated as the same lineage
// (a promotion/resync rotated one into the other); holders are then grouped by the
// connected component of their replids via union-find. More than one component across the
// holders means genuinely independent write histories.
func holdersDiverged(holders []*RedisNodeState) bool {
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
	valid := func(r string) bool { return !isZeroReplid(r) }

	// Pass 1: connect each holder's replid<->replid2 (and register lone replids).
	for _, h := range holders {
		switch {
		case valid(h.Replid) && valid(h.Replid2):
			union(h.Replid, h.Replid2)
		case valid(h.Replid):
			find(h.Replid)
		case valid(h.Replid2):
			find(h.Replid2)
		}
	}
	// Pass 2: count distinct components across holders (after all unions are applied).
	comps := map[string]bool{}
	for i, h := range holders {
		switch {
		case valid(h.Replid):
			comps[find(h.Replid)] = true
		case valid(h.Replid2):
			comps[find(h.Replid2)] = true
		default:
			// A holder with no lineage info at all — treat as its own component.
			comps[fmt.Sprintf("__noreplid_%d", i)] = true
		}
	}
	return len(comps) > 1
}

// IsGhost returns true if the given IP is not in the set of valid pod IPs
func (s *ReplicationState) IsGhost(ip string) bool {
	if ip == "" {
		return false
	}
	return !s.ValidIPs[ip]
}

// HasHealthyKnownReplica reports whether at least one monitoring sentinel knows a
// replica that is neither a ghost nor s_down — i.e. a candidate Sentinel could
// promote during a failover.
func (s *ReplicationState) HasHealthyKnownReplica() bool {
	for _, sn := range s.SentinelNodes {
		if !sn.Reachable || !sn.Monitoring {
			continue
		}
		for _, replica := range sn.Replicas {
			if !s.IsGhost(replica.IP) && !strings.Contains(replica.Flags, "s_down") {
				return true
			}
		}
	}
	return false
}

// ReachableSentinels returns the number of reachable Sentinel pods.
func (s *ReplicationState) ReachableSentinels() int {
	n := 0
	for _, sn := range s.SentinelNodes {
		if sn.Reachable {
			n++
		}
	}
	return n
}

// SentinelsMonitorGhostMaster reports whether a majority of reachable Sentinels are
// monitoring a master IP that is a ghost (no live pod). This is the signature of the
// ghost-master failover deadlock — Sentinel is pinned to a dead master it cannot fail
// over off — as distinct from the bare-Sentinel leaderless state (AllSentinelsBare).
func (s *ReplicationState) SentinelsMonitorGhostMaster() bool {
	reachable, ghost := 0, 0
	for _, sn := range s.SentinelNodes {
		if !sn.Reachable {
			continue
		}
		reachable++
		if sn.Monitoring && sn.MasterIP != "" && s.IsGhost(sn.MasterIP) {
			ghost++
		}
	}
	return reachable > 0 && ghost >= (reachable/2)+1
}

// GhostReplicaResetSafe reports whether it is safe to issue a broadcast SENTINEL
// RESET to prune ghost replica entries from the topology.
//
// SENTINEL RESET wipes Sentinel's ENTIRE replica list, which can only be rebuilt by
// querying the current master's INFO (replicas never self-announce to Sentinel).
// Issuing it while the cluster is missing a node — e.g. a RESET racing a master
// crash — strands every sentinel with an o_down master and zero promotable
// replicas: a permanent, non-self-healing failover deadlock (LR-013).
//
// It returns true only when ALL hold:
//   - a ghost replica was actually observed (ghostFound);
//   - the cluster is whole (clusterWhole): every expected Redis pod is reachable, so
//     a RESET cannot strand us mid-disruption — this is the K8s-grounded guard that
//     the snapshot-time healthyReplicas check (LR-011) missed during the race;
//   - the consensus master is a living, reachable pod, not a ghost (LR-008);
//   - at least one healthy replica is already known to Sentinel (LR-011), so Sentinel
//     can recover from the RESET.
//
// When not whole we simply defer: the ghost entry is harmless and will be pruned on a
// later reconcile once the cluster is whole again. Deferring a RESET never causes a
// deadlock; issuing one at the wrong moment does.
func (s *ReplicationState) GhostReplicaResetSafe(ghostFound, clusterWhole bool) bool {
	if !ghostFound || !clusterWhole {
		return false
	}
	if s.RealMasterIP == "" || s.IsGhost(s.RealMasterIP) {
		return false
	}
	if m := s.RedisNodes[s.RealMasterIP]; m == nil || !m.Reachable {
		return false
	}
	return s.HasHealthyKnownReplica()
}

// GetHealActions returns a list of recommended actions to fix the instance's
// topology (sentinel mode: MONITOR/SLAVEOF/RESET suggestions).
func (s *ReplicationState) GetHealActions(masterName string) []string {
	var actions []string
	if s.RealMasterIP == "" {
		return actions
	}

	for _, sn := range s.SentinelNodes {
		if sn.Reachable && (!sn.Monitoring || sn.MasterIP != s.RealMasterIP) {
			actions = append(actions, "MONITOR "+s.RealMasterIP+" ON "+sn.PodName)
		}
	}

	for _, rn := range s.RedisNodes {
		if !rn.Reachable || rn.IP == s.RealMasterIP {
			continue
		}
		if rn.Role == roleMaster || rn.MasterHost != s.RealMasterIP || rn.LinkStatus == "down" {
			actions = append(actions, "SLAVEOF "+s.RealMasterIP+" ON "+rn.PodName)
		}
	}

	ghostFound := false
	for _, sn := range s.SentinelNodes {
		if sn.Reachable && sn.Monitoring {
			for _, r := range sn.Replicas {
				if s.IsGhost(r.IP) && (strings.Contains(r.Flags, "s_down") || strings.Contains(r.Flags, "o_down")) {
					ghostFound = true
					break
				}
			}
		}
	}
	if ghostFound {
		actions = append(actions, fmt.Sprintf("SENTINEL RESET %s", masterName))
	}

	return actions
}
