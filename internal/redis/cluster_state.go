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
	"slices"
	"sort"
)

const (
	roleMaster  = "master"
	roleReplica = "replica"

	// flagFail is the CLUSTER NODES gossip flag marking a node as failed.
	flagFail = "fail"
)

// ClusterNodeState represents the state of a single node in the Redis Cluster
type ClusterNodeState struct {
	PodName      string
	PodIP        string
	NodeID       string
	Slots        []string
	Role         string // "master" or "replica"
	MasterNodeID string // "-" if master
	LinkStatus   string // "up" or "down"
	Reachable    bool
}

// ClusterGroundTruth represents the combined view of the Redis Cluster topology
type ClusterGroundTruth struct {
	Nodes        map[string]*ClusterNodeState // Map PodName -> ClusterNodeState
	Partitions   [][]string                   // Sets of NodeIDs that see each other
	GhostNodes   []string                     // NodeIDs present in cluster but not in K8s
	ClusterState string                       // "ok" if ANY node says ok, else "fail" or "unknown"
	TotalSlots   int                          // Max slots assigned reported by any node
	AllNodeIDs   map[string]bool              // Set of all NodeIDs seen in the mesh
	// KnownNodes maps a reachable node's NodeID to the set of NodeIDs it directly
	// knows in its own CLUSTER NODES view (excluding fail/noaddr/handshake). It is
	// the same adjacency used to compute Partitions, retained so the operator can
	// avoid issuing CLUSTER REPLICATE before gossip has propagated the target
	// master's NodeID to the executing node (which returns ERR Unknown node).
	KnownNodes map[string][]string
	// AtomicSlotMigration is true iff EVERY reachable node reported support for
	// Redis 8.4+ native atomic slot migration in its CLUSTER INFO. It is a
	// transient, gather-time capability verdict (never persisted): the reshard
	// executor uses it to pick native ASM over the pre-8.4 migrate dance, and a
	// mixed-version cluster mid rolling-upgrade falls back to the dance. Unknown
	// or any node lacking support ⇒ false ⇒ baseline dance. See LR-018 §7.3.
	AtomicSlotMigration bool
}

// NewClusterGroundTruth initializes a new cluster ground truth
func NewClusterGroundTruth() *ClusterGroundTruth {
	return &ClusterGroundTruth{
		Nodes:      make(map[string]*ClusterNodeState),
		AllNodeIDs: make(map[string]bool),
	}
}

func (gt *ClusterGroundTruth) IsHealthy(expectedNodes, expectedShards int32) bool {
	if len(gt.Nodes) < int(expectedNodes) {
		return false
	}
	if len(gt.AllNodeIDs) != int(expectedNodes) {
		return false
	}
	if gt.HasPartitions() {
		return false
	}
	if gt.CountMasters() != int(expectedShards) {
		return false
	}
	// An empty master (a node that is a master with no slots) means a shard is
	// under-replicated and a node is dead weight — not a healthy steady state.
	// Reporting it as healthy lets updateClusterStatus declare Phase=Running and
	// drop to the slow steady requeue cadence, which can stall the empty-master
	// reattach (Step 4) past the e2e topology-sync window. Step 4 always has a
	// reattach target while an empty master exists, so this clause is operator-
	// actionable and cannot deadlock. See RECONCILIATION_ALGORITHM_CHANGELOG.md (LR-014).
	if gt.HasEmptyMasters() {
		return false
	}
	return gt.ClusterState == "ok" && gt.TotalSlots == 16384
}

func (gt *ClusterGroundTruth) HasPartitions() bool {
	return len(gt.Partitions) > 1
}

func (gt *ClusterGroundTruth) HasGhostNodes() bool {
	return len(gt.GhostNodes) > 0
}

func (gt *ClusterGroundTruth) HasOrphanedSlots() bool {
	return gt.TotalSlots < 16384
}

func (gt *ClusterGroundTruth) CountMasters() int {
	count := 0
	for _, n := range gt.Nodes {
		if n.Role == roleMaster && len(n.Slots) > 0 {
			count++
		}
	}
	return count
}

func (gt *ClusterGroundTruth) GetLargestPartitionSeed() *ClusterNodeState {
	if len(gt.Partitions) == 0 {
		for _, n := range gt.Nodes {
			return n
		}
		return nil
	}

	maxIdx := 0
	maxLen := 0
	for i, p := range gt.Partitions {
		if len(p) > maxLen {
			maxLen = len(p)
			maxIdx = i
		}
	}

	targetID := gt.Partitions[maxIdx][0]
	for _, n := range gt.Nodes {
		if n.NodeID == targetID {
			return n
		}
	}
	return nil
}

func (gt *ClusterGroundTruth) GetEmptyMasters() []*ClusterNodeState {
	var empty []*ClusterNodeState
	for _, n := range gt.Nodes {
		if n.Role == roleMaster && len(n.Slots) == 0 {
			empty = append(empty, n)
		}
	}
	return empty
}

// HasEmptyMasters returns true if any node is a master with no slots assigned.
func (gt *ClusterGroundTruth) HasEmptyMasters() bool {
	for _, n := range gt.Nodes {
		if n.Role == roleMaster && len(n.Slots) == 0 {
			return true
		}
	}
	return false
}

// NodeKnows reports whether the node identified by observerID directly knows
// targetID in its own gossip view. Returns false when the observer's view was
// not gathered (e.g. it was unreachable, or KnownNodes was never populated) —
// the safe default for gating CLUSTER REPLICATE, which fails with ERR Unknown
// node if the executing node does not yet know the target.
func (gt *ClusterGroundTruth) NodeKnows(observerID, targetID string) bool {
	return slices.Contains(gt.KnownNodes[observerID], targetID)
}

func (gt *ClusterGroundTruth) GetMastersWithReplicas() map[string][]string {
	m := make(map[string][]string)
	for _, n := range gt.Nodes {
		if n.Role == roleReplica && n.MasterNodeID != "-" {
			m[n.MasterNodeID] = append(m[n.MasterNodeID], n.NodeID)
		}
	}
	return m
}

// MeetVerdict is the attribution verdict for one candidate CLUSTER MEET target
// (LR-043). MEET is the only Redis operation that creates a *fresh* identity
// binding: `clusterStartHandshake` validates nothing but the address syntax, the
// receiver trusts an inbound MEET's whole gossip section, and the initiator adopts
// whatever node ID the responder reports (`clusterRenameNode`). Node-ID keying
// protects only nodes we already know. So the operator must never MEET an address it
// has not attributed to this instance in the current pass.
type MeetVerdict string

const (
	// MeetAllowMember: the candidate already names another node of ours in its own
	// gossip view — a genuinely partitioned or rejoining node of this instance.
	MeetAllowMember MeetVerdict = "member"
	// MeetAllowFresh: the candidate is ISOLATED — its node table names nobody but
	// itself. That is what a new, restarted or wiped pod of ours looks like
	// (bootstrap's normal case), what a survivor whose peers were FORGOTten looks
	// like, and what an LR-018 consolidated master cut off from its peers looks
	// like. It is also what a foreign isolated node looks like: an isolated node
	// cannot be attributed from bus state at all, because the cluster bus carries
	// no instance identity. Admitting it is a deliberate concession, and the reason
	// confirmPodIP (not this predicate) is the primary guard.
	MeetAllowFresh MeetVerdict = "isolated"

	// MeetDenyNoAddress: no pod IP to dial.
	MeetDenyNoAddress MeetVerdict = "no-address"
	// MeetDenyUnidentified: nothing answered our identity probe at that address, so
	// we know nothing about what is there.
	MeetDenyUnidentified MeetVerdict = "unidentified"
	// MeetDenyNoView: the address answered but its CLUSTER NODES view was not
	// gathered, so there is no attribution evidence either way.
	MeetDenyNoView MeetVerdict = "no-gossip-view"
	// MeetDenyUnattributed: the address answers as a cluster node we cannot attribute
	// to this instance — the recycled-IP / foreign-cluster case.
	MeetDenyUnattributed MeetVerdict = "unattributed"
)

// Allowed reports whether the verdict permits a CLUSTER MEET on bus evidence alone.
func (v MeetVerdict) Allowed() bool {
	return v == MeetAllowMember || v == MeetAllowFresh
}

// AdmissibleWhenConfirmed reports whether the verdict permits a CLUSTER MEET once the
// address has been POSITIVELY CONFIRMED at the API server (`confirmPodIP`): our own,
// non-terminating pod object reports this exact address right now.
//
// The two guards are not equal in evidentiary strength, and the first landing of LR-043
// let the weaker one veto the stronger. Kubernetes holds at most one live pod per IP, so
// a confirmed address IS attribution — a fact, not an inference. Bus-state attribution is
// inference over a protocol that carries no instance identity, and its stated purpose is
// narrower: to catch a confirmed-ours address where something FOREIGN is nonetheless
// answering, which is reachable only in the window where a pod object still reports an
// address the CNI has released. `confirmPodIP` now refuses a terminating pod, which closes
// exactly that window — so `unattributed` becomes a WARNING on a confirmed address rather
// than a veto.
//
// Why the demotion is the right direction (regression section of changelog LR-043): a
// guard that can deny a legitimate own node is strictly more dangerous here than one that
// admits a foreign node inside a narrow window. The deny is a PERMANENT stall — a partial
// wipe leaves the surviving data-holder naming only ghosts of recycled peers, so it has no
// "known-ours" anchor and can never acquire one, because acquiring one requires the very
// MEET being refused. The admit needs a rare coincidence of an unattributed address that
// Kubernetes still reports as our own live pod's.
//
// The hard denials are NOT relaxed. `no-address` / `unidentified` / `no-gossip-view` mean
// there is no evidence at all, which no API-server read can supply, and they are
// self-clearing: partitions are computed only over operator-reachable nodes, so an
// unidentified address is in no detected partition and re-enters the plan on the pass where
// it answers. Only `unattributed` is a verdict ABOUT a node we can see.
func (v MeetVerdict) AdmissibleWhenConfirmed() bool {
	return v.Allowed() || v == MeetDenyUnattributed
}

// MeetCandidate is the evidence AttributeMeetTarget decides on. Every field comes from
// data the caller has already gathered (identity probe + CLUSTER NODES), so attribution
// costs no extra Redis round-trip in the repair loop.
type MeetCandidate struct {
	PodName string
	PodIP   string
	// NodeID is the candidate's own node ID, as reported by the address itself.
	NodeID string
	// Identified is true when the address answered an identity probe this pass.
	Identified bool
	// ViewKnown is true when the candidate's own CLUSTER NODES view was gathered.
	// False means "we did not see its view", which is NOT the same as "its view is
	// empty" — the latter would read as an isolated fresh pod.
	ViewKnown bool
	// KnownIDs is the candidate's own non-failed gossip view, including itself.
	KnownIDs []string
	// Slots are the slot ranges the candidate reports owning.
	Slots []string
}

// AttributeMeetTarget decides whether the operator may CLUSTER MEET the candidate's
// address (LR-043). The safety property is "never MEET an address this instance has not
// attributed to itself this pass" — deliberately stronger than "skip unreachable",
// because a foreign pod that shares our password answers our probes perfectly well.
//
// ourNodeIDs is the set of node IDs the operator identified for its own pod names this
// pass.
//
// What this closes and what it concedes. It closes the ESTABLISHED-foreign-cluster
// merge: a node that names peers, none of them ours, is refused — and that is the case
// that costs, because such a node arrives owning slots and carrying a config epoch. It
// concedes the ISOLATED case entirely: an isolated node cannot be attributed from bus
// state, since the bus carries no instance identity and no authentication, and our own
// pods are routinely isolated (fresh, wiped, post-FORGET survivor, LR-018 consolidated
// master cut off from its peers). Slot alignment was tried here and removed: it is a
// pure function of `shards`, so it admitted an aligned foreign node just as readily
// (~no safety) while REFUSING a legitimate own master owning more than one range — the
// LR-018 state, i.e. a repair step that can never fire. The isolated case is protected
// by confirmPodIP instead, which is why that is the primary guard.
func AttributeMeetTarget(c MeetCandidate, ourNodeIDs map[string]bool) MeetVerdict {
	if c.PodIP == "" {
		return MeetDenyNoAddress
	}
	if !c.Identified || c.NodeID == "" {
		return MeetDenyUnidentified
	}
	if !c.ViewKnown {
		return MeetDenyNoView
	}

	peers := 0
	for _, id := range c.KnownIDs {
		if id == "" || id == c.NodeID {
			continue
		}
		peers++
		// A positive attribution: the candidate already knows a node of ours, which a
		// stranger cannot without prior contact. Tolerating unknown IDs alongside it is
		// deliberate — our own nodes routinely still list ghosts of past incarnations.
		if ourNodeIDs[id] {
			return MeetAllowMember
		}
	}
	if peers > 0 {
		// Knows peers, none of them ours: an established cluster that is not this one.
		return MeetDenyUnattributed
	}

	// Isolated: names nobody but itself. Allowed regardless of the slots it holds —
	// see the concession in the doc comment above.
	return MeetAllowFresh
}

// nodeFlagsFailed reports whether a CLUSTER NODES entry's flags exclude it from the
// observing node's live gossip view. It is the single definition of that filter, shared
// by partition detection (gatherTopology) and MEET attribution.
func nodeFlagsFailed(flags []string) bool {
	for _, f := range flags {
		if f == flagFail || f == "noaddr" || f == "handshake" {
			return true
		}
	}
	return false
}

// MeetCandidateFromNodes builds the attribution input for one address from that
// address's own CLUSTER NODES output (its live view plus the slots it claims). Pass
// nodeID when the caller already knows it (CLUSTER MYID), or "" to take it from the
// view's `myself` entry. Callers that hold a gathered ClusterGroundTruth do not need
// this — PlanPartitionMeets derives the same evidence from KnownNodes at no extra cost.
func MeetCandidateFromNodes(podName, podIP, nodeID string, view []ClusterNodeInfo) MeetCandidate {
	if nodeID == "" {
		for i := range view {
			if slices.Contains(view[i].Flags, "myself") {
				nodeID = view[i].NodeID
				break
			}
		}
	}
	c := MeetCandidate{
		PodName: podName, PodIP: podIP, NodeID: nodeID,
		Identified: nodeID != "", ViewKnown: nodeID != "",
	}
	for i := range view {
		if !nodeFlagsFailed(view[i].Flags) {
			c.KnownIDs = append(c.KnownIDs, view[i].NodeID)
		}
		if view[i].NodeID == nodeID {
			c.Slots = view[i].Slots
		}
	}
	return c
}

// MeetSkip records one candidate the operator declined to MEET, for the audit log. A
// silently suppressed MEET is exactly what makes a future partition-healing bug hard to
// diagnose, so every skip is reported with its reason.
type MeetSkip struct {
	PodName string
	PodIP   string
	NodeID  string
	Verdict MeetVerdict
}

// MeetPlan is the Step 1 partition-healing plan: the seed to issue CLUSTER MEET at, the
// attributable targets to introduce to it, and every skipped candidate with its reason.
// Seed == nil means no MEET may be issued this pass (SeedVerdict says why).
type MeetPlan struct {
	Seed        *ClusterNodeState
	SeedVerdict MeetVerdict
	Targets     []*ClusterNodeState
	Skipped     []MeetSkip
	// Unattributed lists the entries of Targets that bus-state attribution alone would
	// have refused, and which are admitted only because the caller confirms the address
	// at the API server. Reported so a genuine cross-instance merge stays diagnosable:
	// these are the MEETs where Kubernetes and the cluster bus disagreed.
	Unattributed []MeetSkip
}

// PlanPartitionMeets builds the Step 1 MEET plan from gathered ground truth. Targets and
// the seed are both screened by AttributeMeetTarget — the seed too, because the MEET is
// issued *at* the seed, so an unattributable seed would be told to meet all of our pods
// (the same cluster merge in the other direction). Targets are returned in deterministic
// pod-name order.
//
// A verdict of `unattributed` does NOT remove a candidate from Targets; it records it in
// Unattributed and leaves the decision to the caller's API-server confirmation, which is
// the stronger evidence (see AdmissibleWhenConfirmed). Only the no-evidence verdicts are
// hard-skipped. THE CALLER MUST CALL confirmPodIP FOR THE SEED AND EVERY TARGET — this
// plan is admissibility, not permission.
func (gt *ClusterGroundTruth) PlanPartitionMeets() MeetPlan {
	ourIDs := make(map[string]bool, len(gt.Nodes))
	for _, n := range gt.Nodes {
		if n.Reachable && n.NodeID != "" {
			ourIDs[n.NodeID] = true
		}
	}

	plan := MeetPlan{}
	seed := gt.GetLargestPartitionSeed()
	if seed == nil {
		plan.SeedVerdict = MeetDenyUnidentified
		return plan
	}
	plan.SeedVerdict = AttributeMeetTarget(gt.meetCandidate(seed), ourIDs)
	if !plan.SeedVerdict.AdmissibleWhenConfirmed() {
		return plan
	}
	plan.Seed = seed

	names := make([]string, 0, len(gt.Nodes))
	for podName := range gt.Nodes {
		names = append(names, podName)
	}
	sort.Strings(names)

	for _, podName := range names {
		n := gt.Nodes[podName]
		if n.NodeID != "" && n.NodeID == seed.NodeID {
			continue
		}
		v := AttributeMeetTarget(gt.meetCandidate(n), ourIDs)
		skip := MeetSkip{PodName: n.PodName, PodIP: n.PodIP, NodeID: n.NodeID, Verdict: v}
		if v.AdmissibleWhenConfirmed() {
			plan.Targets = append(plan.Targets, n)
			if !v.Allowed() {
				plan.Unattributed = append(plan.Unattributed, skip)
			}
			continue
		}
		plan.Skipped = append(plan.Skipped, skip)
	}
	return plan
}

// meetCandidate projects a gathered node into the attribution input. The gossip view
// comes from KnownNodes, which the gather already retains for partition detection
// (LR-014) — so attribution adds no Redis round-trip.
func (gt *ClusterGroundTruth) meetCandidate(n *ClusterNodeState) MeetCandidate {
	view, viewKnown := gt.KnownNodes[n.NodeID]
	return MeetCandidate{
		PodName:    n.PodName,
		PodIP:      n.PodIP,
		NodeID:     n.NodeID,
		Identified: n.Reachable && n.NodeID != "",
		ViewKnown:  n.NodeID != "" && viewKnown,
		KnownIDs:   view,
		Slots:      n.Slots,
	}
}
