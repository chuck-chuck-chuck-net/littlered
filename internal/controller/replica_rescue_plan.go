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
	"sort"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// planReplicaRescue is Rule R's decision seam (LR-009/LR-010): which reachable
// pods are not following the consensus master and must be pointed at it.
//
// Extracted as a pure function by LR-060, because Rule A no longer suppresses
// Rule R during a reported failover and the one pod that must NOT be touched in
// that window needs a guard that is testable without a cluster.
//
// The trigger is unchanged from LR-010 — a definitively wrong Role or MasterHost,
// never LinkStatus alone. That exclusion is load-bearing here and not merely
// historical: it is what makes Rule R unable to interrupt a replica that is
// mid-sync from the CORRECT master, which is the interference that would matter
// most while Sentinel is reconfiguring replicas.
//
// Results are sorted by pod name so the plan is deterministic.
func planReplicaRescue(state *redisclient.ReplicationState) []*redisclient.RedisNodeState {
	if state == nil || state.RealMasterIP == "" {
		return nil
	}
	promoted := state.PromotedIPs()

	var out []*redisclient.RedisNodeState
	for ip, rn := range state.RedisNodes {
		if rn == nil || !rn.Reachable || ip == state.RealMasterIP {
			continue
		}
		// LR-060: never demote the pod Sentinel has just promoted.
		//
		// During the ~2s in which a failover's promotion has happened but the
		// Sentinel majority has not yet caught up, RealMasterIP is still the
		// OUTGOING master while the promoted pod reports role:master — so the
		// unchanged trigger below fires on it and Rule R would issue
		// SLAVEOF <outgoing>, undoing the failover. That is the ENTIRE hazard of
		// letting Rule R run during a failover: every other pod in that window
		// either has the correct MasterHost or is mid-sync, and neither trips the
		// trigger. Measured on t3e 2026-09-03: 2 samples of 179.
		//
		// Note this needs no "only during a failover" qualifier. SRI_PROMOTED is
		// set only while a failover is in flight and is cleared by
		// sentinelResetMaster the moment it ends, so outside one no entry carries
		// the flag and the clause is inert. Fewer conditions, and it cannot be
		// wrong about whether a failover is running.
		if promoted[ip] {
			continue
		}
		if rn.Role == RoleMaster || rn.MasterHost != state.RealMasterIP {
			out = append(out, rn)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PodName < out[j].PodName })
	return out
}
