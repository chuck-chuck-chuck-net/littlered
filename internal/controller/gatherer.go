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

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

type operatorGatherer struct {
	password   string
	tlsEnabled bool
}

func (g *operatorGatherer) GetRedisState(ctx context.Context, podName, ip string) (*redisclient.RedisNodeState, error) {
	// Hard per-probe deadline, same rationale as the cluster probes below: a stale/
	// dead pod IP (INFO on a blackholing address) must fail fast instead of blocking
	// the gather on dial retries. Sentinel mode previously lacked this. See ProbeTimeout.
	ctx, cancel := context.WithTimeout(ctx, redisclient.ProbeTimeout)
	defer cancel()
	addr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.RedisPort)
	snap, err := redisclient.GetReplicationInfo(ctx, addr, g.password, g.tlsEnabled)
	if err != nil {
		return nil, err
	}
	return &redisclient.RedisNodeState{
		PodName:    podName,
		IP:         ip,
		Role:       snap.Role,
		MasterHost: snap.MasterHost,
		LinkStatus: snap.MasterLinkStatus,
		Offset:     snap.Offset,
		Keys:       snap.Keys,
		Replid:     snap.Replid,
		Replid2:    snap.Replid2,
		Reachable:  true,
	}, nil
}

func (g *operatorGatherer) GetSentinelState(
	ctx context.Context, podName, ip, masterName string,
) (*redisclient.SentinelNodeState, error) {
	// Defence in depth for LR-041. The name is a parameter now, so the compiler
	// demands one at every call site and the original defect — an omitted struct
	// field zero-valuing to "" — is no longer expressible. This catches only an
	// explicit empty argument.
	//
	// It has to be refused rather than passed through: `SENTINEL master ""` draws
	// the same `ERR No such master with that name` as a genuine miss, so the
	// not-monitoring branch below would report the sentinel as reachable-but-bare,
	// which is indistinguishable from ordinary post-restart churn and is what let
	// the original bug hide for a whole release. GatherReplicationState maps this
	// error to Reachable:false, a state no rule acts on destructively.
	if masterName == "" {
		return nil, fmt.Errorf("sentinel gather requires a master name, but none was passed")
	}
	podAddr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.SentinelPort)
	sc := redisclient.NewSentinelClient([]string{podAddr}, g.password, g.tlsEnabled)

	masterInfo, err := sc.GetMasterState(ctx, masterName)
	if err != nil {
		if strings.Contains(err.Error(), "ERR No such master") || strings.Contains(err.Error(), "redis: nil") {
			return &redisclient.SentinelNodeState{
				PodName:          podName,
				IP:               ip,
				Monitoring:       false,
				Reachable:        true,
				MonitoredMasters: g.monitoredMasters(ctx, sc, podAddr),
			}, nil
		}
		return nil, err
	}

	state := &redisclient.SentinelNodeState{
		PodName:             podName,
		IP:                  ip,
		Monitoring:          true,
		MonitoredMasters:    g.monitoredMasters(ctx, sc, podAddr),
		MasterIP:            masterInfo.IP,
		MasterFlags:         masterInfo.Flags,
		MasterFailoverState: masterInfo.FailoverState,
		NumOtherSentinels:   masterInfo.NumOtherSentinels,
		NumSlaves:           masterInfo.NumSlaves,
		Reachable:           true,
	}

	if reps, err := sc.GetReplicas(ctx, masterName); err == nil {
		state.Replicas = reps
	}

	return state, nil
}

// monitoredMasters reads every master name this Sentinel monitors.
//
// Issued UNCONDITIONALLY — one extra bounded round trip per Sentinel per pass —
// rather than lazily only when a Sentinel reads bare. A Sentinel carrying BOTH a
// leftover name and the desired one answers `Monitoring: true`, so a probe
// triggered by bareness would never see the two-name state, which is the state a
// previous half-finished rename actually leaves behind. Deriving the trigger from
// `Monitoring` would also couple it to the one field that lies during a rename.
//
// A failure degrades to an empty list, never to Reachable:false. Reporting a
// perfectly healthy Sentinel as dead because one added question went unanswered is
// exactly the LR-041 class of mistake — a plausible-looking lie that silently
// disables every rule gated on the Sentinel's state.
//
// Called only once the Sentinel has already answered GetMasterState, so a dead or
// blackholing address costs no extra probe.
func (g *operatorGatherer) monitoredMasters(
	ctx context.Context, sc *redisclient.SentinelClient, podAddr string,
) []redisclient.MonitoredMaster {
	masters, err := sc.GetMonitoredMasters(ctx, podAddr)
	if err != nil {
		return nil
	}
	return masters
}

func (g *operatorGatherer) GetClusterID(ctx context.Context, podName, ip string) (string, error) {
	// Hard per-probe deadline: a stale/dead pod IP must fail fast instead of
	// blocking the gather (and thus the reconcile loop) on dial retries. See LR-012.
	ctx, cancel := context.WithTimeout(ctx, redisclient.ProbeTimeout)
	defer cancel()
	cc := redisclient.NewClusterClient(g.password, g.tlsEnabled)
	addr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.RedisPort)
	return cc.GetMyID(ctx, addr)
}

func (g *operatorGatherer) GetClusterInfo(ctx context.Context, podName, ip string) (*redisclient.ClusterInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, redisclient.ProbeTimeout)
	defer cancel()
	cc := redisclient.NewClusterClient(g.password, g.tlsEnabled)
	addr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.RedisPort)
	return cc.GetClusterInfo(ctx, addr)
}

func (g *operatorGatherer) GetClusterNodes(ctx context.Context, podName, ip string) ([]redisclient.ClusterNodeInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, redisclient.ProbeTimeout)
	defer cancel()
	cc := redisclient.NewClusterClient(g.password, g.tlsEnabled)
	addr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.RedisPort)
	return cc.GetClusterNodes(ctx, addr)
}
