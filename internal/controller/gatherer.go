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
	// masterName is the instance's Sentinel master name. Sentinel-mode paths must
	// set it; cluster-mode gatherers never reach GetSentinelState and leave it empty.
	masterName string
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

func (g *operatorGatherer) GetSentinelState(ctx context.Context, podName, ip string) (*redisclient.SentinelNodeState, error) {
	podAddr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.SentinelPort)
	sc := redisclient.NewSentinelClient([]string{podAddr}, g.password, g.tlsEnabled)

	masterInfo, err := sc.GetMasterState(ctx, g.masterName)
	if err != nil {
		if strings.Contains(err.Error(), "ERR No such master") || strings.Contains(err.Error(), "redis: nil") {
			return &redisclient.SentinelNodeState{
				PodName:    podName,
				IP:         ip,
				Monitoring: false,
				Reachable:  true,
			}, nil
		}
		return nil, err
	}

	state := &redisclient.SentinelNodeState{
		PodName:           podName,
		IP:                ip,
		Monitoring:        true,
		MasterIP:          masterInfo.IP,
		MasterFlags:       masterInfo.Flags,
		FailoverStatus:    masterInfo.FailoverStatus,
		NumOtherSentinels: masterInfo.NumOtherSentinels,
		NumSlaves:         masterInfo.NumSlaves,
		Reachable:         true,
	}

	if reps, err := sc.GetReplicas(ctx, g.masterName); err == nil {
		state.Replicas = reps
	}

	return state, nil
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
