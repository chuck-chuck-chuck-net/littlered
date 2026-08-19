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

package cmd

import (
	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/types"
)

// masterNameOf resolves the Sentinel master name to use for an instance.
//
// The master name is per-instance — it is the only isolation boundary Sentinel's
// gossip protocol has — so lrctl must never issue a SENTINEL command against a
// hardcoded name: on a captured or mis-scoped instance that would query the wrong
// master, or none, and report a confidently wrong topology.
//
// Discovery fills SentinelMasterName from the CR. It is empty only when the CR could
// not be read (unmanaged discovery), where the legacy constant is the best available
// guess and is also what such an instance is most likely running.
func masterNameOf(cCtx *types.ClusterContext) string {
	if cCtx != nil && cCtx.SentinelMasterName != "" {
		return cCtx.SentinelMasterName
	}
	return littleredv1alpha1.LegacySentinelMasterName
}
