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
	"testing"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestCachedObjectsCarryManagedByLabel enumerates every builder of a type the manager's
// cache filters on app.kubernetes.io/managed-by (ConfigMap, Service, StatefulSet, PDB) in
// every mode, and requires the label on each object. An object without it is invisible
// to the operator's cache: Owns() never sees its events and a cached read returns
// NotFound. The CR also tries to override the label through every per-object label knob
// and through its own labels, since user input must never be able to hide an object.
func TestCachedObjectsCarryManagedByLabel(t *testing.T) {
	withMode := func(mode string) *littleredv1alpha1.LittleRed {
		lr := withCRMetadata(map[string]string{labelAppManagedBy: metaHijackValue}, nil)
		lr.Spec.Mode = mode
		enabled := true
		lr.Spec.Metrics.Enabled = &enabled
		lr.Spec.Service.Labels = map[string]string{labelAppManagedBy: metaHijackValue}
		lr.SetDefaults()
		return lr
	}

	modes := map[string]func(lr *littleredv1alpha1.LittleRed) []client.Object{
		ModeStandalone: func(lr *littleredv1alpha1.LittleRed) []client.Object {
			return []client.Object{buildStatefulSet(lr), buildService(lr), buildConfigMap(lr)}
		},
		ModeSentinel: func(lr *littleredv1alpha1.LittleRed) []client.Object {
			return []client.Object{
				buildRedisStatefulSetSentinel(lr, 3), buildSentinelStatefulSet(lr, 3),
				buildConfigMapSentinelMode(lr), buildSentinelConfigMap(lr),
				buildMasterService(lr), buildReplicasHeadlessService(lr), buildSentinelHeadlessService(lr),
				buildSentinelRedisPDB(lr), buildSentinelPDB(lr),
			}
		},
		ModeCluster: func(lr *littleredv1alpha1.LittleRed) []client.Object {
			return []client.Object{
				buildClusterShardStatefulSet(lr, 0, nil), buildClusterConfigMap(lr),
				buildClusterHeadlessService(lr), buildClusterClientService(lr), buildClusterShardPDB(lr, 0),
			}
		},
		ModeFailover: func(lr *littleredv1alpha1.LittleRed) []client.Object {
			return []client.Object{
				buildRedisStatefulSetFailover(lr), buildConfigMapFailoverMode(lr), buildFailoverRedisPDB(lr),
				buildMasterService(lr), buildReplicasHeadlessService(lr),
			}
		},
	}

	for mode, objects := range modes {
		t.Run(mode, func(t *testing.T) {
			for _, obj := range objects(withMode(mode)) {
				if got := obj.GetLabels()[labelAppManagedBy]; got != managedByValue {
					t.Errorf("%T %q: %s = %q, want %q", obj, obj.GetName(), labelAppManagedBy, got, managedByValue)
				}
			}
		})
	}
}
