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
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManagerClientOptions returns the client options the operator's manager is built with.
//
// Secrets are never read through the informer cache. The operator reads only the Secrets
// a LittleRed names (auth, TLS, CA) and only occasionally, but a cached read starts an
// informer over EVERY Secret the operator may see — cluster-wide unless WATCH_NAMESPACE /
// IGNORE_NAMESPACE narrows it (ADR-014), Helm release Secrets included — so memory scales
// with the cluster instead of with the instances it manages. Disabling the cache for the
// type turns each read into a live GET, which is also why the ClusterRole grants Secrets
// `get` only: without list/watch a cached read cannot sync, so one that slips back in
// fails loudly instead of silently re-growing the cache.
//
// Set here rather than at each call site so every Secret read through the manager's
// client — the reconciler, the sentinel and failover monitors, and any added later — is
// covered without having to remember to use the API reader.
func ManagerClientOptions() client.Options {
	return client.Options{
		Cache: &client.CacheOptions{
			DisableFor: []client.Object{&corev1.Secret{}},
		},
	}
}
