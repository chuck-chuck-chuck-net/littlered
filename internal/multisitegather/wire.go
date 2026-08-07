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

// Package multisitegather holds the JSON wire envelope exchanged between the
// operator's one-shot `--gather` mode (the tier-2 hub Job) and lrctl. The
// operator gathers every site (where it already has working site access) and
// prints this envelope; lrctl parses it and renders with the SAME functions it
// uses for a direct gather — so the tier-2 (via-hub-Job) and tier-3 (direct
// site contexts) paths are one rendering over two transports. The envelope
// travels the Kubernetes API control path (Job pod logs), never a data path
// between hub and sites (docs/MULTI_SITE_DESIGN.md §8 / the tier model).
package multisitegather

import (
	"time"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// Result is the site-attributed multi-site ground truth as gathered on the hub.
// GroundTruth is the union cluster view (keys are site-qualified, site/objName);
// Sites is the per-site observation summary (reachability + pod count) so the
// reader can distinguish "site unreachable" from "site hosts no pods".
type Result struct {
	Instance    string                          `json:"instance"`
	Namespace   string                          `json:"namespace"`
	GatheredAt  time.Time                       `json:"gatheredAt"`
	Sites       []SiteObservation               `json:"sites"`
	GroundTruth *redisclient.ClusterGroundTruth `json:"groundTruth"`
}

// SiteObservation is one site's Kubernetes-API-observed state this gather:
// whether its API answered and how many of this instance's cluster pods it hosts.
type SiteObservation struct {
	Name      string `json:"name"`
	Reachable bool   `json:"reachable"`
	Pods      int    `json:"pods"`
	Error     string `json:"error,omitempty"`
}
