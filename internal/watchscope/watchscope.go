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

// Package watchscope parses the operator's namespace-scoping configuration
// (ADR-014) into a Config that drives the manager cache options and the
// leader-election lease ID. It is a pure, I/O-free helper so the parse/derive
// logic is unit-testable without a live manager.
package watchscope

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

// BaseLeaderElectionID is the fixed lease ID used by the unscoped
// (cluster-scoped) operator today; kept unchanged for backward compatibility.
const BaseLeaderElectionID = "64adfe7c.chuck-chuck-chuck.net"

// namespaceFieldSelector is the field the deny-list scopes on. metadata.namespace
// is a server-supported field selector for List/Watch on every namespaced type.
const namespaceFieldSelector = "metadata.namespace"

// leaseSuffixLen is the number of hex chars of the scope hash appended to the
// base lease ID for scoped operators. 8 hex chars (32 bits) is ample to keep
// disjoint-scope operators from colliding while keeping the name short.
const leaseSuffixLen = 8

// ErrBothSet is returned when both WATCH_NAMESPACE and IGNORE_NAMESPACE are set.
// The two scoping modes are mutually exclusive (ADR-014 Decision 1); the caller
// must treat this as a fatal startup error and never guess a merge.
var ErrBothSet = errors.New(
	"WATCH_NAMESPACE (allow-list) and IGNORE_NAMESPACE (deny-list) are mutually exclusive; set at most one")

// Mode is the operator's namespace-scoping mode.
type Mode string

const (
	// ModeNone is the default cluster-scoped behavior (no scoping).
	ModeNone Mode = "none"
	// ModeAllow watches only the listed namespaces (WATCH_NAMESPACE).
	ModeAllow Mode = "allow"
	// ModeDeny watches all namespaces except the listed ones (IGNORE_NAMESPACE).
	ModeDeny Mode = "deny"
)

// Config is the parsed scoping configuration.
type Config struct {
	// Mode is the scoping mode: none (cluster-scoped), allow, or deny.
	Mode Mode
	// Namespaces is the scope set: sorted, deduped, empty-trimmed. Empty for none.
	Namespaces []string
	// LeaderElectionID is the derived lease ID: the base ID for none, or a
	// scope-derived stable variant for allow/deny so disjoint-scope operators
	// never share a lease.
	LeaderElectionID string
}

// Parse turns the raw WATCH_NAMESPACE / IGNORE_NAMESPACE env values into a
// Config. Each is comma-separated; entries are trimmed, empties dropped,
// deduped, and sorted for determinism. Setting both is a fatal error
// (ErrBothSet). Setting neither (or only whitespace) yields ModeNone.
func Parse(watchNamespace, ignoreNamespace string) (Config, error) {
	watch := splitNamespaces(watchNamespace)
	ignore := splitNamespaces(ignoreNamespace)

	if len(watch) > 0 && len(ignore) > 0 {
		return Config{}, ErrBothSet
	}

	switch {
	case len(watch) > 0:
		return Config{
			Mode:             ModeAllow,
			Namespaces:       watch,
			LeaderElectionID: deriveLeaseID(ModeAllow, watch),
		}, nil
	case len(ignore) > 0:
		return Config{
			Mode:             ModeDeny,
			Namespaces:       ignore,
			LeaderElectionID: deriveLeaseID(ModeDeny, ignore),
		}, nil
	default:
		return Config{
			Mode:             ModeNone,
			Namespaces:       nil,
			LeaderElectionID: BaseLeaderElectionID,
		}, nil
	}
}

// CacheOptions maps the Config to controller-runtime cache.Options.
//
//   - none:  zero options (unscoped — watch all namespaces, today's behavior).
//   - allow: DefaultNamespaces with one entry per namespace.
//   - deny:  DefaultFieldSelector = AND of metadata.namespace != ns, applied
//     uniformly to every watched namespaced type (CRs + owned STS/Svc/CM/Pod).
//
// Invariant: the cache holds EXACTLY the declared scope — the operator's own
// namespace is not added, not even for its own bookkeeping objects. The manager's
// client is cache-backed for every type (client.CacheOptions.DisableFor is empty
// in cmd/littlered), so any operator read of an object OUTSIDE this scope — e.g.
// the multi-site site kubeconfig Secrets, which live in the operator's own
// namespace — must go through an uncached reader, never the manager client.
// Widening the cache instead would be wrong twice over: in allow mode it extends
// the cache for every type including the LittleRed CR, so the operator would
// start reconciling CRs in its own namespace (a silent widening of the declared
// scope); in deny mode the scope is one global metadata.namespace!=... field
// selector with no room for a per-namespace exemption at all.
func (c Config) CacheOptions() cache.Options {
	switch c.Mode {
	case ModeAllow:
		nsMap := make(map[string]cache.Config, len(c.Namespaces))
		for _, ns := range c.Namespaces {
			nsMap[ns] = cache.Config{}
		}
		return cache.Options{DefaultNamespaces: nsMap}
	case ModeDeny:
		selectors := make([]fields.Selector, 0, len(c.Namespaces))
		for _, ns := range c.Namespaces {
			selectors = append(selectors, fields.OneTermNotEqualSelector(namespaceFieldSelector, ns))
		}
		return cache.Options{DefaultFieldSelector: fields.AndSelectors(selectors...)}
	default:
		return cache.Options{}
	}
}

// splitNamespaces splits a comma-separated list into a trimmed, empty-dropped,
// deduped, sorted slice. Returns nil for an empty/whitespace-only input.
func splitNamespaces(raw string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, strings.Count(raw, ",")+1)
	for part := range strings.SplitSeq(raw, ",") {
		ns := strings.TrimSpace(part)
		if ns == "" {
			continue
		}
		if _, dup := seen[ns]; dup {
			continue
		}
		seen[ns] = struct{}{}
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// deriveLeaseID builds a stable, scope-unique lease ID: the base ID plus the
// mode and a short deterministic hash of the sorted namespace set. Stable
// across restarts (no time/random); distinct across disjoint scopes.
func deriveLeaseID(mode Mode, namespaces []string) string {
	h := sha256.Sum256([]byte(string(mode) + "\x00" + strings.Join(namespaces, ",")))
	suffix := hex.EncodeToString(h[:])[:leaseSuffixLen]
	return BaseLeaderElectionID + "-" + string(mode) + "-" + suffix
}
