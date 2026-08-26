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
	"strconv"
	"strings"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// Sentinel reply field keys, shared by the replica and master parsers below.
const (
	fieldName  = "name"
	fieldIP    = "ip"
	fieldFlags = "flags"
)

// atoiSafe parses an integer, yielding 0 rather than an error. Every caller is reading
// a Sentinel reply field for reporting purposes, where a malformed value should degrade
// to "unknown" and not abort a diagnostic.
func atoiSafe(s string) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0
	}
	return n
}

// parseSentinelReplicas turns the flat key/value reply of `SENTINEL replicas <name>`
// into one record per replica, carrying each one's REAL flags.
//
// Records are delimited by the `name` key, which Sentinel emits first for each entry.
// resolve maps a reported identity (which may be a hostname) to an IP; pass nil to keep
// the reported value verbatim.
func parseSentinelReplicas(out string, resolve func(string) string) []redisclient.ReplicaInfo {
	lines := strings.Split(strings.ReplaceAll(out, "\r", ""), "\n")
	var (
		reps []redisclient.ReplicaInfo
		cur  *redisclient.ReplicaInfo
	)
	flush := func() {
		if cur != nil && cur.IP != "" {
			reps = append(reps, *cur)
		}
		cur = nil
	}
	for i := 0; i < len(lines)-1; i++ {
		key := strings.TrimSpace(lines[i])
		val := strings.TrimSpace(lines[i+1])
		switch key {
		case fieldName:
			flush()
			cur = &redisclient.ReplicaInfo{}
		case fieldIP:
			if cur != nil {
				if resolve != nil {
					val = resolve(val)
				}
				cur.IP = val
			}
		case "port":
			if cur != nil {
				cur.Port = val
			}
		case fieldFlags:
			if cur != nil {
				cur.Flags = val
			}
		}
	}
	flush()
	return reps
}

// parseSentinelMasters turns the flat key/value reply of `SENTINEL masters` into
// one record per monitored master name.
//
// It is the CLI half of the operator/CLI parity rule (LR-041): the operator reads
// the same reply over the wire, and the two must not be able to disagree about
// which names a Sentinel carries.
//
// Records are delimited by the `name` key, which Sentinel emits first for every
// entry — the same property parseSentinelReplicas relies on. resolve maps a
// reported identity (which may be a hostname) to an IP; pass nil to keep the
// reported value verbatim.
func parseSentinelMasters(out string, resolve func(string) string) []redisclient.MonitoredMaster {
	lines := strings.Split(strings.ReplaceAll(out, "\r", ""), "\n")
	var (
		masters []redisclient.MonitoredMaster
		cur     *redisclient.MonitoredMaster
	)
	flush := func() {
		// A record with no name is unusable — the name is the whole point of the
		// call — and cannot occur in a well-formed reply, since `name` is what
		// opens each record.
		if cur != nil && cur.Name != "" {
			masters = append(masters, *cur)
		}
		cur = nil
	}
	for i := 0; i < len(lines)-1; i++ {
		key := strings.TrimSpace(lines[i])
		val := strings.TrimSpace(lines[i+1])
		switch key {
		case fieldName:
			flush()
			cur = &redisclient.MonitoredMaster{Name: val}
		case fieldIP:
			if cur != nil {
				if resolve != nil {
					val = resolve(val)
				}
				cur.IP = val
			}
		case fieldFlags:
			if cur != nil {
				cur.Flags = val
			}
		case "failover-state":
			// Source-confirmed field name; there is no `failover-status` in
			// either Redis or Valkey. It is emitted ONLY while a failover is in
			// progress — see redisclient.MonitoredMaster.
			if cur != nil {
				cur.FailoverState = val
			}
		}
	}
	flush()
	return masters
}
