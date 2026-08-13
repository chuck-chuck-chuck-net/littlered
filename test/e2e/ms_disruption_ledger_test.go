//go:build e2e
// +build e2e

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

package e2e

// ms_disruption_ledger_test.go is a DURABLE ledger of outstanding lab disruptions, so the promise
// "leave the platform healthy" survives an INTERRUPTED run. In-process cleanup (AfterEach) is a
// promise we structurally cannot keep: a Ctrl-C / timeout / panic between "disrupt" and "restore"
// leaves the lab damaged with no cleanup ever running. And a whole-site kill is un-discoverable —
// you cannot query a powered-off cluster to learn it is off, or which VMs to restart. So each
// disruption writes a small ConfigMap on the HUB (a "poor-man's custom resource") BEFORE it is
// applied (write-ahead); the matching restore deletes it. The `ms-heal` spec (Label("ms-heal"))
// reads the surviving records and undoes each — reusing the SAME Go restore helpers as inline
// cleanup, so there is one undo implementation.
//
// Scope is deliberately small (the user's explicit fence): exactly the disruption kinds we have
// (whole-site kill, data-plane CNP cut), a flat typed record, and a linear heal loop. NO CRD, NO
// controller, NO reconcile framework, NO generic plugin registry. We assume EXCLUSIVE access to
// the clusters (one ms e2e run at a time), so a single hub namespace holds the whole ledger.

import (
	"fmt"
	"strings"

	. "github.com/onsi/gomega"
)

const (
	// msDisruptionNS is the hub namespace holding the ledger. A dedicated namespace (not the
	// operator's) keeps test artifacts separate; a single one is fine because we assume exclusive
	// (non-parallel) access to the lab.
	msDisruptionNS = "e2e-disruptions"
	// msDisruptionLabel marks a ledger ConfigMap, so the heal spec can list them.
	msDisruptionLabel = "redis.chuck-chuck-chuck.net/e2e-disruption"

	msDisruptionKindSite = "sitekill" // a whole site powered off at the hypervisor (undo: virsh start)
	msDisruptionKindCNP  = "cnpcut"   // a data-plane CiliumNetworkPolicy cut (undo: delete the policy)
)

// msDisruption is one outstanding lab disruption to undo. site is the logical site name (the heal
// spec resolves its kube-context via the registry); ns is the instance namespace (cnpcut only).
type msDisruption struct {
	kind string
	site string
	ns   string
}

// msDisruptionName is the deterministic ConfigMap name for a disruption, so the code that applies
// it and the code that undoes it (inline OR heal) address the same record without coordination.
func msDisruptionName(d msDisruption) string {
	if d.kind == msDisruptionKindSite {
		return "sitekill-" + d.site
	}
	return "cnpcut-" + d.site + "-" + d.ns
}

// ensureDisruptionNS creates the ledger namespace on the hub if absent (idempotent via apply).
func ensureDisruptionNS(reg *msRegistry) {
	nsYAML := fmt.Sprintf("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n", msDisruptionNS)
	_, err := kctlIn(reg.hubContext, nsYAML, "apply", "-f", "-")
	Expect(err).NotTo(HaveOccurred(), "ensuring the %s ledger namespace on the hub", msDisruptionNS)
}

// recordDisruption writes the ledger ConfigMap on the hub. Call it WRITE-AHEAD (before applying the
// disruption): undo is idempotent, so a crash in the record→apply gap merely heals a disruption
// that was not fully applied — the safe direction. Recording AFTER apply would leave a crash window
// with damage but no record, the exact failure this ledger exists to prevent.
func recordDisruption(reg *msRegistry, d msDisruption) {
	ensureDisruptionNS(reg)
	cm := fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: %s
  namespace: %s
  labels:
    %s: "true"
data:
  kind: %q
  site: %q
  ns: %q
`, msDisruptionName(d), msDisruptionNS, msDisruptionLabel, d.kind, d.site, d.ns)
	_, err := kctlIn(reg.hubContext, cm, "apply", "-f", "-")
	Expect(err).NotTo(HaveOccurred(), "recording disruption %s", msDisruptionName(d))
}

// clearDisruption deletes the ledger record after the disruption has been undone (idempotent).
func clearDisruption(reg *msRegistry, d msDisruption) {
	_, _ = kctl(reg.hubContext, "-n", msDisruptionNS, "delete", "configmap",
		msDisruptionName(d), "--ignore-not-found")
}

// listDisruptions returns every recorded disruption on the hub. A missing ledger namespace (never
// disrupted, or already cleaned) reads as none.
func listDisruptions(reg *msRegistry) []msDisruption {
	out, err := kctl(reg.hubContext, "-n", msDisruptionNS, "get", "configmap",
		"-l", msDisruptionLabel,
		"-o", `jsonpath={range .items[*]}{.data.kind}{"|"}{.data.site}{"|"}{.data.ns}{"\n"}{end}`)
	if err != nil {
		return nil
	}
	var ds []msDisruption
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		f := strings.SplitN(line, "|", 3)
		if len(f) != 3 {
			continue
		}
		ds = append(ds, msDisruption{kind: f[0], site: f[1], ns: f[2]})
	}
	return ds
}
