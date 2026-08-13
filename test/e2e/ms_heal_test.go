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

// ms_heal_test.go is the out-of-band recovery for the multi-site lab: it undoes every disruption
// still recorded in the ledger (ms_disruption_ledger_test.go) and asserts the platform is clean.
// It exists because a DEBUG_ON_FAILURE (or interrupted) run deliberately LEAVES disruptions in
// place for investigation — a killed site stays down, a data-plane cut stays applied — and the
// promise that we always restore the lab can only be kept by an out-of-band healer.
//
// Run it on its own:  make test-e2e SKIP_KIND_SETUP=true SKIP_OPERATOR_DEPLOY=true \
//     KUBECONTEXT=<hub> LABEL_FILTER='ms-heal' MS_HUB_CONTEXT=... MS_SITE_CONTEXTS=... \
//     MS_SITE_ENDPOINTS=... MS_VM_INVENTORY=...   (the same env the disrupting run used).
//
// It reuses the SAME restore helpers as inline cleanup (msStartSiteVMs / msHealDataPlane), so there
// is one undo implementation. It is opt-in (its own label, not "multisite"), and it is a no-op when
// the ledger is empty — so running it can only ever move the lab toward healthy.

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Multi-Site Disruption Heal", Label("ms-heal"), func() {
	It("undoes every recorded lab disruption and leaves the platform clean", func() {
		reg := msRegistryFromEnv() // Skips if the multi-site env is absent

		ds := listDisruptions(reg)
		if len(ds) == 0 {
			By("no recorded disruptions — the lab is already clean")
			return
		}

		By(fmt.Sprintf("healing %d recorded disruption(s)", len(ds)))
		inv := msVMInventory()

		// Pass 1: undo INFRA disruptions first — restore killed sites, remove data-plane cuts — so a
		// subsequent instance teardown (pass 2) can reach the sites' spokes to sweep their namespaces.
		for _, d := range ds {
			switch d.kind {
			case msDisruptionKindSite:
				// A killed site can only be restored with the VM inventory — fail loudly (and leave
				// the record) if it is missing, rather than silently declare the lab healed.
				Expect(inv[d.site]).NotTo(BeEmpty(),
					"recorded site-kill of %q needs MS_VM_INVENTORY to heal", d.site)
				msStartSiteVMs(reg, inv, d.site) // powers the VMs on and clears the record
				msWaitSiteReady(reg.site(d.site), 8*time.Minute)
			case msDisruptionKindCNP:
				msHealDataPlane(reg, d.site, d.ns) // deletes the cut policy and clears the record
			case msDisruptionKindInstance:
				// handled in pass 2 (after the sites are back up)
			default:
				Fail(fmt.Sprintf("unknown disruption kind %q in ledger record %q — cannot heal it",
					d.kind, msDisruptionName(d)))
			}
		}

		// Pass 2: tear down leftover test INSTANCES — makes "after heal, ready for tests" literally
		// true (a DEBUG_ON_FAILURE run leaves the instance up; this reclaims it). Sites are up now.
		for _, d := range ds {
			if d.kind == msDisruptionKindInstance {
				By(fmt.Sprintf("tearing down leftover instance %s/%s", d.ns, d.crName))
				msTeardownInstance(reg, d.ns, d.crName) // deletes the CR + namespaces + clears the record
			}
		}

		By("verifying the ledger is empty (every disruption undone)")
		Expect(listDisruptions(reg)).To(BeEmpty(), "some disruptions could not be healed")
	})
})
