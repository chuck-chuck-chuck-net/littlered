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
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/chuck-chuck-chuck-net/littlered/internal/cli/multisite"
	"github.com/chuck-chuck-chuck-net/littlered/internal/mesh"
)

var (
	deriveFromMesh      string
	deriveSitesPerShard int
	deriveShards        int
	deriveReplicas      int
	deriveName          string
	deriveFragment      bool
)

// multisiteDeriveCmd generates spec.multiSite from the mesh inventory.
//
// The CR is the one place where a mistake is not editable. spec.multiSite.sites is
// listType=atomic and a site's POSITION is its placement index (LR-033); the topology is
// immutable after create (LR-032); and sitesPerShard is required and never defaulted
// (LR-033). So a hand-written slip is a delete-and-recreate, which under EmptyDir is the
// data. Deriving the block from the inventory removes the class, and printing the resulting
// placement makes the order visibly load-bearing rather than merely documented as such.
var multisiteDeriveCmd = &cobra.Command{
	Use:   "derive-topology",
	Short: "Generate spec.multiSite (and a CR) from the mesh inventory",
	Long: `Generates the spec.multiSite block — and by default a complete LittleRed CR — from the
mesh inventory, the same file the e2e reads and the same format the sibling project slaptain
uses (see mesh.yaml.sample).

It derives only FACTS about the lab: which clusters exist, in what order, and which hub Secret
holds each one's kubeconfig. The CHOICES are yours and must be stated: --sites-per-shard (the
copy factor, which the CRD deliberately refuses to default) and --shards.

Why generate it at all. A site's POSITION in spec.multiSite.sites is its placement index —
shard K lives in sites (K+j) mod S — and the topology is immutable once created, so a
reordered or mistyped list is not an edit away from correct: it is a delete and recreate,
which with EmptyDir storage is the data. The command also prints where each shard lands and
what the topology survives, which is the half of the decision that is arithmetic rather than
taste (ADR-012 D2).

The manifest goes to stdout and the report to stderr, so it redirects cleanly:

  lrctl multisite derive-topology --from-mesh mesh.yaml \
      --sites-per-shard 2 --shards 4 --name cache -n prod > cache.yaml`,
	RunE: func(cmd *cobra.Command, args []string) error {
		path := strings.TrimSpace(deriveFromMesh)
		if path == "" {
			path = strings.TrimSpace(os.Getenv("E2E_CONFIG"))
		}
		if path == "" {
			path = strings.TrimSpace(os.Getenv("MS_CONFIG"))
		}
		if path == "" {
			return fmt.Errorf("no mesh inventory: pass --from-mesh <file>, or set E2E_CONFIG " +
				"(see mesh.yaml.sample)")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading the mesh inventory %s: %w", path, err)
		}
		inv, err := mesh.Parse(data)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := inv.ValidateForLittleRed(); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		topo, err := multisite.DeriveTopology(inv, multisite.TopologyChoices{
			SitesPerShard: deriveSitesPerShard,
			Shards:        deriveShards,
		})
		if err != nil {
			return err
		}

		// Manifest to stdout, report to stderr — `helm template`'s convention, and
		// slaptain's, so `> file` yields something appliable rather than something with a
		// human-readable preamble baked into it.
		if deriveFragment {
			fmt.Print(topo.MultiSiteYAML())
		} else {
			fmt.Print(renderDerivedCR(topo, deriveName, namespace, deriveShards, deriveReplicas))
		}
		fmt.Fprint(os.Stderr, renderDerivedReport(topo, path))
		return nil
	},
}

// renderDerivedCR wraps the derived topology in a minimal, appliable LittleRed. Everything
// it emits beyond spec.multiSite is a stated CHOICE, never an invention: shards and
// replicasPerShard come from flags whose defaults match the CRD's.
func renderDerivedCR(t *multisite.Topology, name, ns string, shards, replicas int) string {
	var b strings.Builder
	b.WriteString("apiVersion: redis.chuck-chuck-chuck.net/v1alpha1\nkind: LittleRed\nmetadata:\n")
	fmt.Fprintf(&b, "  name: %s\n", name)
	if strings.TrimSpace(ns) != "" {
		fmt.Fprintf(&b, "  namespace: %s\n", ns)
	}
	b.WriteString("spec:\n  mode: cluster\n  cluster:\n")
	fmt.Fprintf(&b, "    shards: %d\n    replicasPerShard: %d\n", shards, replicas)
	b.WriteString(t.MultiSiteYAML())
	b.WriteString("  # If spec.auth is enabled, the password Secret must exist in THIS namespace\n")
	b.WriteString("  # at EVERY site — a pod resolves it locally, and the operator neither copies\n")
	b.WriteString("  # nor checks for it. See changelog LR-064.\n")
	return b.String()
}

func renderDerivedReport(t *multisite.Topology, path string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\nDerived from %s\n", path)
	fmt.Fprintf(&b, "  %d workload site(s), sitesPerShard %d, %d shard(s)\n",
		len(t.Sites), t.SitesPerShard, t.Resilience.Shards)
	fmt.Fprintf(&b, "  members per shard: %d x (replicasPerShard + 1)\n", t.SitesPerShard)
	b.WriteString("\nPlacement (a site's POSITION in the list is its placement index; immutable after create):\n")
	for _, p := range t.Placement {
		fmt.Fprintf(&b, "  shard %-3d %s   (master starts at %s)\n",
			p.Shard, strings.Join(p.Sites, ", "), p.Sites[0])
	}
	r := t.Resilience
	b.WriteString("\nSite-loss resilience (ADR-012 D2):\n")
	fmt.Fprintf(&b, "  takeover floor  %d master(s)/site — above this, that site's loss is unrecoverable unattended\n",
		r.TakeoverSurvivalFloor)
	fmt.Fprintf(&b, "  native target   %d master(s)/site — above this, recovery downgrades to an operator takeover\n",
		r.NativeSurvivalTarget)
	marker := tokenOK
	if !r.SiteLossResilient {
		marker = tokenWarn
	}
	fmt.Fprintf(&b, "  %s %s\n", marker, wrapText(r.Summary, 92, "       "))
	return b.String()
}

// wrapText soft-wraps a summary so a long verdict stays readable in a terminal.
func wrapText(s string, width int, indent string) string {
	words := strings.Fields(s)
	var lines []string
	cur := ""
	for _, w := range words {
		if cur != "" && len(cur)+1+len(w) > width {
			lines = append(lines, cur)
			cur = ""
		}
		if cur == "" {
			cur = w
			continue
		}
		cur += " " + w
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	return strings.Join(lines, "\n"+indent)
}

func init() {
	f := multisiteDeriveCmd.Flags()
	f.StringVar(&deriveFromMesh, "from-mesh", "", "Mesh inventory file (default: $E2E_CONFIG, then $MS_CONFIG)")
	f.IntVar(&deriveSitesPerShard, "sites-per-shard", 0,
		"REQUIRED: how many sites each shard lives in — the copy factor. Never defaulted, "+
			"deliberately: every candidate default is wrong for somebody (LR-033)")
	f.IntVar(&deriveShards, "shards", 3, "Number of shards (also drives the resilience arithmetic)")
	f.IntVar(&deriveReplicas, "replicas-per-shard", 0,
		"Replicas per shard PER SITE. Note a stretched shard always has cross-site replicas "+
			"regardless, so 0 still yields sitesPerShard members per shard")
	f.StringVar(&deriveName, "name", "littlered", "Name of the generated LittleRed")
	f.BoolVar(&deriveFragment, "fragment", false, "Emit only the spec.multiSite block, not a whole CR")
	multisiteCmd.AddCommand(multisiteDeriveCmd)
}
