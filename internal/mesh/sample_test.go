package mesh

import (
	"os"
	"testing"
)

// The shipped sample must parse and validate. A sample that does not is worse than no
// sample: it is the first thing anyone copies.
func TestMeshSampleIsValid(t *testing.T) {
	data, err := os.ReadFile("../../mesh.yaml.sample")
	if err != nil {
		t.Fatalf("reading mesh.yaml.sample: %v", err)
	}
	c, err := Parse(data)
	if err != nil {
		t.Fatalf("mesh.yaml.sample does not parse: %v", err)
	}
	if err := c.ValidateForLittleRed(); err != nil {
		t.Fatalf("mesh.yaml.sample does not validate: %v", err)
	}
	if got := len(c.WorkloadSites()); got != 3 {
		t.Errorf("sample declares %d workload sites, want 3 (the hub carries no pods there)", got)
	}
	if c.HubContext() != "scm-cp" || c.HubPodCIDR() == "" {
		t.Errorf("sample hub = %q podCIDR=%q", c.HubContext(), c.HubPodCIDR())
	}
}
