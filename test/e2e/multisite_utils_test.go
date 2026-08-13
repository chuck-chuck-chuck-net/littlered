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

// multisite_utils_test.go is the multi-site (ADR-012) e2e HARNESS. The rest of the suite
// is single-context/single-namespace; this file adds the machinery to drive a stretched
// Redis Cluster across THREE Kubernetes clusters (one hub + N workload sites) and to inflict
// — always reversibly — the two failures the site-loss takeover gate turns on:
//
//   - a WHOLE-SITE loss (hypervisor virsh destroy of a site's VMs) → the site's Kubernetes API
//     and its redis pods go unreachable from the hub → the survivors drop below quorum and
//     report their OWN cluster_state:fail, and the fence holds (the unreachable slot-serving
//     masters are themselves < q) → the fence-safe even-master-split path fires CLUSTER FAILOVER
//     TAKEOVER on the surviving site (Scenario A);
//   - a HUB-BLIND data-plane cut (a Cilium deny policy on redis 6379 + gossip 16379 to the hub's
//     peer CIDRs) while the sites stay mutually connected and serving → the survivors report
//     their own cluster_state:ok, so condition 2 (the survivors'-vantage rule, LR-017 — never
//     the hub's dial) is not met → the operator must HOLD, never take over (Scenario B, the
//     split-brain guard).
//
// Every disruption helper is paired with a restore helper, and the specs verify the lab
// returns to healthy between scenarios. Nothing here is destructive-without-return: a killed
// site is explicitly powered back on (virsh start), Cilium policies are deleted to heal.
//
// Cluster selection: the whole configuration (hub + site kubecontexts + endpoints + the
// whole-site kill VM inventory + the hub pod CIDR) lives in ONE YAML file named by MS_CONFIG; the
// individual MS_* env vars remain as a backward-compatible fallback. See msLoadConfig and the
// MS_CONFIG schema below. No lab defaults are baked in — absent config ⇒ the specs Skip.

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/test/utils"
	"sigs.k8s.io/yaml"
)

// --- Cluster registry --------------------------------------------------------------------

// msSite is one workload site of the stretched cluster: its logical name (as it appears in
// spec.multiSite.sites and in the site-qualified ground-truth keys) and the kubecontext the
// harness drives it through. Whole-site kill/restore is keyed on the site name via
// MS_VM_INVENTORY (see the disruption helpers), not on anything stored here.
type msSite struct {
	name     string // logical site name (as it appears in spec.multiSite.sites)
	context  string // kubecontext for this site's Kubernetes API
	endpoint string // API server URL routable FROM HUB PODS (the repl-net VIP), baked into the site kubeconfig
}

// msRegistry is the multi-site harness's view of the lab: the hub (where the operator and the
// LittleRed CR live) plus every workload site, keyed by logical name.
type msRegistry struct {
	hubContext   string
	operatorNS   string
	sitesByName  map[string]msSite
	orderedSites []msSite // spec order
}

// --- Multi-site config -------------------------------------------------------------------
//
// The whole multi-site configuration lives in ONE place. Preferred: a single YAML file named by
// MS_CONFIG, so a run is just `MS_CONFIG=<file>`. Fallback (backward compatible): the individual
// MS_* env vars. When neither is provided the multi-site specs Skip, so the rest of the suite is
// unaffected. See docs/E2E_TESTING.md ("Multi-site").
//
// MS_CONFIG schema:
//
//	hubContext:        scm-cp                 # hub kubecontext (operator + CR live here)
//	operatorNamespace: littlered-system       # optional (default littlered-system)
//	hubPodCIDR:        "10.233.224.0/19"       # optional; the Scenario B hub-blind cut denies it
//	sites:
//	  - name: scm-s1
//	    context: scm-s1
//	    endpoint: https://10.2.1.10:6443       # site API URL routable FROM HUB PODS (repl-net VIP)
//	    vms:                                   # optional; the Scenario A whole-site kill inventory
//	      - {name: scm-s1-node1, uri: qemu+ssh://user@host/system}
//
// Env-var fallback (each maps onto the schema): MS_HUB_CONTEXT, MS_SITE_CONTEXTS
// ("name=ctx,..."), MS_SITE_ENDPOINTS ("name=url,..."), MS_OPERATOR_NS, MS_HUB_POD_CIDR, and the
// legacy MS_VM_INVENTORY file ("<site> <vm> <uri>" lines).
type msConfig struct {
	HubContext        string         `json:"hubContext"`
	OperatorNamespace string         `json:"operatorNamespace,omitempty"`
	HubPodCIDR        string         `json:"hubPodCIDR,omitempty"`
	Sites             []msConfigSite `json:"sites"`
}

type msConfigSite struct {
	Name     string       `json:"name"`
	Context  string       `json:"context"`
	Endpoint string       `json:"endpoint"`
	VMs      []msConfigVM `json:"vms,omitempty"`
}

type msConfigVM struct {
	Name string `json:"name"`
	URI  string `json:"uri"`
}

// msLoadConfig resolves the multi-site config from MS_CONFIG (YAML) or, failing that, the legacy
// MS_* env vars. Skips the spec when neither is present. Re-reads on each call (the file is tiny);
// no caching keeps it simple and lets a mid-suite edit take effect.
func msLoadConfig() *msConfig {
	if path := strings.TrimSpace(os.Getenv("MS_CONFIG")); path != "" {
		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred(), "reading MS_CONFIG %s", path)
		var c msConfig
		Expect(yaml.Unmarshal(data, &c)).To(Succeed(), "parsing MS_CONFIG %s as YAML", path)
		Expect(c.HubContext).NotTo(BeEmpty(), "MS_CONFIG %s: hubContext is required", path)
		Expect(c.Sites).NotTo(BeEmpty(), "MS_CONFIG %s: at least one site is required", path)
		return &c
	}
	return msConfigFromEnvVars()
}

// msConfigFromEnvVars builds the config from the individual MS_* env vars (backward compat).
func msConfigFromEnvVars() *msConfig {
	hub := strings.TrimSpace(os.Getenv("MS_HUB_CONTEXT"))
	siteSpec := strings.TrimSpace(os.Getenv("MS_SITE_CONTEXTS"))
	endpointSpec := strings.TrimSpace(os.Getenv("MS_SITE_ENDPOINTS"))
	if hub == "" || siteSpec == "" || endpointSpec == "" {
		Skip("multi-site e2e requires MS_CONFIG (a single YAML config file) OR the individual " +
			"MS_HUB_CONTEXT / MS_SITE_CONTEXTS / MS_SITE_ENDPOINTS env vars — no lab defaults are " +
			"baked in; see docs/E2E_TESTING.md (\"Multi-site\").")
	}
	c := &msConfig{
		HubContext:        hub,
		OperatorNamespace: strings.TrimSpace(os.Getenv("MS_OPERATOR_NS")),
		HubPodCIDR:        strings.TrimSpace(os.Getenv("MS_HUB_POD_CIDR")),
	}
	endpoints := map[string]string{}
	for _, e := range strings.Split(endpointSpec, ",") {
		kv := strings.SplitN(strings.TrimSpace(e), "=", 2)
		Expect(kv).To(HaveLen(2), "MS_SITE_ENDPOINTS entry %q must be 'name=https://host:port'", e)
		endpoints[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	vms := parseLegacyVMInventory(strings.TrimSpace(os.Getenv("MS_VM_INVENTORY")))
	for _, e := range strings.Split(siteSpec, ",") {
		kv := strings.SplitN(strings.TrimSpace(e), "=", 2)
		Expect(kv).To(HaveLen(2), "MS_SITE_CONTEXTS entry %q must be 'name=kubecontext'", e)
		name, ctx := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		ep, ok := endpoints[name]
		Expect(ok).To(BeTrue(), "MS_SITE_ENDPOINTS has no entry for site %q (listed in MS_SITE_CONTEXTS)", name)
		c.Sites = append(c.Sites, msConfigSite{Name: name, Context: ctx, Endpoint: ep, VMs: vms[name]})
	}
	return c
}

// msRegistryFromEnv builds the multi-site harness registry from the resolved config (MS_CONFIG or
// env). When no config is present the multi-site specs Skip, so the rest of the suite is unaffected.
func msRegistryFromEnv() *msRegistry {
	cfg := msLoadConfig()
	reg := &msRegistry{
		hubContext:  cfg.HubContext,
		operatorNS:  cfgOr(cfg.OperatorNamespace, "littlered-system"),
		sitesByName: map[string]msSite{},
	}
	for _, sc := range cfg.Sites {
		Expect(sc.Name).NotTo(BeEmpty(), "a site entry is missing its name")
		Expect(sc.Context).NotTo(BeEmpty(), "site %q is missing its context", sc.Name)
		Expect(sc.Endpoint).NotTo(BeEmpty(), "site %q is missing its endpoint", sc.Name)
		s := msSite{name: sc.Name, context: sc.Context, endpoint: sc.Endpoint}
		reg.sitesByName[sc.Name] = s
		reg.orderedSites = append(reg.orderedSites, s)
	}
	Expect(reg.orderedSites).NotTo(BeEmpty(), "the multi-site config has zero sites")
	return reg
}

func cfgOr(v, def string) string {
	if strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func (r *msRegistry) site(name string) msSite {
	s, ok := r.sitesByName[name]
	Expect(ok).To(BeTrue(), "unknown site %q in the multi-site registry", name)
	return s
}

// --- Context-aware kubectl ---------------------------------------------------------------

// kctl runs kubectl against a specific kubecontext. Every multi-site helper is context-aware
// (unlike the single-context helpers elsewhere in the suite, which use the ambient context).
func kctl(context string, args ...string) (string, error) {
	full := append([]string{"--context", context}, args...)
	return utils.Run(exec.Command("kubectl", full...))
}

// kctlIn runs kubectl against a context with stdin (for apply -f -).
func kctlIn(context, stdin string, args ...string) (string, error) {
	full := append([]string{"--context", context}, args...)
	cmd := exec.Command("kubectl", full...)
	cmd.Stdin = strings.NewReader(stdin)
	return utils.Run(cmd)
}

// --- Per-site bringup: namespace + RBAC + site kubeconfig Secret --------------------------

// msSiteRBAC is the per-site namespace + ServiceAccount + namespaced workload Role + a
// long-lived SA token, applied into EACH workload site so the operator's site credentials can
// manage the instance namespace. It is fully NAMESPACED — multi-site needs no cluster-scoped
// grant (site-loss safety is Domain-M quorum arithmetic, not a Kubernetes node read; ADR-012 D1).
func msSiteRBAC(ns string) string {
	return strings.ReplaceAll(`
apiVersion: v1
kind: Namespace
metadata: {name: NS}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: littlered-site, namespace: NS}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: littlered-site, namespace: NS}
rules:
- {apiGroups: [""], resources: ["configmaps","services"], verbs: ["create","delete","get","list","patch","update","watch"]}
- {apiGroups: [""], resources: ["events"], verbs: ["create","patch"]}
- {apiGroups: [""], resources: ["pods"], verbs: ["delete","get","list","patch","update","watch"]}
- {apiGroups: [""], resources: ["secrets"], verbs: ["get","list","watch"]}
- {apiGroups: ["apps"], resources: ["statefulsets"], verbs: ["create","delete","get","list","patch","update","watch"]}
- {apiGroups: ["policy"], resources: ["poddisruptionbudgets"], verbs: ["create","delete","get","list","patch","update","watch"]}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: littlered-site, namespace: NS}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: littlered-site}
subjects: [{kind: ServiceAccount, name: littlered-site, namespace: NS}]
---
apiVersion: v1
kind: Secret
metadata:
  name: littlered-site-token
  namespace: NS
  annotations: {kubernetes.io/service-account.name: littlered-site}
type: kubernetes.io/service-account-token
`, "NS", ns)
}

// msSetupSites creates the per-site RBAC in every site and assembles the site kubeconfig
// Secrets (site-<ns>-<name>) in the operator namespace on the hub, using each site's endpoint
// (the hub-pod-routable repl-net API VIP) resolved once in msRegistryFromEnv. Idempotent.
func msSetupSites(reg *msRegistry, ns string) {
	// Clean-slate guard: refuse to run onto a leftover instance. A DEBUG_ON_FAILURE run (or a
	// failed/rejected teardown) leaves the instance up; silently reusing it contaminates the run
	// with the prior cluster's post-disruption recovery state (e.g. a stuck-Initializing bring-up
	// as the operator recycles a killed site's returnees). Fail fast with the fix — mirroring the
	// default-namespace clean-slate guard in BeforeSuite.
	if leftover, _ := kctl(reg.hubContext, "-n", ns, "get", "littlered", "-o", "name", "--ignore-not-found"); strings.TrimSpace(leftover) != "" {
		Fail(fmt.Sprintf("multi-site instance already present in namespace %s (%s) — a clean slate is "+
			"required. Tear the leftover down first: run LABEL_FILTER='ms-heal' (now removes leftover "+
			"instances too), or `kubectl --context %s -n %s delete littlered --all` and drop the "+
			"per-site namespaces.", ns, strings.TrimSpace(leftover), reg.hubContext, ns))
	}
	for _, s := range reg.orderedSites {
		By(fmt.Sprintf("multi-site setup: RBAC + token in site %s (ns %s)", s.name, ns))
		_, err := kctlIn(s.context, msSiteRBAC(ns), "apply", "-f", "-")
		Expect(err).NotTo(HaveOccurred(), "apply site RBAC in %s", s.name)

		var token, ca string
		Eventually(func(g Gomega) {
			t, e1 := kctl(s.context, "-n", ns, "get", "secret", "littlered-site-token", "-o", "jsonpath={.data.token}")
			g.Expect(e1).NotTo(HaveOccurred())
			g.Expect(t).NotTo(BeEmpty())
			token = string(mustBase64Decode(t))
			c, e2 := kctl(s.context, "-n", ns, "get", "secret", "littlered-site-token", "-o", "jsonpath={.data.ca\\.crt}")
			g.Expect(e2).NotTo(HaveOccurred())
			ca = c
		}, "30s", "2s").Should(Succeed(), "SA token controller should populate the site token")

		kubeconfig := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters: [{name: %s, cluster: {server: %s, certificate-authority-data: %s}}]
users: [{name: littlered-site, user: {token: %s}}]
contexts: [{name: %s, context: {cluster: %s, user: littlered-site, namespace: %s}}]
current-context: %s
`, s.name, s.endpoint, ca, token, s.name, s.name, ns, s.name)

		secretName := msSiteSecretName(ns, s.name)
		By(fmt.Sprintf("multi-site setup: creating hub Secret %s/%s", reg.operatorNS, secretName))
		yaml, err := kctlIn(reg.hubContext, kubeconfig, "-n", reg.operatorNS,
			"create", "secret", "generic", secretName, "--from-file=kubeconfig=/dev/stdin",
			"--dry-run=client", "-o", "yaml")
		Expect(err).NotTo(HaveOccurred(), "render site secret for %s", s.name)
		_, err = kctlIn(reg.hubContext, yaml, "apply", "-f", "-")
		Expect(err).NotTo(HaveOccurred(), "apply site secret for %s", s.name)
	}
	By("multi-site setup: ensuring the instance namespace exists on the hub")
	_, _ = kctl(reg.hubContext, "create", "ns", ns)
}

func msSiteSecretName(ns, siteName string) string { return "site-" + ns + "-" + siteName }

func mustBase64Decode(s string) []byte {
	out, err := exec.Command("sh", "-c", fmt.Sprintf("printf %%s %q | base64 -d", s)).Output()
	Expect(err).NotTo(HaveOccurred(), "base64 decode")
	return out
}

// msTeardownInstance deletes the CR (letting the multi-site finalizer sweep the spokes) and,
// best-effort, the instance namespace on every cluster and the hub site Secrets.
func msTeardownInstance(reg *msRegistry, ns, crName string) {
	By(fmt.Sprintf("multi-site teardown: deleting CR %s/%s", ns, crName))
	_, _ = kctl(reg.hubContext, "-n", ns, "delete", "littlered", crName, "--ignore-not-found", "--timeout=3m")
	for _, s := range reg.orderedSites {
		_, _ = kctl(s.context, "delete", "ns", ns, "--ignore-not-found", "--wait=false")
		_, _ = kctl(reg.hubContext, "-n", reg.operatorNS, "delete", "secret", msSiteSecretName(ns, s.name), "--ignore-not-found")
	}
	_, _ = kctl(reg.hubContext, "delete", "ns", ns, "--ignore-not-found", "--wait=false")
	// The instance is gone ⇒ clear its ledger record (recorded in the scenario setup). Idempotent:
	// a no-op when nothing was recorded (e.g. the behaviour harness, which does not use the ledger).
	clearDisruption(reg, msDisruption{kind: msDisruptionKindInstance, ns: ns, crName: crName})
}

// --- CR construction ---------------------------------------------------------------------

// msClusterCR renders a multi-site cluster LittleRed referencing the harness-created site
// Secrets. shards/sitesPerShard/replicasPerShard drive the placement; nodeTimeoutMs feeds the
// takeover cooldown (max(60s, 2×node-timeout)).
func msClusterCR(reg *msRegistry, ns, name string, siteNames []string, shards, replicasPerShard, sitesPerShard, nodeTimeoutMs int) string {
	var sites strings.Builder
	for _, sn := range siteNames {
		sites.WriteString(fmt.Sprintf("    - name: %s\n      kubeconfigSecretRef:\n        name: %s\n",
			sn, msSiteSecretName(ns, sn)))
	}
	return fmt.Sprintf(`apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: cluster
  cluster:
    shards: %d
    replicasPerShard: %d
    clusterNodeTimeout: %d
  multiSite:
    sitesPerShard: %d
    network:
      mode: pod-routed
    sites:
%s  resources:
    requests: {memory: 256Mi, cpu: 128m}
    limits: {memory: 256Mi}
  config:
    maxmemoryPolicy: noeviction
`, name, ns, shards, replicasPerShard, nodeTimeoutMs, sitesPerShard, sites.String())
}

// --- Ground-truth readers (context-aware) ------------------------------------------------

// msClusterNodeRow is one parsed CLUSTER NODES row.
type msClusterNodeRow struct {
	id       string
	ip       string
	flags    string
	masterID string
	slots    string
	role     string // "master" | "replica"
}

// msClusterNodes runs CLUSTER NODES on pod (in the given site context) and parses it.
func msClusterNodes(siteCtx, ns, pod string) ([]msClusterNodeRow, error) {
	out, err := kctl(siteCtx, "exec", pod, "-n", ns, "-c", "redis", "--", "redis-cli", "CLUSTER", "NODES")
	if err != nil {
		return nil, err
	}
	var rows []msClusterNodeRow
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 8 {
			continue
		}
		row := msClusterNodeRow{id: f[0], flags: f[2], masterID: f[3], role: "master"}
		if hostport := strings.Split(f[1], "@"); len(hostport) > 0 {
			row.ip = strings.Split(hostport[0], ":")[0]
		}
		if strings.Contains(f[2], "slave") {
			row.role = "replica"
		}
		if len(f) > 8 {
			row.slots = strings.Join(f[8:], ",")
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// msClusterInfo runs CLUSTER INFO on pod and returns the raw fields map.
func msClusterInfo(siteCtx, ns, pod string) (map[string]string, error) {
	out, err := kctl(siteCtx, "exec", pod, "-n", ns, "-c", "redis", "--", "redis-cli", "CLUSTER", "INFO")
	if err != nil {
		return nil, err
	}
	fields := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		kv := strings.SplitN(strings.TrimSpace(line), ":", 2)
		if len(kv) == 2 {
			fields[kv[0]] = strings.TrimSpace(kv[1])
		}
	}
	return fields, nil
}

// msSiteOfPodByIP maps each site's pods to the site name using the pod IP, so a CLUSTER NODES
// row (which carries only an IP) can be attributed to a site — the basis of the cross-site
// placement assertion. Returns ip -> siteName over every reachable site.
func msSiteOfPodByIP(reg *msRegistry, ns string) map[string]string {
	ipSite := map[string]string{}
	for _, s := range reg.orderedSites {
		out, err := kctl(s.context, "-n", ns, "get", "pods", "-l", "app.kubernetes.io/component=cluster",
			"-o", "jsonpath={range .items[*]}{.status.podIP}={.metadata.name}{\"\\n\"}{end}")
		if err != nil {
			continue
		}
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			kv := strings.SplitN(line, "=", 2)
			if len(kv) == 2 && kv[0] != "" {
				ipSite[kv[0]] = s.name
			}
		}
	}
	return ipSite
}

// msVerifyCrossSiteTopology asserts the stretched cluster is healthy AND every shard's master
// and its replica(s) live in DIFFERENT sites (the single-domain-loss survivability invariant,
// ADR-012 / pillar 3.12 extended cross-site). Queried from a survivor pod in the first site.
func msVerifyCrossSiteTopology(reg *msRegistry, ns, crName string, seedSite, seedPod string, expectSlotsAssigned bool) {
	By("verifying healthy cross-site topology (master and replica in different sites, all slots served)")
	Eventually(func(g Gomega) {
		info, err := msClusterInfo(reg.site(seedSite).context, ns, seedPod)
		g.Expect(err).NotTo(HaveOccurred(), "CLUSTER INFO from seed pod")
		g.Expect(info["cluster_state"]).To(Equal("ok"), "cluster_state must be ok")
		if expectSlotsAssigned {
			g.Expect(info["cluster_slots_assigned"]).To(Equal("16384"), "all 16384 slots must be assigned")
		}

		rows, err := msClusterNodes(reg.site(seedSite).context, ns, seedPod)
		g.Expect(err).NotTo(HaveOccurred())
		ipSite := msSiteOfPodByIP(reg, ns)

		// Group by shard via master lineage: each master and the replicas following it must
		// not all be in one site.
		idIP := map[string]string{}
		for _, r := range rows {
			idIP[r.id] = r.ip
		}
		for _, r := range rows {
			if r.role != "replica" {
				continue
			}
			replicaSite := ipSite[r.ip]
			masterSite := ipSite[idIP[r.masterID]]
			g.Expect(replicaSite).NotTo(BeEmpty(), "replica %s IP not attributed to a site", r.ip)
			g.Expect(masterSite).NotTo(BeEmpty(), "master of %s not attributed to a site", r.ip)
			g.Expect(replicaSite).NotTo(Equal(masterSite),
				"cross-site invariant violated: replica in %s follows a master in the same site", replicaSite)
		}
	}, "3m", "5s").Should(Succeed())
}

// --- CR status / condition / event readers -----------------------------------------------

func msGetCR(reg *msRegistry, ns, crName string) (*littleredv1alpha1.LittleRed, error) {
	out, err := kctl(reg.hubContext, "-n", ns, "get", "littlered", crName, "-o", "json")
	if err != nil {
		return nil, err
	}
	var lr littleredv1alpha1.LittleRed
	if err := json.Unmarshal([]byte(out), &lr); err != nil {
		return nil, err
	}
	return &lr, nil
}

// msTakeoverCondition returns the MultiSiteTakeover condition (status/reason), or ("","") if absent.
func msTakeoverCondition(reg *msRegistry, ns, crName string) (status, reason string) {
	lr, err := msGetCR(reg, ns, crName)
	if err != nil {
		return "", ""
	}
	for _, c := range lr.Status.Conditions {
		if c.Type == littleredv1alpha1.ConditionMultiSiteTakeover {
			return string(c.Status), c.Reason
		}
	}
	return "", ""
}

// msHasEvent reports whether the CR has emitted an Event with the given reason.
func msHasEvent(reg *msRegistry, ns, crName, reason string) bool {
	out, err := kctl(reg.hubContext, "-n", ns, "get", "events",
		"--field-selector", "involvedObject.name="+crName+",reason="+reason, "-o", "name")
	if err != nil {
		return false
	}
	return strings.TrimSpace(out) != ""
}

// msOperatorLogSince greps the operator pod log (since the given time) for a substring, and
// returns the matching lines. Used to capture the takeover audit trail verbatim.
func msOperatorLogSince(reg *msRegistry, since time.Time, substr string) (string, error) {
	pod, err := kctl(reg.hubContext, "-n", reg.operatorNS, "get", "pods",
		"-l", "control-plane=controller-manager", "-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		return "", err
	}
	sinceArg := fmt.Sprintf("--since-time=%s", since.UTC().Format(time.RFC3339))
	out, err := kctl(reg.hubContext, "-n", reg.operatorNS, "logs", strings.TrimSpace(pod), sinceArg)
	if err != nil {
		return "", err
	}
	var matched []string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, substr) {
			matched = append(matched, line)
		}
	}
	return strings.Join(matched, "\n"), nil
}

// --- Disruption: data-plane partition (Scenario B) ---------------------------------------

// msDataPlanePolicyName is the Cilium deny policy the harness installs for a data-plane cut.
const msDataPlanePolicyName = "ms-e2e-dataplane-cut"

// msPartitionDataPlane installs a Cilium policy in the target site's instance namespace that
// DROPS redis (6379) and gossip (16379) traffic to/from the given peer CIDRs — and ONLY that —
// while leaving every other path (peer-site pods, intra-site, the kubelet readiness probe via
// reserved:host, the site's own API) untouched. This is what makes the hub-blind Scenario B cut
// selective: deny only hub↔site, keep site↔site so the survivors stay cluster_state:ok.
//
// CRITICAL (LR-030 debug, 2026-08-12): a deny-ONLY CiliumNetworkPolicy does NOT stay scoped to the
// named CIDRs. Selecting an endpoint with any rule — including an ingressDeny/egressDeny stanza —
// puts it into Cilium DEFAULT-DENY for that direction (documented behavior since v1.16:
// docs.cilium.io/en/stable/security/policy/{deny,intro}). With no allow rules, EVERYTHING not
// explicitly allowed is dropped — so a "hub-only" deny actually severed site↔site too, turning the
// intended hub-blind cut into a full inter-site partition (the operator then correctly took over).
// Verified live: deny-only ⇒ s1↔s2 timed out; adding the allow-all below ⇒ s1↔s2 restored while
// hub↔site stayed cut. So we pair the deny with an explicit allow-all (fromEntities/toEntities:
// all); Deny rules take precedence over Allow, so only the named CIDRs on the named ports are cut.
// Fully reversible via msHealDataPlane.
func msPartitionDataPlane(reg *msRegistry, siteName, ns, crName string, peerCIDRs []string) {
	// Write-ahead: record the disruption BEFORE applying it, so an interrupted run can still be
	// healed (ms-heal) even if it dies right after the policy lands. See the ledger file.
	recordDisruption(reg, msDisruption{kind: msDisruptionKindCNP, site: siteName, ns: ns})
	siteCtx := reg.site(siteName).context
	policy := fmt.Sprintf(`apiVersion: cilium.io/v2
kind: CiliumNetworkPolicy
metadata:
  name: %s
  namespace: %s
spec:
  endpointSelector:
    matchLabels:
      app.kubernetes.io/component: cluster
      app.kubernetes.io/instance: %s
  ingress:
  - fromEntities: [all]
  ingressDeny:
  - fromCIDRSet:
%s    toPorts:
    - ports:
      - {port: "6379", protocol: TCP}
      - {port: "16379", protocol: TCP}
  egress:
  - toEntities: [all]
  egressDeny:
  - toCIDRSet:
%s    toPorts:
    - ports:
      - {port: "6379", protocol: TCP}
      - {port: "16379", protocol: TCP}
`, msDataPlanePolicyName, ns, crName, msCIDRSet(peerCIDRs), msCIDRSet(peerCIDRs))
	By(fmt.Sprintf("Scenario B: installing data-plane cut policy on %s (peers %v)", siteName, peerCIDRs))
	_, err := kctlIn(siteCtx, policy, "apply", "-f", "-")
	Expect(err).NotTo(HaveOccurred(), "apply Cilium data-plane deny policy")
}

func msCIDRSet(cidrs []string) string {
	var b strings.Builder
	for _, c := range cidrs {
		b.WriteString(fmt.Sprintf("      - cidr: %q\n", c))
	}
	return b.String()
}

// msHealDataPlane removes the data-plane cut policy, restoring cross-site redis connectivity, and
// clears the ledger record. Reused by both inline cleanup and the ms-heal spec.
func msHealDataPlane(reg *msRegistry, siteName, ns string) {
	siteCtx := reg.site(siteName).context
	By(fmt.Sprintf("removing data-plane cut policy on %s", siteName))
	_, _ = kctl(siteCtx, "-n", ns, "delete", "ciliumnetworkpolicy", msDataPlanePolicyName, "--ignore-not-found")
	clearDisruption(reg, msDisruption{kind: msDisruptionKindCNP, site: siteName, ns: ns})
}

// --- Disruption: whole-site loss via hypervisor power-off (Scenario A) ---------------------
//
// A true whole-site loss must be a sustained outage we control — not a self-reversing guest
// reboot. `talosctl reboot` (any --mode) is guest-timed: the node decides when it returns,
// which races the takeover cooldown (max(60s, 2×cluster-node-timeout) + detection). So we kill
// the site at the hypervisor with `virsh destroy` (hard power-off) and bring it back with
// `virsh start` only when the test decides — a deterministic down-window, no self-reversal.
//
// The site→VM mapping comes from the config's per-site `vms` (MS_CONFIG), or the legacy
// MS_VM_INVENTORY file ("<site> <vm-name> <virsh-uri>" lines). The VMs listed for a site define
// exactly which VMs *are* that site — so they also decide kill scope (include or omit router/lb
// VMs as desired). Requires non-interactive virsh access (key-based qemu+ssh) to each listed URI.

type msVM struct{ name, uri string }

// msVMInventory returns the whole-site kill/restore inventory (site → VMs) from the resolved
// config. Returns nil when no VMs are configured (the caller then Skips the hypervisor scenario).
func msVMInventory() map[string][]msVM {
	inv := map[string][]msVM{}
	for _, s := range msLoadConfig().Sites {
		for _, vm := range s.VMs {
			inv[s.Name] = append(inv[s.Name], msVM{name: vm.Name, uri: vm.URI})
		}
	}
	if len(inv) == 0 {
		return nil
	}
	return inv
}

// parseLegacyVMInventory parses the legacy MS_VM_INVENTORY file (one "<site> <vm-name> <virsh-uri>"
// line per VM; blank lines and #-comments ignored) into site → VMs. Empty path ⇒ nil (no VMs).
func parseLegacyVMInventory(path string) map[string][]msConfigVM {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred(), "reading MS_VM_INVENTORY %s", path)
	inv := map[string][]msConfigVM{}
	for i, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		Expect(f).To(HaveLen(3), "MS_VM_INVENTORY line %d must be '<site> <vm-name> <virsh-uri>': %q", i+1, line)
		inv[f[0]] = append(inv[f[0]], msConfigVM{Name: f[1], URI: f[2]})
	}
	return inv
}

// msKillSiteVMs hard-powers-off every VM of a site (virsh destroy). Tolerates a VM that is
// already off, so the call is idempotent. The VMs stay down until msStartSiteVMs — there is
// no self-reversal, so the outage lasts exactly as long as the test holds it.
func msKillSiteVMs(reg *msRegistry, inv map[string][]msVM, site string) {
	vms := inv[site]
	Expect(vms).NotTo(BeEmpty(), "no MS_VM_INVENTORY entries for site %s", site)
	// Write-ahead: record BEFORE the kill so an interrupted run can be healed (ms-heal). A killed
	// site is un-discoverable — you cannot query a powered-off cluster — so this record is the ONLY
	// thing that knows the site is down and owes a virsh start.
	recordDisruption(reg, msDisruption{kind: msDisruptionKindSite, site: site})
	By(fmt.Sprintf("Scenario A: hard-killing site %s at the hypervisor (virsh destroy, %d VMs)", site, len(vms)))
	for _, vm := range vms {
		out, err := utils.Run(exec.Command("virsh", "-c", vm.uri, "destroy", vm.name))
		_, _ = fmt.Fprintf(GinkgoWriter, "virsh destroy %s @ %s: %s\n", vm.name, vm.uri, strings.TrimSpace(out))
		if err != nil && !strings.Contains(out+err.Error(), "is not running") {
			Expect(err).NotTo(HaveOccurred(), "virsh destroy %s @ %s", vm.name, vm.uri)
		}
	}
}

// msStartSiteVMs powers every VM of a site back on (virsh start). Tolerates an already-running
// VM, and is a no-op when the site has no inventory — so it is safe to call unconditionally in
// cleanup, even after a Skip or an early failure, to guarantee the lab is left healthy.
func msStartSiteVMs(reg *msRegistry, inv map[string][]msVM, site string) {
	vms := inv[site]
	if len(vms) == 0 {
		// No inventory for this site ⇒ we cannot power it on, so we must NOT clear any record: if a
		// kill was recorded, the site is genuinely still down and heal (with the inventory) must
		// still see it. When no kill happened there is no record and this is simply a no-op.
		return
	}
	By(fmt.Sprintf("restoring site %s at the hypervisor (virsh start, %d VMs)", site, len(vms)))
	for _, vm := range vms {
		out, err := utils.Run(exec.Command("virsh", "-c", vm.uri, "start", vm.name))
		_, _ = fmt.Fprintf(GinkgoWriter, "virsh start %s @ %s: %s\n", vm.name, vm.uri, strings.TrimSpace(out))
		if err != nil && !strings.Contains(out+err.Error(), "already active") {
			Expect(err).NotTo(HaveOccurred(), "virsh start %s @ %s", vm.name, vm.uri)
		}
	}
	// The VMs are powered on ⇒ the "site down" disruption is undone; clear the ledger record.
	clearDisruption(reg, msDisruption{kind: msDisruptionKindSite, site: site})
}

// msWaitSiteUnreachable waits until the site's Kubernetes API stops answering (the hub can no
// longer list the site's pods) — the confirmation the reboot has taken the site down.
func msWaitSiteUnreachable(site msSite, timeout time.Duration) {
	By(fmt.Sprintf("Scenario A: waiting for site %s API to go unreachable", site.name))
	Eventually(func() error {
		_, err := kctl(site.context, "get", "--request-timeout=5s", "ns")
		if err == nil {
			return fmt.Errorf("site %s API still reachable", site.name)
		}
		return nil
	}, timeout, "5s").Should(Succeed())
}

// msWaitSiteReady waits until the site's nodes are Ready again after a reboot.
func msWaitSiteReady(site msSite, timeout time.Duration) {
	By(fmt.Sprintf("Scenario A: waiting for site %s nodes to be Ready again", site.name))
	Eventually(func(g Gomega) {
		out, err := kctl(site.context, "get", "nodes", "--request-timeout=8s",
			"-o", "jsonpath={range .items[*]}{.status.conditions[?(@.type==\"Ready\")].status}{\"\\n\"}{end}")
		g.Expect(err).NotTo(HaveOccurred())
		lines := strings.Split(strings.TrimSpace(out), "\n")
		g.Expect(lines).NotTo(BeEmpty())
		for _, l := range lines {
			g.Expect(strings.TrimSpace(l)).To(Equal("True"), "a node is not Ready yet")
		}
	}, timeout, "10s").Should(Succeed())
}
