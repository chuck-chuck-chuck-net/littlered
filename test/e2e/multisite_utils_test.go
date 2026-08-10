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
//   - a WHOLE-SITE loss (talosctl reboot of a site's nodes) → the site's Kubernetes API and
//     its redis pods go unreachable from the hub → witness Unobservable → the fence-safe
//     even-master-split path fires CLUSTER FAILOVER TAKEOVER on the surviving site (Scenario A);
//   - a DATA-PLANE-only partition (a Cilium deny policy on redis 6379 + gossip 16379) while
//     the site's API stays reachable and its pods stay Ready → witness Contradiction → the
//     operator must HOLD, never take over (Scenario B, the split-brain guard).
//
// Every disruption helper is paired with a restore helper, and the specs verify the lab
// returns to healthy between scenarios. Nothing here is destructive-without-return: reboots
// self-reverse on boot, Cilium policies are deleted to heal.
//
// Cluster selection: the harness reads a hub kubecontext and an ordered list of site
// kubecontexts (env MS_HUB_CONTEXT / MS_SITE_CONTEXTS), plus the per-site talosctl endpoints
// for the reboot mechanism (MS_TALOS_ENDPOINTS). Defaults match the scm-* lab.

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
)

// --- Cluster registry --------------------------------------------------------------------

// msSite is one workload site of the stretched cluster: its logical name (as it appears in
// spec.multiSite.sites and in the site-qualified ground-truth keys) plus the kubecontext and
// talosctl endpoints the harness drives it through.
type msSite struct {
	name           string   // logical site name, e.g. "scm-s1"
	context        string   // kubecontext for this site's Kubernetes API
	talosEndpoints []string // talosctl -e endpoints (node management IPs) for the reboot mechanism
}

// msRegistry is the multi-site harness's view of the lab: the hub (where the operator and the
// LittleRed CR live) plus every workload site, keyed by logical name.
type msRegistry struct {
	hubContext   string
	operatorNS   string
	sitesByName  map[string]msSite
	orderedSites []msSite // spec order
}

// msDefaultRegistry builds the registry from env (falling back to the scm-* lab defaults):
//
//	MS_HUB_CONTEXT     hub kubecontext                       (default "scm-cp")
//	MS_OPERATOR_NS     operator namespace on the hub         (default "littlered-system")
//	MS_SITE_CONTEXTS   comma list "name=ctx,..."             (default "scm-s1=scm-s1,scm-s2=scm-s2,scm-s3=scm-s3")
//	MS_TALOS_ENDPOINTS comma list "name=ip;ip;ip,..."        (default per the scm-* repl-net)
func msDefaultRegistry() *msRegistry {
	hub := envOr("MS_HUB_CONTEXT", "scm-cp")
	opNS := envOr("MS_OPERATOR_NS", "littlered-system")
	siteSpec := envOr("MS_SITE_CONTEXTS", "scm-s1=scm-s1,scm-s2=scm-s2,scm-s3=scm-s3")
	talosSpec := envOr("MS_TALOS_ENDPOINTS",
		"scm-s1=10.1.1.4;10.1.1.5;10.1.1.6,scm-s2=10.1.2.4;10.1.2.5;10.1.2.6,scm-s3=10.1.3.4;10.1.3.5;10.1.3.6")

	talos := map[string][]string{}
	for _, e := range strings.Split(talosSpec, ",") {
		kv := strings.SplitN(strings.TrimSpace(e), "=", 2)
		if len(kv) == 2 {
			talos[kv[0]] = strings.Split(kv[1], ";")
		}
	}

	reg := &msRegistry{hubContext: hub, operatorNS: opNS, sitesByName: map[string]msSite{}}
	for _, e := range strings.Split(siteSpec, ",") {
		kv := strings.SplitN(strings.TrimSpace(e), "=", 2)
		if len(kv) != 2 {
			continue
		}
		s := msSite{name: kv[0], context: kv[1], talosEndpoints: talos[kv[0]]}
		reg.sitesByName[s.name] = s
		reg.orderedSites = append(reg.orderedSites, s)
	}
	return reg
}

func (r *msRegistry) site(name string) msSite {
	s, ok := r.sitesByName[name]
	Expect(ok).To(BeTrue(), "unknown site %q in the multi-site registry", name)
	return s
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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

// msSiteRBAC is the per-site namespace + ServiceAccount + namespaced workload Role + the
// cluster-scoped nodes Role the ADR-012 witness needs + a long-lived SA token. Applied into
// EACH workload site so the operator's site credentials can manage the instance namespace and
// read node Ready conditions.
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
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: littlered-site-nodes}
rules: [{apiGroups: [""], resources: ["nodes"], verbs: ["get","list"]}]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: littlered-site-nodes-NS}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: littlered-site-nodes}
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
// Secrets (site-<ns>-<name>) in the operator namespace on the hub. The site API endpoint is
// taken from MS_SITE_ENDPOINTS (the only hub-pod-routable API address — the repl-net VIP),
// falling back to a default per the scm-* lab. Idempotent.
func msSetupSites(reg *msRegistry, ns string) {
	endpointSpec := envOr("MS_SITE_ENDPOINTS",
		"scm-s1=https://10.2.1.10:6443,scm-s2=https://10.2.2.10:6443,scm-s3=https://10.2.3.10:6443")
	endpoints := map[string]string{}
	for _, e := range strings.Split(endpointSpec, ",") {
		kv := strings.SplitN(strings.TrimSpace(e), "=", 2)
		if len(kv) == 2 {
			endpoints[kv[0]] = kv[1]
		}
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
`, s.name, endpoints[s.name], ca, token, s.name, s.name, ns, s.name)

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

// msPartitionDataPlane installs a Cilium deny policy in the target site's instance namespace
// that DROPS redis (6379) and gossip (16379) traffic to/from the given peer CIDRs, while
// leaving everything else — crucially the kubelet readiness probe (reserved:host) and the
// site's own API — untouched. Uses ingressDeny/egressDeny, which do NOT trip Cilium's
// default-deny, so the pods stay Ready and the witness stays Contradiction (site alive, data
// path cut). Fully reversible via msHealDataPlane.
func msPartitionDataPlane(reg *msRegistry, siteCtx, ns, crName string, peerCIDRs []string) {
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
  ingressDeny:
  - fromCIDRSet:
%s    toPorts:
    - ports:
      - {port: "6379", protocol: TCP}
      - {port: "16379", protocol: TCP}
  egressDeny:
  - toCIDRSet:
%s    toPorts:
    - ports:
      - {port: "6379", protocol: TCP}
      - {port: "16379", protocol: TCP}
`, msDataPlanePolicyName, ns, crName, msCIDRSet(peerCIDRs), msCIDRSet(peerCIDRs))
	By(fmt.Sprintf("Scenario B: installing data-plane cut policy on %s (peers %v)", siteCtx, peerCIDRs))
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

// msHealDataPlane removes the data-plane cut policy, restoring cross-site redis connectivity.
func msHealDataPlane(siteCtx, ns string) {
	By(fmt.Sprintf("Scenario B: removing data-plane cut policy on %s", siteCtx))
	_, _ = kctl(siteCtx, "-n", ns, "delete", "ciliumnetworkpolicy", msDataPlanePolicyName, "--ignore-not-found")
}

// msPodCIDRsForSite returns the instance's redis pod IPs (as /32 CIDRs) in a site — the exact
// peers to deny so the cut is surgical (only this instance's cross-site redis, nothing else).
func msPodCIDRsForSite(reg *msRegistry, siteCtx, ns string) []string {
	out, err := kctl(siteCtx, "-n", ns, "get", "pods", "-l", "app.kubernetes.io/component=cluster",
		"-o", "jsonpath={range .items[*]}{.status.podIP}{\"\\n\"}{end}")
	Expect(err).NotTo(HaveOccurred())
	var cidrs []string
	for _, ip := range strings.Split(strings.TrimSpace(out), "\n") {
		if ip = strings.TrimSpace(ip); ip != "" {
			cidrs = append(cidrs, ip+"/32")
		}
	}
	return cidrs
}

// --- Disruption: whole-site loss via talosctl reboot (Scenario A) -------------------------

// msRebootSite reboots every node of a workload site via talosctl. A reboot self-reverses on
// boot (also exercising the fresh-pod rejoin path), so it needs no explicit power-on. The
// site's Kubernetes API and its redis pods go unreachable for the reboot window (> the
// takeover cooldown for a 3-node control-plane site), yielding an Unobservable witness.
func msRebootSite(site msSite) {
	Expect(site.talosEndpoints).NotTo(BeEmpty(), "no talos endpoints configured for site %s", site.name)
	By(fmt.Sprintf("Scenario A: rebooting whole site %s via talosctl (nodes %v)", site.name, site.talosEndpoints))
	// Pin the talos context to the site being rebooted: its talosconfig carries the CA/PKI
	// for that site's nodes. Without this, talosctl inherits whatever context is current —
	// in the lab a client config whose CA cannot verify these nodes ("certificate signed by
	// unknown authority ... Ed25519 verification failure"). The per-site talos context is
	// named after the site (scm-s1/s2/s3), mirroring how the kubectl helpers pin --context.
	// --wait=false: fire-and-return. Newer talosctl defaults --wait=true, which blocks the
	// command until every node is back healthy (up to 30m) — that would consume the very
	// outage window this scenario must observe (the site going API-unreachable). We issue the
	// reboot and track recovery ourselves via msWaitSiteReady.
	args := []string{"reboot", "--context", site.name, "--wait=false"}
	for _, ep := range site.talosEndpoints {
		args = append(args, "-e", ep, "-n", ep)
	}
	// talosctl reboot returns once the reboot is issued (--wait=false); it does not block on boot.
	out, err := utils.Run(exec.Command("talosctl", args...))
	_, _ = fmt.Fprintf(GinkgoWriter, "talosctl reboot output:\n%s\n", out)
	Expect(err).NotTo(HaveOccurred(), "talosctl reboot %s", site.name)
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
