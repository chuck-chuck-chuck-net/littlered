# Design Note: In-Cluster E2E Soak Harness (Repeated Runs for Statistics)

> **Status:** Exploratory design — not yet committed, not yet implemented.
> **Created:** 2026-06-26 (design discussion). This note captures the reasoning and the
> committed direction (Path B) so a future session can pick up without re-deriving it.
> **Decision owners:** the littlered authors (spare-time OSS); the immediate driver is wanting
> "a sample of N e2e runs on the same code" for flake/reliability statistics.
>
> **Update 2026-08-14 — much of §5 has landed, and a second workload arrived.**
> §5.1 (`WATCH_NAMESPACE` scoping) shipped as **ADR-014**; §5.2 (namespaced chart `Role`) shipped
> (`charts/littlered/templates/role.yaml` + `rolebinding.yaml`). §5.3 (operator-namespace
> parametrization) is **still open** — `operatorNamespace` is hardcoded `littlered-system` in
> `e2e_suite_test.go:40` and in the `Makefile` helm target. §5.4 (the Job harness) is untouched.
> Separately, the multi-site work built a **write-ahead disruption ledger + out-of-band `ms-heal`**
> (`test/e2e/ms_disruption_ledger_test.go`, `ms_heal_test.go`) — a stronger answer to
> crash-safe cleanup than this note contemplated, and now load-bearing for §9.
> **§9 records the second workload — the multi-site test matrix — and the decision to make this
> harness the shared execution substrate for both.**

---

## 1. What we want

Run the existing e2e suite **repeatedly on a fixed image** — e.g. 20 iterations, or "let it
run for 24h" — to gather statistics on flakiness/reliability of the *same code*. Each run must
be:

- **Strictly sequential** (max concurrency == 1). No two runs active at once.
- **Self-contained**: a fresh e2e namespace *and* a fresh operator per run.
- **Forensics-friendly on failure**: a failed run is **left standing, idle**, for later
  inspection — including its own operator pod (live, not just logs). Subsequent runs must **not
  clobber** it.

Image building / loading is **explicitly out of scope** for now. We run many iterations on one
pre-built, pre-pushed image. Anything that needs building images comes later.

The mental model is an HPC batch array: N jobs, a concurrency cap of 1, each claiming its own
slice of resources, failed jobs left parked for forensics.

## 2. Established facts (verified against the code, 2026-06-26)

1. **There is no in-cluster e2e driver in this repo.** The suite is `//go:build e2e` Ginkgo,
   driven from the *host* via `make test-e2e` → `setup-test-e2e` (Kind) → `run-test-e2e`
   (`go test -tags=e2e ./test/e2e/ -timeout 120m`) → `cleanup`. It talks to the cluster from
   outside via `kubectl exec`/`apply`. The only thing injected *into* the cluster is the
   ephemeral `chaos-client` pod per chaos test — not a driver. (The "in-cluster driver" memory
   was from a different project.)

2. **The operator has no namespace-scoping today — it is cluster-wide only.**
   `cmd/littlered/main.go` builds the manager with no `Cache.DefaultNamespaces` and no
   `WATCH_NAMESPACE`; RBAC is `ClusterRole` + `ClusterRoleBinding`; `SetupWithManager` watches
   all `LittleRed` CRs cluster-wide. It is a cluster-wide singleton with leader election.

3. **The operator touches only namespaced resources.** `config/rbac/role.yaml` grants verbs on
   `configmaps, services, pods, secrets, statefulsets, servicemonitors, poddisruptionbudgets,
   littlereds` (+ `/status`, `/finalizers`). No nodes, no storageclasses, **no `pods/exec`**
   (the operator reaches Redis over TCP; only the e2e harness execs, from outside). The CRD
   *definition* is the only cluster-scoped object, and it is installed once and shared.
   ⇒ A plain namespaced `Role` with these exact rules is a drop-in for namespace-scoped runs.

4. **The suite already deploys a fresh operator per run** in `BeforeSuite` (`make deploy`,
   readiness wait, image preflight), into a **hardcoded** operator namespace (`littlered-system`).
   The test namespace defaults to `littlered-e2e` and **must not pre-exist** (clean-slate guard).
   Tests are `Ordered` (sequential by design); the suite timeout is 120m and a full run is
   ~40min+.

## 3. The decision: Path B (namespace-scoped operator, one per run)

**We commit to Path B: each run gets its own operator scoped to its own namespace.**

The requirement "keep a failed run's operator alive for forensics without it being clobbered by
the next 19 runs" is **not a preference that can be shortcut — it forces namespace-scoping.**
Reasoning:

- If a kept operator-1 stays cluster-wide and run 2 starts operator-2 (also cluster-wide), they
  both watch *all* namespaces and **double-reconcile each other's CRs** — corrupting precisely
  the frozen forensic state we wanted to preserve.
- Leader election does **not** save us: each per-run operator lives in its own namespace, so its
  lease is separate ⇒ **both become leaders** and both reconcile everything.

So "N live operators parked side by side" is only coherent if each operator is **blind to the
others' namespaces**, i.e. `WATCH_NAMESPACE`. There is no version of the requirement that avoids
the feature. The alternative considered (Path A — one shared cluster-wide operator, fresh
namespace per run) cannot keep operators alive and was rejected by the requirement owner up front.

### 3.1 Dual payoff
Namespace-scoping is a **genuine product feature** (multi-tenancy / least-privilege via a
namespaced `Role` instead of `ClusterRole`), not test-only plumbing. It is worth landing on its
own merits and carries forward independently of this harness.

### 3.2 On "Path A first"
The Job harness itself (Section 4, items 3–4) is mode-agnostic and **100% reused** regardless of
A vs B. The *only* A-specific piece — wiring a single shared operator — is exactly the thing the
requirement forbids, so it would be pure throwaway. We therefore go **straight to B**. (Total B
effort is small enough — see Section 5 — that an interim A milestone does not pay for itself.)
The one legitimate "stats-tonight" stepping stone, if ever wanted, is *per-run operator torn down
after capturing its log to durable storage* — no scoping needed because only one operator is alive
at a time — but that is a fallback, not the plan.

## 4. Design sketch

### 4.1 Sequencing primitive — native, no custom mutex
Use a Kubernetes **Indexed Job**: `completions: N`, `parallelism: 1`. That *is* the batch array
with a concurrency cap of 1 — pods run strictly one after another, each gets a unique
`JOB_COMPLETION_INDEX` (0..N-1). No lease/mutex machinery to invent.

- Run `i` → operator namespace `littlered-op-$i` watching test namespace `littlered-e2e-$i`.
- **"24h instead of N":** bump `completions` and set `activeDeadlineSeconds`, or have the driver
  loop on a wall-clock deadline. Either is trivial.

### 4.2 The "leave the broken one standing and continue" trick
A failed e2e pod that exits non-zero counts against the Job's `backoffLimit` and stops the array —
the opposite of what we want. So the **driver script always `exit 0`** and records pass/fail
itself (annotation / ConfigMap / structured log line) per index. Failure handling = *do nothing*:
skip cleanup, leave the namespace and its operator running, advance to the next index. (This pairs
with the suite's existing `DEBUG_ON_FAILURE=true` skip-cleanup behavior.)

### 4.3 The driver container
Bundles Go + kubectl + helm + the repo, runs as an in-cluster ServiceAccount (in-cluster config —
no kubeconfig file; the suite's `KUBECONTEXT_PINNING` defaults off, so it works headless). Per
index it invokes the suite with:

```
make run-test-e2e SKIP_KIND_SETUP=true OPERATOR_IMAGE=<pinned> CHAOS_CLIENT_IMAGE=<pinned> \
    TEST_NAMESPACE=littlered-e2e-$IDX OPERATOR_NAMESPACE=littlered-op-$IDX WATCH_NAMESPACE=littlered-e2e-$IDX
```

The suite keeps doing the operator deploy in `BeforeSuite` (it already handles readiness +
image preflight); we just parametrize the two namespaces and pass the watch scope through.

### 4.4 RBAC for the driver
The driver SA needs to create namespaces, helm-install a (now namespaced-`Role`) operator, create
CRs, and `exec` into pods. Broad, but acceptable on a dedicated test cluster. The *operator's*
own RBAC, by contrast, narrows from `ClusterRole` to a namespaced `Role` (Section 2.3).

## 5. Work breakdown & effort (~2.5–3 days)

1. **Namespace-scoping feature** (`cmd/littlered/main.go`): read `WATCH_NAMESPACE` (comma-sep) →
   `cache.Options{DefaultNamespaces: {...}}`. Land with a unit/envtest. **~½ day.**
   *Standalone product feature — candidate for its own issue in `littlered-internal`.*
2. **Chart RBAC**: render namespaced `Role` + `RoleBinding` (the exact rules in §2.3) when a watch
   namespace is set, else keep `ClusterRole` + `CRB`. Leader-election Role is already namespaced;
   CRD stays cluster-installed. **~½ day.**
3. **Parametrize operator namespace**: it is hardcoded `littlered-system` in the suite and in
   `make deploy`/the Helm release. Drive operator-ns and watch-ns from env. **~½ day.**
4. **Job harness**: Indexed Job (`completions/parallelism:1`), driver image, driver script
   (always `exit 0`, record result, leave-standing-on-failure), result aggregation. **~1 day.**

## 6. Open pieces (TODO)

- **Result aggregation / reporting.** Where do per-run pass/fail + timing land so "20 runs, 17
  green" is a one-glance answer? (ConfigMap rollup, or scrape pod logs/annotations.)
- **Garbage budget.** N parked failed runs = N idle operators fast-requeuing + N namespaces of
  Redis pods. Confirmed acceptable footprint on a single-node test cluster, but worth a cap /
  "stop parking after K failures" guard so a pathological night doesn't fill the node.
- **Image pinning.** Decide the fixed image ref + where it is pushed (registry the cluster pulls
  from). Out of scope to *build*, but the harness must pin *one* ref for the whole array.
- **Operator-namespace cleanup semantics.** On *success*, do we delete both namespaces, or keep a
  rolling few? (Default: success → delete both; failure → keep both.)
- **`activeDeadlineSeconds` / per-run timeout** sizing for the 24h mode vs the 120m suite timeout.
- **Verify** the suite's `BeforeSuite` deploy path cleanly accepts an injected operator namespace
  and watch scope (no other `littlered-system` hardcodes lurking).

## 7. Next-session entry points

Pick one:
1. **Land the namespace-scoping feature** (§5 items 1–2) — smallest, highest-reuse, shippable on
   its own merits; everything else depends on it.
2. **Build the Job harness against a manually-scoped operator** (§5 item 4) to validate the
   Indexed-Job + always-exit-0 + leave-standing loop end-to-end before wiring §5 item 3.

## 8. Pointers

- `CLAUDE.md` §4 (modes), §6 (directory structure), §8 (commands).
- Suite & build: `test/e2e/e2e_suite_test.go` (BeforeSuite deploy, namespace guards), `Makefile`
  targets `setup-test-e2e` / `run-test-e2e` / `cleanup-test-e2e`, `docs/E2E_TESTING.md`.
- Operator wiring: `cmd/littlered/main.go` (manager options), `config/rbac/role.yaml`
  (drop-in rule set for the namespaced `Role`), `charts/littlered/templates/` (clusterrole,
  clusterrolebinding, deployment).
- Related design context: `docs/FAILOVER_MODE_DESIGN.md` (same "fewer moving parts, fewer races"
  spirit).


## 9. Second workload: the multi-site test matrix (2026-08-14)

`docs/MULTI_SITE_E2E_TEST_MATRIX.md` plans a tiered torture programme whose upper tiers run
**6–8 h (Tier 3) and 12–24 h (Tier 4)**. Driving that from a laptop over `kubectl` is a setup for
failure: a closed lid, a dropped VPN or a suspended host aborts the run *mid-fault*, leaving a
site powered off and netem qdiscs installed. **The matrix needs a resident driver — which is
exactly what this note designs.** Verdict: **combine.** The matrix is the forcing function that
makes this harness worth building now; the soak workload alone never quite justified it.

### 9.1 The generalization

The two workloads differ only in their iteration variable:

- **Soak:** `for i in 1..N: run(the whole suite)` — same work, N times, for flake statistics.
- **Matrix:** `for cell in cells: run(cell)` — different work, once each, for coverage.

Both are "an indexed array of work items, a concurrency cap, always-`exit 0`, a recorded per-item
result, artifacts on failure". So the driver takes a **work list** rather than a repeat count, and
the soak workload becomes the degenerate case where every item is identical. Everything in §4
(Indexed Job, `JOB_COMPLETION_INDEX`, the always-`exit 0` trick, the driver container) is reused
unchanged.

### 9.2 What the matrix forces this design to change

These are genuine divergences, not reuse:

1. **Failure handling inverts for lab-global faults.** §4.2 says failure handling is *do nothing —
   leave it standing*. That is right for namespace-scoped state and **wrong** for a powered-off
   site or an installed qdisc: parking those poisons every subsequent item, so one red cell at
   hour 3 silently invalidates the next 17. The rule becomes **park the namespace, heal the lab**
   — and the in-tree write-ahead ledger + `ms-heal` already implement exactly the second half.
   The driver runs `ms-heal` between items unconditionally; the failed namespace and its operator
   stay parked for forensics as designed.
2. **Two concurrency caps, not one.** The matrix's §8 splits faults into namespace-scoped
   (parallelizable) and lab-global (strictly serial). That is two Indexed Jobs — Phase P with
   `parallelism: 3`, Phase S with `parallelism: 1` — not one array with a cap of 1.
3. **The ledger is currently single-run-scoped.** Its own header states the assumption:
   *"we assume EXCLUSIVE access to the clusters (one ms e2e run at a time), so a single hub
   namespace holds the whole ledger."* Phase-P concurrency breaks that — three namespaces
   applying CNP cuts share one ledger. **Decision needed:** either key ledger records by
   run/cell, or restrict Phase P to fault kinds the ledger does not track. The second is free and
   probably sufficient (Phase-P faults are namespace-scoped and die with the namespace), but it
   must be stated rather than assumed.
4. **Run-scoped resident infrastructure.** The split-brain monitor (matrix P11) must run for the
   whole array, spanning items — this note has no concept of infrastructure that outlives a work
   item. The driver needs a setup/teardown phase around the array, not only per item.
5. **Per-*item* timeout and artifact capture**, not per-*suite*. A 100-cell array needs a per-cell
   deadline (a wedged cell must not eat the night) and per-cell artifacts: operator log,
   `lrctl debug-dump`, chaos-client metrics, split-brain monitor slice.
6. **Per-item image override.** §6 pins *one* image for the array. Matrix cell **G9** deliberately
   starts on a pre-LR-031 operator and upgrades mid-cell, so the work item — not the array — owns
   the image ref, defaulting to the array's.
7. **Result aggregation stops being a TODO.** §6 lists it as open; for a 20 h / 100-cell run a
   one-glance rollup is the primary output, not a nicety.

### 9.3 Where the driver runs — the "not from a laptop" decision

Multi-site is four Kubernetes clusters plus two hypervisors plus router VMs, so "in-cluster with
in-cluster config" (§4.3) does not cover it. **Run the driver as an Indexed Job on the hub:**

- The hub already holds every site's kubeconfig as a Secret (the operator's own site credentials
  — same pattern, same objects).
- The disruption ledger already lives on the hub.
- The hub is the one cluster guaranteed to survive any *site* fault, which is precisely what the
  array inflicts.

For the faults that need host access (virsh power-off, `tc` netem, router iptables, `talosctl`
clock skew), **do not mount a general SSH key into the pod.** Use restricted `command=` entries in
`authorized_keys` on each hypervisor and router VM, so the driver can invoke only the exact
verbs it needs (`virsh destroy <dom>`, `virsh start <dom>`, a fixed `tc qdisc` form). Auditable,
minimal, and a real safety property for a pod that can otherwise power off the lab.

**The honest caveat:** the hub is itself lab infrastructure. Matrix cell **A15** kills the hub —
the driver cannot live on the thing it is killing. A15 (and any future hub-fault cell) is either
run interactively or needs a small external driver; everything else runs on the hub. Worth saying
out loud rather than discovering at hour 3.

### 9.4 Revised effort

Most of §5 is done (see the status note at the top). What remains for *both* workloads:

| Item | Note |
|---|---|
| §5.3 operator-namespace parametrization | still hardcoded in the suite + Makefile; blocks per-item operators |
| §5.4 Job harness, generalized to a **work list** | the bulk; §9.1 |
| `ms-heal` between items + park-namespace-only-on-failure | §9.2.1 — small, reuses in-tree helpers |
| Ledger run-keying **or** a stated Phase-P fault restriction | §9.2.3 — decide before Phase P |
| Run-scoped monitor setup/teardown around the array | §9.2.4 |
| Restricted-`command=` fault agents on hypervisors/routers | §9.3 — also unblocks matrix P7–P10 |
| Result rollup + per-item artifacts | §9.2.7 |

## 10. Substrate split: which cluster runs which workload (2026-08-14)

The two workloads of §9 do **not** share a cluster. The rule:

| Workload | Runs against | Needs |
|---|---|---|
| **Classic** (this note's original subject: the single-site suite, repeated for statistics) | **any plain workload cluster** — a site cluster of the scm-* lab, or eventually anything | one cluster, the CRD, no disruption privilege, no multi-site notion |
| **Multi-site matrix** (`MULTI_SITE_E2E_TEST_MATRIX.md`) | **the control-plane cluster** (`scm-cp`, the hub) | per-site kubeconfig Secrets, the disruption ledger, host-level fault agents |

Verified premise for the classic half: `E2E_TOPOLOGY` defaults to `single`
(`cluster_harness_test.go:64`) and the suite needs **nothing cluster-scoped except the CRD**. It
is genuinely substrate-portable.

### 10.1 Why classic must stay off the control plane

Three reasons, in increasing sharpness — the third is the one that actually decides it:

1. **A control-plane cluster is not provisioned for workloads.** True, but weak alone.
2. **The parked-forensics model *accumulates* workload.** That is this note's core mechanic (§1,
   §4.2): a failed run stays standing — namespace, operator, and 6–16 Redis pods, indefinitely.
   Twenty iterations with three failures is three such namespaces resident. That is a
   workload-cluster capacity profile, not a control-plane one.
3. **The CRD is a cluster-scoped singleton, and `WATCH_NAMESPACE` does not isolate it.** Namespace
   scoping isolates *reconciliation*, not the *API surface*: two workloads on one cluster share one
   CRD definition. A classic run needing a different CRD version — a schema change under
   development, an older operator image — would clobber the live multi-site instances beside it.
   Separation is the only thing that keeps CRD versions independent between the two workloads.

Two further properties point the same way: classic needs **no** site kubeconfigs, ledger, or
hypervisor access, so hosting it on the hub would hand a buggy test the keys to the whole lab for
no benefit; and keeping it portable is precisely what would let it become the PR-gate subset
`HOMEWORK.md` §1.3 asks for.

### 10.2 The standing requirement: a matrix run takes the whole lab

**Running the multi-site matrix means exclusive access to every involved cluster — the hub and all
sites.** That is the requirement, and it is the only thing this section fixes.

It is not a new constraint, only a stated one: the disruption ledger already assumes it
(*"we assume EXCLUSIVE access to the clusters (one ms e2e run at a time)"*), and the matrix makes
it unavoidable, since it kills every site in turn (A1 kills s2, A2 kills s1, A7 kills any) — no
site is spare, so nothing can be running on one during a matrix run.

**Where classic runs is deliberately left open.** A site cluster between matrix runs, a dedicated
single-node cluster, or a plain VM on a workstation are all fine — classic is substrate-portable
by §10.1, and **no part of the deliverable depends on the choice**: the driver, the work list and
the harness are identical either way. Descoped, not deferred; revisit only if the two workloads'
cadences actually start contending.

### 10.3 Rejected: classic on the control plane, orchestrating parallel runs on the sites

A tempting "smart" variant: run the classic driver on `scm-cp` and have it schedule parallel suite
executions across the site clusters. **Rejected**, and the effort argument is the *weakest* of the
three reasons:

1. It re-imports the multi-cluster credential surface into the one workload that deliberately does
   not need it (§10.1), undoing the blast-radius benefit.
2. It couples classic's availability to hub health — a hub problem then takes out *both* workloads,
   which is exactly the coupling the split exists to remove.
3. It reinvents parallelism the Indexed Job already provides natively (§4.1), at materially higher
   complexity.

### 10.4 One driver image, two deployments

The split costs no duplication. Both workloads run the same work-list driver container (§9.1),
differing only in: which cluster hosts the Job, whether the restricted-`command=` fault-agent
credentials are mounted (§9.3), and the work list. Nothing forks.

## 11. Implementation plan — substrate first, in thin vertical slices (2026-08-14)

**Sequencing thesis:** get the harness *running* against the easiest workload, then widen the
workload, and only then fill the matrix. Each milestone must end in something independently
useful, so the effort pays out even if the next milestone slips. Concretely:

> trivial work item → classic soak → multi-site bringup → multi-site with faults → cells.

Two disciplines make the difference between this working and needing rework:

- **Build the work-list driver on day one, not a repeat counter.** The soak case is
  `work list = [suite] × N` (§9.1). A repeat-counter retrofitted into a work list later is the one
  avoidable rework in the whole plan, and it costs nothing to get right up front.
- **Prove the loop with a work item that takes ~2 minutes, not the 40-minute suite.** Otherwise
  every harness iteration costs 40 minutes. Swap in the real suite only once the array mechanics
  (sequencing, always-`exit 0`, parking, rollup) are demonstrably correct.

### M0 — Spike: can a pod on the hub drive four clusters? — **RUN 2026-08-14, VERDICT: YES**

The riskiest unknown, answered cheaply and first. **Executed and confirmed:** a pod on `scm-cp`
assembled a four-context kubeconfig from per-site SA tokens + CA data and successfully
`kubectl exec`-ed into a pod in each of `scm-s1`, `scm-s2`, `scm-s3`.

Evidence: the three site API endpoints on the repl-net segment (`10.2.{1,2,3}.10:6443`) answered
TLS + `401` in **single-digit milliseconds** from a hub pod — full network path, no router-hop
penalty. Three driver pods then each ran `kubectl --context <site> -n lr-preflight exec sleeper --
echo hello-from-<site>` and all three Completed with the expected output. All four `lr-preflight`
namespaces were deleted afterwards and confirmed gone (independently re-verified by the
orchestrator). No cluster-scoped objects were created.

**Finding 1 — the harness's site credentials are not the operator's.** `msSiteRBAC` grants pods
`delete/get/list/patch/update/watch` but **no `pods/exec`** — deliberately, because the operator
reaches Redis over TCP and only the *harness* execs. Today that exec comes from the host's admin
kubecontexts (`MS_SITE_CONTEXTS`), which an in-cluster driver does not have. The driver needs its
**own** per-site SA. Confirmed as designed.

**Finding 2 — namespace creation cannot ride a namespaced Role. This corrects the plan.** The
earlier text here said the harness SA needs "the operator's rules plus `pods/exec` plus namespace
create/delete", as though that were one more rule in a `Role`. It is not: `namespaces` is a
**cluster-scoped resource**, so a namespaced `Role` cannot grant `create` on it at any rule content
(`kubectl auth can-i create namespaces` → `no`, *"resource 'namespaces' is not namespace scoped"*).
So the per-site harness credential needs **either** a `ClusterRole`+`ClusterRoleBinding` granting
`namespaces: create/delete` on each site, **or** the per-item namespaces must be pre-provisioned by
an out-of-band bootstrap step. **Decide this at M3, not at M4.**

> This does **not** weaken ADR-012's "multi-site keeps the namespaced per-site Role — no
> cluster-scoped RBAC" property, and the two must not be conflated: that property is about the
> **operator's** site credential, which is unchanged. The harness is a *test driver* with a
> separate, deliberately broader credential. Keep them separate objects with separate names so
> nobody later "simplifies" the operator's Role by merging them.

**Finding 3 — driver base image.** The hub pulls `ghcr.io/littlered-operator/*` and Docker Hub
fine. `rancher/kubectl` works but ships **no shell and no `sleep`** — usable only for a
single-`kubectl`-invocation entrypoint, not a looping driver. `bitnami/kubectl:1.31` is a dead tag
(retired scheme), not a network problem. The driver image needs a shell regardless (§11 M1).

**Honest gap:** no multi-site instance was live at spike time (no `LittleRed` CR, no `site-*`
Secrets anywhere), so the spike hand-assembled the kubeconfig from the known-good lab config
(`docs/multisite-e2e-config.sample.yaml` shape) instead of reading a real operator Secret. The
*mechanism* (SA token + CA + endpoint → context → exec) is proven; that `msSetupSites`' own
Secrets work unchanged in-pod is **inferred, not demonstrated** — confirm during M3.

### M1 — The work-list driver, on a trivial item (~1–1.5 days)

Indexed Job (§4.1), parametrized `parallelism`, always-`exit 0` with a per-item result record
(§4.2), per-item artifact directory, a readable rollup. Work list comes from a ConfigMap/arg.
Item = something that finishes in ~2 minutes (the preflight spec, or a single narrow label).

**Done when:** an array of five items runs to completion with one item deliberately failing — it
parks, the remaining items proceed, and the rollup says "5 items, 4 green" at a glance.

> **LANDED 2026-08-15.** Acceptance criterion met verbatim on `s1`: `5 items, 4 green`, the failing
> item parked with its namespace and pod intact, items 3 and 4 proceeding normally. A second array
> exercised the `timeout` field and parked a namespace still holding a live `Running` LittleRed.
> Lives in `test/harness/` (seam `worklist/`, CLI `cmd/lrharness/`, image `driver/`); `Makefile`
> additions are purely additive. Pure seam at 90.4%; build/test/lint green.
>
> Design decisions taken during implementation, all reviewed and accepted:
>
> - **The driver is not in `IMAGES` and does not follow `cmd/<name>/Dockerfile`.** It needs Go,
>   make, kubectl and the repo *at runtime* (1.6 GB), unlike both shipped images which discard
>   their builder. Decisive extra reason: `make build-images` is invoked **by the e2e suite
>   itself**, so listing the driver there would rebuild a 1.6 GB image on every e2e run.
> - **Staged build context** (`make e2e-harness-context`): the root `.dockerignore` excludes
>   `**/*_test.go` — precisely what this image must execute. Staging via `git ls-files` is
>   tool-agnostic and picks up an uncommitted driver without requiring a commit.
> - **`render` emits the whole bundle**, so the ConfigMap the Job mounts and the list the rollup
>   counts are provably the same list.
> - **One result ConfigMap per item**, never one shared map patched N times — required before
>   §9.2.2's `parallelism: 3` can exist, where concurrent patches would conflict and lose results.
> - **Parking fails safe.** The namespace is deleted **only** on an explicit `"parked":false`; a
>   missing or unparseable result record parks instead. An unknown outcome is a reason to keep
>   evidence, never to destroy it. (This corrected a fail-open bug found during implementation.)
> - **Enumerated `ClusterRole`, not `cluster-admin`** — §4.4 blesses "broad", but enumerated keeps
>   the blast radius reviewable.
> - Image tag and registry are **Job parameters** (there is no `.git` in the image); the committed
>   default registry is unchanged.
>
> **Known gap carried forward:** artifact relocation is verified in isolation but not yet against a
> real *spec-level* failure — the acceptance failure occurs in `BeforeSuite`, where the suite's
> `AfterEach` collector never runs, so no `debug-artifacts-*` directory existed to move. Close this
> in M2, whose longer items fail at spec level.

### M2 — Classic soak (~½–1 day) — *first independently valuable output*

Swap the trivial item for the real single-site suite; per-item test namespace **and**
namespace-scoped operator. Runs on a plain workload cluster, never the control plane (§10).
**Done when:** this note's original requirement is met — "20 runs, 17 green" unattended, failures
parked for forensics.

> **Scope corrections from the 2026-08-14 code survey.** M2 is bigger than "§5.3, a rename". Four
> findings, all verified against the tree:
>
> 1. **§5.3 is two gaps, not one.** `operatorNamespace` is hardcoded at `e2e_suite_test.go:40`
>    with no env override (unlike `TEST_NAMESPACE`, which *is* wired end-to-end) — but also
>    **there is no `OPERATOR_NAMESPACE` Make variable at all**: `deploy`/`undeploy` write
>    `-n littlered-system` as a literal (`Makefile:375,382`). And although the chart fully
>    supports namespace scoping (`scope.watchNamespaces` → `Role`+`RoleBinding` per watched
>    namespace), **`make deploy` exposes no flag for it**. Three edits, not one rename.
> 2. **There is no "deploy fresh, don't rebuild" mode — and this contradicts §1.**
>    `SKIP_OPERATOR_DEPLOY=true` means *skip deployment entirely* (assume an operator is already
>    running); there is no third mode meaning "helm-install into a fresh namespace from an
>    already-built, already-pushed pinned image". So §4.3's example command would trigger a full
>    `build-images`+`push-images` on **every work item**, flatly contradicting §1's "image
>    building is explicitly out of scope… one pre-built, pre-pushed image". **This is new work,
>    and it is the real blocker for M2**, more so than the namespace plumbing.
> 3. **`build-images`/`push-images` ignore `IMG`/`OPERATOR_IMAGE` entirely**, hardcoding
>    `$(GIT_TAG)` (`Makefile:317-321`). `deploy` *does* honour an override — so it is possible to
>    ask `deploy` to pull an image that build/push never produced. A latent trap for exactly this
>    harness's "pin one ref for the whole array" requirement.
> 4. **Debug artifacts are CWD-relative with no override** (`debug_artifacts.go:58`) and are
>    collected **only** when `DEBUG_ON_FAILURE=true`. Parking a failed item for forensics needs
>    the artifact directory relocatable, or the driver must `cd` into a mounted volume before
>    invoking the suite.
> 5. **The pull side has the mirror-image trap.** Because `build-images` reuses `$(GIT_TAG)` and a
>    dirty tree does not change it, re-pushing during development overwrites a **mutable tag** —
>    and the target nodes are warm and cache by tag, so `imagePullPolicy: IfNotPresent` then serves
>    the **stale** image and the code change silently does not take effect. Use `Always` (or a
>    distinct dev tag) while iterating; use a pinned ref with `IfNotPresent` for array runs.
>
> **Recommended: push harness images to a local private registry.** Set the existing
> `LITTLERED_REGISTRY` variable (`Makefile:8`) to a registry on your own network for all harness
> work. It is **highly recommended**, and for a long-running array it is close to required:
>
> - **No pull rate limits.** A 20 h array plus its build loop is exactly the workload that trips
>   anonymous public-registry quotas, and it fails in the least helpful way — mid-run, as an
>   `ImagePullBackOff` on item 47.
> - **LAN-speed push/pull**, which dominates the build→test loop while the driver image is still
>   changing.
> - **No external dependency** for a test substrate that is otherwise entirely local.
>
> Requirements are modest: DNS-resolvable, a valid certificate, and reachable **from inside the
> target cluster's pods** — verify the last one from a pod, not from your workstation, since they
> are different network positions. Anonymous/no-auth is fine (and appropriate) for a
> LAN-only registry; confirm it accepts *pushes*, not only pulls.
>
> **The committed default stays `ghcr.io/littlered-operator`.** littlered is public OSS; a LAN
> hostname as the shipped default would break every other user. This is an environment variable
> you set, never a checked-in value.
>
> **Rejected: a pull-through registry cache.** A different answer to the same problem, and not
> needed here. Such a cache pays off when clusters are torn down and rebuilt constantly, so every
> node is cold and re-pulls everything. Our targets are **long-lived clusters that already hold the
> fixed images** (redis, chaos client), and an array pins one operator/driver ref for its whole run
> — so with `IfNotPresent` there is almost nothing left to pull. Wiring it up would additionally
> mean pushing proxy env + a MITM CA into each node's machine config. The private registry above
> gets the same benefits for a fraction of the setup. Revisit only if the harness ever grows a
> rebuild-the-cluster-per-item mode.
>
> Two traps worth carrying into the implementation: **`OPERATOR_NAMESPACE` is already taken** —
> `site_registry.go:159-179` reads it for the operator's *own* multi-site self-namespace
> detection, unrelated to where the operator is deployed; and
> `sentinel_advanced_failover_test.go:215` hardcodes `-n littlered-system` in a shell string,
> bypassing the package var, so a parametrization pass must catch it.

### M3 — Multi-site bringup, no faults (~1 day)

Driver moves to the hub. Four-context kubeconfig from M0, per-site harness RBAC, the frugal
fixture builder (matrix §3), and the run-scoped setup/teardown phase around the array (§9.2.4 —
introduce it here even though only M5 needs it, so monitors slot in without restructuring).

Work item = *deploy a stretched cluster and verify it gets happy*: bring up F2-K4, converge, assert
cross-site placement, tear down. No faults, no ledger, no SSH. **Done when:** that item runs
repeatedly, unattended, green.

### M3.5 — The cells that need no fault primitives — *pull these in early*

A correction to matrix §9's ordering: a substantial set of cells is reachable **immediately after
M3**, before any fault agent exists, because their "fault" is an API call or a pod delete:

- **MS-G (G1, G4, G5, G9)** — the master rebalancer. Drift is induced by deleting a master pod;
  no fault agent needed. This is the **largest never-executed surface in multi-site** (matrix MS-G
  preamble) and LR-031 just switched it on for the recommended topology.
- **F1** — topology immutability: pure API calls, the cheapest cell in the matrix.
- **I1 / I3** — bringup regressions (LR-026 `clusterFormed` veto, LR-027 `rps:0` reattach).

Landing these here means real matrix coverage exists before the expensive infrastructure work
starts, and it exercises the driver on varied items while the array mechanics are still fresh.

### M4 — Fault agents + the 15-minute smoke (~1–1.5 days)

Restricted-`command=` SSH agents on the hypervisors (§9.3), starting with **virsh only** — netem
and MTU come later with their own inventory. Ledger integration and `ms-heal` between items
(§9.2.1). Cells **A1** (FIRE) and **C1** (HOLD).

**Done when:** the matrix's Tier-1 smoke (matrix §7.1) runs as a resident job in ≤15 minutes. This
is the milestone that starts paying daily, and it is the natural point to stop and consolidate.

### M5 onward — widening

Then, in matrix order: the verdict assertion helper and the split-brain monitor (P11) → Tier 2
cells → the router-VM/netem/MTU/clock primitives (P7–P10) → Tier 3 → the fault scheduler (P12) and
Tier 4 soak → the mutant catalogue and teeth run.

### Why this order

M0 answers the only question that could invalidate the design. M1 makes every later iteration
cheap. M2 discharges this note's original requirement and proves the driver against a *simple*
substrate before the four-cluster one. M3 proves multi-site driving without entangling it with
fault injection — so when M4 breaks, the cause is unambiguous. M3.5 buys real coverage of the
riskiest untested code path before spending days on SSH agents. M4 produces the daily net.

The alternative — building fault agents early because they are the interesting part — couples the
two hardest unknowns (in-pod multi-cluster driving, and out-of-band host control) into one
debugging session. Avoid that.
