# Design: Multi-Site Cluster Mode (Hub-and-Spokes)

> **Status:** Implemented, ships **experimental** (ADR-012, CLAUDE.md pillar 3.15).
> Steady state is live-verified on a real 4-cluster lab; the site-loss recovery
> (quorum-arithmetic takeover + master rebalancer + reduced-resilience warning) is
> unit/envtest-verified, live validation pending.
> **Created:** 2026-08-01.
> **Scope:** `mode: cluster` only (see §2).

---

## 1. What we are building

Run a **single stretched Redis Cluster across multiple Kubernetes clusters** ("sites"),
managed by **one littlered operator** running in a designated management Kubernetes
cluster (the "hub"). Every shard keeps replicas in more than one site, so the loss of
a whole site loses no replicated data: the affected shards fail over to their replicas
in the surviving sites and the instance keeps serving.

The data plane stays plain OSS Redis/Valkey Cluster — same gossip, same failover, same
pure in-memory posture (no persistence, EmptyDir, pillar 3.1). Multi-site is expressed
in *placement* and *operator plumbing*, not in a new replication mechanism.

### Terminology (extends CLAUDE.md §2)

| Term | Meaning |
|------|---------|
| **site** | One workload Kubernetes cluster hosting Redis pods of the instance |
| **hub** | The Kubernetes cluster hosting the `LittleRed` CR and the operator |
| **spoke** | A site, viewed from the hub (hub-and-spokes) |
| **stretched Redis Cluster** | One Redis Cluster whose nodes span multiple sites |

"Cluster" on its own still always means Redis Cluster. Kubernetes clusters are always
called *site*, *hub*, or spelled out as "Kubernetes cluster".

The hub may be one of the sites (it can host workload pods too), or a dedicated
management Kubernetes cluster. Whether the hub itself is highly available across
failure domains (e.g. a Kubernetes cluster whose control plane spans sites) is a
**deployment property, not a code property** — littlered only requires *a* Kubernetes
home for the CR and the operator lease. Operators wanting the management plane to
survive a site loss deploy the hub accordingly.

## 2. Scope

- **Cluster mode only.** `standalone` has nothing to replicate. `sentinel` is out of
  scope: its HA machinery (Sentinel quorum) would become a second cross-site
  failure-detector next to the operator, exactly the two-decider class we avoid; if a
  non-sharded multi-site topology is wanted later, the `failover` mode (ADR-011,
  `docs/FAILOVER_MODE_DESIGN.md`) is the natural carrier, not sentinel.
- **Replication network: direct-routed pod IPs first.** The target environment routes
  pod CIDRs between all sites (and the hub). NodePort and Multus transports are
  deferred (§9).
- **Two sites initially, three or more supported by the same design.** The failure
  model differs materially between the two (§6) — this is documented, not hidden.

## 3. Architecture: hub-and-spokes, flat control

One operator (leader-elected among replicas via the ordinary controller-runtime lease
in the hub) manages **all** resources in **all** sites directly:

- The `LittleRed` CR lives in the hub. One CR, one authority — no per-site CR copies
  to keep consistent.
- The operator holds one Kubernetes client per site, built from per-site kubeconfig
  Secrets referenced in the spec (§7). It creates/updates the per-site workload
  objects (StatefulSets, Services, ConfigMaps) and watches pods in every site.
- All Redis-level orchestration (MEET, FORGET, slot assignment, reshard, failover
  takeover) is issued by the hub operator, which dials pod IPs directly over the
  routed pod network — same as single-site, just across sites.

### 3.1 Flat, not tiered — decision

We considered a tiered model: subordinate littlered operators in each site owning
their local StatefulSets in a restricted mode, with the hub owning global topology and
delegating via intermediate resources. A subordinate can be *configured to comply*
(strict "know your limits" mode), so this is not inherently a two-decider conflict.

It is declined for a simpler reason: **a reduced local operator has almost nothing
safe to do alone.** In a stretched Redis Cluster every topology decision needs the
global view; the only genuinely local duty left is stuck-pod recycling (the ADR-008
pattern), and kubelet + the StatefulSet controller already own most pod lifecycle.
The tier would buy a delegation CRD, a coordination contract, cross-tier version skew,
and status-bubbling plumbing — to delegate approximately one job.

To keep the option open cheaply, all per-site access goes through one narrow interface
(**`SiteClient`**: workload CRUD, pod list/watch, Redis dial). A future tiered agent
could implement it without touching any reconciliation logic.

### 3.2 Degradation behavior

If the hub cannot reach a site (or the operator is down entirely), that site's data
plane is **unmanaged, not broken**: pods keep running, Redis keeps serving and keeps
failing over natively. Management resumes on reconnect. This is the same trade
already accepted for operator-led recovery in single-site mode — the operator is a
repair authority, not a data-path dependency.

## 4. Placement model

Extends the per-shard StatefulSet model (ADR-007) with a site dimension:

- For each shard K, **one StatefulSet per hosting site** (working shape:
  `{name}-shard-K` in each site's Kubernetes cluster; names may collide across sites
  because they live in different clusters). All of a shard's StatefulSets carry the
  same `redis.chuck-chuck-chuck.net/shard: "<K>"` label.
- Sizing keeps `replicasPerShard` with a uniform invariant: **every per-site shard
  StatefulSet has `replicasPerShard + 1` pods** — single-site mode is the degenerate
  case (exactly today's `1 + replicasPerShard`). The "+1" is one more pod, which in
  exactly one of the shard's sites happens to be the master at a given moment (no STS
  is special; see fluid roles below). Note the reading trap: `replicasPerShard: 1` in
  a dual-site instance yields 4 members per shard (1 master + 3 replicas), not 2 — the
  docs must state the total-member formula `sitesPerShard × (replicasPerShard + 1)`
  prominently. A future rename remains possible if the name proves too surprising.
- `sitesPerShard` selects **how many sites host each shard** (default: all sites, i.e.
  every site hosts every shard). With many sites, pinning `sitesPerShard` at 2 or 3
  scales out capacity instead of multiplying copies. Assignment is a deterministic
  round-robin over the ordered site list (shard K → sites `(K+j) mod S` for
  `j < sitesPerShard`), evening out capacity when shards don't divide evenly by sites.
  **Validated `≥ 2`, deliberately.** `sitesPerShard = N+1` tolerates N site losses
  without data loss; at `sitesPerShard == len(sites)` data survives anything short of
  all sites down. Requiring ≥2 is what lets this design *descope* single-site-loss
  data repair entirely (no orphan-slot adoption, no lossy re-seeding — the repair
  families a `sitesPerShard: 1` topology would force us to build). Not a placeholder
  restriction; permitting 1 would re-import that machinery.
- **Roles are fluid** (pillar 3.5): the operator places pods and wires initial
  replication so that each shard has members in multiple sites, then lets Redis
  Cluster elect and move masters as it sees fit. The operator observes; it does not
  pin a "master site" per shard. Consequently there is no "master STS" — any of a
  shard's per-site StatefulSets may hold the master at a given time.
- Masters are *initially* distributed as evenly across sites as the arithmetic
  allows — an odd number of masters over two sites always leaves one site holding the
  majority (`shards: 3`, the standard mental model, is a 2+1 split). Roles then drift as
  Redis fails over, so the operator runs a **master rebalancer** (ADR-012 D2, §10
  item 2): a slow, drift-triggered, no-data-loss graceful `CLUSTER FAILOVER`. It holds
  **two thresholds**, because the two ways a site loss survives impose *different*
  conditions on `mᵢ` (the slot-serving masters in site `i`) — with `K` shards and
  `q = ⌊K/2⌋+1` the Redis master majority:
  - **takeover floor** (hard): `max_i mᵢ ≤ ⌊K/2⌋` — the *lost* side lacks quorum, so the
    survivors hold a blocking minority, the takeover's fence holds, and the loss recovers
    unattended. Breaching it makes that site's loss a **drift-refusal**: a human or
    `multiSite.allowUnsafeSiteTakeover`.
  - **native target** (standard): `max_i mᵢ ≤ ⌈K/2⌉−1` — the *survivors* keep quorum, so
    Redis elects on its own with the operator uninvolved. Breaching it only downgrades
    recovery to a takeover (cooldown + a live hub), not to an outage.

  They coincide for odd `K` and differ by exactly one for even `K`. The rebalancer plans
  to the target when a move set reaches it, otherwise to the floor — floor-only progress
  is a valid plan, since the floor is what keeps a site loss recoverable at all (LR-031).
  Consequence for site loss (§6, ADR-012): losing a site at or below the native target
  heals natively; between the target and the floor it recovers by the automated takeover,
  which is safe *exactly* in the even split (`reachable = unreachable = K/2`); above the
  floor — the lost side could hold the master majority — it lands in the
  **drift-refusal**, surfacing a condition and waiting for the opt-in or a human. That
  refusal is an expected dual-site outcome, not a rare corner: an odd `K` over two sites
  cannot satisfy the floor at all, and drift can outrun the rebalancer.
- The shard-aware reattach (LR-020 `chooseReattachTarget`) keeps its rule "attach an
  empty pod to the under-replicated master of *its own shard*" — membership is per
  shard across sites; `cluster-allow-replica-migration no` stays. **A shard always
  has replicas in multi-site, regardless of `replicasPerShard`** — it spans
  `sitesPerShard ≥ 2` per-site StatefulSets, each sized `replicasPerShard + 1`, so
  its replica count is `sitesPerShard × (replicasPerShard + 1) − 1` (e.g.
  `replicasPerShard: 0` still yields one cross-site replica per shard). The repair
  loop's expected-replica count is therefore this formula in multi-site, not
  `replicasPerShard` — the single-site "`replicasPerShard == 0` ⇒ no replicas"
  short-circuit must not disable cross-site reattach (LR-027).
- Identity stays announced **pod IPs** (native Redis Cluster behavior); with routed
  pod CIDRs nothing needs `cluster-announce-*` overrides. Clients are ordinary
  cluster-aware clients reaching pod IPs from any site; replica reads can be kept
  site-local by latency-aware client read policies (client concern, not operator's).

## 5. Redis configuration deltas

- **`cluster-require-full-coverage no`** becomes mandatory for multi-site instances
  (littlered currently leaves the Redis default `yes`): while a site's shards are
  failing over, the surviving shards must keep serving their slots instead of the
  whole keyspace going down.
- `cluster-node-timeout` stays at its default unless proven necessary (pillar 3.5);
  cross-site RTT is orders of magnitude below the 15s default.

## 6. Failure model

Verified against Redis 8.4 `cluster_legacy.c` (`clusterUpdateState()`): a node whose
reachable slot-serving masters fall below `(size/2)+1` sets `cluster_state:fail`
**unconditionally** — independent of `cluster-require-full-coverage`. This single
fact shapes the dual-site rows below.

| Failure | Sites | Behavior |
|---|---|---|
| Pod / node loss | any | Native Redis failover to a replica (same or other site). Existing repair loop unchanged. |
| Full site loss | ≥3 | **Native**: survivors keep a master majority, FAIL-mark the dead masters, elect cross-site replicas. Operator cleans up ghosts (FORGET) afterwards. |
| Full site loss | 2 | **Operator takeover** (quorum-gated, ADR-012 D1): with an even master split, survivors hold exactly half the masters — below quorum — so native failover cannot trigger. The operator (which survives if the hub does) decides purely on Domain-M arithmetic — the survivors' own `cluster_state:fail` plus the reachable/unreachable master counts — and issues `CLUSTER FAILOVER TAKEOVER` on the surviving replicas, then FORGETs the dead nodes, only when the **fence** holds: the *unreachable* masters are themselves below quorum, so an isolated lost side has self-fenced. If the *lost* side could hold the master majority (odd shards, or drift), the fence is unavailable and the operator **refuses** (drift-refusal), waiting for the opt-in, a human, or the Phase-2 fence. |
| Cross-site link cut | 2 | Both halves are minority ⇒ both self-fence. **Operator recovers the even split** (ADR-012 D1): to the survivors this is indistinguishable from a site loss and safe for the same reason (the isolated side self-fenced, serves nothing), so the operator takes over the reachable side — one side serves again; the isolated minority stays fenced, takes no writes, and demotes on heal (no divergence). A strict win over the old "total outage until heal". The **hub-blind** variant (only the hub↔site path is cut, the sites stay mutually connected and serving, survivors `cluster_state:ok`) is **HELD**, not taken over — condition 2 / the survivors' own vantage (LR-017). |
| Cross-site link cut | ≥3 | Only the isolated site fail-stops; majority side continues, fails over the isolated site's shards. Rejoining site's demoted ex-masters resync. |
| Multi-site / total failure | any | **Descoped**: manual re-bootstrap (the instance is a pure in-memory store; total loss is a documented, accepted outcome — cf. ADR-008 for the single-site analog). |

Safety notes:

- The takeover must never fire against a site that is alive and merely partitioned from
  the hub — that would create two masters per shard. Safety does **not** rest on the hub
  detecting whether the site is dead (a failure detector cannot decide safely under a
  partition — FLP): it rests on the **fence**, the unreachable slot-serving masters
  being below quorum. An alive-but-isolated site with a master minority has itself set
  `cluster_state:fail` within `cluster-node-timeout` (Redis's unconditional minority
  check), so it is no longer serving — the same check that causes the outage is the
  split-brain guarantee. The gate layers a cooldown on top (the conservative `…Since`
  gating style of Rule L / ADR-008), and the losing side is demoted on heal
  (async-replication write-loss semantics, the class Redis failover always has).
- **The fence-safe automated path is *exactly* the even-split case.** "Survivors
  below quorum" (takeover needed) and "lost side below quorum" (fence holds) can hold
  together only when the slot-serving masters split exactly in half. Any odd master
  count makes a dual-site loss either native (survivors hold quorum) or a
  drift-refusal (the lost side could hold it). See ADR-012 condition 3.
- **Dual-site takeover presumes the hub survives the lost site.** A hub co-located in
  one of the two sites cannot recover that site's loss — the operator dies with it.
  Automated dual-site recovery therefore effectively requires the hub in a third
  failure domain (or stretched across sites): a third domain holding **no data and no
  quorum participant**, which is exactly why the takeover still beats the witness
  shard (§6.1).
- **Ghost handling becomes multi-site aware** (pillar 3.4): a lost site's node is
  FORGOTTEN only while **every hosting site is observable** (`allSitesObservable`) — a
  no-backing-pod ghost is indistinguishable by NodeID from a node on a
  merely-unreachable site, so FORGET trails idempotently until the site answers again.
- **Site rejoin is a full-sync storm**: every returning shard replica pulls a full RDB
  cross-site at once (EmptyDir ⇒ always full sync). The operator staggers the
  returning site's StatefulSet (re)creation per shard, reusing the serialized-rollout
  pattern (LR-021).

### 6.1 Rejected alternative for dual-site: witness shard

Redis computes quorum over **slot-serving masters only** (verified: nodes with
`numslots == 0` do not count). A minimal third-domain member — one tiny master owning
a single hash slot ("witness shard"), e.g. scheduled on the hub — would make the
master count odd and restore fully native failover for dual-site.

Rejected (ADR-012, Alternatives). The vote requirement is the decisive part: because
only slot-serving masters vote, an arbiter **must hold data**, so it is not a cheap
witness at all — it is a third data site, which is just Tier C by another name and
carries a data site's placement, sizing and teardown obligations. A voting *empty*
primary would fix that, but it does not exist in OSS Redis; it is an upstream proposal
(Valkey #1600, discussion-phase), and forking Redis's vote logic to get it violates
pillar 3.5 and the OSS-only stance. Revisit as a no-fork Tier-D option only if it
lands upstream.

Independently, the mechanism has its own hazard: partial mesh partitions (sites cut
from each other but both reaching the witness) can authorize **mutual cross-site
failovers** with messy convergence. The takeover path (§6) has neither problem and
reuses existing machinery.

## 7. API sketch (tentative)

```yaml
spec:
  mode: cluster
  cluster:
    shards: 3
    replicasPerShard: 1       # per-site STS size = replicasPerShard + 1 (see §4)
  multiSite:
    sites:
      - name: alpha
        kubeconfigSecretRef: {name: site-alpha-kubeconfig}   # omitted ⇒ hub-local site
      - name: beta
        kubeconfigSecretRef: {name: site-beta-kubeconfig}
    sitesPerShard: 2          # default: number of sites; how many sites host each shard
    network:
      mode: pod-routed        # future: nodeport | multus
```

- Absent `multiSite` ⇒ existing single-site behavior, unchanged.
- `kubeconfigSecretRef` names a Secret in the operator namespace holding a kubeconfig
  whose credentials are bound to a **namespaced Role** in the target site — workload
  CRUD + pod read in the instance namespace, not cluster-admin, and **nothing
  cluster-scoped**. A helper (`lrctl` or a script) generates the
  ServiceAccount/Role/RoleBinding, the token, and the Secret.
  - **No cluster-scoped grant is needed.** Site-loss safety is pure Domain-M quorum
    arithmetic (ADR-012 D1), decided from the Redis gather — not a Kubernetes liveness
    read — so the operator never reads per-`Node` `Ready` state and each site's
    credential stays fully namespaced.
- Validation: `multiSite` only with `mode: cluster`; site names unique; ≥2 sites;
  `2 ≤ sitesPerShard ≤ len(sites)`.
- Status gains per-site observed state (reachable, pods, discovered addresses) and
  conditions for degraded sites.

Remaining field-shape details are settled in the ADR, not here.

## 8. Operator plumbing

- **Per-site clients & caches:** one client + informer cache per site, constructed at
  CR admission time from the kubeconfig Secrets; controller-runtime has no mature
  multi-cluster layer, so this is explicit plumbing behind `SiteClient` (the
  cluster-api "remote cluster client" pattern). Watches on spoke pods feed the same
  event stream as local watches do today.
- **Gather:** unchanged in shape — concurrent, per-probe `ProbeTimeout`-bounded dials
  (LR-017) already tolerate unreachable addresses; cross-site RTT sits well inside
  the 3s bound. Pod↔node correlation uses the union of all sites' pod lists.
- **Reconcile gating:** `allPodsReady`-style gates and wipe recovery (ADR-008) are
  evaluated **per site**, so one unreachable site does not blind repair of the rest.
- **Network prerequisites (two distinct ones):** the hub operator needs (a) routed
  reachability to all sites' **pod IPs** (data-path: Redis dials) and (b) reachability
  of all sites' **Kubernetes API endpoints** (discovery/management). These are
  different network paths and must both be verified at setup; `lrctl` should check
  both explicitly.
- **RBAC blast radius:** the hub operator can modify workloads in every site — the
  accepted cost of flat control, bounded by **namespaced Roles per site**. Nothing
  reaches outside the instance namespace and no cluster-scoped grant is required:
  site-loss safety is Domain-M quorum arithmetic, not a Kubernetes liveness read
  (§7, ADR-012 D1).
- **Spoke-side garbage collection:** owner references do not work across Kubernetes clusters — a spoke-side object referencing the hub-resident `LittleRed` UID dangles, and the spoke's GC controller would treat the owner as absent and delete the object out from under us. Spoke-side children are therefore created **without** owner references (hub-local children keep `SetControllerReference` unchanged); instead the CR carries a finalizer whose delete path sweeps the instance-labeled workload objects through each `SiteClient` before the CR is released. A site unreachable at deletion time keeps the finalizer with bounded retries and a surfaced condition; the documented escape hatch is manual finalizer removal.

## 9. Deferred transports

- **NodePort:** requires per-pod Services plus `cluster-announce-ip/port/bus-port`
  overrides on every node, because every announced address must be reachable by every
  other node and by clients. Feasible, clumsy; deferred until someone without routed
  pod networks actually needs it.
- **Multus:** a dedicated secondary replication network; requires announce overrides
  onto the secondary interface and the operator itself attached to that network.
  Deferred.

## 10. Open items

1. Takeover trigger: designed in **ADR-012 D1** (quorum-gated site-loss takeover) and
   **implemented** — the pure `planSiteTakeover` (Domain-M quorum arithmetic, no
   Kubernetes failure detector), the survivor-vantage gather fields, and the
   TAKEOVER→FORGET→recycle executor, all unit/envtest-verified (changelog LR-026,
   LR-029). Remaining: **live site-loss validation** on the two-site substrate.
2. Master rebalancer: **implemented** (ADR-012 D2, changelog LR-029, corrected by
   LR-031) — the pure `planMasterRebalance` + the healthy-path `rebalanceMasters`
   executor hold the two thresholds (§4), planning to the native target
   `nativeSurvivalTarget(K) = ⌈K/2⌉−1` where a move set reaches it and otherwise to the
   takeover floor `takeoverSurvivalFloor(K) = ⌊K/2⌋`, with one graceful
   `CLUSTER FAILOVER` per reconcile. The structural `ReducedSiteLossResilience` warning
   fires when the **floor** is unreachable (`S·⌊K/2⌋ < K`) — given Redis Cluster's
   three-master minimum that is exactly **two sites with an odd `K`**; for `S ≥ 3` the
   floor is always reachable, so the warning never fires there. Unit/envtest-verified;
   live validation pending.
3. ~~Witness-shard exploration~~ — **rejected**, not deferred (§6.1, ADR-012
   Alternatives): only slot-serving masters vote, so an arbiter must hold data and is
   just a third data site. Reopens only if a voting empty primary lands upstream.
4. Rollout serialization across sites (LR-021 currently serializes shards within one
   site; define the cross-site ordering). *Implemented in the initial multi-site work
   as "at most one shard StatefulSet rolling across the whole instance at a time" —
   stronger than the minimum "never restart a shard's masters in >1 site at once".*
5. e2e substrate: two Kubernetes clusters with mutually routed pod CIDRs; red-first
   scenarios: dual-site takeover on site kill, minority fail-stop reproduction,
   rejoin staggering, hub↔site partition must-not-takeover.
6. `lrctl` multi-site support: site-aware `status`/`verify`/`inspect`/`debug-dump`
   (authority view aggregates all sites), a connectivity preflight, and a
   site-kubeconfig-Secret generator (`lrctl multisite setup-site`, Milestone 5). **Implemented**
   — see `docs/LRCTL.md` "Multi-Site Clusters". The read verbs auto-detect `spec.multiSite` and
   read the CR on the hub; the union of per-site pods runs through the existing
   `GatherClusterGroundTruth` + checks, plus a cross-site placement check (a shard's master and
   replica must live in different sites). The preflight validates the two §8 network
   prerequisites honestly; the generator provisions the §7 RBAC — a fully **namespaced
   Role** per site, no cluster-scoped grant.

   **Three gather tiers (M6)** — lrctl no longer requires direct site access or per-site
   kube-contexts. **Tier 1 (read-only hub):** the operator publishes a compact, site-attributed
   cross-site view to `status.multiSite` (overall `clusterState`/`slotsAssigned`/`crossSiteOK`,
   per-shard master/replica sites + link-down, `lastReconcileTime`), computed each reconcile
   from the union ground truth; `status` and `verify --from-status` render it with only
   `get littlered`. **Tier 2 (exec/run on hub, the DEFAULT for verify/inspect/debug-dump):** the
   operator binary gains `--gather <ns>/<name> -o json`, which builds the SAME per-site
   `SiteClient`s and runs the SAME gather, printing the site-attributed ground truth as JSON;
   `lrctl` runs it as a **throwaway hub Job** (operator SA + image discovered from the operator
   Deployment, `ttlSecondsAfterFinished` for server-side GC, resource-isolated and operator-pod
   independent), reads the Job pod log, and renders. The gather runs *on the hub* — only the
   *result* returns over the K8s API control path, so no hub↔site data mesh is added. **Tier 3
   (direct site access):** the former default, demoted to an explicit `--direct`/`--site-context`
   fallback; the site-name==context-name convention now applies only *within* tier 3. RBAC:
   tier 1 = `get littlered`; tier 2 = lrctl user `create jobs` + `pods/log` in the operator
   namespace (the Job runs as the operator SA, no new operator-SA grant); tier 3 = per-site
   contexts. Pure decision seams are unit-tested red-first (`SelectTier`, `BuildGatherJob`,
   `ExtractOperatorInfo`, `StatusVerdict`, `ExtractResultJSON`, `buildCrossSiteView`, wire
   round-trip). **Live-verified on the lab (M5.1):** from a hub-only kubeconfig (site
   contexts stripped), tier-1 `status` renders the published cross-site view and tier-2
   `verify` gathers live via a throwaway hub Job that is then GC'd — and tier-2 still
   gathers with the operator scaled to zero (the operator-pod-independence payoff of the
   Job over exec). Independently re-verified by the orchestrator.
7. **Multi-site scale-down / topology-shrink guards (pre-graduation). Implemented
   (LR-028).** The single-site path refuses (surfaces a condition and waits, never
   deletes) when `shards` is reduced or a legacy topology is detected — the "never
   delete data by default" pillar. Multi-site now carries the per-site equivalent:
   before creating any resources, `reconcileMultiSiteCluster` runs
   `enforceMultiSiteScaleDownGuard`, which observes the per-site shard StatefulSets
   across the sites and diffs them (pure `planMultiSiteScaleDown`) against the
   desired round-robin placement set (`desiredMultiSiteShardSet`). Any observed
   `(site, shard)` StatefulSet the desired topology no longer includes — from a
   `shards` decrease, a `sitesPerShard` decrease, or a removed site — is an orphan,
   and the operator **refuses**: a `MultiSiteScaleDownRefused` Warning event + Ready
   condition (`False`, reason `MultiSiteScaleDownRefused`, phase `Failed`), requeue
   and wait, **never delete** (cluster storage is EmptyDir; the orphaned STS still
   holds its shard's slot data). All three shrink dimensions collapse to the one
   "observed but not desired" diff. Resolution: restore the topology, or migrate the
   data and delete the orphaned StatefulSet(s) manually — reconciliation resumes
   automatically once no orphan remains. (The legacy single-STS guard is correctly
   moot: a multi-site instance is post-0.3.0 by construction.)

   **Observability limitation (documented, by design).** Refusal requires
   observation, and the operator only builds a `SiteClient` for sites still named in
   `spec.multiSite.sites`. So `shards`↓ and `sitesPerShard`↓ are refused in full
   (their orphans live in sites still in the spec), but a site **fully removed** from
   the list is no longer resolved — its StatefulSets become un-observable drift the
   operator cannot see or refuse. The pure diff is general (given an observation of a
   removed site it would flag it); the limit is in the I/O layer. Operators should
   therefore **drain a site via a `sitesPerShard` reduction (or manual cleanup)
   *before* deleting its entry** from `spec.multiSite.sites`; removing the entry
   first strands that site's data silently.
8. **Operator self-namespace for out-of-cluster runs.** The site-kubeconfig Secret
   lookup needs the operator's own namespace; in-cluster the ServiceAccount namespace
   file resolves it, but an out-of-cluster hub (e.g. during development before the
   management cluster exists) needs a downward-API `POD_NAMESPACE`/`OPERATOR_NAMESPACE`
   env on the manager Deployment. Deploy-wiring follow-up, due with the e2e substrate.

## 11. Pointers

- CLAUDE.md §2 (terminology), §3.4 (Kubernetes as source of truth), §3.5 (minimal
  interference), §3.12 (per-shard StatefulSets), §3.13 (wipe recovery).
- ADR-007 (per-shard StatefulSets), ADR-008 (operator-owned recycle; gating style),
  ADR-012 (site-loss takeover).
- `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md`: LR-017 (bounded probes), LR-020
  (shard-aware reattach), LR-021 (serialized rollout), LR-023 (wipe recovery).
- Redis 8.4 `cluster_legacy.c`: `clusterUpdateState()` (minority fail-stop; quorum
  counts slot-serving masters), `clusterDelNode()` (FORGET unassigns slots).
- ADR-011 / `docs/FAILOVER_MODE_DESIGN.md` (non-sharded sibling mode).
