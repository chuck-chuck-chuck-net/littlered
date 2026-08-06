# Design: Multi-Site Cluster Mode (Hub-and-Spokes)

> **Status:** Design draft — reviewed direction, not yet implemented.
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
  majority (`shards: 3`, the standard mental model, is a 2+1 split). No active master
  balancing in the first iteration; a gentle rebalance (graceful `CLUSTER FAILOVER`
  on a replica, no data loss) is a planned follow-up (§10) — without it, repeated
  failovers can concentrate masters further. Consequence for site loss (§6, ADR-012):
  losing a minority-master site heals natively; losing a majority-master site is
  automatically recoverable only when the site's death is Kubernetes-confirmed —
  under an unobservable witness it lands in the **drift-refusal** (the fence argument
  is unavailable), surfacing a condition and waiting for the opt-in or a human. That
  refusal is an expected, common dual-site outcome, not a rare corner: uneven spreads
  exist by construction (odd shards) and by drift (even shards).
- The shard-aware reattach (LR-020 `chooseReattachTarget`) keeps its rule "attach an
  empty pod to the under-replicated master of *its own shard*" — membership is per
  shard across sites; `cluster-allow-replica-migration no` stays. **A shard always
  has replicas in multi-site, regardless of `replicasPerShard`** — it spans
  `sitesPerShard ≥ 2` per-site StatefulSets, each sized `replicasPerShard + 1`, so
  its replica count is `sitesPerShard × (replicasPerShard + 1) − 1` (e.g.
  `replicasPerShard: 0` still yields one cross-site replica per shard). The repair
  loop's expected-replica count is therefore this formula in multi-site, not
  `replicasPerShard` — the single-site "`replicasPerShard == 0` ⇒ no replicas"
  short-circuit must not disable cross-site reattach (LR-026).
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
| Full site loss | 2 | **Operator takeover** (fence-gated, ADR-012): with an even master split, survivors hold exactly half the masters — below quorum — so native failover cannot trigger. The operator (which survives if the hub does) detects the site loss via the site's Kubernetes API and pods, then issues `CLUSTER FAILOVER TAKEOVER` on the surviving replicas and FORGETs the dead nodes — the existing quorum-loss machinery with a new trigger. If the *lost* site held the master majority (odd shards, or drift), the fence argument is unavailable: under an unobservable witness the operator **refuses** (drift-refusal) and waits for Kubernetes-confirmed death, the opt-in, or a human. |
| Cross-site link cut | 2 | Both halves are minority ⇒ both fail-stop until the link heals; recovery is automatic on heal (rejoin delay applies). **Descoped**: no operator action; a runbook documents it. Link redundancy is the real mitigation. |
| Cross-site link cut | ≥3 | Only the isolated site fail-stops; majority side continues, fails over the isolated site's shards. Rejoining site's demoted ex-masters resync. |
| Multi-site / total failure | any | **Descoped**: manual re-bootstrap (the instance is a pure in-memory store; total loss is a documented, accepted outcome — cf. ADR-008 for the single-site analog). |

Safety notes:

- The takeover trigger must distinguish **site death from hub-side unreachability**.
  Signals: the site's Kubernetes API state (pods gone vs. listed Ready) where
  reachable, Redis-level reachability from surviving sites' pods, and a cooldown —
  the same conservative gating style as Rule L / ADR-008 (`…Since` timestamp,
  fire only on persistent state). A takeover during a mere hub↔site partition while
  the site still serves would create two masters per shard; the gate must make this
  practically impossible, and the losing side is demoted on heal (async-replication
  write-loss semantics, same class Redis failover always has).
- **The fence-safe automated path is *exactly* the even-split case.** "Survivors
  below quorum" (takeover needed) and "lost side below quorum" (fence holds) can hold
  together only when the slot-serving masters split exactly in half. Any odd master
  count makes a dual-site loss either native (survivors hold quorum) or a
  drift-refusal (the lost side could hold it). See ADR-012 condition 4.
- **Dual-site takeover presumes the hub survives the lost site.** A hub co-located in
  one of the two sites cannot recover that site's loss — the operator dies with it.
  Automated dual-site recovery therefore effectively requires the hub in a third
  failure domain (or stretched across sites): a third domain holding **no data and no
  quorum participant**, which is exactly why the takeover still beats the witness
  shard (§6.1).
- **Ghost handling stays Kubernetes-informed and becomes multi-site aware** (pillar
  3.4): a node is only FORGOTTEN when *its* site's Kubernetes API confirms the pod is
  gone — never merely because it is unreachable from the hub.
- **Site rejoin is a full-sync storm**: every returning shard replica pulls a full RDB
  cross-site at once (EmptyDir ⇒ always full sync). The operator staggers the
  returning site's StatefulSet (re)creation per shard, reusing the serialized-rollout
  pattern (LR-021).

### 6.1 Explored alternative for dual-site: witness shard (deferred)

Redis computes quorum over **slot-serving masters only** (verified: nodes with
`numslots == 0` do not count). A minimal third-domain member — one tiny master owning
a single hash slot ("witness shard"), e.g. scheduled on the hub — makes the master
count odd and restores fully native failover for dual-site. Deferred because partial
mesh partitions (sites cut from each other but both reaching the witness) can
authorize **mutual cross-site failovers** with messy convergence; that behavior needs
dedicated analysis and e2e before we trust it. The takeover path (§6) does not have
this failure mode and reuses existing machinery, so it goes first.

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
  whose credentials are bound to a **namespaced Role** in the target site (workload
  CRUD + pod read in the instance namespace — not cluster-admin) **plus one
  cluster-scoped read: `nodes` get/list**. A helper (`lrctl` or a script) generates
  the ServiceAccount/Role/RoleBinding + a ClusterRole/ClusterRoleBinding for the node
  read, the token, and the Secret.
  - **Why the cluster-scoped node read (ADR-012, a real widening beyond a namespaced
    Role).** The site-loss witness must tell a kubelet-attested pod death (the pod's
    Node is Ready) from a node-controller inference (the Node is NotReady/Unknown — a
    remote guess from missed heartbeats, the LR-017 trap). That needs each pod's Node
    `Ready` condition, and Nodes are cluster-scoped, so the per-site cache (namespaced)
    cannot serve them; the operator does a bounded, **uncached** direct read via
    `SiteClient.NodeReady`. This is the only place per-site access reaches outside the
    instance namespace. A failed/absent node read is treated conservatively (as the
    node-controller downgrade → the takeover fence is required), so the widened read
    can only make the witness more cautious.
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
  accepted cost of flat control, bounded by namespaced Roles per site — **plus one
  cluster-scoped read per site (`nodes` get/list), required by the site-loss witness
  (ADR-012 condition 3, §7).** That node read is the only per-site permission outside
  the instance namespace; everything else stays namespaced. The operator's own
  (hub-side) RBAC likewise gains `nodes` get/list.
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

1. Takeover trigger: designed in **ADR-012** (fence-gated site-loss takeover) and
   **implemented (Milestone 3)** — the pure `planSiteTakeover` + `classifySiteWitness`
   planner, the survivor-vantage gather fields, the `SiteClient.NodeReady` witness read,
   and the TAKEOVER→FORGET→recycle executor, all unit/envtest-verified (changelog LR-025).
   Remaining: the two-site e2e suite (Milestone 4) and the `lrctl` site-kubeconfig helper's
   cluster-scoped `nodes` grant (Milestone 5).
2. Master rebalance follow-up (graceful `CLUSTER FAILOVER`), keeping the native
   failover path armed against master drift.
3. Witness-shard exploration (§6.1) — separate design + e2e.
4. Rollout serialization across sites (LR-021 currently serializes shards within one
   site; define the cross-site ordering). *Implemented in the initial multi-site work
   as "at most one shard StatefulSet rolling across the whole instance at a time" —
   stronger than the minimum "never restart a shard's masters in >1 site at once".*
5. e2e substrate: two Kubernetes clusters with mutually routed pod CIDRs; red-first
   scenarios: dual-site takeover on site kill, minority fail-stop reproduction,
   rejoin staggering, hub↔site partition must-not-takeover.
6. `lrctl` multi-site support: site-aware `status`/`verify` (authority view must
   aggregate all sites), connectivity preflight.
7. **Multi-site scale-down / topology-shrink guards (pre-graduation).** The
   single-site path refuses (surfaces a condition and waits, never deletes) when
   `shards` is reduced or a legacy topology is detected — the "never delete data by
   default" pillar. The initial multi-site reconcile does *not* yet carry per-site
   equivalents: reducing `shards`, lowering `sitesPerShard`, or removing a site from
   the list leaves the now-orphaned per-site StatefulSets **un-managed (drift), not
   deleted** — no active data loss, but no refusal either. Per-site orphan detection
   + refusal must land before multi-site graduates from design-draft. (The legacy
   single-STS guard is correctly moot: a multi-site instance is post-0.3.0 by
   construction.)
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
