# lrctl - LittleRed Command Line Interface

`lrctl` is a powerful diagnostic and management tool for Redis clusters. While it is designed specifically for clusters managed by the LittleRed operator, it also provides robust support for "unmanaged" (non-LittleRed) Redis deployments using a discovery engine.

`lrctl` performs deep-dive consistency checks by communicating directly with Redis and Sentinel processes inside your pods, cross-referencing this live data with the Kubernetes API.

## Installation

### Building from source
From the root of the repository, run:
```bash
make lrctl
```
The binary will be created in `bin/lrctl`. To install it globally:
```bash
make install-lrctl
```

### Use as a kubectl plugin
If `lrctl` is named `kubectl-lr` and available in your `PATH`, you can use it as a standard plugin.
```bash
# Set up the symlinks automatically
make install-plugin

# Now use it via kubectl
kubectl lr status <name>
```
*Note: This also enables shell completion for resource names and namespaces when used via `kubectl`.*

## Global Flags
`lrctl` supports standard Kubernetes flags and some specialized diagnostic toggles:
- `-n, --namespace`: Specify the namespace (defaults to your current context).
- `-A, --all-namespaces`: List or verify resources across all namespaces.
- `--kubeconfig`: Path to a specific kubeconfig file.
- `--context`: Kube-context to use. For a multi-site instance this is the **hub** context (where the `LittleRed` CR lives). Defaults to the kubeconfig's current context.
- `--site-context`: Multi-site only. Map site names to kube-contexts, e.g. `--site-context alpha=ctxA,beta=ctxB`. When omitted, each site resolves to a context of the **same name** (convention). See [Multi-Site Clusters](#multi-site-clusters).
- `--json`: Output results in pure JSON for automation/scripting.
- `--unmanaged`: Treat the target as a raw set of pods rather than a LittleRed CR.
- `--kind [sentinel|cluster]`: Used with `--unmanaged` to hint at the cluster type.

---

## Commands

### 1. status
Provides a high-level summary of the health and configuration.

**Usage:**
```bash
lrctl status [name] [-n namespace] [-A]
```
If `name` is omitted, it lists all LittleRed resources in the namespace.

**Example Output (Sentinel Mode):**
```text
Cluster: store-sentinel
Namespace: default
Phase: Running
Mode: sentinel
Master: store-sentinel-redis-0 (IP: 10.233.66.107)
Sentinels: 3/3 Ready
Redis Nodes: 3/3 Ready
```

**Example Output (Cluster Mode):**
```text
Cluster: store-cluster
Namespace: default
Phase: Running
Mode: cluster
Master: <none>
Redis Nodes: 6/6 Ready
```

**Example Output (Failover Mode):**
```text
Cluster: store-failover
Namespace: default
Phase: Running
Mode: failover
Master: store-failover-redis-0 (IP: 10.233.66.112)
Replicas: 2/2 Ready
Redis Nodes: 3/3 Ready
Assignment Epoch: 1
Last Transition: 2026-08-01T12:38:36+02:00
```

Failover mode additionally surfaces the ADR-011 monitoring fields from `status.failover`: the
assignment epoch mirrored from the pod annotations, `Master Down Since` while a detection window
is running, `Last Transition` (the last master-intent stamp), and — when set — the
`FailoverRecovery` condition (the refuse-and-wait state on diverged data holders).

---

### 2. inspect
Performs a "Deep Inspect" by executing diagnostic commands inside every pod.

**Usage:**
```bash
lrctl inspect <name>
```

**What it does:**
- **Sentinel Mode**: Runs `SENTINEL master` on every sentinel pod and `INFO replication` on every redis pod.
- **Cluster Mode**: Runs `CLUSTER NODES` and `CLUSTER INFO` on every node.
- **Failover Mode**: Runs `INFO replication` on every redis pod and prints each pod's
  operator-stamped assignment (the ADR-011 intent record) above the raw output:
  ```text
  Redis Pod: store-failover-redis-1 (IP: 10.233.65.205)
    Assignment: role=replica, master-ip=10.233.66.112, epoch=1 (label: replica)
    # Replication
    role:slave
    ...
  ```
- **Result**: Aggregates all raw output into a single report, allowing you to see exactly what every individual process believes the state to be.

---

### 3. verify
The primary troubleshooting tool. It detects inconsistencies between Kubernetes state and live Redis state.

**Usage:**
```bash
lrctl verify <name> [--json]
```

**Example Output (Sentinel Mode):**
```text
Verifying Cluster: default/store-sentinel (Mode: sentinel)
Gathering Cluster Ground Truth...

Sentinel Status:
  - Sentinel store-sentinel-sentinel-1: monitoring 10.233.66.107
  - Sentinel store-sentinel-sentinel-2: monitoring 10.233.66.107
  - Sentinel store-sentinel-sentinel-0: monitoring 10.233.66.107

Redis Status:
  - Redis store-sentinel-redis-0: role:master
  - Redis store-sentinel-redis-1: role:slave, following:10.233.66.107, link:up
  - Redis store-sentinel-redis-2: role:slave, following:10.233.66.107, link:up

Ground Truth Summary:
  [OK] Authority Master: store-sentinel-redis-0 (10.233.66.107)

[OK] Cluster configuration is consistent.
```

**Example Output (Cluster Mode):**
```text
Verifying Cluster: default/store-cluster (Mode: cluster)
Gathering Cluster Ground Truth...

Cluster State: ok
Total Slots Assigned: 16384 / 16384

Node Status:
  - Pod store-cluster-shard-0-0: role:master, id:db7c8c37cc2badde942fc5cb37b8f11c05d6996f, slots:0-5461
  - Pod store-cluster-shard-0-1: role:replica, id:b7645a823e77866a607b9557507eed42ba6bac77, following:db7c8c37cc2badde942fc5cb37b8f11c05d6996f, link:up
  - Pod store-cluster-shard-1-0: role:master, id:f8c8f5c33c309771dfca901442d7ace22f006dcd, slots:5462-10922
  - Pod store-cluster-shard-1-1: role:replica, id:4ef162bdbdfba00dfeebaa6c078ed358e0c1a555, following:f8c8f5c33c309771dfca901442d7ace22f006dcd, link:up
  - Pod store-cluster-shard-2-0: role:master, id:f450773574d834f4ab549a76f5804fc66bcdf1cb, slots:10923-16383
  - Pod store-cluster-shard-2-1: role:replica, id:34139e215f431b9582b17fb4fd4d8cf210ba73ce, following:f450773574d834f4ab549a76f5804fc66bcdf1cb, link:up

Cluster Topology:
  Master: store-cluster-shard-0-0 (db7c8c37cc2badde942fc5cb37b8f11c05d6996f)
    Slots: 0-5461
    └── Replica: store-cluster-shard-0-1 (b7645a823e77866a607b9557507eed42ba6bac77, link:up)
  Master: store-cluster-shard-1-0 (f8c8f5c33c309771dfca901442d7ace22f006dcd)
    Slots: 5462-10922
    └── Replica: store-cluster-shard-1-1 (4ef162bdbdfba00dfeebaa6c078ed358e0c1a555, link:up)
  Master: store-cluster-shard-2-0 (f450773574d834f4ab549a76f5804fc66bcdf1cb)
    Slots: 10923-16383
    └── Replica: store-cluster-shard-2-1 (34139e215f431b9582b17fb4fd4d8cf210ba73ce, link:up)

Summary:
  [OK] Cluster is healthy and consistent.
```

The cluster summary has three verdicts: `[OK]` (healthy and shards colocated), `[DEGRADED]` (functional but a
replica's replication link is down — reduced redundancy; often a transient resync, exit 0), and `[FAIL]` (a
health/topology problem — missing slots, empty masters, or a **shard-colocation violation**: a replica whose master is
in a different shard StatefulSet, which breaks per-shard failure-domain placement; see ADR-007).

**Example Output (Failover Mode):**
```text
Verifying Cluster: default/store-failover (Mode: failover)
Gathering Replication Ground Truth...

Assignment Intent:
  Intended Master: store-failover-redis-0 (10.233.66.112, epoch 1)
  Max Assignment Epoch: 1

Redis Status:
  - Redis store-failover-redis-0: role:master, offset:25508, keys:0, assigned:master@1, label:master
  - Redis store-failover-redis-1: role:slave, following:10.233.66.112, link:up, offset:25508, keys:0, assigned:replica@1, label:replica
  - Redis store-failover-redis-2: role:slave, following:10.233.66.112, link:up, offset:25508, keys:0, assigned:replica@1, label:replica

Ground Truth Summary:
  [OK] Authority Master: store-failover-redis-0 (10.233.66.112)

[OK] Instance configuration is consistent.
```

Failover mode (ADR-011) has no Sentinels: the operator's assignment annotations on the data pods
are the intent record, and verification is the comparison **intent ∩ observation**:

- **Assignment Intent** — the *intended* master is the pod with an `assigned-role: master`
  annotation at the highest `assignment-epoch` (the operator's semantics). Each per-pod line shows
  the observed role next to the assignment (`assigned:<role>@<epoch>`) and the routing label
  (`label:`); a pod in the epoch-yield park state is marked `[PARKED]`, and terminating/not-ready
  pods are marked too.
- **Authority Master** — the intended master *iff* it is reachable and observed `role:master`
  (mirrors the operator's `determineFailoverLiveMaster`). No authority master is a `[FAIL]`.
- **Findings** — classified `[FAIL]` (verification fails, non-zero exit) or `[WARN]` (functional
  but degraded, exit 0): label-vs-authority disagreement, a straggler (unintended `role:master`),
  a replica following the wrong master IP, missing or duplicated master assignments, and **lineage
  divergence** across data holders (`master_replid`/`master_replid2` disjoint — electing any one
  node would discard writes) all FAIL; a parked pod, a fresh pod awaiting authorization, an
  unreachable replica, and `link:down` toward the authority master (transient resync) WARN.

The failover summary uses the same three verdicts as cluster mode: `[OK]`, `[DEGRADED]` (warnings
only), `[FAIL]` (any FAIL finding, non-zero exit).

**Advanced Checks:**
- **Consensus**: Do all Sentinels agree on the master?
- **Ghost Detection**: Is the master reported by Redis/Sentinel actually a living Kubernetes pod?
- **Role Alignment**: Do the `redis.chuck-chuck-chuck.net/role` labels match the actual process role?
- **Topology (Cluster Mode)**: Visualizes the tree of Master -> Replica relationships and slot coverage.
- **Shard colocation (Cluster Mode)**: Each Redis shard's master and replica(s) must live in one shard StatefulSet
  (`{name}-shard-K-*`); a cross-StatefulSet pairing fails verification (LR-020).
- **Partition Detection**: Identifies if nodes see different versions of the cluster topology.

---

## Multi-Site Clusters

For a **stretched (multi-site) cluster** (`spec.multiSite`, managed hub-and-spokes by one
operator — see `docs/MULTI_SITE_DESIGN.md`), the read verbs (`status`, `verify`, `inspect`,
`debug-dump`) are **site-aware automatically**: they detect `spec.multiSite` on the CR and
aggregate across **all** sites. Point `lrctl` at the **hub** (`--context <hub>`), where the
CR lives; it discovers the ordered site list and the instance namespace from the CR, then
gathers each site.

### The two-network wrinkle (why you pass contexts, not the operator's Secrets)

The operator reaches sites over the **replication-net** endpoint embedded in its per-site
kubeconfig Secrets — an address reachable **only from hub pods**. A workstation cannot use
those Secrets. So `lrctl` gathers each site through **your own kube-contexts** (the
admin/site-net path). Site name → context resolution:

- an explicit `--site-context <site>=<context>` mapping wins;
- otherwise the **convention** applies: a site is reached through a context of the **same
  name**;
- a site with neither is reported unresolved (and `verify` FAILs — the gather is incomplete).

`docs/MULTI_SITE_DESIGN.md §8` describes the **two distinct** network prerequisites the
operator needs: (a) hub-pod → spoke **pod-IP** data path (Redis dials) and (b) hub → site
**Kubernetes API**. They are different paths; the `preflight` verb checks each honestly.

### Site-aware verify

```text
Verifying Multi-Site Cluster: ms-smoke/ms-smoke (Mode: cluster, 3 sites)
Gathering Cluster Ground Truth across sites...

Sites:
  - scm-s1 (context scm-s1): reachable, 2 pod(s)
  - scm-s2 (context scm-s2): reachable, 2 pod(s)
  - scm-s3 (context scm-s3): reachable, 2 pod(s)

Cluster State: ok
Total Slots Assigned: 16384 / 16384

Node Status:
  - Pod scm-s1/ms-smoke-shard-0-0 [site scm-s1]: role:master, id:25fa735f…, slots:0-5461
  - Pod scm-s1/ms-smoke-shard-2-0 [site scm-s1]: role:replica, id:72ac630d…, following:4a84fb37…, link:up
  - Pod scm-s2/ms-smoke-shard-0-0 [site scm-s2]: role:replica, id:14d1b9e7…, following:25fa735f…, link:up
  - Pod scm-s2/ms-smoke-shard-1-0 [site scm-s2]: role:replica, id:05f09cc6…, following:10c76ca0…, link:up
  - Pod scm-s3/ms-smoke-shard-1-0 [site scm-s3]: role:master, id:10c76ca0…, slots:5462-10922
  - Pod scm-s3/ms-smoke-shard-2-0 [site scm-s3]: role:master, id:4a84fb37…, slots:10923-16383

Cluster Topology (site-attributed):
  Master: scm-s1/ms-smoke-shard-0-0 [site scm-s1] (25fa735f…)
    Slots: 0-5461
    └── Replica: scm-s2/ms-smoke-shard-0-0 [site scm-s2] (14d1b9e7…, link:up, cross-site)
  Master: scm-s3/ms-smoke-shard-1-0 [site scm-s3] (10c76ca0…)
    Slots: 5462-10922
    └── Replica: scm-s2/ms-smoke-shard-1-0 [site scm-s2] (05f09cc6…, link:up, cross-site)
  Master: scm-s3/ms-smoke-shard-2-0 [site scm-s3] (4a84fb37…)
    Slots: 10923-16383
    └── Replica: scm-s1/ms-smoke-shard-2-0 [site scm-s1] (72ac630d…, link:up, cross-site)

Cross-Site Placement:
  [OK] every shard spans >= 2 sites (a shard's master and replica live in different sites)

Summary:
  [OK] Stretched cluster is healthy and consistent across all sites.
```

Nodes are keyed by a **site-qualified key** (`<site>/<pod>`) because pod names collide
across sites (each site has its own `{name}-shard-K-*`). Two multi-site-specific checks run
on top of the shared cluster checks (state, slot coverage, ghosts, partitions):

- **Cross-Site Placement** — the multi-site **inversion** of the single-site shard-colocation
  invariant. Single-site wants a shard's master and replica in one shard StatefulSet;
  multi-site wants them in **different sites** so a whole-site loss keeps a copy. A shard
  whose members all live in one site is a `[FAIL]` (no cross-site copy). Each replica line is
  tagged `cross-site` or `SAME-SITE`.
- **Shard colocation** (reused unchanged, LR-020) — a replica must still follow a master in
  its **own shard K**; a cross-shard pairing FAILs.

An unreachable/unresolved site is reported and makes the verdict `[FAIL]` ("verification is
incomplete") — its nodes then appear as ghosts in the survivors' gossip view and its shards
show as placement violations. Verdicts and exit codes match cluster mode (`[OK]`/`[DEGRADED]`
exit 0, `[FAIL]` non-zero). `--json` adds `multiSite`, `sites[]`, site-attributed `nodes[]`,
`siteSpreadViolations`, and `unreachableSites`.

### Site-aware status

`status` reads the operator's **aggregated** per-site observed state from the CR
(`status.multiSite`) — no cross-site gather needed:

```text
Cluster: ms-smoke
Namespace: ms-smoke
Phase: Running
Mode: cluster
Master: <none>
Multi-Site: 3 sites, sitesPerShard=2, network=pod-routed
Cluster State: ok
Sites:
  - scm-s1: reachable, pods 2/2 Ready
  - scm-s2: reachable, pods 2/2 Ready
  - scm-s3: reachable, pods 2/2 Ready
Site-Loss Takeover: [recovered] Recovered: No site-loss takeover needed.
Redis Nodes: 6/6 Ready
```

`inspect` and `debug-dump` are likewise site-aware: `inspect` execs the cluster diagnostics
in every site's pods (output grouped by site, pods keyed `<site>/<pod>`); `debug-dump` writes
per-site subdirectories `site-<name>/` (pod logs, live cluster state, K8s resources) via each
site's context, keeping the hub-only CR and operator logs at the top level.

### Connectivity preflight

`lrctl multisite preflight <name>` validates the operator's setup, honestly reporting which
network path each check used:

```text
Multi-Site Preflight: ms-smoke/ms-smoke (3 sites, operator namespace "littlered-system")

Site "scm-s1":
  [OK] kubeconfig Secret littlered-system/site-scm-s1 exists and is well-formed
  [INFO] operator path (hub-pod -> site-API): https://10.2.1.10:6443 — NOT tested (reachable only from hub pods)
  [OK] workstation path: site Kubernetes API reachable via context "scm-s1"
…
[INFO] hub-pod -> spoke-pod-IP DATA path (Redis dials over the routed pod network):
       REQUIRES an in-cluster probe (a pod on the hub dialing a spoke pod IP); NOT verified here.
       This is a DIFFERENT network path from the site-API checks above (design §8).

Summary:
  [OK] Secrets well-formed and every site's Kubernetes API reachable over the workstation path.
```

It checks (a) each site's `kubeconfigSecretRef` Secret exists on the hub and is well-formed
(parses, has the `kubeconfig` key, names an endpoint); (b) each site's Kubernetes API is
reachable over the **workstation** path. It **reports but does not test** the operator's
repl-net site-API endpoint (hub-pod-only), and reports the hub-pod → spoke-pod-IP **data
path** as requiring an in-cluster probe — it never claims a pass it did not observe.

### Site-Secret setup helper

`lrctl multisite setup-site` automates onboarding a site (the manual `docs/MULTI_SITE_DESIGN.md
§7` steps). In the **site** it creates (in the instance namespace) a ServiceAccount, a
namespaced Role (the operator's workload CRUD), a ClusterRole + ClusterRoleBinding granting
cluster-scoped **`nodes get;list`** (the ADR-012 site-loss witness), and a long-lived token;
it then assembles a kubeconfig and creates it as a Secret (data key `kubeconfig`) in the
operator namespace on the **hub**.

```bash
lrctl multisite setup-site \
  --site scm-s1 \                       # site name (also the generated context name)
  --site-admin-context scm-s1 \         # workstation context for the site (default: --site)
  --hub-context scm-cp \                # hub context (default: --context)
  -n ms-smoke \                         # instance namespace
  --repl-endpoint https://10.2.1.10:6443 \   # REQUIRED — see below
  --operator-namespace littlered-system \    # where the hub Secret is created
  --secret-name site-scm-s1             # default: site-<site>
```

`--repl-endpoint` is **required** and is the crux of the two-network wrinkle: the generated
kubeconfig's server must be the site's **replication-net** endpoint (what hub pods use), which
**cannot** be derived from your workstation kubeconfig (that holds the admin endpoint). Pass
`--dry-run` to print the objects without applying. The command is idempotent (re-running
updates in place). The cluster-scoped ClusterRole/ClusterRoleBinding are namespace-suffixed
(`littlered-site-nodes-<namespace>`) so multiple instances never clobber one another.

## Advanced Usage

### Working with Unmanaged Clusters
If you have a Redis cluster that wasn't created by LittleRed (e.g., via a manual StatefulSet), you can still use `lrctl`:

```bash
# Inspect a manually created Sentinel cluster named 'my-custom-redis'
lrctl inspect my-custom-redis --unmanaged --kind sentinel
```
The tool will use heuristics to find the pods and perform the same deep-dive diagnostics.

### Automated Auditing (JSON)
All major commands support the `--json` flag. This is ideal for CI/CD pipelines or custom monitoring:

```bash
# Check if a cluster is healthy via script
if [ "$(lrctl verify store --json | jq '.[0].healthy')" = "true" ]; then
  echo "Cluster is OK"
fi
```

## Troubleshooting Failovers
If a failover seems stuck, run `lrctl verify`. It specifically looks for common failure modes:
1.  **Ghost Masters**: Sentinels pointing to an IP of a pod that was deleted.
2.  **Split Brain**: Different nodes disagreeing on the master.
3.  **Role Mismatch**: A pod labeled as `master` that is actually running as a `slave`.
