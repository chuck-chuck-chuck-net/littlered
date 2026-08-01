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
