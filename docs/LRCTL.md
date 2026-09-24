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
- `--site-context`: Multi-site **tier 3** (direct) only. Map site names to kube-contexts, e.g. `--site-context alpha=ctxA,beta=ctxB` (implies `--direct`). Within tier 3 an unmapped site falls back to the site-name==context-name convention. See [Multi-Site Clusters](#multi-site-clusters).
- `--from-status`: Multi-site **tier 1**. Render the cross-site view the operator published to `status.multiSite` (needs only `get littlered` on the hub; no site access).
- `--direct`: Multi-site **tier 3**. Gather each site directly through your own per-site kube-contexts, bypassing the operator.
- `--operator-namespace` / `--operator-deployment`: Multi-site **tier 2**. Where the operator lives on the hub (default `littlered-system` / `littlered`), used to discover the SA + image for the gather Job.
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

#### Declared heavy operations

When a **declared heavy operation** is in progress (ADR-020 — a human edited one of the small,
registered set of spec fields whose change cannot safely proceed alongside regular healing),
`status` adds an `Operation:` line above every other extra, because that mechanism decides whether
the operator is healing this instance or standing down. Captured live during a
`spec.sentinel.masterName` rename:

```text
Cluster: m51
Namespace: m51-ops
Phase: Initializing
Mode: sentinel
Master: m51-redis-0 (IP: 10.233.192.221)
Sentinels: 3/3 Ready
Redis Nodes: 2/3 Ready
Operation: SentinelMasterNameRename — Running for 3s
  SentinelMasterNameRename converged, but the instance is still rolling; acknowledgment waits for it to settle
```

The line carries the operation's registry name, its reason, and how long it has been running; the
indented line beneath is the `OperationInProgress` condition's message — the operator saying, in its
own words, what it is waiting on. A queue behind the running operation is printed as a
`  Pending: A, B` line. `Blocked` and `Stalled` additionally carry the
`[!] ACTION MAY BE REQUIRED` marker `status` already uses for `FailoverRecovery` and
`LeaderlessRecovery`, because neither state ever resolves on its own.

**The block is absent whenever nothing is in flight**, so `status` for an instance that is not
mid-operation is byte-for-byte what it always was — verified by diffing this build against the
previous one on a live instance.

The `--json` output additionally carries `operation` and `acknowledgedOperations` under the CRD's
own key names, so it and `kubectl get littlered -o json` are the same shape. `acknowledgedOperations`
is the completion record — one row per operation *name*, each a keyed digest of the value that
finished, plus when. It is deliberately **not** in the human output: an HMAC identifies nothing a
reader can look up, and printing it invites reading it as "the previous master name", which ADR-018
refuses to remember and ADR-020 keeps unrecoverable by construction. Nothing in `lrctl` derives a
verdict from it (ADR-020 D3); it is emitted only because `--json` is a faithful projection of the CR
and a script may legitimately ask *did this finish, and when* (real output, other keys elided):

```json
{
  "name": "m51",
  "phase": "Running",
  "acknowledgedOperations": [
    {
      "name": "SentinelMasterNameRename",
      "fingerprint": "1d0f9affa9f03791",
      "acknowledgedAt": "2026-09-01T06:10:09Z"
    }
  ]
}
```

---

### 2. inspect
Performs a "Deep Inspect" by executing diagnostic commands inside every pod.

**Usage:**
```bash
lrctl inspect <name>
```

**What it does:**
- **Sentinel Mode**: Runs `SENTINEL masters` and `SENTINEL master <name>` on every sentinel pod,
  and `INFO replication` on every redis pod. The full monitored-master list is printed above the
  raw single-name reply, because that reply can only ever confirm or deny the name it was given —
  a Sentinel carrying a *leftover* name answers it perfectly well:
  ```text
  Sentinel Pod: rn-sentinel-0 (IP: 10.233.192.10)
    Monitored master names:
      - "lr048.rn" at 10.233.192.95, flags:master  (desired)
      - "mymaster" at 10.233.192.95, flags:master  (stale — ours)
    name
    lr048.rn
    ...
  ```
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

Sentinel Identity:
  Master name: default.store-sentinel
  Monitored master names (every name each Sentinel carries):
    - store-sentinel-sentinel-0: "default.store-sentinel" at 10.233.66.107, flags:master  (desired)
    - store-sentinel-sentinel-1: "default.store-sentinel" at 10.233.66.107, flags:master  (desired)
    - store-sentinel-sentinel-2: "default.store-sentinel" at 10.233.66.107, flags:master  (desired)
  [OK] Every reachable Sentinel monitors only "default.store-sentinel".
  [OK] No foreign Sentinel contact observed (3 sentinels, 2 replicas expected).

[OK] Cluster configuration is consistent.
```

#### Sentinel Identity — which master names are monitored

The master name is the **only isolation boundary Sentinel's gossip protocol has** (LR-039), so
`verify` reports not just *whether* each Sentinel monitors the name the CR asks for but **every**
name it carries. That distinction is the whole point: a single-name query returns a healthy answer
from a Sentinel that also carries a second, leftover entry, so before this check a two-name
instance — two `sentinel monitor` lines, two config epochs, two independent failover state
machines over the same three pods — reported as entirely healthy (LR-048).

Each name is classified against the CR's `spec.sentinel.masterName`:

| class | meaning | verdict |
|---|---|---|
| `desired` | the name the CR asks for | — |
| `stale — ours` | a leftover entry of ours: its address is one of this instance's pods, **or** Sentinel flags it down (a dead ex-master is debris) | `[FAIL]` |
| `FOREIGN` | its address is neither one of our pods nor flagged down, so **something else is alive there** — the signature of a cross-instance capture | `[FAIL]` |

The discriminator is exactly the operator's own (Rule N gate G5, `planForsaken` clause 3), so the
tool and the operator cannot disagree about what counts as debris. An entry whose address Sentinel
did not report is treated as foreign: it cannot be attributed to us, and refusing to call it debris
is the safe direction.

**Both classes fail verification** — a name other than the CR's is a defect whatever else is true —
but they call for different actions. A *stale* name is cleaned up by the operator (`Rule N`); read
the `StaleMasterName` condition on the CR, whose message names the gate that refused. A *foreign*
name means the instance may be captured: **do not rename to escape a capture**, because that
converts a diagnosed, self-healing capture into an undiagnosed leaderless refusal — let the
quarantine complete first (ADR-016), then rename the empty instance.

**Read the cross-instance evidence with the pod list in view.** The block ends in a pointer to
the capture runbook, and there is one ordinary state that presents the same way: a pod of yours
that has just been **replaced** — by a rolling update, a drain, a node loss — leaves the pod list
at once, while Sentinel goes on listing its address, unflagged, for a whole
`down-after-milliseconds`. No address set can attribute it (`OwnedIPs` covers the pod that is
still listed, LR-053, and the object that would attribute a replaced one is gone), which is why
the **operator withholds the same attribution while the instance is unsettled** (LR-050) and says
nothing at all. `verify` instead reports the evidence and **qualifies** it: while any Redis pod of
the instance is terminating or its redis container is not Ready, the block carries a `[!]` caveat
naming those pods and asking you to let them settle and re-run before following the runbook
(LR-062). The evidence is never suppressed — this check exists to catch a *partial* capture, and
it is the only signal for a capture shape the operator cannot diagnose at all (LR-054) — so a
caveat means "not attributable right now", never "ignore this".

With `--unmanaged` there is no CR to read the wanted name from, so the check is **skipped** with a
`[WARN]` rather than judged against the fallback guess — classifying against a guess would accuse a
correctly-named foreign instance of carrying a stale name. The `--json` output omits
`masterNameScope` entirely in that case.

A reachable Sentinel whose master list could not be read is reported `[WARN]` and does **not** fail:
an unread list is no evidence either way, and rendering it as convergence would be exactly the
plausible-looking lie this check exists to remove (LR-041).

Captured live on a real two-name instance (t3e), the state a half-finished rename leaves behind —
this is the whole `verify` output, and the last line is `lrctl`'s own exit message:

```text
Verifying Cluster: lr048-red/rn-full-1787768094 (Mode: sentinel)
Gathering Cluster Ground Truth...

Sentinel Status:
  - Sentinel rn-full-1787768094-sentinel-2: monitoring 10.233.192.112
  - Sentinel rn-full-1787768094-sentinel-1: monitoring 10.233.192.112
  - Sentinel rn-full-1787768094-sentinel-0: monitoring 10.233.192.112

Redis Status:
  - Redis rn-full-1787768094-redis-2: role:slave, following:10.233.192.112, link:up, keys:500
  - Redis rn-full-1787768094-redis-0: role:master, keys:500
  - Redis rn-full-1787768094-redis-1: role:slave, following:10.233.192.112, link:up, keys:500

Ground Truth Summary:
  [OK] Authority Master: rn-full-1787768094-redis-0 (10.233.192.112)

Sentinel Identity:
  Master name: lr048-red.rn-full-1787768094
  Monitored master names (every name each Sentinel carries):
    - rn-full-1787768094-sentinel-0: "lr048-red.rn-full-1787768094" at 10.233.192.112, flags:master  (desired)
    - rn-full-1787768094-sentinel-0: "mymaster" at 10.233.192.112, flags:master  (stale — ours)
    - rn-full-1787768094-sentinel-1: "lr048-red.rn-full-1787768094" at 10.233.192.112, flags:master  (desired)
    - rn-full-1787768094-sentinel-1: "mymaster" at 10.233.192.112, flags:master  (stale — ours)
    - rn-full-1787768094-sentinel-2: "lr048-red.rn-full-1787768094" at 10.233.192.112, flags:master  (desired)
    - rn-full-1787768094-sentinel-2: "mymaster" at 10.233.192.112, flags:master  (stale — ours)
  [FAIL] Stale master name(s) "mymaster" are still monitored alongside "lr048-red.rn-full-1787768094".
         One instance under two names runs two independent failover state machines
         over the same pods, which can promote different replicas (LR-039, LR-048).
         The operator prunes them once its gates pass — read the StaleMasterName
         condition on the CR, whose message names the gate that refused.
1 of 1 resource(s) failed verification
```

Everything above the `Sentinel Identity` block reads healthy — an authority master, three
Sentinels agreeing on it, all three Redis pods holding the same 500 keys — which is exactly why
this check had to be added: the instance *is* serving, and the defect is the second `sentinel
monitor` line no single-name query can see.

Note what the capture does **not** show: this binary predates a late revision in which the
`[OK] No foreign Sentinel contact observed` line was un-suppressed. Current builds print it beside
the `[FAIL]`, and deliberately — the two answer different questions, and "the leftover name is
**ours** and nothing foreign is in contact" is precisely what separates a botched rename from a
capture.

The foreign case is **fixture-derived**, not captured: no genuine foreign master existed on the
validation cluster, and a foreign capture must not be manufactured by editing addresses in the one
above. It is rendered by the shipped code from the unit fixtures:

```text
Sentinel Identity:
  Master name: lr048.rn
  Monitored master names (every name each Sentinel carries):
    - rn-sentinel-0: "mymaster" at 10.233.192.152, flags:master  (FOREIGN — not one of our pods, and alive)
    - rn-sentinel-1: "mymaster" at 10.233.192.152, flags:master  (FOREIGN — not one of our pods, and alive)
    - rn-sentinel-2: "mymaster" at 10.233.192.152, flags:master  (FOREIGN — not one of our pods, and alive)
  [FAIL] Master name(s) "mymaster" point at an address that is not one of this instance's
         pods and is not flagged down — someone else's live master. This instance
         may be captured, and a rename does not escape a capture: it converts a
         diagnosed, self-healing capture into an undiagnosed leaderless refusal.
  [FAIL] Evidence of another Sentinel deployment sharing this master name:
         - monitored master is not one of this instance's pods, and is alive: 10.233.192.152
         - rn-sentinel-0 reports 5 other sentinels; 2 were deployed
         ...
         This instance's data may already have been overwritten. See the
         "Recovering a sentinel instance captured by another Sentinel deployment"
         runbook in docs/USAGE.md.
```

A capture is reported **once**, not in two voices: the foreign-name finding and the foreign-contact
evidence are two observations of one state, printed in one block under one heading, sharing a
single pointer to the recovery runbook.

> **Exit code — a behaviour change.** `lrctl verify` on a sentinel instance previously exited
> non-zero only when there was no authority master or when healing actions were recommended. It now
> **also** exits non-zero when any reachable Sentinel monitors a master name other than the CR's.
> An instance that a script previously read as healthy while carrying a leftover name now reports
> failure — which is the point, and the reason the rename runbook's verification step is only
> implementable from this version on. The chain is `verify`'s error → `errCount` → the command's
> error → `os.Exit(1)`; the `1 of 1 resource(s) failed verification` line in the capture above is
> printed on that path, immediately before the non-zero exit. The `--json` output changes the same way: `healthy` is false
> for such an instance, and the new `masterNameScope` object plus the per-Sentinel
> `monitoredMasters` array carry the detail.

#### Declared operations — what the operator is standing down for

A **declared heavy operation** (ADR-020) is a change to one of a small, registered set of spec
fields whose reconciliation cannot safely proceed alongside regular healing — registry v1 has
exactly one member, `SentinelMasterNameRename`. While one runs, the operator deliberately stands
down the healing rules that **assign authority**. `verify` prints that state first, before the live
gather, so the first thing an owner reads is whether the operator is healing this instance at all.
The block is mode-neutral (it is read from `status.operation` on the CR) and is **absent** for
`--unmanaged` targets, which have no CR.

Captured live on t3e, mid-rename — three seconds after the `spec.sentinel.masterName` patch, with
the Redis StatefulSet still rolling:

```text
Verifying Cluster: m51-ops/m51 (Mode: sentinel)

Declared Operation:
  SentinelMasterNameRename — Running for 3s (started 2026-09-01T08:11:10+02:00)
  Operator: SentinelMasterNameRename converged, but the instance is still rolling; acknowledgment waits for it to settle
  [OK] A declared heavy operation is in progress. This is a normal, expected
       state and not a fault — regular healing that assigns authority is
       deliberately stood down until it completes.
Gathering Cluster Ground Truth...
```

**A healthy in-progress operation is not a failure**, and the `[OK]` is load-bearing. An owner doing
the supported thing — renaming an instance per the runbook — must not see `verify` go red for it;
a check that cries wolf on the happy path is a check nobody reads, which is the same reasoning that
keeps `ClusterRolloutBlocked` off a held-but-progressing cluster rollout (ADR-017) and keeps Rule N's
G6 deferral classified as progress rather than blockage.

The severity and the exit code:

| `status.operation.reason` | rendered | exit code | why |
|---|---|---|---|
| *(absent)* | nothing at all | unaffected | steady state; output is unchanged |
| `Running` | `[OK]` | unaffected | a supported change is being carried out |
| `Quarantined` | `[WARN]` | unaffected | the change is **held**, not broken: the instance is quarantined (ADR-016) and has no pods to carry it out. Reported so a held change is never invisible, but the quarantine is what to investigate — and such an instance already fails verification on its own topology, so failing twice would send the reader after the wrong thing |
| `Blocked` | `[FAIL]` | **non-zero** | the driver cannot proceed and is never auto-skipped |
| `Stalled` | `[FAIL]` | **non-zero** | the operation has outlived its `StallAfter` budget and is never auto-exited |
| *(anything else)* | `[WARN]` | unaffected | this `lrctl` is older than the operator; an unrecognised state is not evidence of a defect |

In the capture above `verify` nevertheless exited **1** — and for a live-topology reason, not for
the operation: mid-roll, one replica was still `link:down` behind a replaced pod, so the ordinary
`Recommended Healing Actions` path fired. That is exactly the separation this block is meant to
preserve. The operation says *the operator is deliberately standing down*; the rest of `verify`
still says what is actually true about the topology, and the two verdicts are independent.

`Blocked` and `Stalled` reach the exit code for one reason: **ADR-020 guarantees neither ever
auto-resolves.** There is no auto-exit timer and no auto-skip, on ADR-017's lesson that a timer is
the defect with a delay — so each one is a standing request for a human, and a script that read
exit 0 there would be told the opposite of the truth. `verify` counts a failing operation and a
failing topology as **one** failing resource, not two.

Captured live on t3e — the same instance, renamed again with one replacement pod deliberately made
unschedulable so the rollout can never settle, fifteen minutes later:

```text
Verifying Cluster: m51-ops/m51 (Mode: sentinel)

Declared Operation:
  SentinelMasterNameRename — Stalled for 15m9s (started 2026-09-01T08:14:59+02:00)
  Operator: SentinelMasterNameRename has run past its StallAfter budget; it is not auto-exited (a timer would be the defect with a delay)
  [FAIL] The operation has outlived its StallAfter budget and is not auto-exited
         (ADR-017: a timer would be the defect with a delay). It will stay in
         this state until a human acts. Regular healing that ASSIGNS AUTHORITY
         is stood down for as long as it holds.
Gathering Cluster Ground Truth...
```

and in `status`, where the same state carries the `[!]` marker:

```text
Redis Nodes: 2/3 Ready
Operation: [!] ACTION MAY BE REQUIRED — SentinelMasterNameRename — Stalled for 15m9s
  SentinelMasterNameRename has run past its StallAfter budget; it is not auto-exited (a timer would be the defect with a delay)
```

The matching `--json` (other keys elided) — note that `needsAction` and the non-zero exit are the
same decision:

```json
{
  "name": "m51",
  "healthy": false,
  "operation": {
    "name": "SentinelMasterNameRename",
    "startedAt": "2026-09-01T06:14:59Z",
    "reason": "Stalled",
    "message": "SentinelMasterNameRename has run past its StallAfter budget; it is not auto-exited (a timer would be the defect with a delay)",
    "needsAction": true
  }
}
```

`Blocked` renders the same shape with its own `[FAIL]` text (*"cannot proceed and is not
auto-skipped — head-of-line blocking here is deliberate"*); it is covered by unit tests rather than
by a live capture, because reaching it needs one of Rule N's G1-G5 refusals, which this validation
run did not produce.

In `--json`, the same information is under a mode-neutral `operation` object on every mode's result,
carrying `name`, `startedAt`, `reason`, `pending`, the condition `message`, and `needsAction` —
which is exactly the boolean that drives the exit code. A `Blocked` or `Stalled` operation also sets
`healthy: false`, so a JSON consumer cannot read "healthy" off an instance whose `verify` exited
non-zero. Instances with no operation emit no `operation` key and their `healthy` value is untouched.

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
- **Master-name scope (Sentinel Mode)**: Does every Sentinel monitor *only* the name the CR asks for?
  A leftover or foreign name fails verification (see **Sentinel Identity** above).
- **Ghost Detection**: Is the master reported by Redis/Sentinel actually a living Kubernetes pod?
- **Role Alignment**: Do the `redis.chuck-chuck-chuck.net/role` labels match the actual process role?
- **Topology (Cluster Mode)**: Visualizes the tree of Master -> Replica relationships and slot coverage.
- **Shard colocation (Cluster Mode)**: Each Redis shard's master and replica(s) must live in one shard StatefulSet
  (`{name}-shard-K-*`); a cross-StatefulSet pairing fails verification (LR-020).
- **Partition Detection**: Identifies if nodes see different versions of the cluster topology.

---

## Multi-Site Clusters

### `derive-topology` — generate `spec.multiSite` from the mesh inventory

```bash
lrctl multisite derive-topology --from-mesh mesh.yaml \
    --sites-per-shard 2 --shards 4 --name cache -n prod > cache.yaml
```

Reads the **mesh inventory** (`mesh.yaml.sample`; `--from-mesh`, else `$E2E_CONFIG`, else
`$MS_CONFIG`) and emits an appliable `LittleRed`, or just the `spec.multiSite` block with
`--fragment`. The manifest goes to **stdout** and the report to **stderr**, so `> file`
yields something you can apply.

**Why generate it rather than write it.** A site's *position* in `spec.multiSite.sites` is
its placement index — shard K lives in sites `(K+j) mod S` — and the topology is **immutable
after create**. So a reordered or mistyped list is not an edit away from correct: it is a
delete and recreate, which with EmptyDir storage is the data. See LR-032/LR-033.

It derives only **facts about the lab** — which clusters exist, in what order, and which hub
Secret holds each one's kubeconfig (hub-local sites correctly get *no* `kubeconfigSecretRef`,
since that absence is what marks them hub-local). The **choices** are yours and are refused
rather than guessed: `--sites-per-shard` has no default, exactly as the CRD refuses to default
it, because it is the copy factor of an in-memory store and every candidate default is wrong
for somebody.

The report on stderr carries the two things a reader cannot get from the CR: where each shard
lands, and what the topology survives (ADR-012 D2's two thresholds). A topology that cannot
auto-survive an arbitrary single-site loss — which, given Redis Cluster's three-master
minimum, is exactly **two sites with an odd shard count** — is marked `[WARN]` and names the
`ReducedSiteLossResilience` condition the operator will raise for it.


For a **stretched (multi-site) cluster** (`spec.multiSite`, managed hub-and-spokes by one
operator — see `docs/MULTI_SITE_DESIGN.md`), the read verbs (`status`, `verify`, `inspect`,
`debug-dump`) are **site-aware automatically**: they detect `spec.multiSite` on the CR and
aggregate across **all** sites. Point `lrctl` at the **hub** (`--context <hub>`), where the
CR lives; it discovers the ordered site list and the instance namespace from the CR.

### Three ways to get ground truth (privilege tiers)

The pods of a stretched cluster live in the **sites**, not on the hub — so ground truth has
to come from the sites. `lrctl` offers three transports matched to the access you have, and
**announces the one in use** (`Transport: …`) in every command's output:

| Tier | Access needed | How | Selected by |
|------|---------------|-----|-------------|
| **1** | read-only **hub** (`get littlered`) | render the cross-site view the **operator publishes** to `status.multiSite` each reconcile | `status` always; `verify --from-status` |
| **2** | exec/run on the **hub** (`create jobs`, `pods/log`) | run the operator's own gather as a **throwaway hub Job**; it gathers every site and returns JSON over the K8s API control path | **default** for `verify`/`inspect`/`debug-dump` |
| **3** | **direct site** kube-contexts | gather each site from the workstation through your own per-site contexts, bypassing the operator | `--direct`, or any `--site-context` |

Why this shape: the operator already has working site access (it is its job); tier 2 borrows
it rather than duplicating site connectivity on the workstation, and — crucially — needs **no
direct site access and no per-site kube-contexts** on the workstation. The gather runs *on the
hub* and only the *result* travels back over the Kubernetes API (the Job's pod logs); the
hub↔site data path is never proxied. Tier 2 is a **Job**, not a `kubectl exec` into the
operator, so it is resource-isolated and works even when the operator pod is unhealthy or
crash-looping (a prime debug case).

Tier-2 discovery: the Job runs under the **operator's own ServiceAccount and image**,
discovered from the operator Deployment on the hub (default `littlered-system/littlered`,
override with `--operator-namespace` / `--operator-deployment`). It carries
`ttlSecondsAfterFinished` so the API server GCs it even if `lrctl` dies; `lrctl` also deletes
it proactively on the happy path.

**RBAC.** Tier 1 needs only `get littlered` on the hub. Tier 2 needs the **lrctl user** to
have `create` on `jobs` and `get` on `pods/log` in the operator namespace; the **Job** runs
as the operator SA and inherits its existing Secret + site access (no new operator-SA grant
is required — it does exactly what the operator already does each reconcile). Tier 3 needs a
kube-context per site with pod read in the instance namespace.

### The two-network wrinkle (tier 3: why you pass contexts, not the operator's Secrets)

The operator reaches sites over the **replication-net** endpoint embedded in its per-site
kubeconfig Secrets — an address reachable **only from hub pods**. A workstation cannot use
those Secrets. So the tier-3 (`--direct`) path gathers each site through **your own
kube-contexts** (the admin/site-net path). Site name → context resolution (tier 3 only):

- an explicit `--site-context <site>=<context>` mapping wins;
- otherwise the **convention** applies: a site is reached through a context of the **same
  name** (this convention is a tier-3 resolution rule, **not** a default gather mode);
- a site with neither is reported unresolved (and `verify` FAILs — the gather is incomplete).

`docs/MULTI_SITE_DESIGN.md §8` describes the **two distinct** network prerequisites the
operator needs: (a) hub-pod → spoke **pod-IP** data path (Redis dials) and (b) hub → site
**Kubernetes API**. They are different paths; the `preflight` verb checks each honestly.

### Site-aware verify (tier 3, `--direct`)

```text
Verifying Multi-Site Cluster: ms-smoke/ms-smoke (Mode: cluster, 3 sites)
Transport: gathered via direct site contexts
Gathering Cluster Ground Truth across sites (direct contexts)...

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
exit 0, `[FAIL]` non-zero). `--json` adds `tier`, `asOf`, `multiSite`, `sites[]`,
site-attributed `nodes[]`, `siteSpreadViolations`, and `unreachableSites`.

The **default** `verify` transport is now tier 2 (live via a hub Job): the same checks over
the same ground truth, but gathered on the hub and returned over the control path — so it
works from a **hub-only kubeconfig** with no `scm-s*` contexts. The output prepends
`Transport: gathered live via hub Job` and a `Running gather Job …` line; the Sites section
shows reachability + pod counts (no context column, since none is used). `verify --from-status`
(tier 1) renders the same verdict shape from the operator's published cross-site view, with an
`(as of last reconcile N ago)` freshness line and no gather at all.

### Site-aware status (tier 1)

`status` reads the operator's **aggregated** per-site observed state and **published
cross-site view** from the CR (`status.multiSite`) — a pure tier-1 render, no gather, hub-only
access:

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
Slots Assigned: 16384 / 16384
Cross-Site Placement: [OK] every shard spans >= 2 sites
  - shard 0: master@scm-s1, replicas@[scm-s2] (cross-site)
  - shard 1: master@scm-s3, replicas@[scm-s2] (cross-site)
  - shard 2: master@scm-s3, replicas@[scm-s1] (cross-site)
View freshness: as of last reconcile 3s ago (at 2026-08-07T…)
Site-Loss Takeover: [recovered] Recovered: No site-loss takeover needed.
Redis Nodes: 6/6 Ready
```

The `Slots Assigned` / `Cross-Site Placement` / per-shard / `View freshness` block is the
operator's published tier-1 surface (`status.multiSite.{clusterState,slotsAssigned,crossSiteOK,
shards,lastReconcileTime}`), computed each reconcile from the union ground truth.

`inspect` and `debug-dump` also follow the tier model. By default (tier 2) `inspect` renders
the site-attributed topology from the hub gather Job (for full per-pod raw `redis-cli` output,
use `--direct`), and `debug-dump` saves the operator's hub gather as `multisite-gather.json`;
`--direct` (tier 3) restores the full per-site collection — `inspect` execs the cluster
diagnostics in every site's pods, `debug-dump` writes per-site `site-<name>/` subdirectories
(pod logs, live cluster state, K8s resources) via each site's context — keeping the hub-only
CR and operator logs at the top level.

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

# Is a declared heavy operation stuck? (ADR-020 — neither Blocked nor Stalled
# ever resolves on its own, so this is the one that needs a human.)
lrctl verify store --json | jq -r '.[] | select(.operation.needsAction) |
  "\(.name): \(.operation.name) is \(.operation.reason) — \(.operation.message)"'
```

## Troubleshooting Failovers
If a failover seems stuck, run `lrctl verify`. It specifically looks for common failure modes:
1.  **Ghost Masters**: Sentinels pointing to an IP of a pod that was deleted.
2.  **Split Brain**: Different nodes disagreeing on the master.
3.  **Role Mismatch**: A pod labeled as `master` that is actually running as a `slave`.
