# Cross-Instance Sentinel Capture — Field Incident Analysis

**Status:** analysis complete; fix direction decided (§9), implementation in progress
(prospective ADR + LR-nnn)
**Mode:** sentinel
**Severity:** critical — this is a **safety** failure, not a liveness failure
**Observed:** 2026-08-17, a managed cloud, operator v0.2.1
**Evidence:** `lrctl debug-dump` of the affected instance; Sentinel behaviour verified against
`redis/redis@7.4 src/sentinel.c`

---

## 1. Summary

Two unrelated LittleRed sentinel-mode instances, running in the same Kubernetes namespace on a
shared pod network, **merged into a single Sentinel quorum**. The larger instance's Sentinel
configuration won on config-epoch and reassigned the smaller instance's master to a Redis pod
belonging to the *other* instance. The smaller instance's master was demoted, pointed at the
foreign master via `SLAVEOF`, and **flushed its dataset** on the first replication attempt.

The instance has been down and unrecoverable since. No operator healing rule fires — not in
v0.2.1, and not on `main` either.

The enabling condition is that `SentinelMasterName` is the package constant `"mymaster"`
(`internal/redis/client.go:56`) for every instance the operator manages, and the master *name*
is the only isolation boundary Sentinel's gossip protocol has.

**The most important finding is not the outage.** The two instances ran different Redis major
versions, so the cross-instance replication failed on an RDB format mismatch and the breakage
was loud. Had they run the same version, the merge would have completed silently: the affected
instance would have reported healthy, with a unanimous and self-consistent Sentinel consensus,
while serving the *other* instance's keyspace to its own clients. See §7.

---

## 2. Environment (sanitized)

| | |
|---|---|
| `instance-A` | the affected instance — `mode: sentinel`, Redis **7.4.3**, 3 Redis + 3 Sentinel pods, `auth.enabled: false` |
| `instance-B` | a second, unrelated `mode: sentinel` LittleRed instance in the **same namespace** — Redis **8.x** (inferred from RDB format v13), larger dataset (~46 GB of replication stream vs A's ~2 GB) |
| also present | two further `mode: cluster` LittleRed instances in the same namespace (not involved) |
| operator | one operator instance managing all of them, v0.2.1 |
| network | flat, routed pod network; all pods mutually reachable on 6379 and 26379 |

Pod addresses referenced below are ephemeral in-cluster pod IPs from the `10.233.0.0/16` pod CIDR.

| Address | Occupant |
|---|---|
| `10.233.74.53` | A `redis-0` — A's master before the incident |
| `10.233.67.102`, `10.233.72.39` | A `redis-1`, `redis-2` |
| `10.233.67.101`, `10.233.70.20`, `10.233.69.239` | A `sentinel-0/1/2`, current generation |
| `10.233.70.37` | A `sentinel-1`, **previous** generation — **later recycled to a B sentinel** |
| `10.233.71.144` | A `sentinel-2`, previous generation |
| `10.233.68.8` | B's master |
| `10.233.74.245`, `10.233.70.88`, `10.233.72.89` | B's replicas |

---

## 3. Sentinel mechanisms the incident turns on

All three verified against `redis/redis@7.4 src/sentinel.c`.

### 3.1 The master name is the entire isolation boundary

`sentinelProcessHelloMessage()` (sentinel.c:2838) begins:

```c
master = sentinelGetMasterByName(token[4]);
if (!master) goto cleanup; /* Unknown master, skip the message. */
```

`token[4]` is the master name carried in the hello payload. That lookup is the **only** check
before a hello message is treated as authoritative gossip about the receiver's own master.
There is no instance identifier, no namespace, no shared secret, and no authentication between
Sentinels beyond whatever `requirepass`/`auth-pass` guards the *Redis* instances themselves.

Two Sentinel deployments that use the same master name and can reach each other are, by
protocol design, **one deployment**.

### 3.2 The hello graph is wider than "sentinels meet through the master"

`sentinelSendPeriodicCommands()`:

```c
/* PUBLISH hello messages to all the three kinds of instances. */
if ((now - ri->last_pub_time) > sentinel_publish_period) sentinelSendHello(ri);
```

Three kinds: masters, replicas, **and other Sentinels**. A Sentinel accepts inbound `PUBLISH` on
its own port 26379 and routes it straight into the hello processor
(`sentinelPublishCommand`, sentinel.c:4504):

```c
if (strcmp(c->argv[1]->ptr, SENTINEL_HELLO_CHANNEL)) { addReplyError(...); return; }
sentinelProcessHelloMessage(c->argv[2]->ptr, sdslen(c->argv[2]->ptr));
```

Receiving is narrower: a Sentinel *subscribes* to `__sentinel__:hello` only on masters and
replicas, never on another Sentinel (`if ((ri->flags & (SRI_MASTER|SRI_SLAVE)) && link->pc == NULL)`,
sentinel.c:2432).

The graph is therefore asymmetric, and the consequence is the one that matters here:
**holding a peer's address is sufficient to introduce yourself to it.** A Sentinel connects to
every address in its known-sentinel list and hands over its hello unprompted. The recipient
learns the sender's address and, on its next tick, pushes its own hello back. One round and the
two are peers — *initiated by whichever side holds the stale address.*

### 3.3 A stale known-sentinel entry never expires

The only code paths that delete a known Sentinel are runid-conflict resolution inside hello
processing (`removeMatchingSentinelFromMaster`, called at sentinel.c:2864 and 2889) and an
explicit `SENTINEL RESET`. There is no time-based pruning and no "subjectively down for N
minutes, therefore forget". A dead Sentinel's address is retained and retried indefinitely.

This is the same structural property already documented in LR-024 for dead *replica* entries.

---

## 4. Timeline

Times are UTC, from `instance-A` `sentinel-0`'s container log. The instance's Sentinel
StatefulSet was being replaced pod by pod across this window.

### 04:26:39 — A `sentinel-0` starts fresh

```
[Startup] Starting Sentinel node with IP 10.233.67.101 (auth: no)
* Sentinel ID is cb40ff6f34acd2080be8d7a3ff3eeaa4350ae905
```

Storage is EmptyDir (pillar 3.1), so the pod boots with an empty config: `current_epoch 0`,
no monitored master, no known Sentinels.

### 04:26:45.074 — the operator bootstraps it, correctly

```
# +monitor master mymaster 10.233.74.53 6379 quorum 2
* +slave slave 10.233.72.39:6379  … @ mymaster 10.233.74.53 6379
* +slave slave 10.233.67.102:6379 … @ mymaster 10.233.74.53 6379
# +set master mymaster 10.233.74.53 6379 down-after-milliseconds 30000
```

`SENTINEL MONITOR mymaster 10.233.74.53 6379 2` — A's own `redis-0`. The operator's behaviour is
correct throughout this window; nothing it does contributes to the failure. Sentinel now opens a
pub/sub subscription to `__sentinel__:hello` on `10.233.74.53:6379` and discovers the two
replicas from the master's `INFO`.

### 04:26:45.525 — it learns the previous generation of its own peers

```
* +sentinel sentinel 5362efc2… 10.233.70.37 26379 @ mymaster 10.233.74.53 6379
* +sentinel sentinel d1a3514a… 10.233.71.144 26379 @ mymaster 10.233.74.53 6379
```

451 ms after `MONITOR`. Read the timing: `sentinel-0`'s known-sentinel list was empty at
04:26:45.074, so it had nobody to publish to. These hellos can only have arrived **passively, on
`redis-0`'s hello channel** — meaning both senders were already monitoring `redis-0`.

Both are A's own previous-generation Sentinel pods, still running while the StatefulSet is
replaced. The 22 minutes of quiet that follow prove it: their hellos carried
`master_name=mymaster` but never a higher `master_config_epoch`, so no configuration update
fired. Had either been a B Sentinel advertising `10.233.68.8` at epoch 3, the capture would have
happened here rather than at 04:48:49.

The `@ mymaster 10.233.74.53 6379` suffix is `sentinelEvent()`'s `%@` formatter printing the
**receiver's** master context. It is not a claim by the sender.

This event is entirely normal — and it is where the weapon is planted. `sentinel-0` has now
persisted `known-sentinel mymaster 10.233.70.37 26379 5362efc2…`, and per §3.3 it will keep that
address forever.

### 04:26:45.531 — epoch recovery

```
# +new-epoch 2
```

From the same hello:

```c
if (current_epoch > sentinel.current_epoch) {
    sentinel.current_epoch = current_epoch; ...
    sentinelEvent(LL_WARNING,"+new-epoch", master, "%llu", ...);
}
```

The freshly booted pod was at epoch 0; its surviving peers had lived through two elections and
were at 2. This is ordinary state recovery from peers and is not itself a fault. It matters only
as a baseline: anything advertising epoch ≥ 3 now outranks A.

### 04:40:17 → 04:47:53 — the hinge: an address changes hands under a live trust relationship

```
04:40:17 # +sdown sentinel 5362efc2… 10.233.70.37 26379 @ mymaster 10.233.74.53 6379
04:47:53 # -sdown sentinel 5362efc2… 10.233.70.37 26379 @ mymaster 10.233.74.53 6379
```

At 04:40:17 the previous-generation `sentinel-1` pod stops answering PING — it is being deleted.
`sentinel-0` marks it subjectively down **and keeps the entry** (§3.3).

At 04:47:53 the address answers PING again, so `sentinel-0` clears `s_down` and resumes full
contact — including publishing its hello there every 2 s. But a PING reply carries no runid.
Sentinel cannot distinguish *"my peer recovered"* from *"a different process now holds that
address"*, and does not treat the distinction as significant.

**This is where a foreign process silently inherits A's trust, and no event marks it.** It is
pillar 3.7's IP-only identity assumption (ADR-001) failing at the Sentinel layer rather than the
Redis layer: the pillar reasoned about *our own* pods returning on new IPs, not about *other
tenants'* pods arriving on ours.

A's replacement `sentinel-1` comes up 14 seconds later on a different address, `10.233.70.20`.
The CNI had already handed `10.233.70.37` to a Sentinel pod of `instance-B`.

### 04:48:49.030–.035 — capture: one hello message, five events

```
04:48:49.030 * +sentinel-invalid-addr sentinel 5362efc2… 10.233.70.37 26379 @ mymaster 10.233.74.53 6379
04:48:49.030 * +sentinel              sentinel fe839ba1… 10.233.70.37 26379 @ mymaster 10.233.74.53 6379
04:48:49.033 * Sentinel new configuration saved on disk
04:48:49.035 * Sentinel new configuration saved on disk
04:48:49.035 # +new-epoch 3
04:48:49.035 # +config-update-from   sentinel fe839ba1… 10.233.70.37 26379 @ mymaster 10.233.74.53 6379
04:48:49.035 # +switch-master mymaster 10.233.74.53 6379 10.233.68.8 6379
```

These are **not five independent events**. They are one hello message processed by one call to
`sentinelProcessHelloMessage()`, emitting five events in source order within 5 ms:

| Source (sentinel.c:2838 ff.) | Emitted |
|---|---|
| `getSentinelRedisInstanceByAddrAndRunID(…, token[2])` → NULL | unknown runid at a known address |
| `other = getSentinelRedisInstanceByAddrAndRunID(…, NULL)` → found | `+sentinel-invalid-addr` on `5362efc2`, then purge it **across all masters** |
| `createSentinelRedisInstance(token[2], SRI_SENTINEL, …)` | `+sentinel fe839ba1` |
| `current_epoch (3) > sentinel.current_epoch (2)` | `+new-epoch 3` |
| `master->config_epoch (2) < master_config_epoch (3)`, address differs | `+config-update-from`, `+switch-master`, then `sentinelResetMasterAndChangeAddress(master, "10.233.68.8", 6379)` |

`+sentinel-invalid-addr` means exactly: *"a hello arrived from runid `fe839ba1` at
`10.233.70.37`, but I have a different runid recorded there; the old record must be stale, so
purge it and register the new occupant."* It is Sentinel's built-in IP-recycling handler, and its
resolution is to **trust the new occupant** — in Sentinel's model an address is a peer, and peers
are honest.

**Delivery direction is the part that matters for the fix.** A's `sentinel-0` initiated. It held
the stale `10.233.70.37` entry, so *it* connected to port 26379 there and published its own
hello (§3.2). The B Sentinel on the other end read `master_name=mymaster`, found that name in its
own dictionary (§3.1), and registered A's `sentinel-0` as a peer of *B's* `mymaster`. On its next
tick it pushed its hello back — and that reply is the message above.

**instance-A introduced itself to instance-B and then accepted instance-B's configuration.**

`sentinelResetMasterAndChangeAddress()` then discards A's replica list and rebuilds it around
`10.233.68.8`, which is why B's replicas appear in A's `SENTINEL master` output one second later.

### 04:48:59.108 — A's master is demoted

```
* +convert-to-slave slave 10.233.74.53:6379 10.233.74.53 6379 @ mymaster 10.233.68.8 6379
```

The routine reconfiguration pass sees `10.233.74.53` reporting `role:master` while the current
configuration names `10.233.68.8`, and issues `SLAVEOF 10.233.68.8 6379`. From Sentinel's
perspective this is a correction: an instance that disagrees with the winning config epoch is by
definition wrong.

### 04:48:59 onward — the dataset is flushed

`instance-A` `redis-0`, repeating ~182 times over the sampled log window and continuously since:

```
* MASTER <-> REPLICA sync: Flushing old data
* MASTER <-> REPLICA sync: Loading DB in memory
# Can't handle RDB format version 13
# Failed trying to load the MASTER synchronization DB from socket, check server logs.
* Reconnecting to MASTER 10.233.68.8:6379 after failure
```

The flush precedes the load. The **first** flush, at ~04:48:59, is where A's data went. All three
of A's pods now report `slave_repl_offset:1` and hold nothing.

RDB format v13 is Redis 8; A is pinned to 7.4.3 and cannot parse it, so every sync attempt fails
and retries forever.

---

## 5. End state

`instance-A`, at the time of the dump (~13.5 h after capture):

- All 3 Redis pods `1/2 Running`, `role:slave`, `master_host:10.233.68.8`,
  `master_link_status:down`, empty. Not Ready, because the readiness probe requires `link:up`.
- All 3 Sentinels Ready and monitoring `mymaster` at `10.233.68.8` with **`flags: master`,
  `last-ok-ping-reply: 100 ms`** — the foreign master is perfectly healthy from their vantage.
  It is not `s_down`, so no failover will ever be attempted.
- `SENTINEL master` reports `num-slaves 6` (A's 3 + B's 3) and `num-other-sentinels 8`.
- CR: `phase: Initializing`, `master: {}`, `Ready=False (PodsNotReady)`,
  `"Redis: 0/3, Sentinels: 3/3, Sentinel-known replicas: 0/2"`.

---

## 6. Why no operator rule recovers this

`DetermineRealMaster` behaves correctly and, as designed, yields `RealMasterIP == ""`: a majority
of reachable Sentinels agree on `10.233.68.8`, which is not one of the instance's pod IPs, so the
Redis-only fallback is deliberately suppressed (LR-004). Every consensus-master-gated rule then
short-circuits.

| Rule | Gate | Outcome here |
|---|---|---|
| Rule 0 (re-register bare sentinel) | `RealMasterIP != ""` | no-op |
| Rule R / LR-009 (replica rescue) | `RealMasterIP != ""` | no-op |
| LR-005 (divergent living master) | `RealMasterIP != ""` | no-op |
| LR-008 (ghost-master REMOVE+MONITOR) | `RealMasterIP != ""` | no-op |
| Rule L / LR-015 (leaderless deadlock) | `AllSentinelsBare` | false — they monitor a master |
| LR-024 (`planGhostMasterRecovery`) | `SentinelsMonitorGhostMaster && !HasHealthyKnownReplica` | first half **true**, second half **vetoes** |

LR-024 is one predicate away from firing. `SentinelsMonitorGhostMaster()` is satisfied — the
monitored master is not in `ValidIPs`. But `HasHealthyKnownReplica()` returns true, because
Sentinel's replica list still contains A's own three pods as non-ghost, non-`s_down` entries, and
that veto exists to avoid stealing a failover Sentinel is about to perform on its own.

**The veto's premise is false in this state.** It assumes "a promotable replica exists ⇒ Sentinel
is about to act". Here Sentinel will never act, because from its own vantage the master is
healthy. There is nothing to fail over from.

Neither of the two recovery rules existed in the deployed v0.2.1 (`recoverLeaderlessDeadlock` and
`ghost_master_recovery.go` are both absent at that tag), but this changes nothing: `main` does not
recover it either. The operator version is a footnote, not a mitigation.

---

## 7. Impact

This ranks above every prior entry in `RECONCILIATION_ALGORITHM_CHANGELOG.md`, and the reason is
categorical rather than a matter of degree.

Every LR-nnn to date is a **liveness** failure — something deadlocks, stalls, refuses, or
converges too slowly — and the invariant that survives is *we never serve the wrong data*.
LR-016 and LR-025 came closest to a safety failure and were caught before shipping. This one
breaks safety outright:

1. **Cross-tenant integrity.** Two unrelated instances merge into one replication topology; one
   instance's master is demoted and re-pointed at the other's.

2. **The loud failure was luck.** The RDB version mismatch is the only thing separating this
   outage from a *silent* substitution. Pin both instances to the same Redis tag and the
   replication link comes up: `instance-A` reports healthy, `master_link_status:up`, a unanimous
   Sentinel consensus, no alarms — and serves `instance-B`'s keyspace to its own clients. The
   affected instance's own data is gone either way; the flush happens before the load.

3. **No independent detector exists.** The operator, the CR status, and `lrctl verify` all read
   the topology *through* Sentinel's consensus — precisely the thing that was captured. There is
   no external witness, so all three would report a consistent, healthy, wrong answer.

4. **Blast radius is the pod network, not the namespace.** Any two sentinel-mode LittleRed
   instances that can reach each other's port 26379 are exposed. We do not control that boundary.

5. **The trigger is routine.** A Sentinel pod replacement, a recycled pod IP, and a neighbouring
   instance. That is a normal rollout on a busy shared cluster. It has plausibly occurred before
   and merely lost the config-epoch race, in which case it would have left no trace.

Contributing conditions worth stating plainly, because each is individually addressable:

- `SentinelMasterName` is a package constant shared by every managed instance (§3.1).
- `auth.enabled: false` on the affected instance. Authentication would have blocked the capture
  independently: without the password, neither side could read the other's hello channel or
  accept its `PUBLISH`. **This is not a recommended mitigation on its own** — it makes the
  isolation depend on a user-facing optional setting — but it explains why not every co-located
  pair has been hit.
- EmptyDir Sentinel storage means every Sentinel restart re-learns its peer set from gossip,
  widening the window in which a stale address can be introduced.

### Secondary finding, unrelated to the incident

The affected CR sets `config.maxmemory: 1500mb` against `resources.limits.memory: 500M`. An
explicit `maxmemory` bypasses `CalculateMaxmemory` (90 % of limit) with no sanity check, so
`allkeys-lru` never engages and the container is OOMKilled first. Worth a validation rule.

---

## 8. Cross-mode audit (development rule §7.11)

- **Cluster mode** — narrower, but needs checking rather than assuming. There is no shared-name
  gossip channel, so no direct analogue of the fusion. However the operator issues `CLUSTER MEET`
  against pod IPs sourced from the informer cache, and LR-012 established that this cache can
  return a **stale `Status.PodIP`**. A stale IP now held by a foreign cluster-mode pod would be
  MEETed onto our cluster bus. Same root cause (IP identity + recycling), smaller window. Two
  cluster-mode instances were co-located in the same namespace in this environment.
- **Standalone** — no peer protocol, no exposure.
- **Failover mode (ADR-011)** — structurally immune. Role intent is stamped by the operator into
  pod annotations and consumed through a downward-API volume; there is no peer-to-peer topology
  protocol available to capture, and the operator is the sole authority. This is a genuine
  argument in the mode's favour that ADR-011 does not currently make, and it belongs in the
  graduation-gate discussion.

---

## 9. Fix direction

**Prevention only.** Capture → `+convert-to-slave` was 10 s, and the flush follows the `SLAVEOF`
within about a second. Any operator-side detector runs on a reconcile cadence behind a cooldown
and is two orders of magnitude too slow. **Only prevention preserves data** — and automated
recovery turns out to be not merely late but unwinnable (§9.2).

### 9.1 Prevention — an instance-unique Sentinel master name

Verified complete for the observed mechanism: with distinct names, both directions of
introduction die at `sentinelGetMasterByName` (§3.1). Their hello reaches us and is discarded;
ours reaches them and is discarded, so they never learn us and never push a configuration back.
The `is-master-down-by-addr` vote path cannot substitute, because it requires the asker to
already hold our address as a peer.

`spec.sentinel.masterName`, suggested value `<namespace>.<name>` — unambiguous (a namespace is a
DNS-1123 *label* and cannot contain a dot), free of the comma and whitespace that would break the
hello payload and the sentinel config file, human-readable, and derivable by a chart at render
time, which matters because Sentinel-aware clients must carry the string.

**Decided (working session, 2026-08-19), and implemented: a required field with no default; the
webhook is deferred and may never be built.** See ADR-015 for the full decision record.

**The floor — required, no default, no leniency.** `spec.sentinel.masterName` is required, with
no default, static or derived, no acceptance of an absent field on upgrade, and no special-casing
of `mymaster`. **Fail loud, fail hard, fail early**, on the grounds that under GitOps and
automation a hard failure is increasingly the only kind that gets noticed at all. Paired with
**explicit and loud documentation** telling people to choose something other than `mymaster`,
since validation can force the decision to be *visible* but never *correct* (the invariant is a
property of the pod network, not of the object).

> **Implemented as a plain `required` marker, not a CEL rule** — the reverse of what this section
> originally proposed. §11 measured why: a spec-level CEL rule rejects the *operator's own* status
> writes below Kubernetes 1.33, silently stopping reconciliation of every existing instance. Plain
> `required` never wedges anything on any version. The loudness for the installed base is carried
> by the runtime `SentinelMasterNameUnscoped` condition instead.

**Deferred: a mutating admission webhook** stamping `<namespace>.<name>` on CREATE when the field
is absent. It was the "help people do the right thing" option and remains available, but it is
ergonomics on top of the floor, not a substitute for it, and the cost is real (§11.2).

If it is ever built, two constraints hold. It **must not replace the CEL rule**: a mutating
webhook is skippable — `failurePolicy: Ignore` during an outage, an expired cert, a stale CA
bundle, a `MutatingWebhookConfiguration` that was never applied — and a skipped *defaulting*
webhook silently reproduces the exact hazard this fix exists to remove, which is the worst
outcome in the whole design space. Keeping the requirement underneath means a skipped webhook
degrades to a loud rejection rather than a silent wrong value; mutating admission runs before
schema/CEL validation, so a stamped value satisfies the rule. And it must stamp on **CREATE
only** (see below).

**`mymaster` stays a legal explicit value.** Two independent reasons: the pre-migration state
must be expressible or no data-preserving migration exists at all (§9.3), and the value is
plausibly hardcoded in client applications — it is the value in every upstream tutorial and in
other operators' documented client contracts, so a legacy application may not even expose it as a
parameter. An instance that sets it explicitly gets the `SentinelMasterNameUnscoped` warning
condition and keeps running.

**Stamp on CREATE only.** A webhook that also filled the field on UPDATE would silently rename an
existing instance's master on its next write, breaking every Sentinel-aware client with no user
action to correlate the outage to.

### 9.2 Recovery for an already-captured instance — DECLINED

An earlier draft of this section proposed one: split LR-024's `HasHealthyKnownReplica` veto on
whether the ghost master is flagged down (`s_down`/`o_down` → LR-024's dead ghost, veto correct;
flags clean → captured, veto's premise false), then elect a survivor and re-`MONITOR`. The
predicate is sound and would be admissible — "the monitored master is a live address that is not
one of my pods" is a *configuration* judgement from the Kubernetes pod list, not failure
detection.

**It was rejected anyway, on two grounds that do not depend on the deployment being
misconfigured.**

**Nothing survives to be salvaged.** The flush happens when the replica starts its full sync,
about a second after the `SLAVEOF` — long before any reconcile could observe the capture. All
three pods in the incident ended at `slave_repl_offset:1`. So a recovery restores an *empty*
instance, which is precisely what deleting and recreating the CR already achieves at no
engineering cost. The one path where data might survive — replication blocked before the sync
starts — is unreachable: the capture itself requires the gossip to be accepted, which requires a
shared or absent password, which is the same condition under which the replication auth also
succeeds. The RDB version mismatch seen in the incident does not help, because it fails *after*
the flush.

**The operator structurally cannot win the reclaim.** `createSentinelRedisInstance` initialises
`ri->config_epoch = 0` (sentinel.c:1304), so a `SENTINEL MONITOR` issued by the operator creates
the master entry at **epoch 0**. Against a merged population still holding the captor's epoch that
loses on the very next hello, roughly two seconds later — and the operator has no way to raise a
config epoch, since only a genuine failover election does that. The rule would therefore reissue
`REMOVE` + `MONITOR` every reconcile forever, never converging. Worse, each `REMOVE` + `MONITOR`
wipes that sentinel's replica list, which is **LR-013's hazard exactly** — the mechanism that
produced a permanent `no-good-slave` failover deadlock. The recovery's failure mode is to turn a
broken-but-static instance into one thrashing its own topology, while two instances sharing a name
ping-pong it between them.

A rule that cannot win, living in the rule chain that already produced LR-001/007/011/013/024, is
a liability rather than defence in depth. What it would have insured against — some future path to
"Sentinels monitor a live non-pod master" that is not a name collision — has no known instance;
the address-adoption path (§9.4) does not produce it, because there our Sentinels still monitor
our own master correctly.

**Instead, make the state fast to identify and documented to fix by hand:**

- **`lrctl verify` diagnostic** (implemented): reports the effective master name, the
  Sentinel-known sentinel and replica counts against what we deployed, and any Sentinel-known
  address that is not one of our pods. `num-other-sentinels: 8` on a three-sentinel instance was
  the loudest signal in this dump and nothing surfaces it today. A tool run when someone is
  already suspicious makes no safety claim by being silent, so it carries none of the
  false-assurance problem that sank the controller-side collision check (§10).
- **A runbook** in the user documentation: scale the operator down, `SENTINEL REMOVE` +
  `MONITOR <intended master>` on each sentinel, `REPLICAOF NO ONE` on that master, scale back up
  — stating plainly that the instance returns **empty**, which is the part a reader needs told
  rather than discovered.

Detection is not the gap: a captured instance sits at `Ready=False` / `Initializing`, so ordinary
Kubernetes alerting already catches it. It is stuck *loudly* until a human acts.

### 9.3 Migration — data-preserving, not seamless

**There is no rolling cutover.** Monitoring one master under two names is possible (Sentinel's
duplicate check is name-keyed — `if (dictFind(table, sdsname)) { errno = EBUSY; return NULL; }` —
address is not considered), but the two entries are independent failover state machines on one
address: on a master death both reach `o_down`, both elect, and they can promote **different
replicas**. Neutering one with a large `down-after` is worse, since it then stops following
failovers and serves clients a permanently stale master.

So the master-name change is **client-breaking and requires a coordinated cutover** — but only
for **Sentinel-aware** clients. Clients using the label-routed `{name}` Service, which exists
precisely so a Sentinel-aware client is not required, see nothing. Establishing which client
style each application uses is step one of any migration plan.

The migration's value is therefore *"you take a client-reconfiguration outage and your data
survives it"* — as against deleting and recreating the CR, which is total loss. It must be
**user-triggered** (mutating the field is the trigger), never automatic on operator upgrade,
which would break every Sentinel-aware client in a fleet with no correlating user action.

Field guidance: **enable auth in the same cutover.** Auth is equally client-breaking on its own,
so doing them separately costs two outages; together it is one, and auth additionally closes the
residual in §9.4 and protects against anything else sharing the network.

### 9.4 What a unique name does *not* close

If instance-B's master pod dies and its IP is recycled onto instance-A's master, B's Sentinels —
still holding that address as *their* master — will monitor A's master directly, read its `INFO`,
adopt A's replicas, and can `SLAVEOF` them. No hello, no name check, purely address-driven.
Narrower than the observed path (it needs the recycled IP to land on a master specifically) and
less damaging (no epoch war, no demotion of a live master), but real. Only authentication closes
it.

Related, and the reason a stable name is nonetheless sufficient: **severity scales with whether
the capturing party has a live master.** Delete-and-recreate of the *same* instance can also
self-capture — the terminating generation holds a higher epoch and repoints the fresh one — but
it names a *dead* address, the `SLAVEOF` never completes a sync, and the flush therefore never
runs. That degrades to LR-024's ghost-master deadlock: a liveness failure we already detect and
recover, not data loss. A random or UID-derived name would close it, at the cost of not being
derivable by a chart at render time — disqualifying, given clients must carry the string.

---

## 10. Rejected alternatives

**A controller-side collision check** ("two managed sentinel-mode instances with the same
effective `masterName`"). Rejected on two grounds. Its coverage story is misleading — it can
never see a Sentinel deployment we do not manage, so silence reads as an all-clear it cannot
give. And its *unique* value is thin: with the name required and `<ns>.<name>` documented,
same-operator collisions largely stop happening by construction, leaving only deliberate
identical values, most of which the `mymaster` warning already catches. The capability belongs in
`lrctl verify` as **diagnosis** instead — reporting the effective master name, the count of
sentinels and replicas Sentinel knows versus the count we deployed, and any Sentinel-known
address that is not one of our pods. A tool run when someone is already suspicious makes no
safety claim by being silent. `num-other-sentinels: 8` on a three-sentinel instance was the
loudest signal in this dump and nothing surfaces it today.

**Rejecting the literal value `mymaster` at admission.** It would defeat the migration: every
existing instance's current value *is* `mymaster`, so if it is inexpressible the only upgrade
path becomes delete-and-recreate — the total data loss the migration exists to avoid. It is also
a folk rule rather than the invariant: `mymaster` is not uniquely dangerous, only *popularly* so,
and two instances both named `redis` or `cache` collide identically while passing validation.

**An `allowBrokenDefaultMasterName` escape hatch.** It is a different animal from
`allowUnsafeRebootstrapOnDeadlock` / `allowUnsafeSiteTakeover`, which authorise an *operator
action* whose safety the operator cannot establish, at the moment of the action. This would
disable a validation, and the knowledge being asserted — "nothing else shares my pod network" —
is usually unknowable and **becomes false without the asserting party doing anything**. A
standing risk acceptance that expires on a third party's deploy is the worst kind. Unnecessary
anyway: the value being legal *is* the escape hatch.

**A Go-side derived default in `SetDefaults()`.** LR-033's ruling applies — "a *static* default is
applied by the API server at create time and is harmless; a *derived* default cannot be, and
re-imports all four problems." Two of the four survive here even though the inputs
(`metadata.namespace`, `metadata.name`) are immutable, because they concern where the derivation
*lives*, not what it reads: the stored spec diverges from the effective value (worse here than in
LR-033 — the effective value is a client-config string, so a reader of the CR cannot configure an
application), and a change to the derivation between operator versions would silently rename
every unset instance on upgrade. And the write-back is not hypothetical: `littlered_controller.go:180`
calls `SetDefaults()` on the fetched object and `:195` `r.Update()`s it fifteen lines later, so
anything added there is persisted into the user's spec on first reconcile. A mutating webhook
(§9.1) is the clean way to get the same outcome, because it makes the value static from creation.

---

## 11. Measured: how a newly-required field behaves on pre-existing objects

Throwaway envtest probe (same technique LR-033 used for its in-place CRD upgrade), run against
**six real `kube-apiserver` versions**. Method: install a lax CRD, create an object omitting the
field, tighten the CRD in place, then attempt the four writes that matter. The CRD tighten itself
was accepted in every case, so an in-place CRD upgrade is never the problem.

The governing mechanism is **CRD validation ratcheting**: the API server skips validation at
schema locations whose value is *unchanged* between old and new object. Its rollout differs for
plain `required` and for CEL, and that difference decides everything.

**A. Spec-level CEL rule** (a rule on the `spec` object, e.g. requiring the sentinel master name):

| kube-apiserver | operator `/status` write | user edits spec | finalizer / metadata write | CREATE without the field |
|---|---|---|---|---|
| 1.29 – 1.32 | **REJECTED** | REJECTED | 1.29 rejected, ≥1.30 accepted | REJECTED |
| **≥ 1.33** | **ACCEPTED** (with an API warning) | **REJECTED** | ACCEPTED | REJECTED |

**B. Plain `required` nested inside the optional `spec.sentinel` object** — the natural modelling:

| kube-apiserver | operator `/status` write | user edits spec (sentinel block untouched) | finalizer / metadata write | CREATE without the field |
|---|---|---|---|---|
| 1.29 | ACCEPTED | REJECTED | **REJECTED** | REJECTED |
| **≥ 1.30** | ACCEPTED | **ACCEPTED — silently** | ACCEPTED | REJECTED |

**Three conclusions, none of them the assumed one.**

1. **The operator never wedges under plain `required`** — status writes are accepted as far back
   as 1.29, because `Status().Update()` is a subresource write. The pessimistic assumption that a
   required field would stop reconciliation of every existing instance is **wrong for `required`**
   — but **right for CEL below 1.33**, where the status write is rejected outright.
2. **Nested `required` does not deliver "fail loud on upgrade" on any current cluster.** On ≥1.30,
   ratcheting excuses the violation because `spec.sentinel` itself is unchanged, so a `helm
   upgrade` that touches anything *other* than the sentinel block is accepted and the instance
   keeps running on `mymaster` with nothing said. It forces the decision on **new** instances only.
3. **A spec-level CEL rule delivers exactly the stated intent — on Kubernetes ≥ 1.33 only.** The
   operator keeps reconciling (status accepted, and the API warning surfaces the violation in the
   operator log for free), the user's next CR edit fails loudly, and creates are rejected. Below
   1.33 the same rule stops the operator from writing status at all.

**Therefore the fail-loud stance requires declaring a minimum Kubernetes version of 1.33.** The
repo builds against `k8s.io/api v0.36.3` and currently states no minimum anywhere. 1.33 shipped in
early 2025, so the floor is conservative — but managed clouds lag, and **the incident
environment's Kubernetes version is not recorded in the dump and must be checked** before shipping
a rule that would wedge it.

One residual with the CEL shape: the rule must be written at spec level (`self.mode != 'sentinel'
|| has(self.sentinel.masterName)`) rather than as `required` inside `SentinelSpec`, both to get
the ratcheting behaviour above and because `spec.sentinel` may legitimately be omitted entirely —
in which case a nested `required` is never evaluated and the field is silently absent. The runtime
`SentinelMasterNameUnscoped` warning condition remains the backstop for anything validation
misses.

---

## 12. Open questions

1. ~~**Does a newly-required field block the operator's own `/status` writes on existing
   objects?**~~ **ANSWERED — measured, see §11.** The answer decides the shape of the fix and
   is not what either candidate assumed.
2. **Webhook cost and coupling.** No webhook exists: `config/webhook`, `config/certmanager` and
   the `PROJECT` entry are all absent, and only kubebuilder's stock server boilerplate is in
   `main.go`. Needs a Service, a `MutatingWebhookConfiguration`, CA-bundle injection and cert
   rotation, chart wiring, and a `failurePolicy` decision. Interacts with the namespace-scoped
   operator work (ADR-014/WS1), since a webhook is cluster-scoped infrastructure, and with the
   multi-site hub deploy wiring.
3. **Where responsibility sits — explicitly unresolved.** One reading: Sentinel's isolation model
   is upstream's design and the network trust model is the platform's, so LittleRed is a
   vicarious agent that documents the hazard. The other: LittleRed *selected* `mymaster` on the
   user's behalf — a hand-rolled deployment has an admin who typed that line and could have typed
   another; our users never see it — and pillars 3.1/3.7 (no persistence, IP-only identity,
   gossip-relearned peer sets on every restart) deliberately widen the window the protocol is
   weakest at. The disagreement is not academic: it decides whether `mymaster` remains a
   supported fallback indefinitely or is a defect carried for compatibility with an intent to
   stop shipping it, and whether the documentation reads as advice or as disclosure of a known
   defect with an upgrade obligation.
4. **Cluster-mode sibling** (development rule §7.11). No shared-name gossip channel, so no direct
   analogue — but `CLUSTER MEET` is issued against pod IPs from the informer cache, and LR-012
   established that cache can return a stale `Status.PodIP`. A stale IP now held by a foreign
   cluster-mode pod would be MEETed onto our bus. Needs checking, not assuming.

---

## 13. Test plan

Everything below is **red-first** (development rule §7, Test Discipline): each check must be
observed to fail for the defect's actual reason before the fix lands.

**The headline e2e, and the one that proves the rest have teeth.** Stand up two sentinel-mode
instances in one namespace and assert that instance A's Sentinels only ever know A's own three
sentinels and A's own pods, and that A's monitored master never becomes an address outside A's
pod set. **Red against current code** — that is the incident. Green once names differ by default.
The IP recycling is only the *introduction*, not the mechanism, so no recycled address is needed
to reproduce; deploying two instances that share a name is sufficient.

Further coverage:

- Webhook stamps `<ns>.<name>` on CREATE when the field is absent; does **not** stamp on UPDATE.
- With the webhook disabled or failing, CREATE without the field is **rejected**, not silently
  defaulted (the §9.1 composition — this is the check that makes the webhook safe to rely on).
- `masterName: mymaster` set explicitly is accepted and raises `SentinelMasterNameUnscoped`.
- Migration: change `masterName` on a live, healthy instance → all Sentinels converge on the new
  name → **data intact** across the flip.
- ~~Recovery from a capture~~ — no longer applicable: automated recovery is declined (§9.2), so
  there is nothing to assert. LR-024's veto is untouched, which is itself the point: no test is
  needed for a guard that did not change.
