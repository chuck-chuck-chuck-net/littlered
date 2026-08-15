# Multi-Site E2E Test Matrix (ADR-012)

> **Status:** Plan — not yet implemented. Written 2026-08-11 against `feat/multi-site` @ `3b095f2`.
> Both findings this plan surfaced have since been fixed, and the matrix is updated accordingly:
> §11.1 settled 2026-08-13 (**LR-031**, ADR-012 D2/D3 rewritten) unblocking **A8**, **G4**, **G5**
> and the `ReducedSiteLossResilience` state of the 2-site fixtures; §11.2 settled 2026-08-14
> (**LR-032/LR-033**, topology immutability) collapsing five **MS-F** cells into one and adding the
> negative-fixture rule in §2.1. `spec.multiSite.sitesPerShard` is now **required** — every fixture
> must set it. **C15 settled 2026-08-14** (§11.5, derivation only — no code change): it splits into
> an inert C15a and a drift-conditional C15b, and yields a second, independent argument for the D2
> balance floor. **No contested expectations remain.**
> **Purpose:** turn "we have 2 multi-site e2e tests" into a systematic, tiered torture programme
> that exercises every failure mode the quorum-arithmetic design makes a claim about — and that
> is *shown to have teeth* rather than assumed to.
> **Companion docs:** ADR-012 (the claims), `MULTI_SITE_DESIGN.md` §6 (failure model),
> `E2E_TESTING.md` (how to run), `E2E_SOAK_HARNESS_DESIGN.md` (repeat-run harness).

---

## 1. What is actually under test

Multi-site is not "cluster mode with more YAML". It ships a small number of **falsifiable
claims**, and the matrix exists to attack each one. Everything below is derived from these:

| # | Claim (ADR-012) | How it can be falsified |
|---|---|---|
| **C1** | A takeover fires **iff** the slot-serving masters split exactly in half (`reachable = unreachable = K/2`). | A cell where it fires on an uneven split, or fails to fire on an even one. |
| **C2** | A takeover is **safe** because the unreachable minority has self-fenced (`cluster_state:fail`) within `cluster-node-timeout`. | Any observed interval where two nodes serve writes for the same slot. |
| **C3** | Safety rests on **Domain-M arithmetic only** — never on the hub observing whether a site is alive. | A cell where a hub-side observation (informer cache, dial reachability, latency) changes the verdict. |
| **C4** | The survivors'-vantage rule: the trigger is a survivor's **own** `cluster_state:fail`, not the hub's dial (LR-017). | A hub-blind cut where the operator takes over anyway. |
| **C5** | Drift removes survivability, so the rebalancer maintains the two balance thresholds — the takeover floor always, the native target where the topology reaches it. | A drift state the rebalancer fails to correct, or thrash on a healthy cluster. |
| **C6** | An unreachable site is **unmanaged, not broken** (pillar 3.2) — Redis keeps serving, management resumes on reconnect. | A cell where losing hub↔site management causes a data-plane action or data loss. |
| **C7** | The operator **never deletes data** — shrink/scale-down/topology change refuses instead. | Any cell where an orphaned per-site shard STS is silently removed. |
| **C8** | `recoverSiteLoss` is the **sole** quorum-loss takeover and ghost-FORGET authority in multi-site. | Generic `repairCluster` firing a takeover or an unsafe FORGET. |

Everything in §5 is one of these claims pointed at a specific fault.

### 1.1 The planner verdict vocabulary (the primary assertion target)

`planSiteTakeover` emits exactly eight verdicts. **Every disruptive cell asserts a verdict**,
not just an outcome — an outcome can be right by luck, a verdict cannot:

`None` · `WaitNative` · `Hold` · `Arm` · `Wait` · `Refuse` · `Takeover` · `TakeoverUnsafe`

Verdicts are read from the operator's audit log (`msOperatorLogSince`), corroborated by
`status.multiSite.takeoverPendingSince`, the `MultiSiteTakeover` condition, and the
`SiteTakeoverExecuted` / `RefusedSiteTakeover` events.

### 1.2 The arithmetic, in one place

For `K` slot-serving masters, Redis's unconditional minority check fail-stops a node below
`q = ⌊K/2⌋ + 1`. Writing `mᵢ` for the masters in site `i` and `L` for the lost site:

- **native survival**  ⟺ `K − m_L ≥ q`  ⟺ `m_L ≤ ⌈K/2⌉ − 1`
- **fence-safe takeover** ⟺ `K − m_L < q` **and** `m_L < q`  ⟺ `m_L = K/2` (K even)
- **refusal**  ⟺ `m_L ≥ q`

⇒ **auto-survival of losing site `i` (by either path) ⟺ `mᵢ < q` ⟺ `mᵢ ≤ ⌊K/2⌋`** — i.e.
*no site holds a master majority*. Keep this line in view: it is the **takeover floor**
(`takeoverSurvivalFloor`), the hard bound every expectation below is derived from. The stricter
`⌈K/2⌉ − 1` is the **native target** (`nativeSurvivalTarget`) — breaching it only downgrades a
loss from a Redis election to an operator takeover. The two coincide for odd `K` and differ by
one for even `K`; conflating them was the §11.1 finding, now resolved (LR-031).

---

## 2. Method: the cell contract

Every matrix cell is specified as a five-tuple, and nothing goes into the suite without all five:

| Field | Meaning |
|---|---|
| **Fault** | The exact primitive invocation (§4), including scope and duration. |
| **Fixture** | The named topology (§3) it runs on. |
| **Expected** | The planner verdict **and** the system outcome, derived from §1.2 *before* the test is written. |
| **Assertion** | What is observed. Always includes the verdict; disruptive cells also include the cross-cutting invariants (§6). |
| **Teeth** | How the cell is shown to go red — a mutant (§10), a pre-fix commit, or genuinely-new behaviour. |

**Derive `Expected` from the arithmetic first, then run.** If the run disagrees, one of the two
is wrong and that is the finding — this is the entire value of the exercise. Do not adjust the
expectation to match observed behaviour without writing down why.

### 2.1 The rule for negative cells

**A fixture that is invalid for two reasons proves nothing about either.** Every negative cell —
a rejected CR, a refused reconcile, a declined takeover — must be valid in *every* respect except
the one under test, and must assert on the **specific** reason (condition reason, event reason,
error substring), never merely that something failed.

This is not theoretical: during LR-033 a mutation check that deleted the site-name-uniqueness rule
failed to turn the `rejects duplicate site names` spec red, because that spec (and five others)
also omitted the newly-required `sitesPerShard` and was being rejected for *that* instead. Six
negative specs were silently testing the wrong thing. A negative cell that cannot be shown to go
green when its own guard is removed is not coverage.

---

## 3. Topology fixtures

Named, reusable, and sized frugally (`100m` CPU / `128Mi` per redis) so several non-destructive
fixtures coexist. `sps` = `sitesPerShard`, `rps` = `replicasPerShard`;
members per shard = `sps × (rps + 1)`; initial master of shard `K` lives in site `K mod S`.

| Fixture | Sites | `K` | `sps` | `rps` | Pods | Initial master split | `q` | Why it exists |
|---|---|---|---|---|---|---|---|---|
| **F2-K4** | s1,s2 | 4 | 2 | 0 | 8 | 2+2 | 3 | **The recommended dual-site.** Even split ⇒ auto-takeover either direction. Primary fixture. |
| **F2-K4R** | s1,s2 | 4 | 2 | 1 | 16 | 2+2 | 3 | Same, but each shard has an intra-site replica too — proves native intra-site failover is preferred over cross-site. |
| **F2-K3** | s1,s2 | 3 | 2 | 0 | 6 | 2+1 | 2 | **The "1+2" people will actually run.** Asymmetric: losing s1 refuses, losing s2 is native. The **only** fixture that warns — 2 sites with an odd `K` is the single `ReducedSiteLossResilience` regime. |
| **F2-K6** | s1,s2 | 6 | 2 | 0 | 12 | 3+3 | 4 | Multi-orphan takeover: three concurrent no-consensus epoch bumps → tests epoch-collision convergence. |
| **F3-K3** | s1,s2,s3 | 3 | 2 | 0 | 6 | 1+1+1 | 2 | **Tier C reference.** Any single-site loss is native; `planSiteTakeover` should barely be reached. |
| **F3-K4** | s1,s2,s3 | 4 | 2 | 0 | 8 | 2+1+1 | 3 | The former "headline trap", **now Tier C/0b** (§11.1): pigeonhole forces `2+1+1`, which is *at the takeover floor* ⇒ **no** `ReducedSiteLossResilience`, and every single-site loss auto-survives — the 2-master site's by takeover, the others natively. |
| **F3-K6** | s1,s2,s3 | 6 | 2 | 0 | 12 | 2+2+2 | 4 | Tightest balance that reaches the *native* target (`S·target = 6 = K`). Losing a site leaves exactly `q`. |
| **F3-K3-M** | s1,s2,s3 | 3 | 3 | 0 | 9 | 1+1+1 | 2 | Full mesh (`sps = S`): every shard in every site. Two-site loss still leaves a replica. |

**Site choice is not free** (see §4.1): fixtures that need a *controllable inter-site link* must
use **s1 + s2** (or s1 + s3). s2↔s3 shares a bridge and only supports pod-level (Cilium) cuts.

---

## 4. Fault primitive inventory

### 4.1 The lab, and its asymmetry

```
                    ┌──────── hypervisor A ────────┐   ┌────────── hypervisor B ──────────┐
  hub (operator) ── │  scm-s1 (3 nodes)            │   │  scm-s2 (3n)      scm-s3 (3n)    │
                    │        │                     │   │      └── shared repl bridge ──┘  │
                    └────────┼─────────────────────┘   └──────────────┼───────────────────┘
                             └── router VMs ── dedicated eth cable ───┘
```

Consequences for the matrix, and they are load-bearing:

- **s1 ↔ {s2,s3}** transits router VMs on a dedicated cable ⇒ *full* control: netem (delay,
  jitter, loss, reorder), iptables DROP/REJECT, port-selective cuts, MTU clamps, asymmetric
  rules. **All infrastructure-level link cells use this path.**
- **s2 ↔ s3** shares a bridge ⇒ node-level isolation is hard. **Pod-level** cuts still work
  (CiliumNetworkPolicy is CNI-level and bridge-agnostic), and `tc`/`ebtables` filtered by peer
  IP on the bridge is a fallback to be prototyped, not assumed.
- ⇒ **Rule:** any cell needing a *node/infrastructure* cut between two sites is written for the
  (s1, s2) pair. Cells needing only a *pod-network* cut may use any pair.

### 4.2 Primitives

| ID | Primitive | Mechanism | Status |
|---|---|---|---|
| P1 | Whole-site power-off / on | `virsh destroy` / `start` all site VMs, `MS_VM_INVENTORY` | **have** |
| P2 | Pod-level data-plane cut | CiliumNetworkPolicy ingress/egressDeny on 6379+16379 | **have** |
| P3 | Continuous write load + integrity | chaos client pod, `-cluster`, metrics snapshot | **have** |
| P4 | Ground truth / verdicts | `msClusterNodes`, `msClusterInfo`, `msOperatorLogSince`, `lrctl verify` | **have** |
| P5 | **Partial site outage** | `virsh destroy` *n of 3* VMs of a site | build (trivial: reuse P1 per-VM) |
| P6 | **Node NIC link-down** | `virsh domif-setlink <dom> <iface> down` | build |
| P7 | **Infrastructure link cut** (s1↔s2) | iptables DROP/REJECT on the router VM, direction- and port-selective | build |
| P8 | **netem shaping** | `tc qdisc` on the router VM egress: delay, jitter, loss, reorder | build |
| P9 | **MTU blackhole** | MTU clamp on the router VM path **without** ICMP frag-needed | build |
| P10 | **Clock skew** | `talosctl patch machineconfig /machine/time` → controlled chrony with an offset; applies **live, no reboot** | build (feasible — see §4.3) |
| P11 | **Split-brain monitor** | dual-vantage slot-ownership + epoch recorder (§6.1) | build — **highest value** |
| P12 | **Fault scheduler** | randomized fault/heal driver for soak (§7 Tier 4) | build |
| P13 | **Site credential faults** | revoke SA token / delete or corrupt the hub site Secret | build (trivial) |
| P14 | **Per-namespace operator** | namespace-scoped operator (ADR-014) per concurrent fixture | **have** (feature); harness wiring to build |

Every primitive ships with its inverse and is exercised by a self-test (`fault → assert
inflicted → heal → assert healed`) before any cell depends on it. A fault that cannot be proven
inflicted produces a green test that measured nothing — the worst outcome in this whole
programme.

### 4.3 Clock skew on Talos — feasible

Researched: `machine.time.servers` is applied **live without a reboot** via
`talosctl patch machineconfig -p '[{"op":"replace","path":"/machine/time","value":{...}}]'`,
verified with `talosctl get timestatus` / `talosctl time`. So the primitive is: run a chrony
instance we control per skewed site, point that site's nodes at it, step the served time, and
revert by pointing back at the lab NTP. Guests only — hypervisor clocks untouched.

Documented thresholds: nodes should be within ~1s; >5s starts breaking things; large skew
breaks TLS/cert/token validity. That maps directly onto the two D-cells: a **small** skew must
be a no-op by design, a **large** skew must degrade into "site unmanageable, data plane fine"
(claim C6) — *not* into a takeover.

---

## 5. The matrix

Legend for **Tier**: `0` pure/unit · `1` smoke (≤15 min) · `2` regression (~90 min) ·
`3` torture (hours) · `4` soak. Cells marked **†** are new-behaviour cells whose expected
outcome must be settled (§11) before they are written.

### MS-A — Whole-site loss (hypervisor power-off, P1)

| ID | Fault | Fixture | Expected | Key assertion | Tier |
|---|---|---|---|---|---|
| A1 | Kill s2 (2 masters) | F2-K4 | `Arm` → `Wait` → **`Takeover`** after 60s | 16384 slots served from s1; corruptions 0; replicated data intact | 1 |
| A2 | Kill s1 (the other 2) | F2-K4 | identical to A1 (symmetry) | same — proves neither site is privileged | 2 |
| A3 | A1, then `virsh start` s2 | F2-K4 | Returnees rejoin **fresh** (new node IDs), staggered; ghosts FORGOTten once `allSitesObservable` | no epoch conflict; cluster healthy; slot ownership stable | 2 |
| A4 | Kill s1 (2 masters, `m_L = 2 ≥ q = 2`) | F2-K3 | **`Refuse`** — `RefusedSiteTakeover` event, `MultiSiteTakeover` condition | cluster stays fail-stopped; **no** TAKEOVER issued; no data loss | 2 |
| A5 | A4 + set `allowUnsafeSiteTakeover: true` | F2-K3 | **`TakeoverUnsafe`** | fires, surfaced loudly; slots served | 2 |
| A6 | Kill s2 (1 master) | F2-K3 | **`WaitNative`** — Redis elects | operator issues nothing; native failover observed in `CLUSTER NODES` | 2 |
| A7 | Kill any one site | F3-K3 | **`WaitNative`** (Tier C) | native election; FORGET **trails** while the site is unobservable | 2 |
| A8 | Kill the 2-master site | F3-K4 | **`Takeover`** — `2 < q = 3` both ways (fixture is at the takeover floor) | fires with **no** opt-in; slots served from s2+s3; **no** `ReducedSiteLossResilience` condition on this fixture (§11.1) | 2 |
| A9 | Kill a 1-master site | F3-K4 | **`WaitNative`** (3 ≥ q) | native | 3 |
| A10 | Kill one site (2 masters) | F3-K6 | **`WaitNative`** — exactly `q = 4` survive | proves the tightest satisfiable topology holds | 3 |
| A11 | Kill s2 (3 masters) | F2-K6 | **`Takeover`** on **3 orphaned shards** | three near-simultaneous epoch bumps converge; no slot owned twice | 3 |
| A12 | Kill both sites | F2-K4 | **`None`** (descoped total loss) | no wild action; documented manual re-bootstrap path works | 3 |
| A13 | Kill 2 of 3 sites | F3-K3-M | **`Refuse`** — full mesh means a replica survives (condition 1 holds) and survivors are minority (1 < q=2), but the unreachable 2 masters are **not** < q=2 ⇒ the fence fails | proves the fence refuses a majority-loss even when a promotable survivor exists — the D1 refusal is about the *lost* side, not the survivor | 3 |
| A14 | Kill s2, then after recovery kill s1 | F2-K4 | Both recover independently | second recovery not poisoned by first's ghosts | 3 |
| A15 | Kill the **hub** while sites healthy | F2-K4 | Sites keep serving (C6) | zero data-plane change; on hub return, **no** spurious takeover | 2 |
| A16 | Kill s2, then kill the hub **mid-cooldown**, restore hub | F2-K4 | `takeoverPendingSince` survives in status (hub etcd) → fires correctly on return | cooldown arithmetic not restarted-then-skipped | 3 |
| A17 | Delete the operator pod right **after** the first TAKEOVER, before FORGET | F2-K6 | Resumable from live state | converges; FORGET completes; idempotent, no double-promotion | 3 |

### MS-B — Partial site outage (node-level, P5/P6)

| ID | Fault | Fixture | Expected | Key assertion | Tier |
|---|---|---|---|---|---|
| B1 | Kill 1 of 3 nodes in s2 | F2-K4R | Native reschedule + failover; **no** site-loss verdict | `None`/`WaitNative`; data intact; shard anti-affinity honoured on reschedule | 2 |
| B2 | Kill 2 of 3 nodes in s2 (site control plane loses etcd quorum, some pods survive) | F2-K4 | **The hybrid case:** site unmanageable, data plane partly alive → **no takeover** | `SiteUnreachable`; no destructive action; surviving masters keep serving | 3 |
| B3 | Kill only the node hosting a shard master | F3-K3 | Native failover (intra- or cross-site) | operator does not compound; converges | 3 |
| B4 | Kill 1 node in **every** site simultaneously | F3-K6 | Multiple concurrent native failovers | converges; rebalancer stays quiet during healing (quiet gate) | 3 |
| B5 | NIC link-down (P6) on one node, API still up via other NIC | F2-K4R | Pod unreachable, kubelet marks not-Ready | proves the readiness path, not the dial path, drives pod-level decisions | 3 |

### MS-C — Network faults (the rich domain)

Sub-axes: **path** (data plane / control plane / both) × **scope** (pod, node, site, hub) ×
**shape** (drop, reject, port-selective, asymmetric, lossy, slow, MTU) × **duration**
(transient, flapping, sustained).

| ID | Fault | Fixture | Expected | Key assertion | Tier |
|---|---|---|---|---|---|
| C1 | **Hub-blind** data-plane cut: hub↔s2 pods denied, s1↔s2 intact and serving | F2-K4 | **`WaitNative`/`Hold` — never a takeover** (C4) | survivors report `cluster_state:ok`; no TAKEOVER for ≥3× cooldown | 1 |
| C2 | Full inter-site cut s1↔s2 (P7, all traffic) | F2-K4 | Both halves minority ⇒ both self-fence ⇒ **`Takeover`** on the reachable side | isolated side serves nothing (verify from *inside* s2); no divergence | 2 |
| C3 | C2, then heal | F2-K4 | Fenced-alive returnees are **recycled** (ADR-008 machinery), demote, resync | ADR-012's explicit claim; never-restarted pods must not re-MEET as stale ex-masters | 2 |
| C4 | Inter-site cut of **gossip only** (16379), 6379 open | F2-K4 | Both sides fail-stop on gossip minority ⇒ **`Takeover`**, still safe (fenced side rejects writes despite reachable 6379) | strongest single test of claim **C2**: reachable ≠ serving | 3 |
| C5 | Inter-site cut of **replication only** (6379), gossip open | F2-K4 | `cluster_state:ok` everywhere ⇒ **no takeover**; replicas go `link:down` | degradation must be *surfaced* (condition / `lrctl verify [DEGRADED]`) — if it is not, that is a gap to file | 3 |
| C6 | Asymmetric drop s1→s2 only | F2-K4 | Degenerates to a symmetric cut (a one-way IP drop breaks TCP both ways) — messier, slower detection, same verdict | confirm no novel behaviour; keep as one cell, do not over-invest | 3 |
| C7 | **Flapping** cut: 20s down / 40s up × 10 | F2-K4 | `Arm` → clear → `Arm` … **never fires** (cut < 60s cooldown). Rebalance moves only if a flap leaves the masters over the floor (e.g. a native failover drifting 2+2 → 3+1), never on a still-balanced 2+2 | proves the cooldown has teeth against flap; rebalance churn stays *bounded and drift-justified*, not zero-by-inertness (§11.1) | 3 |
| C8 | C7 followed by a sustained cut | F2-K4 | Fires normally | proves flapping did not wedge the marker | 3 |
| C9 | netem 500 ms RTT s1↔s2 | F2-K4 | No effect (node-timeout 15s) | zero failovers, zero verdicts | 3 |
| C10 | netem **5 s** RTT s1↔s2 — above the operator's 3s `ProbeTimeout`, below node-timeout | F2-K4 | Hub's dial times out ⇒ site looks unreachable, **but survivors report `ok`** ⇒ **no takeover** | the sharpest test of **C3/C4**: latency must never be mistaken for site loss | 2 |
| C11 | netem 8 s RTT + 200 ms jitter | F2-K4 | Redis PFAIL churn; operator must not compound | no takeover; quiet gate blocks rebalance; converges on heal | 3 |
| C12 | 5% / 20% / 50% packet loss, sustained | F2-K4 | Graceful degradation; sync retries | no data loss; no wrong verdict at any loss level | 3 |
| C13 | **MTU blackhole** (P9) during a full-sync of a large dataset | F2-K4R | Replica sync stalls indefinitely; masters fine ⇒ **no takeover** | classic prod failure; assert it is *visible* (stuck `link:down` surfaced), not silent | 3 |
| C14 | 3-site: cut s1 from {s2,s3}, s2↔s3 intact | F3-K3 | s1 minority self-fences; majority fails over s1's shard | no split-brain; on heal s1 demotes and resyncs | 2 |
| C15a | **Non-transitive**, balanced: s1↔s3 cut, both reach s2 | F3-K3 (1+1+1) | **Inert at the Redis level** (§11.5): no PFAIL→FAIL promotion is possible, so no election, no demotion. Every node reports `cluster_state:ok`; operator verdict **`None`** | assert the *absence* of action: no FAIL flag anywhere, zero elections, zero operator actions, all 16384 slots continuously served. The **only** effects are one broken cross-site replication link (shard2's replica in s1, whose master is in s3) and PFAIL flags in the s1/s3 vantages. Assert the broken link is **surfaced** (cf. C5) — a silently unreplicated shard is the real exposure here | 3 |
| C15b | Same cut, but **drifted over the floor** first (operator scaled to 0, induce 1+0+2 by crashing s2's master so shard1 fails over to s3) | F3-K3 | The heavy site now *can* reach the FAIL quorum alone (2 ≥ q=2) ⇒ it FAIL-marks the light side's master and elects its replica; the isolated old master demotes when it learns the higher config epoch **via s2** ⇒ a **real but bounded dual-writer window** | this is the cell the split-brain monitor (P11) exists for: measure the window, assert it *closes* (no permanent divergence) and that writes accepted by the demoted master are the only losses. Then scale the operator back up: assert it takes **no** corrective action during convergence (no FORGET, no reshard, no takeover) and that the **rebalancer removes the exposure** | 3 |
| C16 | Isolate a single pod (P2, pod-scoped) | F3-K6 | Native FAIL + failover for that shard | operator does not escalate to a site verdict | 3 |
| C17 | hub↔site **control plane** cut only (K8s API unreachable, pods fine) | F2-K4 | Site unmanaged, not broken (**C6**) | `SiteUnreachable`; FORGET trails (`allSitesObservable` false); zero data-plane action | 2 |
| C18 | hub↔**all** sites cut (hub isolated) | F3-K3 | Nothing happens anywhere; management resumes on heal | no verdict, no drift, no teardown | 3 |

### MS-D — Time and clock (P10)

Design expectation, verified by code audit: **no operator decision compares a site-supplied
timestamp against the hub clock.** `time.Since` appears only on hub-written status timestamps
(`RebalancePendingSince`, `takeoverPendingSince`); site pod `DeletionTimestamp` is used as a
*presence* check. Redis's own timers are local/monotonic and config epochs are logical. So the
predicted blast radius of cross-site skew is Kubernetes TLS/token validity, not the arithmetic.

| ID | Fault | Fixture | Expected | Key assertion | Tier |
|---|---|---|---|---|---|
| D1 | ±30 s skew on s2 | F2-K4 | **No effect** | cluster healthy; cooldown timing unchanged | 3 |
| D2 | +10 min skew on s2 | F2-K4 | Site's own kubelet/API certs & SA tokens break ⇒ degenerates into C17 | **no takeover**, no data loss; recovers when skew is removed | 3 |
| D3 | Skew s2 while a takeover cooldown is running | F2-K4 | Cooldown is hub-clock-only ⇒ unaffected | fires at exactly the same time as A1 | 3 |
| D4 | **Hub clock jumps backwards** mid-cooldown | — | `time.Since(since) < 0` ⇒ cooldown never elapses ⇒ takeover stalls (fails *safe*, but stalls) | **pure test** — `planSiteTakeover(in, since, now < since, cd)`. Decide whether stalling is acceptable or the cooldown should use a monotonic anchor | **0** |
| D5 | **Hub clock jumps forwards** mid-cooldown | — | Cooldown elapses early, skipping the fence settle window (fails *unsafe*) | same pure test; this is the direction that matters for safety | **0** |
| D6 | TTL'd keys + promotion of a replica in a skewed site | F2-K4 | Expiry semantics are absolute-timestamp based | document observed behaviour; assert no *silent* mass expiry | 3 |

D4/D5 are cheap (`planSiteTakeover` is pure and already takes `now` and `since` as parameters)
and cover a real class. They belong in Tier 0 regardless of the rest of this document.

### MS-E — Control plane, credentials, operator

| ID | Fault | Fixture | Expected | Key assertion | Tier |
|---|---|---|---|---|---|
| E1 | Site API slow (netem on the API VIP) | F2-K4 | Client timeouts ⇒ site unobservable; no destructive action | C6 | 3 |
| E2 | Revoke the site SA token (P13) | F2-K4 | `SiteUnreachable`; no takeover, no teardown | credentials ≠ liveness | 2 |
| E3 | Delete the hub site Secret | F2-K4 | Same as E2, plus a clear condition | must not be read as "site removed" | 3 |
| E4 | **Informer cache lies:** hard-kill s2; the hub's cache still reports its pods `Ready` | F2-K4 | **Takeover fires anyway** | proves claim **C3** directly — the decision never consults pod readiness. Mutant `M-READY` (§10) makes this cell red. | 2 |
| E5 | Operator scaled to zero during a site loss; scaled back up | F2-K4 | Nothing while down; on return the marker arms and fires | also proves `lrctl` tier-1/2 gather works with the operator at zero | 3 |
| E6 | CR deleted while s2 unreachable | F2-K4 | Finalizer retains; `MultiSiteTeardown` condition; sweep completes on heal | plus the documented manual finalizer escape hatch | 2 |
| E7 | Two instances in two namespaces, one under chaos | F2-K4 ×2 | Full isolation | namespace-scoped operators (ADR-014); also the concurrency precondition for §8 | 2 |
| E8 | `repairCluster` must not act as a second authority | F2-K4 | No repair-driven takeover / unguarded FORGET (**C8**) | mutant `M-GUARD` (§10) flips `repairMayForceTakeover` on and this cell must go red | 2 |

### MS-F — Topology and configuration change

> **Collapsed by LR-032/LR-033 (§11.2).** The multi-site topology is now **immutable for an
> instance's lifetime**, enforced by four CRD CEL transition rules at *admission*: `sites`
> (names **and** order), `sitesPerShard`, `shards` while `multiSite` is set, and adding/removing
> the `multiSite` block. So the five separate topology-change cells this domain used to carry —
> shard shrink, `sitesPerShard` shrink, site removal, site addition, shard scale-out — are all
> the same thing now: **the API server rejects the edit**. They fold into **F1**.
>
> The LR-028 runtime guard survives as defence in depth, but it can no longer be *reached* by
> editing the CR (F2). And note the CR-visible break: **`sitesPerShard` is now required** — every
> fixture in §3 must set it explicitly (the e2e builder `msClusterCR` already does).

| ID | Change | Fixture | Expected | Key assertion | Tier |
|---|---|---|---|---|---|
| F1 | **The one topology-mutation cell.** Against a running instance, attempt in turn: `shards` ↑ and ↓; `sitesPerShard` ↑ and ↓; add a site; remove a site; **reorder** the site list; add/remove the `multiSite` block | F2-K4 (+ F3-K3-M for the `sps` cases) | Every one **rejected by the API server** on `Update` | error mentions immutability; **and — the assertion that matters — no per-site shard StatefulSet created or deleted in *any* site**, i.e. growth cannot half-succeed. Cheap, deterministic, no outage needed | 2 |
| F2 | LR-028 runtime guard (defence in depth) — hand-create a surplus per-site shard StatefulSet | F2-K4 | **`MultiSiteScaleDownRefused`**, phase `Failed`, **no deletion** (**C7**) | the orphan still exists and still holds its data. No longer reachable via the CR, so trigger it by hand — or skip it, its no-delete invariant is already unit-proven | 3 |
| F6 | `replicasPerShard` 0 → 1 (**still mutable** — not a topology dimension) | F2-K4 | Per-site STS grows; new pods reattach shard-aware (LR-027 path) | no disruption to serving | 3 |
| F7 | Image/config change (rolling update) while healthy | F3-K6 | At most one shard STS rolling instance-wide (LR-021 cross-site) | no two masters restarting at once | 2 |
| F8 | Rolling update **during** a site loss | F2-K4 | Must not proceed while degraded | serialization gate holds | 3 |
| F9 | Flip `allowUnsafeSiteTakeover` while refusing | F2-K3 | Refusal converts to `TakeoverUnsafe` within one reconcile | the documented operator escape hatch actually works | 2 |
| F10 | Creation-time invalid specs: `sitesPerShard` > sites / missing / < 2, 1 site, duplicate site names, `nodeport` mode, `multiSite` + `mode: sentinel` | — | Validation rejects, **each for the reason under test** (§2.1) | **Tier 0** table test. The four *transition* rules are already envtest-covered (9 specs, 7 observed red first) — reference them, do not duplicate | **0** |

**Dropped deliberately:** a growth-convergence scenario (MEET + shard-aware reattach of a newly
added site's members). That path is unreachable through the API now, so testing it would be
testing an unsupported operation.

### MS-G — Master balance and the rebalancer

> **This domain is first contact, not regression.** Pillar 3.15 already records the master
> rebalancer as unit/envtest-verified with live validation pending — and that is still true after
> LR-031, which changed the *planner's* thresholds but left the executor untouched. So
> `rebalanceMasters → pickRebalanceReplica → ClusterFailover` has **never executed against real
> Redis anywhere**, in any topology. G1/G4/G5 are the first time that chain runs end-to-end.
>
> LR-031 raises the stakes rather than lowering them: the rebalancer is now **active on
> topologies where it was previously inert** (K=4 over 2 or 3 sites — including the
> D3-recommended dual-site). Two consequences:
>
> - The G domain moves ahead of most of Tier 3 in build order (§13) — it is the largest
>   never-executed surface in multi-site.
> - **Rollout note:** any existing multi-site instance at K=4 over 2 or 3 sites that is carrying
>   drift will start issuing graceful `CLUSTER FAILOVER`s after upgrade. Bounded — healthy path
>   only, quiet-gated, one per 30s cooldown, on the no-data-loss LR-025 primitive, worst case an
>   availability blip — but it is new actuation on live instances, and **G9 is the cell that
>   validates it before anyone upgrades into it.**

| ID | Scenario | Fixture | Expected | Key assertion | Tier |
|---|---|---|---|---|---|
| G1 | Induce drift (crash a master so its shard fails over cross-site) → 2+0+1 | F3-K3 | Rebalancer restores 1+1+1 with **one graceful `CLUSTER FAILOVER` per reconcile**, ≥30s apart | zero data loss; `status.multiSite.MasterCounts` reflects it; `lrctl verify` renders it | 2 |
| G2 | Healthy balanced cluster, 30 min idle | F3-K6 | **Zero** failovers issued | no-thrash (claim C5's other half) | 3 |
| G3 | Drift + a concurrent pod failure | F3-K6 | No rebalance until quiet | the healthy-path-only gate | 3 |
| G4 | Drift to 3+1 | F2-K4 | s1 is **over the floor** (3 > 2) and the native target 1 is out of reach for `S=2, K=4` ⇒ the floor tier plans ⇒ **one graceful `CLUSTER FAILOVER` restores 2+2** | assert the *arithmetic consequence*, not just the move: a subsequent s1 loss then **`Takeover`s** instead of refusing. `lrctl verify` shows `[OVER FLOOR]` before and `[OVER TARGET]` on both sites after (2+2 is the best `S=2, K=4` reaches) | 2 |
| G5 | Drift to 3+1+0 | F3-K4 | Same two-tier path: target 1 unreachable (`S·target = 3 < 4`) ⇒ floor tier ⇒ **one move restores 2+1+1** | recipient is constrained to a site holding a promotable replica of the moved shard (`sps=2` — cf. G7); afterwards every single-site loss auto-survives (A8/A9) | 2 |
| G6 | Rebalance target site holds only a **lagging** replica | F2-K4R | Move not emitted (in-sync `link:up` requirement) | never hand mastership to a lagging replica | 3 |
| G7 | `sps=2` in a 3-site: recipient constrained to the shard's 2 hosting sites | F3-K6 | Planner respects replica placement | no move to a site without a replica of that shard | **0**+3 |
| G8 | Drift induced repeatedly during soak | F3-K6 | Convergent, bounded churn | measured: failovers/hour stays bounded | 4 |
| G9 | **Upgrade actuation:** bring up F2-K4 on a **pre-LR-031** operator image, drift it to 3+1 (which that build leaves uncorrected), then upgrade the operator in place | F2-K4 | On the first healthy reconcile after upgrade the floor tier plans and issues **exactly one** graceful `CLUSTER FAILOVER`, restoring 2+2 and stopping | the literal rollout scenario: assert **one** move (not a burst), write availability dip within the LR-025 band, corruptions 0, and that a second reconcile is a no-op. Uses the pre-fix commit as its own control — the same drift is provably uncorrected before the upgrade | 2 |

> **G9 CRD caveat.** A pre-LR-031 operator image is also **pre-LR-032/033**, which *did* change the
> CRD (`sites` `listType` map→atomic, `sitesPerShard` required). **Pin the CRD at the current
> version and vary only the operator binary.** Old-binary-against-new-CRD is the benign direction —
> the old Go-side `validateMultiSite` is laxer than the new schema, so the schema is the binding
> constraint and the fixture stays valid. The reverse (new binary, old CRD) is not, and must not be
> what the cell accidentally builds. This is also why the two workloads cannot share a cluster
> (soak §10.1): the CRD is a cluster-scoped singleton, so a cell that pinned a *different* CRD
> would clobber every other instance on that cluster.

### MS-I — Bringup and lifecycle races

| ID | Scenario | Fixture | Expected | Key assertion | Tier |
|---|---|---|---|---|---|
| I1 | Bring up with s2 **unreachable from the start** | F2-K4 | **`None`** — the `clusterFormed` veto (LR-026) | **regression cell for a real bug**: without the veto this arms a takeover across pods that never joined, deadlocking bringup | 2 |
| I2 | s2 becomes reachable mid-bootstrap | F2-K4 | Bootstrap completes normally | no half-formed cluster | 3 |
| I3 | Bring up with `rps: 0` (multi-site always has replicas) | F2-K4 | Cross-site members reattach as replicas (LR-027) | **regression cell**: the single-site `rps==0 ⇒ no replicas` short-circuit must stay disabled | 2 |
| I4 | Rapid CR delete + recreate | F2-K4 | Clean teardown then clean bringup | no leftover spoke workloads (finalizer sweep) | 3 |
| I5 | Site rejoin after **hard power-off** (fresh EmptyDir) | F2-K4 | New node IDs, ordinary MEET + shard-aware reattach | contrast with I6 | 2 |
| I6 | Site rejoin after **network heal** (pods never restarted, stale state) | F2-K4 | Returnees **recycled** so they rejoin fresh | ADR-012's explicit claim; the contrast with I5 is the point | 2 |

### MS-J — Redis-level chaos inside multi-site

| ID | Fault | Fixture | Expected | Key assertion | Tier |
|---|---|---|---|---|---|
| J1 | `kill -9` every redis in s2 (EmptyDir survives) | F2-K4 | Per-site ADR-008 wipe recovery recycles the stuck pods | no cross-site escalation; no takeover | 2 |
| J2 | `kill -9` everything, all sites | F2-K4 | Cluster-wide wipe recovery | converges or documented manual path | 3 |
| J3 | OOM-kill a master | F3-K6 | Not recyclable (OOMKilled excluded); native failover | ADR-008 safety gate holds cross-site | 3 |
| J4 | CPU starve one site's pods | F2-K4R | Lagging replicas; rebalancer avoids them (G6) | no wrong promotion | 3 |
| J5 | Fill a shard to `maxmemory` (`noeviction`) during a site loss | F2-K4 | Writes error, cluster stays consistent | pillar 3.2 honesty under stress | 3 |

---

## 6. Cross-cutting invariants

Applied to **every** disruptive cell, not asserted per-cell in prose:

1. **No split-brain** — never two nodes serving writes for the same slot (§6.1).
2. **`corruptions == 0`** — the chaos client's read-after-write integrity counter. Non-negotiable.
3. **Replicated data survives** — data acknowledged before the fault and replicated cross-site is
   readable after recovery. Unreplicated tail loss is accepted (pillar 3.1, RPO > 0) but must be
   *bounded and reported*, never silent.
4. **Converges** — within a stated per-cell budget after the fault heals: all 16384 slots served,
   `cluster_state:ok` from every vantage, expected master distribution, no ghosts.
5. **Write availability** — measured per cell with a declared band (e.g. A1: dip ≈ cooldown +
   convergence ≈ 90s; steady-state cells: 100%). Regressions in the band are failures.
6. **No unrequested destruction** — no StatefulSet or PVC-equivalent removed, no pod deleted
   outside the documented recycle paths.

### 6.1 The split-brain monitor (P11) — the crown jewel

Claim **C2** is the one that would be catastrophic if false, and no single-vantage assertion can
detect its violation: during a partition each side reports a locally-consistent story. So:

- A sidecar process per site, running **inside** that site (so it survives the hub losing the
  site), samples `CLUSTER NODES` + `CLUSTER INFO` + a canary write attempt per slot range at
  ~1 Hz, and appends `(timestamp, vantage, slot-range, owner-node-id, config-epoch,
  write-accepted?)`.
- After the run, a checker merges the per-site logs on the hub clock and flags **any interval
  where two distinct node IDs both accepted a write for the same slot**.
- This is what converts "we saw no problem" into "we proved the fence held", and it is the only
  instrument that makes the exotic cells (C4, C15, A11) worth running.

Build this **before** the Tier-3 network cells. Without it those cells measure very little.

---

## 7. Tiers and runtime budget

| Tier | Name | Budget | Runs on | Content |
|---|---|---|---|---|
| **0** | Pure / unit | seconds | every commit | Planner tables: all 8 `planSiteTakeover` verdicts × the arithmetic boundaries; `planMasterRebalance` (restore-to-target, fall-back-to-floor, no-op, nothing-reachable, constrained recipients) and the cross-seam property test binding the two thresholds to `planSiteTakeover`'s real verdicts; `siteLossResilient` (odd `K` over 2 sites warns, `S ≥ 3` never does); `planMultiSiteScaleDown`; `repairMayForceTakeover` / `ghostForgetPlan`; validation (F10); clock jumps (D4/D5) |
| **1** | **Smoke** | **≤15 min** | every push to an integration branch | One FIRE + one HOLD, on one fixture — see §7.1 |
| **2** | Regression | ~90 min | nightly, and before any release cut | One cell per distinct decision branch and per historical bug — the ~30 Tier-2 rows above |
| **3** | Torture | 6–8 h | weekly / before a graduation decision | Everything else: netem, MTU, asymmetric, non-transitive, clock, flapping, partial-node, large-dataset |
| **4** | Soak | 12–24 h | before graduating multi-site out of experimental | Randomized fault schedule (P12) under continuous load with the split-brain monitor and a convergence budget after each fault window |

### 7.1 The 15-minute smoke, concretely

The design constraint is the 60s cooldown floor: a smoke run that skips it does not test the
takeover at all. Composition:

| Step | Budget |
|---|---|
| Build + push image (cached layers; tag = git hash) | ~3 min |
| Deploy namespace-scoped operator + F2-K4 bringup to healthy (8 pods, 100m each) | ~3 min |
| Seed data + start chaos client | ~15 s |
| **A1**: `virsh destroy` s2 → assert `Arm` ≤ 20s → cooldown → assert **`Takeover`** → 16384 slots from s1, corruptions 0 | ~2 min |
| Fire `virsh start` s2 **immediately**, assert data/slots while it boots (overlap the wait) | — |
| **C1**: hub-blind Cilium cut → assert **no takeover** for ≥2 min, survivors `ok` → heal | ~2.5 min |
| Teardown + assert lab healthy | ~2 min |
| | **≈ 13 min** |

One FIRE and one HOLD — deliberately opposite decisions, so a gate inverted in either direction
fails the smoke. This is the minimum that is worth calling a multi-site regression net.

---

## 8. Execution model: what can run in parallel

The decisive structural fact:

- **Namespace-scoped faults** (P2 Cilium policies, P13 credentials, F-domain config changes,
  J-domain pod kills, G-domain drift) affect only their own namespace ⇒ **parallelizable** across
  fixtures, given frugal sizing and one namespace-scoped operator per fixture (P14, ADR-014).
- **Lab-global faults** (P1/P5/P6 virsh, P7/P8/P9 router VM, P10 clock) affect shared
  infrastructure ⇒ **strictly serial**, and must hold a lab-wide mutex.

So Tier 3 splits into **Phase P** (parallel, ~3 fixtures at a time) and **Phase S** (serial),
and the ordering matters: run Phase P first — it is cheap, and it catches the config/logic
regressions that would otherwise waste an expensive serial slot.

Fixture reuse is the other big lever: bringup dominates per-cell cost, so cells are grouped into
`Ordered` contexts per fixture — bring up once, inflict → heal → assert-converged between cells.
This is what takes Tier 3 from ~100 cells × 10 min (17 h) down to 6–8 h. It costs cell
independence, so each context must assert *converged-to-healthy* between cells or a leaked state
silently contaminates the next.

### 8.1 Where it runs: not from a laptop

Tier 3 is 6–8 h and Tier 4 is 12–24 h. Driving that from a workstation over `kubectl` is a setup
for failure — a closed lid, a dropped VPN or a suspended host aborts the run *mid-fault*, leaving
a site powered off and qdiscs installed. **The execution substrate is
`E2E_SOAK_HARNESS_DESIGN.md` §9**, which this matrix and the soak workload now share: an Indexed
Job driver over a **work list**, always-`exit 0` with a recorded per-item result, per-item
namespace + namespace-scoped operator, artifacts on failure.

The matrix-specific decisions recorded there, because they change that design rather than reuse it:

- **The driver runs on the hub** (`scm-cp`) — it already holds every site's kubeconfig Secret and
  the disruption ledger, and it is the one cluster that survives any *site* fault. The **classic**
  suite deliberately runs elsewhere, on a plain workload cluster (soak §10): it needs no
  disruption privilege and no multi-site notion, and the CRD is a cluster-scoped singleton that
  `WATCH_NAMESPACE` does not isolate, so the two workloads must not share a cluster.
- **A matrix run takes the whole lab** — exclusive access to the hub and every site, for the
  duration. The matrix kills each site in turn, so nothing else may be running on one; this only
  states the exclusivity the disruption ledger already assumes (soak §10.2).
- **Park the namespace, heal the lab.** The soak rule "on failure, do nothing" is right for
  namespace-scoped state and wrong for lab-global state: a parked powered-off site poisons every
  later item. The in-tree write-ahead ledger + `ms-heal` run between items unconditionally.
- **Host-level faults via restricted `command=` SSH keys** on the hypervisors and router VMs, not
  a general key in the pod — the driver can invoke only the exact verbs (P7–P10).
- **The ledger is currently single-run-scoped** ("we assume exclusive access… one ms e2e run at a
  time"). Phase-P concurrency needs either run-keyed records or a stated restriction of Phase P to
  fault kinds the ledger does not track. **Decide before building Phase P.**
- **A15 (kill the hub) cannot run on the hub.** That cell stays interactive, or needs a small
  external driver.

---

## 9. Harness work plan (ordered)

> **The *substrate* milestones live in `E2E_SOAK_HARNESS_DESIGN.md` §11** (M0 spike → M1 work-list
> driver → M2 classic soak → M3 multi-site bringup → M3.5 no-fault cells → M4 fault agents +
> smoke). This section is the **matrix-content** plan: which primitives and cells, in which order,
> *given* a working driver. The two are sequenced together in §11 there.

1. **Frugal fixtures + P14** — the `msFixture` builder (§3) and one namespace-scoped operator per
   fixture. Lands with substrate M3.
2. **Verdict assertion helper** — `msAssertVerdict(since, expected …)` reading the audit log +
   condition + event, so cells assert decisions rather than outcomes. Needed by the very first
   cell, so it comes before any of them.
3. **The no-fault cells (M3.5)** — **MS-G** (G1/G4/G5/G9, the never-executed rebalancer executor),
   **F1** (topology immutability, pure API calls), **I1/I3** (bringup regressions). These need
   nothing but a pod delete or an API call, so they land *before* any fault primitive and buy real
   coverage of the riskiest untested path early. *(Reordered 2026-08-14: this used to sit behind
   the infrastructure work.)*
4. **Tier 1 smoke** (A1 + C1) wired as a Ginkgo label (`multisite-smoke`), with the virsh fault
   agent — the first thing that pays for itself daily.
5. **P11 split-brain monitor** + the invariant checker. Everything exotic depends on it, but
   nothing before this point does — it gates Tier 3, not Tier 2.
6. **Tier 2 regression** cells, in matrix order.
7. **P5/P6** (partial site, NIC down) — trivial extensions of the existing virsh inventory.
8. **P7/P8/P9** (router-VM cuts, netem, MTU) with an `MS_LINK_INVENTORY` mapping site-pair → the
   router VM and interface, mirroring `MS_VM_INVENTORY`'s no-naming-convention style.
9. **P10 clock skew** (controlled chrony + `talosctl patch machineconfig`).
10. **Tier 3** cells, then **P12 fault scheduler** + Tier 4 soak.

Steps 1–3 are the force multipliers; 4 is the first daily payoff; 5–10 are then mostly mechanical.

---

## 10. Teeth: the mutant catalogue

Red-first is straightforward for Tier 0 (assertion before implementation) and for historical-bug
cells (red on the pre-fix commit). It is *not* straightforward for guards that already exist —
a cell asserting "the fence held" is green whether or not the fence is what held it.

The systematic answer: an env-gated **defect injection** build of the operator (behind a
`//go:build e2e_mutants` tag so it cannot ship), and a periodic **teeth run** that enables one
mutant at a time and asserts the mapped cells go **red**. A mutant that no cell catches is a
coverage hole, stated as a number.

| Mutant | Defect injected | Cells that MUST go red |
|---|---|---|
| `M-FENCE` | Drop condition 3 (unreachable `< q`) | A4, A5 (should fire without opt-in), A13 |
| `M-VANTAGE` | Use the hub's dial instead of the survivor's own `cluster_state` (condition 2) | C1, C10, C17 |
| `M-COOLDOWN` | Cooldown → 0 | C7, A1 (fires too early) |
| `M-FORMED` | Drop the `clusterFormed` veto (LR-026) | I1 |
| `M-READY` | AND the gate with "site pods not Ready" (the rejected witness) | E4, A1 (informer cache lies) |
| `M-GUARD` | `repairMayForceTakeover` → always true | E8, C1 |
| `M-OBSERVABLE` | Drop `allSitesObservable` from the FORGET gate | A7, C17 |
| `M-QUIET` | Drop the quiet gate | B4, G3 |
| `M-LINEAGE` | Drop the in-sync/`link:up` requirement on rebalance targets | G6 |
| `M-RPS0` | Restore the single-site `rps==0 ⇒ no replicas` short-circuit | I3 |
| `M-FLOOR` | Drop `planMasterRebalance`'s floor tier (plan to the native target only — the pre-LR-031 behaviour) | G4, G5 |

The teeth run is cheap relative to its value: it only needs the ~12 mapped cells, not the full
matrix, and it is the only evidence that the suite measures what it claims to.

---

## 11. Findings and expectations to settle before writing cells

These emerged from deriving the matrix's expected outcomes from the arithmetic. Each needs a
decision; some are likely bugs. **Do not write the corresponding cells until these are settled**,
otherwise the tests will simply encode the current behaviour. §11.1 is **settled and fixed**;
§11.2 and §11.3 are still open and still gate their cells.

### 11.1 The balance invariant was the *native-survival* bound, but gated the *auto-survival* path — **RESOLVED (LR-031)**

**Settled 2026-08-13; the fix has landed.** The analysis was confirmed in full. ADR-012 D2/D3 and
`internal/controller/site_rebalance.go` now carry the corrected two-threshold model, and the cells
this finding gated are unblocked — see *Resolution* and *Unblocked cells* below. The derivation is
kept because it is the reasoning those cells assert against.

**The finding.** `masterBalanceTarget(K) = ⌈K/2⌉ − 1` — the single pre-fix bound, since split into
`nativeSurvivalTarget` / `takeoverSurvivalFloor` — is exactly the condition for **native** survival
(`K − mᵢ ≥ q`). But ADR-012's own prose, and CLAUDE.md pillar 3.15, glossed the invariant as
"**no site holds a Redis master majority**" — which is `mᵢ ≤ ⌊K/2⌋`, i.e. the condition for
survival by *either* path (native, or the D1 fence-safe takeover). For odd `K` these coincide;
**for even `K` they differ by one**, and that difference lands squarely on the recommended
configuration:

| Topology | `q` | target `⌈K/2⌉−1` | `S·target ≥ K`? (warned, pre-fix) | floor `⌊K/2⌋` | `S·⌊K/2⌋ ≥ K`? (warns now) | Loss of the heaviest site actually… |
|---|---|---|---|---|---|---|
| **2 sites, K=4** (D3-recommended) | 3 | 1 | 2 < 4 → **warned** | 2 | 4 ≥ 4 → resilient | **`Takeover` fires** (2<3 and 2<3) |
| 3 sites, K=4 (the "headline trap") | 3 | 1 | 3 < 4 → **warned** | 2 | 6 ≥ 4 → resilient | **`Takeover` fires** (2<3 and 2<3) |
| 2 sites, K=3 | 2 | 1 | 2 < 3 → warned | 1 | 2 < 3 → not resilient | **`Refuse`** — the one regime that still warns |
| 3 sites, K=6 | 4 | 2 | 6 ≥ 6 → ok | 3 | 9 ≥ 6 → ok | native — agree |

Two concrete consequences, both confirmed against the pre-fix code:

1. **The warning fired on the recommended dual-site config**, and its message
   ("cannot auto-survive an arbitrary single-site loss … rely on manual /
   `allowUnsafeSiteTakeover` recovery") was **false** there — D1 auto-survives either loss with no
   opt-in. Same for 3-site `K=4`. That is alarm fatigue on precisely the topology we tell people
   to deploy. Affected cells **A8**, and the expected `ReducedSiteLossResilience` state of every
   2-site fixture.
2. **The rebalancer stopped correcting real drift.** `planMasterRebalance` returns `nil` when the
   threshold it is planning to cannot be reached — correct as written, but with target 1 it deemed
   `S=2, K=4` unreachable and therefore never corrected a **3+1** drift back to **2+2**, even
   though 2+2 is reachable and is exactly the state that keeps the takeover fence safe. A 3+1
   drift then turned a survivable site loss into a `Refuse`. Same for 3-site `K=4` (3+1+0 → never
   corrected to 2+1+1). Affected cells **G4**, **G5**.

**Resolution.** The proposed fix — keep `⌈K/2⌉−1` as the aspirational standard, fall back to the
majority bound `⌊K/2⌋`, and derive the warning from the majority bound — was adopted, with one
refinement: the fallback is **not** keyed on the strict target being *structurally* unsatisfiable,
it is a plain **two-tier plan**. That also fixes 3-site `K=4`, where the planner could reach
`2+1+1` in a single move but discarded it as partial churn. What landed:

- **Two named thresholds** (ADR-012 D2). `nativeSurvivalTarget(K) = ⌈K/2⌉ − 1` is the *standard*:
  breaching it only downgrades recovery from a native Redis election to an operator takeover — the
  `max(60s, 2×cluster-node-timeout)` cooldown plus a live hub — not an outage.
  `takeoverSurvivalFloor(K) = ⌊K/2⌋` is the *hard floor*: breaching it means that site's loss
  cannot be recovered unattended (drift-refusal; needs a human or `allowUnsafeSiteTakeover`). The
  derivation: native survival needs the **survivors** to keep quorum, takeover survival only needs
  the **lost side** to lack it — the survivors hold a blocking minority. They coincide for odd `K`
  and differ by exactly one for even `K`.
- **`planMasterRebalance` plans two-tier**: the move set reaching the target if one exists, else
  the one reaching the floor, else nil. Progress that reaches only the floor is real progress, not
  partial churn.
- **`siteLossResilient` keys off the floor** (`S·⌊K/2⌋ ≥ K`). Given Redis Cluster's three-master
  minimum, `ReducedSiteLossResilience` now fires in exactly **one** regime: **two sites with an odd
  `K`**. For `S ≥ 3` the floor is always reachable (`3⌊K/2⌋ ≥ K` for every `K ≥ 3`), so it never
  fires there.
- **`lrctl verify` distinguishes `[OVER FLOOR]`** (an actionable availability exposure) **from
  `[OVER TARGET]`** (a recovery downgrade, reported but not alarming). Any expectation phrased as
  `[IMBALANCED]` predates this.
- **The rebalancer is now active where it was inert** — `K=4` over 2 or 3 sites — and will issue
  graceful `CLUSTER FAILOVER`s there. It still emits nothing on a distribution already at the best
  reachable threshold, so the fence-safe steady states `2+2` (F2-K4) and `2+1+1` (F3-K4) stay
  quiet; only genuine drift past them moves.

Verified by unit/envtest — including a new **cross-seam property test binding the thresholds to
`planSiteTakeover`'s real decisions** — plus `make lint`. Live site-loss validation is still
pending, so the cells below remain the integration guard.

**Unblocked cells, with their corrected expectations:**

| Cell | Was (gated **†**) | Now |
|---|---|---|
| **A8** — kill the 2-master site of F3-K4 | contested; the warning implied a refusal | **`Takeover`**, no opt-in (`2 < q = 3` both ways) — F3-K4 sits *at* the floor |
| **G4** — drift to 3+1 on F2-K4 | predicted no move ⇒ drift persists ⇒ a later s1 loss refuses | over the floor ⇒ **one graceful failover restores 2+2**; the later loss takes over. `[OVER FLOOR]` before, `[OVER TARGET]` on both sites after |
| **G5** — drift to 3+1+0 on F3-K4 | same | target unreachable ⇒ floor tier ⇒ **one move restores 2+1+1** |
| `ReducedSiteLossResilience` on the 2-site fixtures | expected to warn on all of them | **F2-K4 / F2-K4R / F2-K6 (even `K`): no warning. F2-K3 (odd `K`): warns** — the only regime that does. `S ≥ 3` fixtures never warn |

### 11.2 Adding a site to a running instance — **RESOLVED (LR-032/LR-033)**

The original framing ("adding a site is probably refused, so in-place growth is impossible") was
**right in direction, wrong in conclusion, and the premise was the real finding**:

- **The refusal was mechanical, not designed.** Nothing in ADR-012, `MULTI_SITE_DESIGN.md` or
  `API_SPEC.md` ever mentioned changing the site set — it was never a stated requirement.
  Bootstrap is one-shot over the full pod set and every ADR-012 predicate is arithmetic over a
  fixed `(S, K)`. The LR-028 guard merely *responded*, because its predicate is dimension-blind
  ("observed but not desired") and it was built for shrink. A mechanism was being read as a
  capability.
- **"Most shards" overstated it.** `K=4`, `[A,B]`, `sps:2` pinned, add `C` ⇒ exactly **two**
  orphans out of 8 STSs. One is enough to trip the guard, so the practical conclusion held.
- **"Impossible" was wrong — and the counter-case was the bug.** `sitesPerShard` had no static
  CRD default; it resolved dynamically to `len(sites)`. So for a CR that never set it, adding a
  third site also took `sps: 2→3`, and `sps == S` means every site hosts every shard ⇒ desired is
  a strict **superset** of observed ⇒ **zero orphans, guard passes** ⇒ the operator stands up the
  new site's StatefulSets for an operation nobody designed, landing on the untested MEET/reattach
  path. **The guard refused the case that looks dangerous and permitted the one that doesn't.**

**Resolution:** the topology is declared a non-goal to mutate and enforced at **admission** —
four CRD CEL transition rules (`sites` incl. order, `sitesPerShard`, `shards` under `multiSite`,
adding/removing the block). Admission rather than a runtime refusal is the load-bearing choice: a
runtime detector would have to distinguish "a site was added" from "a site is temporarily
unobservable" — a failure-detection input, exactly what ADR-012 D1 forbids — and it runs *ahead
of* `recoverSiteLoss`, so a false positive would refuse during the very outage the takeover
exists to handle. LR-033 additionally made `sites` `listType=atomic` (order is load-bearing;
`listType=map` made CEL equality order-blind and let SSA silently reorder) and `sitesPerShard`
**required** (its old `len(sites)` default *multiplied copies* of a RAM-resident store on site
addition — the opposite of the design's own advice).

**Matrix consequences** — all applied: five cells collapse into the single negative cell **F1**;
the growth-convergence scenario is dropped as unreachable; the LR-028 cell survives as **F2**
(defence in depth, hand-triggered only); §2.1 records the negative-fixture rule this work
surfaced.

### 11.3 Cross-site replication degradation may not be surfaced (cell C5)

A replication-only cut leaves `cluster_state:ok` everywhere while cross-site replicas sit
`link:down` — a silently unreplicated cluster, which is the exact state that makes a later site
loss lossy. Confirm whether any condition/event/`lrctl verify` output surfaces it; if not, that
is a gap to file before writing C5.

### 11.4 Contested expectations (marked **†** in the matrix)

**None left.** Every cell now carries a derived expectation.

Settled since drafting: A13 (`Refuse` — derived, see the row); A8, G4, G5 (§11.1 / LR-031);
F4 (§11.2 / LR-032 — the cell no longer exists, it folded into F1); C15 (§11.5 — split into
C15a/C15b, and my original speculation was wrong). The **†** is dropped from all of them.

### 11.5 C15 — the non-transitive partition, derived (and my first guess was wrong)

The matrix originally predicted "both sides see `q=2` ⇒ cross-failovers via s2 ⇒ transient
dual-writer window". **That is wrong for a balanced cluster**, and the reason is worth writing
down because it generalises into an argument ADR-012 does not currently make.

**The FAIL-quorum rule.** A node is marked `FAIL` only by `markNodeAsFailingIfNeeded`, which
(i) returns immediately unless *the marking node itself* has the target timed out
(`if (!nodeTimedOut(node)) return;`), and (ii) requires `failures ≥ (size/2)+1` where the
failure reports are contributed only by **voting (slot-serving) masters** and `size` counts the
same. Gossip propagates *reports*, and a report exists only because its author independently
observed the timeout — a node that can still reach the target never generates one.

**Applied to F3-K3.** Placement (`sitesForShard(K,3,2)`) gives s1 = master(shard0) +
replica(shard2), s2 = master(shard1) + replica(shard0), s3 = master(shard2) + replica(shard1);
`K=3`, `q=2`. Cut s1↔s3, both reach s2:

- Only s1's masters time out s3's master, and vice-versa. **s2 times out nobody**, so it
  contributes no failure report in either direction.
- The count for "s3's master is failing" is therefore exactly `m_{s1} = 1 < q = 2`. **No FAIL.**
  Symmetrically for s1's master. No FAIL ⇒ no replica election (automatic failover requires the
  master in `FAIL`, not `PFAIL`).
- `clusterUpdateState` on s1 sees 2 reachable slot-serving masters (its own + s2's) `≥ q` ⇒
  `cluster_state:ok`; same on s3; s2 sees all three. **Nothing fail-stops, nothing elects.**
- The hub reaches every site, so no shard is orphaned ⇒ `planSiteTakeover` condition 1 fails ⇒
  **`None`**. The operator is correctly a spectator.

**The general theorem.** For a non-transitive partition in which only the masters of site set `P`
lose reachability to a target, FAIL requires `Σ_{i∈P} mᵢ ≥ q`. When exactly one site is cut off
from another, that is `mᵢ ≥ q` — precisely what the D2 **takeover floor** (`mᵢ ≤ ⌊K/2⌋ = q−1`)
forbids. So:

> **A cluster satisfying the D2 balance floor is immune to non-transitive partitions:** no single
> site holds enough voting masters to promote PFAIL→FAIL on its own, so the partition degrades
> replication without ever triggering an election.

That is a *second*, independent payoff of the balance invariant, alongside site-loss
survivability — and it converts the "legendary SDN failure" from a scenario family into one
inert cell plus one drift-conditional cell. It is also a sharper motivation for the rebalancer
than availability alone: **drift does not merely reduce site-loss resilience, it re-arms a whole
partition class.** Worth folding into ADR-012 D2 as a rationale bullet (a separate change; this
document only records the derivation).

**Verification tier.** This is derived from the documented FAIL-quorum rule and the two source
facts above. Per project habit for load-bearing Redis claims (cf. the `clusterUpdateState`
minority check), **confirm `markNodeAsFailingIfNeeded` against the Redis 8.4 tree when the cell
is written** — specifically the `nodeTimedOut` early return and that failure reports come only
from voting primaries. If either is false, C15a flips back to my original prediction and the
theorem collapses.

---

## 12. When we have more sites (future plan)

Deliberately out of scope now (lab capacity); recorded so it is not re-derived later. More sites
are only worth hardware if they test *different behaviour*, not just bigger numbers:

| Topology | What only it can test |
|---|---|
| **4 sites, K=4, `sps=2`** | Even site count: a 2-site simultaneous loss is an exact even master split ⇒ the D1 fence at *site-pair* granularity. Not reachable with 3 sites. |
| **4 sites, K=8** | Native target 3 (floor 4) with real freedom — the first topology where the rebalancer has multiple valid plans and determinism/minimality actually matter. |
| **5 sites** | Losing 2 sites still leaves a majority ⇒ the first genuinely "cloud-like" regime where the operator should essentially never be needed. Proves the takeover path is *rarely reached*, which is itself a claim. |
| **3 sites + a hub in a 4th domain** | The stated precondition for dual-site automation ("the hub must be a third failure domain"). Currently assumed, never tested. |
| **Asymmetric site sizes** (1-node site) | Placement and anti-affinity under a site that cannot spread a shard internally. |

Nearest cheap approximation with current hardware: shrink sites to 1 node each and run 4–5
single-node sites for the *arithmetic* cells (which do not need intra-site redundancy), keeping
the 3-node sites for the placement/anti-affinity cells. Worth prototyping before buying anything.

---

## 13. Summary of what to build first

1. ~~Settle §11.1 and §11.2~~ — **both done.** §11.1 (LR-031) *was* a real bug in the recommended
   topology; the two-tier thresholds landed with Tier-0 coverage, so A8/G4/G5 are writable and now
   serve as the live guard for a fix so far only unit/envtest-verified. §11.2 (LR-032/033) turned
   out to be a design non-goal plus a permissive-path bug, now closed at admission — it *removed*
   matrix work rather than adding it. **F1 is the cheapest cell in the whole document** (pure API
   calls, no outage, no fault primitive) — write it whenever, it needs nothing from steps 2–4.
2. Split-brain monitor (P11) + verdict assertion helper.
3. Frugal fixtures + per-fixture namespace-scoped operators.
4. The 15-minute smoke (A1 + C1).
5. **The MS-G cells (G1, G4, G5, G9) — promoted ahead of the rest of Tier 2.** The rebalancer
   executor is the largest never-executed surface in multi-site (see the MS-G preamble), and
   LR-031 just switched it on for the recommended dual-site topology. Needs only P3 + P4 and
   drift induction — no new fault primitives — so it is also the cheapest of the remaining work.
6. The rest of Tier 2 regression, matrix order.
7. Router-VM/netem primitives, then Tier 3.
8. Mutant catalogue + teeth run — the evidence that any of the above is worth trusting.
