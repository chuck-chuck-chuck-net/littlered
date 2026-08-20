# ADR-015: Per-Instance Sentinel Master Name (Required, No Default)

## Status

Accepted and implemented (branch `fix/sentinel-master-name-scoping`, cut from `main`). Breaking:
`spec.sentinel.masterName` is required on create. Changelog entry **LR-039**; incident analysis
`docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md`.

> ADR number: 015, not 010–012 (claimed on sibling branches: 010 ghost-replica prune, 011 failover
> mode, 012 multi-site site-loss takeover). Likewise LR-039 rather than LR-026 — IDs are allocated
> globally so they survive a merge, at the cost of chronological order within one branch's file.
>
> **Cross-branch references.** This ADR cites **LR-033** (multi-site line) and **ADR-011** (failover
> mode), neither of which exists on `main` yet. The arguments that depend on them quote the relevant
> reasoning inline, so nothing here rests on a document the reader cannot open; the citations are
> provenance, not load-bearing.

## Context

Two unrelated sentinel-mode instances, in one namespace on a shared pod network, **merged into a
single Sentinel quorum** in production. The larger instance's configuration won on config epoch and
reassigned the smaller instance's master to a Redis pod belonging to the *other* instance; the
demoted master was told `SLAVEOF` and **flushed its dataset**. The instance was unrecoverable for
13+ hours and no healing rule fired — not in the deployed v0.2.1, and not on `main`.

This is the project's first **safety** failure. Every prior LR entry is a liveness failure — a
deadlock, a stall, a refusal — whose surviving invariant is *we never serve the wrong data*. Here
that invariant broke, and the only reason it was loud is that the two instances ran different Redis
majors, so the cross-instance replication failed on an RDB format mismatch. On matched versions the
merge completes silently: the victim reports healthy, with a unanimous and self-consistent Sentinel
view, while serving another tenant's keyspace.

**The enabling condition was ours.** `SentinelMasterName` was a package constant (`"mymaster"`)
applied to every instance the operator manages, and the master name is the *only* isolation
Sentinel's gossip protocol has:

```c
/* sentinelProcessHelloMessage(), redis 7.4 src/sentinel.c */
master = sentinelGetMasterByName(token[4]);
if (!master) goto cleanup; /* Unknown master, skip the message. */
```

No instance identifier, no namespace, no authentication between Sentinels beyond the optional
password. Two deployments sharing a name and able to reach each other are, protocol-wise, one
deployment. A hand-rolled Sentinel has an admin who typed `sentinel monitor mymaster …` and could
have typed something else; LittleRed's users never see that line — we chose the value for them.

**The governing constraint on any fix:** the invariant is *"unique among Sentinel deployments
reachable on this pod network"* — a property of the network, not of the object. Admission validation
can force the decision to be **visible**. It cannot force it to be **correct**. Every design below
is judged against that line, and several rejected alternatives fail precisely by pretending to cross
it.

## Decision

1. **`spec.sentinel.masterName` is required**, `MinLength=1`, `MaxLength=128`, pattern
   `^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$`. Comma and whitespace are excluded because the
   Sentinel hello payload is comma-separated and `sentinel.conf` is space-separated — either would
   corrupt the wire format or the config file. Recommended value `<namespace>.<name>`.

2. **No default — neither static nor derived.** A static CRD default cannot express
   `<namespace>.<name>`; a derived one is rejected outright (Alternative A).

3. **`mymaster` remains a legal explicit value.** Two independent reasons: it is the current value
   of every existing instance, so forbidding it makes the pre-migration state inexpressible and the
   only upgrade path becomes delete-and-recreate — the total data loss the migration exists to
   avoid; and a legacy client may hardcode it with no way to parameterise it.

4. **Objects predating the field fall back to a fixed constant** (`LegacySentinelMasterName`) via a
   pure accessor that never writes to the spec, and surface a **`SentinelMasterNameUnscoped`**
   warning condition plus a one-shot event — never a refusal (the opt-in-not-block norm, as
   `ReducedSiteLossResilience`). The condition fires **only when the field is unset**; setting
   `mymaster` explicitly is a decision and is not second-guessed.

5. **Plain `required`, not a spec-level CEL rule.** Measured, not assumed (see Rationale).

6. **Authentication is strongly recommended in sentinel mode, not mandated.** It is the only thing
   that closes the address-adoption path (Consequences), and the same password is Sentinel's
   peer-membership credential, not merely a client-edge control. It stays opt-in because the network
   trust model is the platform's call — but a safety boundary that a user can switch off by leaving
   a default alone cannot be *the* boundary, which is why the naming decision carries that weight
   instead.

7. **`redisclient.SentinelMasterName` is deleted, not left unused**, so the compiler locates every
   call site and no future code can silently reintroduce a shared name.

8. **A mutating admission webhook is deferred, not rejected** — see "Deferred: the mutating
   admission webhook".

## Rationale

### Why plain `required` and not CEL — measured across six API server versions

A throwaway envtest probe (the technique LR-033 used to verify its in-place CRD upgrade) installed a
lax CRD, created an object omitting the field, tightened the CRD in place, and attempted the four
writes that matter. The in-place tighten was accepted in every case. The governing mechanism is
**CRD validation ratcheting**, which skips validation at schema locations whose value is unchanged —
and whose rollout differs for plain `required` and for CEL.

**Spec-level CEL rule:**

| kube-apiserver | operator `/status` | user edits spec | CREATE without field |
|---|---|---|---|
| 1.29 – 1.32 | **REJECTED** | REJECTED | REJECTED |
| ≥ 1.33 | ACCEPTED (+ API warning) | **REJECTED** | REJECTED |

**Plain `required`, nested in the optional `spec.sentinel`:**

| kube-apiserver | operator `/status` | user edits spec (sentinel block untouched) | CREATE without field |
|---|---|---|---|
| 1.29 | ACCEPTED | REJECTED | REJECTED |
| ≥ 1.30 | ACCEPTED | **ACCEPTED — silently** | REJECTED |

Three findings, none of them what either candidate assumed:

- **The pessimistic assumption was wrong for `required`.** Status writes go through as far back as
  1.29, because status is a subresource. The operator never wedges.
- **It was right for CEL below 1.33**, where the rule rejects the operator's own status write and
  stops reconciliation of every existing instance — a *silent* loss of management, since the
  instances keep serving. That is the exact opposite of the "hard failures are the only ones
  noticed" argument that motivated failing loud.
- **Ratcheting means nested `required` forces the decision on new instances only.** On ≥1.30 a
  `helm upgrade` touching anything other than the sentinel block is accepted and the instance keeps
  running unscoped.

CEL would deliver the stated intent — but only by declaring a **minimum Kubernetes 1.33**, which the
project does not state anywhere today. We chose the option that never wedges anything on any
version, and gave the loudness for the installed base to the runtime condition instead. We are not
better placed than the Kubernetes maintainers to decide what the correct behaviour of a
newly-required field is on update; we document what it does.

### Why the runtime condition is load-bearing rather than decorative

It is the *only* signal a pre-existing instance ever gets, precisely because ratcheting puts those
instances beyond validation's reach. Warning on **unset only** keeps it honest: it reports the
absence of a decision, not a decision we dislike.

### Verification

Red-first at three tiers, each observed failing for the defect's reason before the fix: the accessor
(`= "" want "team-a.cache"`), the CRD schema (five negative specs, `Expected failure, but got no
error` — which also exposed a pre-existing false pass, an older negative spec being rejected for the
new field rather than the mode mismatch it tests), and e2e against `main`'s operator, where the
capture reproduced in 45 s with `config-epoch: 9999`, B's replicas adopted, and the stolen master
reported `flags: master` — healthy, hence never failed over. Details in LR-039.

## Consequences

- **Breaking and client-visible.** Sentinel-aware clients carry the master name, so changing it
  requires reconfiguring them. Clients using the label-routed `{name}` Service are unaffected, which
  may be most of them.
- **No rolling cutover exists.** Monitoring one master under two names is accepted by Sentinel
  (its duplicate check is name-keyed) but runs two independent failover state machines that can
  promote different replicas; neutering one makes it serve a permanently stale master. So the
  migration is a coordinated outage — its value is *"your data survives it"*, as against
  delete-and-recreate, which loses everything. Since the outage is unavoidable, the guidance is to
  enable authentication in the same window.
- **Existing instances keep running** and are forced to state a name only on their next change to
  `spec.sentinel`. Verified by measurement, not assumption.
- **Downgrade is loud.** `kubectl apply` defaults to strict field validation, so against an older
  CRD a CR carrying `masterName` is rejected (`strict decoding error: unknown field`) rather than
  silently pruned back to the shared name.
- **Not closed: the address-adoption path.** A unique name ends *gossip* fusion. It does not stop a
  foreign instance whose dead master's IP was recycled onto **our** master from monitoring our
  master directly, reading its `INFO`, adopting our replicas and issuing `SLAVEOF` to them — no
  hello, so the name is never consulted. Only distinct passwords close it. This is the concrete
  reason auth is recommended rather than merely nice.
- **A captured instance is not recovered automatically, by decision.** It stays at `Ready=False` /
  `Initializing` — loud to ordinary alerting — until a human runs the runbook, and comes back
  empty. Automated recovery was designed and then **declined**, not deferred; see Alternative J,
  which is the one rejection where building the thing would have made matters actively worse.
- **Self-capture on delete-and-recreate is accepted.** A terminating previous generation holds a
  higher epoch and can repoint the fresh one — but at a *dead* address, so the `SLAVEOF` never
  completes a sync and no flush occurs. That degrades to LR-024's ghost-master deadlock: a liveness
  failure we already recover. Severity scales with whether the capturing party has a **live**
  master, which is why a stable, chart-templatable name beats a random one.

### This is a sentinel-mode problem, and that pattern is now hard to ignore

Recorded as an observation, not a roadmap. Sentinel mode has required a long chain of operator
compensation — LR-001, 004, 005, 007, 008, 011, 013, 015, 016, 017, 024 and now 038 — and the
recurring shape is that Sentinel is a second, autonomous decision-maker whose failure modes the
operator must detect and undo. This entry is the first where that cost is paid in *data* rather than
availability.

The alternatives are structurally different, not merely newer:

- **Cluster mode** has no shared-name gossip channel. Membership is by explicit `CLUSTER MEET` over
  a dedicated bus, so there is no analogue of a stranger's hello being accepted on name alone. It is
  not immune to IP recycling in general (a stale `Status.PodIP` from the informer cache could be
  MEETed — untested, tracked in the analysis doc's cross-mode audit), but the fusion-by-name class
  does not exist there.
- **Failover mode (ADR-011)** is structurally immune to *this* class: role intent is stamped by the
  operator into pod annotations and read through a downward-API volume, and there is no
  peer-to-peer topology protocol available to capture. This is a genuine argument in that mode's
  favour that ADR-011 does not currently make, and it belongs in its graduation discussion.

**This ADR does not deprecate sentinel mode, nor promise promotion of any alternative.** Sentinel
remains fully supported. What it records is that operators choosing between modes should weigh this
class, and that if the pattern continues, the mode question — not another compensating rule — is the
one worth reopening.

## Alternatives considered

### A. A derived default in Go (`<namespace>.<name>` in `SetDefaults`) — rejected

The most tempting option, and rejected on LR-033's ruling: *"a **static** default is applied by the
API server at create time and is harmless; a **derived** default cannot be, and re-imports all four
problems."*

Two of those four survive here even though the inputs are immutable (`metadata.namespace` and
`metadata.name` cannot change), because they concern where the derivation *lives*, not what it
reads:

- **Stored spec diverges from effective value** — worse here than in LR-033, where it hid a sizing
  knob. Here the effective value is the literal string an application must put in its connection
  config, so a reader of the CR cannot configure a client.
- **A change to the derivation between operator versions** would silently rename the master of every
  instance that left the field unset, breaking every Sentinel-aware client on an upgrade with no
  user action to correlate the outage to. Immutable inputs do not make the *function* immutable.

And the write-back is not hypothetical: `littlered_controller.go` calls `SetDefaults()` on the
fetched object and `r.Update()`s it fifteen lines later for the finalizer, so anything added there is
persisted into the user's spec on first reconcile.

### B. A static CRD default — not available

`default:` is a literal JSON value; it cannot express `<namespace>.<name>`. A single constant default
is what we already had.

### C. Reject the literal value `mymaster` at admission — rejected

Superficially attractive: it is the one value *known* to be dangerous, and it is the value in every
upstream tutorial. Rejected on two grounds.

It **defeats the migration**: every existing instance's current value is `mymaster`, so if that is
inexpressible the pre-migration state cannot be declared and the only upgrade path is
delete-and-recreate. The ban would destroy the data the fix exists to protect.

It is also a **folk rule rather than the invariant**. `mymaster` is not uniquely dangerous, only
popularly so; two instances both named `redis` or `cache` collide identically and pass validation.
It would hand users a validation error that reads like a safety guarantee while the general case
walks past — precisely the "validation cannot enforce correctness" line above.

### D. An `allowBrokenDefaultMasterName` opt-out — rejected

It is a different animal from `allowUnsafeRebootstrapOnDeadlock` / `allowUnsafeSiteTakeover`. Those
authorise an *operator action* whose safety the operator cannot establish, at the moment of the
action. This would disable a validation, and the knowledge being asserted — "nothing else shares my
pod network" — is usually unknowable and **becomes false without the asserting party doing
anything**. A standing risk acceptance that expires on a third party's deploy is the worst kind.
Unnecessary anyway: the value being legal *is* the escape hatch (Decision 3).

### E. Controller-side collision detection — rejected

"Two managed sentinel-mode instances with the same effective `masterName`" is the real invariant,
checked against reality rather than a string blacklist, and it would have caught this incident. Still
rejected:

- **Its coverage story is misleading.** It can never see a Sentinel deployment we do not manage, so
  silence reads as an all-clear it cannot give — a false sense of safety in exactly the dimension
  that matters.
- **Its unique value is thin.** With the name required and `<namespace>.<name>` documented,
  same-operator collisions largely stop by construction, leaving only deliberately identical values,
  most of which the `mymaster` warning already covers.

The capability belongs in `lrctl verify` as **diagnosis** instead: report the effective master name,
the Sentinel-known sentinel/replica counts against what we deployed, and any known address that is
not one of our pods. A tool run when someone is already suspicious makes no safety claim by being
silent. (`num-other-sentinels: 8` on a three-sentinel instance was the loudest signal in the
incident dump, and nothing surfaced it.) **Implemented** — `SentinelClusterState.DetectCrossInstance`
plus reporting in `verify`'s text and JSON output. Wiring it exposed that the `lrctl` gatherer
fabricated replica flags (`"found"` / `"s_down,ghost"`) where the operator-side gatherer keeps what
Sentinel actually reported, which would have made the diagnostic structurally unable to work.

### F. A spec-level CEL rule requiring the field — rejected

Delivers the stated "fail loud on the next CR edit" intent, but only on Kubernetes ≥ 1.33; below that
it rejects the operator's own status writes and silently stops managing every existing instance. It
would require declaring a minimum Kubernetes version the project does not currently state. See
Rationale for the measured table.

### G. Required on create only (`optionalOldSelf`) — rejected

`oldSelf.hasValue() || has(self.masterName)` expresses "required on create, tolerated on update",
which never wedges an existing object. Rejected as a decision, not a mechanism: it is precisely the
leniency-on-upgrade that was ruled out, arriving through a more sophisticated door. It also needs
Kubernetes ≥ 1.30.

Worth noting honestly: **ratcheting means plain `required` behaves almost identically in practice**
(new instances forced, existing ones untouched until they edit the sentinel block). The difference is
that plain `required` gets there by the platform's own semantics rather than by us encoding an
exemption, and it carries no version floor.

### H. Do nothing — "Sentinel's protocol, the platform's network, the user's configuration" — considered, rejected

A legitimate position, and it was argued: Sentinel's isolation model is upstream's design, the pod
network's trust model is the platform's, and an operator should not try to fix another project's
protocol semantics (pillar 3.5 — enablement over intervention).

Rejected because of one distinction. The framing works when the user made the configuration choice.
**LittleRed selected `mymaster` on the user's behalf and did not tell them** — our users never see the
`sentinel monitor` line. Pillar 3.5 is about not fighting Redis's *mechanisms*; it is not a licence to
make a configuration decision for someone and then assign them the consequences. Two further points
weighed: pillars 3.1 and 3.7 (no persistence, IP-only identity, peer sets relearned from gossip on
every restart) deliberately *widen* the window the protocol is weakest at; and the failure is
cross-tenant, silent on matched versions, and unrecoverable — the party we would be assigning it to
cannot see it happen.

**Where the position holds and was adopted:** the network trust model. Whether the pod network is
shared with untrusted neighbours, and whether auth is on, is the platform's call — hence Decision 6
recommends rather than mandates.

### I. Authentication as the primary boundary — rejected as primary, adopted as secondary

Auth genuinely closes both the observed path and the address-adoption residual, and it is why
co-located instances *with* auth were never hit. But it is a user-facing optional field: a safety
boundary that can be switched off by leaving a default alone is not a boundary. It is also equally
client-breaking on its own, so it is no cheaper than the rename. Adopted as a strong recommendation
(Decision 6) and paired with the rename in one maintenance window.

### J. Automated recovery for an already-captured instance — rejected

The obvious follow-up, designed in full before being dropped: split LR-024's
`HasHealthyKnownReplica` veto on whether the ghost master is flagged down — `s_down`/`o_down` is
LR-024's dead ghost and the veto is correct; clean flags mean **captured**, where the veto's
premise ("a promotable replica means a failover is imminent") is false because the stolen master
looks perfectly healthy and Sentinel will never act. Then elect a survivor and re-`MONITOR`. The
discriminator is already on the wire (`SENTINEL master` returns `flags`; the gatherer just does not
retain them), and the predicate is admissible — "the monitored master is a live address that is not
one of my pods" is a *configuration* judgement from the Kubernetes pod list, not failure detection.

Rejected on two grounds, **neither of which depends on the deployment being misconfigured**.

**There is nothing left to salvage.** The flush happens when the replica starts its full sync,
about a second after the `SLAVEOF` — long before any reconcile could observe the capture (the
neighbouring rules carry 30–120 s cooldowns precisely so they do not steal legitimate failovers).
All three pods in the incident ended at `slave_repl_offset:1`. So recovery restores an *empty*
instance, which is exactly what deleting and recreating the CR already achieves at no engineering
cost and with no new code in the sentinel healing loop. The one path where data might survive —
replication blocked before the sync begins — is unreachable: the capture requires the gossip to be
accepted, which requires a shared or absent password, which is the same condition under which the
replication auth also succeeds. The RDB version mismatch seen in the incident does not help,
because it fails *after* the flush.

**The operator structurally cannot win the reclaim.** `createSentinelRedisInstance` initialises
`ri->config_epoch = 0` (sentinel.c:1304), so a `SENTINEL MONITOR` issued by the operator creates
the master entry at **epoch 0**. Against a merged population still holding the captor's epoch that
loses on the next hello, ~2 s later — and the operator has no way to raise a config epoch, because
only a genuine failover election does. The rule would reissue `REMOVE` + `MONITOR` every reconcile,
forever, never converging. And each `REMOVE` + `MONITOR` wipes that sentinel's replica list, which
is **LR-013's hazard exactly** — the mechanism that produced a permanent `no-good-slave` failover
deadlock. Its failure mode is therefore to convert a broken-but-static instance into one thrashing
its own topology, while two instances sharing a name ping-pong it between them.

A rule that provably cannot win, added to the chain that already produced LR-001/007/011/013/024,
is a liability rather than defence in depth — and one whose code path would never execute in a
correct configuration, so it would sit permanently unexercised in the most fragile part of the
system. What it would insure against — some future route to "Sentinels monitor a live non-pod
master" that is not a name collision — has no known instance; the address-adoption path does not
produce it, since there our Sentinels still monitor our own master correctly.

**Adopted instead, and both now implemented:** the `lrctl verify` diagnostic of Alternative E, and
the runbook in `USAGE.md` stating plainly that the instance returns empty. Detection is not the gap: a captured instance sits at `Ready=False` and ordinary alerting
catches it.

## Deferred: the mutating admission webhook

**Not rejected — deferred until re-litigated.** A mutating webhook stamping `<namespace>.<name>` into
the stored spec on CREATE when the field is absent is the clean way to get what Alternative A gets
wrong: it makes the value **static from creation**, exactly like an API-server default, so divergence
and derivation-drift both vanish and new instances are safe without the user knowing anything. It is
the "help people do the right thing" option and it remains on the table.

Reasons not now:

- **It can be skipped, and a skipped defaulting webhook silently reproduces the hazard.**
  `failurePolicy: Ignore` during an outage, an expired cert, a stale CA bundle, a
  `MutatingWebhookConfiguration` that was never applied — any of these yields an instance running
  unscoped with nobody informed. That is the worst outcome in the design space. `failurePolicy: Fail`
  trades it for putting the webhook on the critical path of every CR write.
- **Therefore it never replaces the requirement, only softens it.** Mutating admission runs before
  schema/CEL validation, so the correct composition is webhook *plus* the required field, and the
  required field is doing the load-bearing work either way. The webhook buys ergonomics, not safety.
- **Cost and failure surface.** The project has no webhook today: no `config/webhook`, no
  `config/certmanager`, no `PROJECT` entry, only kubebuilder's stock server boilerplate. It needs a
  Service, a `MutatingWebhookConfiguration`, CA-bundle injection and cert rotation, chart wiring, and
  a `failurePolicy` decision — a permanent operational surface, and the operator's first.
- **Couplings.** It is cluster-scoped infrastructure, which interacts with the namespace-scoped
  operator work (ADR-014), and with the multi-site hub deploy wiring.
- **Awkwardness.** A webhook-defaulted field appears in the live object but not the applied manifest,
  so GitOps tooling reports drift unless explicitly configured to ignore it — trading one confusing
  behaviour for another.

### When to revisit

Reopen if any of these becomes true:

1. **Collisions keep happening** — a second incident, or field reports of instances running unscoped
   because someone did not read the requirement. That is the signal that "forced to state a value" is
   not enough and "given a safe value" is needed.
2. **A significant population never migrates** — many instances sitting on the
   `SentinelMasterNameUnscoped` warning long-term, since ratcheting means nothing forces them.
3. **A webhook is introduced for another reason.** Once the cert and deployment machinery exists, the
   marginal cost of this defaulter drops to near zero and the calculus changes.
4. **Kubernetes gains templated CRD defaults** (or an equivalent), which would make the webhook
   unnecessary rather than merely deferred.

If revisited, the constraints are fixed: **stamp on CREATE only** (stamping on UPDATE would silently
rename an existing instance's master and break its clients), and **keep the required field
underneath** so a skipped webhook degrades to a loud rejection rather than a silent wrong value.

## References

- `docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md` — incident analysis, annotated timeline, the
  Sentinel source citations, and the measured ratcheting matrix.
- `docs/RECONCILIATION_ALGORITHM_CHANGELOG.md` — **LR-039**; and LR-033 (derived defaults), LR-024
  (ghost-master recovery and the veto this does not yet split), LR-004/005/008 (why every existing
  rule short-circuits when `RealMasterIP == ""`).
- ADR-001 (strict IP-only identity — the assumption whose cross-tenant face this is), ADR-011
  (failover mode), ADR-014 (namespace-scoped operator).
- Redis `src/sentinel.c` @ 7.4 — `sentinelProcessHelloMessage`, `sentinelPublishCommand`,
  `sentinelSendPeriodicCommands`, `sentinelSendAuthIfNeeded`.
