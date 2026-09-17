# ADR-021: Label and Annotation Inheritance, and a Configurable App Name

## Status

Proposed. Implements [issue #96](https://github.com/littlered-operator/littlered/issues/96)
("Configurable `kubernetes.io/name`"). Additive and backwards compatible: `spec.appName`
defaults to the previous constant, so an existing instance's selectors are byte-identical
after upgrade.

> ADR number: 021. Authored as 015 on a branch cut before ADR-015 (per-instance Sentinel
> master name), 016, 017, 018 and 020 landed on the mainline; renumbered on merge. 010
> (ghost-replica prune), 012 (multi-site) and 019 remain unclaimed.

## Context

Every label on a resource the operator owns is currently authored by the operator.
`commonLabels` stamps five keys (`app.kubernetes.io/name`, `/instance`, `/managed-by`,
`/version`, `redis.chuck-chuck-chuck.net/mode`) and the selector helpers stamp
`app.kubernetes.io/name` + `/instance` + `/component` (+ `shard` / `role`). The only user
input is `spec.podTemplate.labels`/`annotations`, which reach **pods only**, plus
`spec.service.labels`/`annotations` and `spec.metrics.serviceMonitor.labels` for their
respective objects.

Two gaps follow from that, both reported in #96:

1. **The app name is not configurable.** A user whose monitoring groups workloads by
   `app.kubernetes.io/name` cannot make a LittleRed instance appear under their own
   application name — it is always the literal `littlered`.
2. **Ordinary metadata does not propagate.** Labels like `team=payments` or
   `environment=production`, set on the `LittleRed` resource where an operator user
   naturally puts them, do not reach the pods, Services or ServiceMonitor that a scrape
   config actually selects on. Every use case would otherwise need its own spec knob.

There is also a latent defect. `spec.podTemplate.labels` is merged **last**:

```go
maps.Copy(podLabels, redisSelectorLabels(lr))
maps.Copy(podLabels, lr.Spec.PodTemplate.Labels)   // user input wins
```

So a user *can* already set `app.kubernetes.io/name` on pods — and thereby make the pod
template disagree with the StatefulSet's own `spec.selector`, which the API server rejects
outright (`selector does not match template labels`). The instance stops reconciling with
an error pointing at the StatefulSet, not at the CR field that caused it.

### Why the naive reading of "user labels win" cannot be implemented

The obvious design — inherit everything, let explicit user values beat operator defaults —
founders on one Kubernetes constraint: **`StatefulSet.spec.selector` is immutable.** The
labels in it are not decoration; they are how the workload finds its pods. Letting user
input change them has three failure modes:

| If the user changes… | Result |
|---|---|
| a selector label at creation | works, as long as pod labels and selector agree |
| a selector label later | StatefulSet update rejected; instance stops reconciling |
| a selector label to a value that collides with another instance | two workloads fight over the same pods |

And because storage is EmptyDir (pillar 3.1), the escape hatch for an immutable selector —
delete and recreate the StatefulSet — **discards the data**. So a design that permits
selector-label edits converts a label typo into data loss.

## Decision

Split the metadata into what the operator owns and what the user contributes, and inherit
the latter.

### 1. Inheritance

Labels and annotations on the `LittleRed` resource are inherited by **every** resource the
operator owns: StatefulSets, Services, ConfigMaps, PodDisruptionBudgets, the ServiceMonitor
and the pod templates. Object-level metadata gets them via `commonLabels` /
`inheritedAnnotations`; pods via `podTemplateLabels` / `podTemplateAnnotations` (the
operator owns no object-level annotations of its own — only pod-template ones).

Precedence, least to most specific:

```
inherited from the LittleRed resource
  → operator-owned keys            (always win)
  → spec.podTemplate.labels        (pods; structural keys dropped)
  → spec.service.annotations/labels, spec.metrics.serviceMonitor.labels (their object only)
```

### 2. Operator-owned keys are never inherited

Two tiers, both excluded from inheritance:

- **Structural** — `app.kubernetes.io/name`, `/instance`, `/component`,
  `redis.chuck-chuck-chuck.net/shard`, `/role`. These constitute selectors. Additionally
  rejected in `spec.podTemplate.labels` by a CRD CEL rule, so the failure lands on the CR
  field the user edited instead of on a StatefulSet apply.
- **Descriptive** — `app.kubernetes.io/managed-by`, `/version`. The operator keeps these
  current (`/version` tracks `spec.image.tag`); a user value would go stale.

Everything under the operator's own key prefix `redis.chuck-chuck-chuck.net/` is excluded
too, which covers `mode`, the config/pod-spec hashes and the debug annotations.

### 3. Tool bookkeeping is not inherited

Keys under `kubectl.kubernetes.io/`, `argocd.argoproj.io/`, `meta.helm.sh/`, `helm.sh/`,
`kustomize.toolkit.fluxcd.io/` and `helm.toolkit.fluxcd.io/` do not propagate. These are
stamped by whatever applied the CR and describe *that* relationship, not the children:
Argo CD's tracking labels on a child confuse its own pruning, and
`last-applied-configuration` would embed a full copy of the CR into every child object.

### 4. `spec.appName`, immutable

A first-class field supplies the `app.kubernetes.io/name` value (default `littlered`), and
is threaded through **every** selector helper as well as `commonLabels`, so the label and
the selectors can never disagree. It is immutable, enforced by a CEL transition rule
(`self == oldSelf`) — no webhook required.

Immutability is the honest constraint, not a limitation of the implementation: the value
lands in `StatefulSet.spec.selector`, so changing it on a live instance is precisely the
un-performable update described above. Rejecting the edit at the CR costs the user a clear
error message; permitting it would cost them their data.

## Consequences

**Editing CR labels or annotations rolls the pods.** Pod labels live in the pod template,
and Kubernetes has no in-place pod-label update through a StatefulSet — a template change
is a rolling update. Per mode: cluster rolls shard by shard (serialized, LR-021, and
within a shard one pod at a time on state, LR-047); sentinel fails over; failover mode
fails over too, with the operator performing the handover itself and fencing the outgoing
master (LR-038); **standalone restarts its single pod, which discards the data** (EmptyDir).
Object-level metadata (Services, ConfigMap, PDBs, ServiceMonitor) changes in place with no
restart. Users who annotate CRs frequently should prefer annotations that they are content
to see roll the workload, or set them on a wrapper object instead.

**A stray label on the CR now reaches production objects.** That is the point of the
feature, but it means CR metadata is no longer inert. The skip-list keeps the common
tooling cases out; anything else propagates.

**`spec.podTemplate.labels` becomes stricter.** Setting a structural key there is now
rejected by CRD validation. Any CR doing so today is already broken (its StatefulSet is
being rejected), so this converts a confusing runtime failure into an actionable one; it
does not break a working configuration.

**Sentinel pods gain user labels.** `buildSentinelStatefulSet` never applied
`spec.podTemplate.labels`, unlike the other three builders. Routing it through the shared
helper fixes that inconsistency, at the cost of one rolling restart of the sentinel pods for
an instance that sets those labels.

## Alternatives considered

**Decouple the selectors from `app.kubernetes.io/*` entirely** — move selectors onto
operator-owned `redis.chuck-chuck-chuck.net/*` keys so every `app.kubernetes.io/*` label
becomes freely settable *and* mutable. This is the cleanest end state and was declined only
on migration cost: changing a live StatefulSet's selector requires an orphan-delete and
re-adopt dance (delete with `--cascade=orphan`, re-label pods, recreate the StatefulSet to
adopt them) for every existing instance, which is ADR-013-scale work. Worth revisiting if
mutable app names are ever asked for.

**Reserve all five structural keys and ship no `appName`** — additive inheritance only.
Simplest and safest, but does not deliver #96's actual request.

**Propagate CR metadata verbatim, no skip-list** — the most literal reading of the issue
discussion. Rejected: Argo CD tracking metadata on children is actively harmful, and
`last-applied-configuration` propagation would bloat every owned object.

## Verification

- Pure filter and precedence rules: `internal/controller/metadata_test.go` (table-driven,
  written before the implementation and observed red).
- The two CEL guards: `internal/controller/metadata_envtest_test.go`, against a real
  API server. Both were confirmed red by stripping the rules from the generated CRD —
  the immutability spec failed with "Expected an error to have occurred" and the
  structural-key spec with "expected app.kubernetes.io/name to be rejected" — then green
  with the rules restored.
- Inheritance reaching real objects: `TestBuildersCarryInheritedMetadata` enumerates
  **every object-producing builder in every mode**, not one builder per kind. The
  per-kind sampling it replaced is what let failover mode ship uncovered — see the
  addendum below.
- `test/e2e/metadata_test.go` (label `metadata`) asserts the round trip on a live cluster,
  **in all four modes**: pods findable by an inherited label alone, and a custom
  `spec.appName` instance reaching `Running` with matching Service endpoints.
  **Executed on t3e, 2026-09-17: `6 Passed | 0 Failed`** in 108s.

  The failover tier banked an honest RED first, against the pre-fix build deployed to the
  same cluster: the instance reached `Running` and then **no pod was findable by the
  inherited label** for the full 120s window (`[]v1.Pod | len:0 ... not to be empty`),
  which is issue #96's own symptom. Green on the fixed build.

  Two defects in the e2e itself were found by running it for the first time, both in
  never-executed code: it looked the Redis StatefulSet up as `<name>` where the builder
  names it `<name>-redis`, and its sentinel-mode CR carried no `sentinel.masterName`, which
  is required per pillar 3.7 — that instance sat at an empty phase until the 5-minute
  timeout. Worth stating plainly: a test that has never run is not coverage, and these two
  would have failed on any cluster, at any time, for reasons unrelated to the feature.

## Addendum (2026-09-17): failover mode, and why the per-kind test missed it

This ADR was authored on a branch cut before `failover` mode existed, so it reasoned about
three modes and wired three modes' builders. `resources_failover.go` arrived on the
mainline afterwards, and on merge its three builders (`buildConfigMapFailoverMode`,
`buildRedisStatefulSetFailover`, `buildFailoverRedisPDB`) were **half-covered by
accident**: they call `commonLabels`, so object *labels* inherited from day one, while
object annotations and the entire pod template did not. A failover-mode pod could not be
found by an inherited label — the exact thing issue #96 asked for.

`buildFailoverRedisPDB` was correct throughout, because it is one line delegating to
`buildSentinelRedisPDB`. That is the shape worth copying: a mode that *reuses* a builder
gets the behaviour, a mode that *re-implements* one does not, and re-implementation is
invisible at review time.

**The test is the finding, not the fix.** `TestBuildersCarryInheritedMetadata` was written
to cover "one builder per kind", which is a reasonable-sounding sampling rule that is
structurally unable to see this class: `buildStatefulSet` stood in as *the* StatefulSet, so
three other StatefulSet builders were never asked the question. It is now exhaustive per
mode. A builder added to any mode from here fails it until it is wired — which is the only
version of this check that survives the next mode being added.

Widening the test also surfaced a second, pre-existing gap it had not been looking for:
`buildReplicasHeadlessService` and `buildSentinelHeadlessService` declared
`var annotations map[string]string` and populated it only when metrics were enabled, so
neither inherited CR annotations in either sentinel or failover mode. Both now seed from
`inheritedAnnotations` and layer the `prometheus.io/*` keys over it, per the precedence
this ADR already specifies.

Generalizable, and the same shape as LR-041 and LR-052: **a check whose coverage is defined
by sampling reports green about the thing it never sampled.** The three builders were not
missed by a wrong decision — nobody ever decided anything about them, because the branch
predated them and the test could not notice.
