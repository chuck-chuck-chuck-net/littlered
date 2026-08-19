# ADR-015: Label and Annotation Inheritance, and a Configurable App Name

## Status

Proposed. Implements [issue #96](https://github.com/littlered-operator/littlered/issues/96)
("Configurable `kubernetes.io/name`"). Additive and backwards compatible: `spec.appName`
defaults to the previous constant, so an existing instance's selectors are byte-identical
after upgrade.

> ADR number: 015. 010–012 are claimed on sibling branches (010 ghost-replica prune, 011
> failover, 012 multi-site).

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
is a rolling update. Per mode: cluster rolls shard by shard (serialized, LR-021); sentinel
fails over; **standalone restarts its single pod, which discards the data** (EmptyDir).
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
- Inheritance reaching real objects: `TestBuildersCarryInheritedMetadata` covers one
  builder per kind.
- `test/e2e/metadata_test.go` (label `metadata`) asserts the round trip on a live cluster:
  pods findable by an inherited label alone, and a custom `spec.appName` instance reaching
  `Running` with matching Service endpoints. **Not yet executed** — it compiles
  (`go vet -tags e2e`) but the run needs a cluster and an image registry; see the PR notes.
