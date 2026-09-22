# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Record changes under `[Unreleased]`; see [RELEASING.md](RELEASING.md) for how to
cut a release (`scripts/prepare-release.sh`).

## [Unreleased]

## [0.4.0] - 2026-09-22

Everything below has landed since `v0.3.0`. Three headlines, in the order they are
likely to affect you.

**Sentinel isolation is now enforced, and it is the one breaking change.**
`spec.sentinel.masterName` is required, because a shared master name is what let two
unrelated Sentinel deployments on one pod network merge into one and destroy a
dataset — in production, silently, with both instances reporting healthy. Existing
instances keep running; see **Changed** for exactly when you are forced to act, and
where the requirement does *not* bite.

**A new `failover` mode, experimental.** The same job as sentinel mode — one master,
N replicas, automatic failover — with the operator as the sole failure detector and
no Sentinel processes. Offered alongside sentinel mode, which stays fully supported.

**Cluster rolling updates now wait for redundancy instead of for a timer**, after a
routine rolling update destroyed a shard's entire dataset and reported complete
success. A held rollout stalls loudly and is never released on a timeout.

No change to the persistence posture: LittleRed remains a pure in-memory store with
no RDB/AOF and no PersistentVolumes.

**Upgrading: apply the CRDs before the operator.** `helm upgrade` does not update
CustomResourceDefinitions — they are cluster-scoped and shared between releases, so
Helm deliberately declines to own them. Skipping this does not fail: the API server
prunes unknown fields silently, so the operator comes up against the 0.3.0 schema and
every field 0.4.0 added is dropped on write — `spec.sentinel.masterName`,
`spec.appName` and `spec.failover` in the spec, and `status.operation`,
`status.acknowledgedOperations`, `status.quarantinedSince`, `status.forsakenSince`
and `status.failover` in the status. The spec half is visible (the field you set is
simply not there); the status half is not, and it silently disables the quarantine of
a captured instance (ADR-016) and the acknowledgment of a declared operation
(ADR-020). The CRD ships as a release asset from 0.4.0 on:

```bash
kubectl apply --server-side --force-conflicts -f \
  https://github.com/chuck-chuck-chuck-net/littlered/releases/download/v0.4.0/littlered-crds.yaml
helm upgrade littlered oci://ghcr.io/chuck-chuck-chuck-net/charts/littlered \
  -n littlered-system --version 0.4.0
```

### Added

- **`failover` mode (experimental)** — a fourth deployment mode: 1 master +
  `spec.failover.replicas` replicas (default 2), **no Sentinel processes**. The
  operator is the failure detector and the failover decider, which removes the "two
  cooks" problem of running Sentinel underneath an operator that also has opinions
  about the same state. Writer routing is unchanged — the `{name}` Service still
  selects on the operator's `role: master` label.
  - Roles are assigned by annotating the data pods (`assigned-role`,
    `assigned-master-ip`, `assignment-epoch`); each pod reads its own annotations
    back through a downward-API volume, so no data pod needs API-server access.
  - Death is declared on corroborated evidence: the kubelet's own readiness verdict
    or a pod that is gone, or — for a network failure — the operator being unable to
    reach the master for `spec.failover.downAfterMilliseconds` **and** every reachable
    replica agreeing its link is down. The operator's own dial is never sufficient on
    its own.
  - `spec.failover.minReplicasToWrite` (default `1`) lets an isolated master fence
    itself during a partition, the one case operator-side fencing cannot reach. Set it
    to `0` explicitly at `replicas: 1`.
  - **Accepted trade-off: HA is coupled to operator liveness.** Sentinel mode's hard
    failures already were. `sentinel` remains fully supported; `failover` is offered
    as an option, not a migration you are expected to make.
  - Experimental: e2e coverage is at parity with sentinel mode, including chaos and
    durability tiers, but it has not seen real-world usage. See
    [docs/RECONCILIATION_LOOP_FAILOVER.md](docs/RECONCILIATION_LOOP_FAILOVER.md) and
    ADR-011.

- **In-place rename of the Sentinel master name** (ADR-018). Edit
  `spec.sentinel.masterName` on a running instance and the operator re-points its
  Sentinels to the new name and prunes every other name they carry, **dataset
  preserved**. Progress shows on a `StaleMasterName` condition. This is the supported
  way to move an instance off a colliding or legacy name; the runbook, its
  preconditions and the roughly three-minute window are in
  [docs/USAGE.md](docs/USAGE.md).

- **Automatic containment when a capture happens anyway** (ADR-016). An instance whose
  Sentinels have been taken over by another deployment sharing its master name is
  declared `Forsaken` and **held at zero replicas** until the captor has healed, then
  re-bootstrapped empty. This is default-on and has no opt-out.
  - **Read this if you run sentinel mode:** the captured instance's pods are deleted.
    It is already unrecoverable at that point — its identity and data cannot be
    salvaged, which ADR-015 established and did not reverse — and the purpose is the
    *neighbour*: the captor is silently healthy with the victim's pods in its
    failover-candidate set, so its next master death can promote a foreign pod.
  - It refuses to act when a reachable pod holds keys the capture does not explain, or
    when a pod cannot be proven empty. Bounded to two attempts, then it latches and
    stops.

- **Declared operations** (ADR-020). A change to a *heavy* spec field — one whose
  change cannot safely proceed alongside ordinary healing — is now declared,
  carried out and acknowledged on completion. `status.operation` and an
  `OperationInProgress` condition report it. The registry has exactly one member,
  the master-name rename above.
  - A CEL rule refuses an apply that changes more than one heavy field at once, so
    the ambiguous state cannot be created.
  - **A `Blocked` or `Stalled` operation never clears itself.** There is no timer; a
    timer would just be the same defect with a delay.

- **Labels and annotations on a `LittleRed` resource are now inherited by every resource
  the operator creates for it** — StatefulSets, Services, the ConfigMap,
  PodDisruptionBudgets, the ServiceMonitor and the pods — so `team=payments` or
  `environment=production` set where a user naturally puts it reaches the objects a scrape
  config actually selects on. Resolves
  [#96](https://github.com/chuck-chuck-chuck-net/littlered/issues/96). The operator's own keys
  always win and are never inherited, and bookkeeping stamped on the CR by Helm, Argo CD,
  Flux or kubectl is not propagated (Argo CD's tracking labels on a child confuse its
  pruning; `last-applied-configuration` would embed a copy of the CR into every child
  object). See ADR-021 and `docs/API_SPEC.md` §5.4. Designed and implemented by Michael
  Koch.

  **Note:** pod labels live in the pod template, so editing CR metadata triggers a rolling
  update — a failover in sentinel and failover mode, a serialized per-shard roll in cluster
  mode, and in standalone mode a restart that **discards the data** (EmptyDir, no
  persistence).

- **`spec.appName` sets the `app.kubernetes.io/name` value** (default `littlered`, so
  existing instances are unaffected), threaded through every selector so the label and the
  selectors can never disagree. It is **immutable**, enforced by a CEL rule: the label is
  part of `StatefulSet.spec.selector`, which Kubernetes does not allow to change, and
  recreating the StatefulSet would discard the data. This is the direct request in #96 —
  grouping an instance under the user's own application name for monitoring.

- **`spec.config.tcpKeepalive`** — the Redis `tcp-keepalive` interval in seconds. Zero
  disables keepalive, which is a real Redis setting and not a "leave it default" value.

- **New conditions**, all surfaced on the CR: `SentinelMasterNameUnscoped` (warning —
  no master name has been decided), `StaleMasterName`, `Forsaken`, `FailoverRecovery`,
  `ClusterRolloutBlocked`, `OperationInProgress`, and `OperatorCannotAuthenticate`
  (the operator's credential was refused by a pod — its keyspace is therefore unknown,
  not empty, and every data-safety gate treats it that way).

- **`lrctl` reports the new state.** No new verbs — `status`, `verify`, `inspect`,
  `debug-dump` and `import` are unchanged — but `verify` now covers failover-mode
  topology, flags a foreign Sentinel contact whether or not the operator has reached a
  capture verdict (it has no evidentiary floor of its own, deliberately: only the
  operator's verdict deletes pods), and both `status` and `verify` render any declared
  operation in flight.

### Changed

- **The project has moved to `github.com/chuck-chuck-chuck-net/littlered`, and so have the
  images and the chart.** The old location redirects, but the container registry does not:
  packages belong to the GitHub organisation, so nothing at
  `ghcr.io/littlered-operator/...` moves on its own.

  | | Old | New |
  |---|---|---|
  | Operator image | `ghcr.io/littlered-operator/littlered` | `ghcr.io/chuck-chuck-chuck-net/littlered` |
  | Chaos client | `ghcr.io/littlered-operator/littlered-chaos-client` | `ghcr.io/chuck-chuck-chuck-net/littlered-chaos-client` |
  | Helm chart | `oci://ghcr.io/littlered-operator/charts/littlered` | `oci://ghcr.io/chuck-chuck-chuck-net/charts/littlered` |

  If you installed by chart, `helm upgrade` from the new OCI reference picks up the new image
  default. If you pin `image.repository` yourself, update it. **The old packages stay
  published** — an existing deployment keeps pulling and will not break on a node reschedule
  — but they will not receive new versions.

- **BREAKING — `spec.sentinel.masterName` is required, and you should set it.** A
  Sentinel master name is the *only* isolation Sentinel's gossip protocol has: a
  Sentinel that receives a hello looks the name up, discards it if unknown, and checks
  nothing else — no instance identity, no namespace. Two instances sharing a name and
  able to reach each other are, protocol-wise, **one deployment**, and the one with the
  higher config epoch can reassign the other's master to a foreign Redis pod, whose
  replicas then flush their datasets to resynchronise from a stranger. This happened in
  production. Use `<namespace>.<name>`.
  - **Existing instances keep running.** One is forced to state a value only on its next
    change to `spec.sentinel`, and reports the `SentinelMasterNameUnscoped` warning
    condition until then.
  - **The requirement is not a hard gate.** `masterName` is required *within*
    `spec.sentinel`, and that block is itself optional — so a CR that omits it entirely
    is accepted even in sentinel mode and falls back to the legacy shared name
    `mymaster`, with the same warning. Set one explicitly.
  - **Authentication is now strongly recommended in sentinel mode.** It is the Sentinel
    peer-membership credential too, and the only thing closing the narrower path a unique
    name leaves open. It remains off by default.
  - `masterName` is part of a Sentinel-aware client's configuration. Changing it means
    reconfiguring those clients in the same window; clients reaching the master through
    the `{name}` Service are unaffected.

- **Cluster rolling updates are gated on redundancy, not on a timer** (ADR-017). The
  operator holds each shard's StatefulSet at a `partition` and releases the next pod only
  once every pod above it is at the new revision, Ready per the kubelet, **and** a
  link-`up` replica of its shard's slot owner. The state where a node owns slots with no
  synced replica no longer exists on this path.
  - **The cost is time.** The bound moves from `shards x pods x (ready + minReadySeconds)`
    to `shards x pods x (schedule + rejoin + full sync)`, which for a large dataset is
    minutes per pod.
  - **The chosen failure direction is a stall.** A rollout that cannot restore redundancy
    holds, leaving the old pods serving, and reports `ClusterRolloutBlocked`. It is never
    released on a timeout — a time-released rollout is the lossy path.
  - This governs operator-triggered rollouts. A manual `kubectl rollout restart`, a drain
    or an eviction bypasses the operator; those are covered by a pod-local preStop fence
    that makes a last-copy master refuse writes rather than acknowledge and lose them.

- Default `redis_exporter` sidecar image is now **v1.89.0** (from v1.88.0). Instances
  that do not pin `metrics.exporter.tag` pick the new tag up when the CRD is applied.

- Dependency updates: prometheus-operator apis 0.93.1 and Ginkgo 2.32.1. CI-only:
  the lint workflow now uses `azure/setup-helm@v5`.

### Fixed

Data-loss and data-safety fixes first; each names its entry in
`docs/RECONCILIATION_ALGORITHM_CHANGELOG.md`.

- **A cluster rolling update could destroy a shard's entire dataset and report success**
  (LR-047). Nothing gated the handover within a shard on the replacement actually being a
  copy: a replaced pod returns on a wiped EmptyDir with a new node ID and is a copy of
  nothing until the operator rejoins it and it full-syncs, so the whole window to restore
  redundancy was `minReadySeconds` after the replacement answered a local `PING`. Observed:
  96 seconds with zero copies of a shard's slot range, after which the repair loop
  *healed the dead shard into a healthy-looking empty one*. Fixed by the state gate above.

- **In failover mode, a graceful master delete lost acknowledged writes** (LR-038).
  Promoting a replica says who the new master is and nothing about the old one, which on a
  graceful delete is still alive and still mastering for its whole termination window —
  and an established client connection is not re-routed by the label flip. **Measured: 202
  of 1171 acknowledged writes lost, with zero corruptions and 97.66% write availability**,
  so nothing caught it. The operator now demotes the outgoing master, converting silent
  loss into visible `-READONLY` write failures. **Verified: 202 of 1171 lost → 0 of 990.**

- **In failover mode, a `kill -9` of a promoted master could return it as an empty master**
  that the operator believed was healthy, then repoint the replicas holding the only copy
  onto it. **Measured: 352 of 1145 acknowledged writes destroyed**, fixed to 0. The
  assignment epoch was being used to answer an identity question ("was this instruction
  issued for *my* incarnation?"), which it cannot; a master start now additionally requires
  an authorization the operator stamps only after establishing that no data is at risk.

- **Rotating an auth Secret could make the operator reseed an empty master over live data**
  (LR-051). This is the most serious fix in the release, it needed no CR edit to trigger, and
  it shipped in v0.3.0.
  - The password reaches the pods through an env `secretKeyRef` and appears in no pod
    template, so changing it restarts nothing: the pods keep the old credential and the
    instance looks perfectly healthy. The operator picks the new one up and every probe then
    fails — and because the failure was **discarded rather than classified**, a pod that
    refused the credential was byte-identical to a pod that did not answer at all. The data
    holders filter on reachability, so every holder went invisible, which is precisely the
    "no data, safe to reseed" signature. **The ≥2-holder refusal — the gate whose entire job
    is to stop the operator discarding data — could never fire**, and the reseed needed no
    opt-in.
  - Reaching it required the operator to reach the Sentinels but not the Redis pods, which a
    *partial* restart creates — including `kubectl rollout restart statefulset/<name>-sentinel`,
    which this project's own runbook recommended.
  - The gather now classifies the error. A pod that refused the credential is treated as a
    live server with an **unknown** keyspace rather than an empty one, it blocks the recovery
    outright, and the instance reports `OperatorCannotAuthenticate`. The accepted consequence
    is that a permanent credential mismatch holds recovery open until the Secret is fixed —
    which destroys nothing.

- **A healthy instance could be quarantined and its pods deleted during an ordinary
  rename** (LR-050). A pod of ours that had just been replaced was indistinguishable from a
  foreign captor's master, and a supported rename presented that signature for a measured
  42.5 seconds. The operator no longer attributes addresses at all while its own StatefulSet
  is rolling.

- **A blackholing dead pod IP could stall a reconcile for ~117 seconds** (LR-040),
  starving the recovery it was meant to perform. The Sentinel *write* paths were left
  unbounded on the premise that a guard upstream would stop them during churn; that guard
  runs *after* them. Also established that a context deadline alone does not bound these
  calls — the client's own dial/read/write timeouts must be set too.

- **Every Sentinel read as "reachable but monitoring nothing", silently, forever**
  (LR-041). The gather asked Sentinel about an empty master name, which Sentinel answers
  exactly like an unknown one, so ghost-replica pruning, ghost-master correction and the
  healthy-replica check all went quietly dead while status kept reporting healthy. The name
  is now a parameter, so the omission cannot compile.

- **A deference guard that had never once fired** (LR-052). The check for "a Sentinel
  failover is already in progress, stay out of its way" read a reply field neither Redis
  nor Valkey has ever emitted, so it was permanently false for the product's entire
  history. Now reads the real field, through a single shared predicate.

- **`CLUSTER MEET` could merge two unrelated clusters** (LR-043). Pod IPs are recycled
  across instances on a shared pod network, and MEET validates nothing. Every MEET target
  is now confirmed uncached against the API server before it is introduced.

- **A captured sentinel instance was treated as converging, indefinitely** (LR-042) —
  roughly 30 reconciles a minute re-deriving a dead end, and during a partial capture
  actively wiping a Sentinel's replica list every pass. The state is now named and the
  operator stops managing it.

- **`spec.podTemplate.labels` could break an instance.** It was merged last, so it could
  override the operator's structural labels — making the pod template disagree with its
  StatefulSet's `spec.selector`, which the API server rejects outright. The instance then
  stopped reconciling with an error pointing at the StatefulSet rather than at the field
  that caused it. Those five keys (`app.kubernetes.io/name`, `/instance`, `/component`,
  `redis.chuck-chuck-chuck.net/shard`, `/role`) are now rejected by CRD validation, so the
  error lands on the CR. Use `spec.appName` for the app name.

- **Sentinel pods now get `spec.podTemplate.labels`.** `buildSentinelStatefulSet` was the
  one builder that never applied them, unlike standalone, sentinel-mode Redis and cluster.

- **Failover mode inherits CR metadata like every other mode.** Its builders were written
  after the inheritance work and were never wired, so a failover-mode instance inherited
  object *labels* but not object annotations and not its pod template — a failover-mode pod
  could not be found by an inherited label, which is the thing #96 asked for.

- **The sentinel and failover headless Services inherit CR annotations.** Both built their
  annotation map from scratch and populated it only when metrics were enabled, so CR
  annotations never reached either. The `prometheus.io/*` keys now layer over the inherited
  ones, per the documented precedence.

- **Both container images were being built by a Go release candidate.** The
  Dependabot `golang` group moved the operator and chaos-client builders from
  `golang:1.26.5` to `golang:1.27rc2`: a Docker tag like `1.27rc2` has no semver
  pre-release separator, so it ranks above the stable line instead of being skipped.
  `go.mod` still declared `go 1.26.0`, so the language version never moved, but the
  published images were compiled by a pre-release toolchain. Both builders are pinned
  to `golang:1.26.6` (the current stable patch), and the `golang` group now ignores
  minor and major updates — patch-level security fixes stay automatic, while moving
  the Go line stays a deliberate change made together with the `go` directive.

- **Dependabot was watching a Helm chart directory that does not exist.** The `helm`
  update entry pointed at `/charts/littlered-operator`; the chart lives in
  `/charts/littlered`, so the entry could never resolve a chart. No chart currently
  declares `dependencies:`, so nothing was missed in practice — the fix makes the
  entry effective if one ever does.

- **The Helm chart did not parse** — `helm install`/`helm upgrade` failed with
  `parse error at (littlered/templates/rolebinding.yaml:16): unexpected {{end}}`
  for **every** chart version from 0.2.2 on. Making leader election
  non-configurable removed the `{{- if .Values.leaderElection.enabled }}` guard
  from `role.yaml` and `rolebinding.yaml`, but the namespace-scoping change
  (ADR-014) was integrated with the guard's closing `{{- end }}` left behind in
  both files, closing nothing. `values.yaml` likewise carried a duplicated
  `scope:` block. Charts 0.2.2 and 0.3.0 have been republished with the fix.

- **CI now lints and renders the chart** (`make helm-lint`, run on every push and
  again before the release job pushes to the registry) in the default,
  allow-list and deny-list scoping modes. A chart template error is invisible to
  the Go linter and previously only surfaced on the user's cluster.

- **`lrctl verify` no longer reads a pod of your own as another Sentinel deployment without
  saying so.** During any rolling update — a config change, an image bump, a drain — a replaced
  pod's address leaves the pod list at once while Sentinel keeps listing it, unflagged, for a
  whole `down-after-milliseconds`, and a departed pod also takes the expected-replica count with
  it. `verify` reported that as *"Evidence of another Sentinel deployment sharing this master
  name"* and pointed at the capture runbook, whose remedy deletes pods. The evidence is still
  printed — it must be, since it is the only signal for a capture shape the operator cannot
  diagnose at all — but it now carries a caveat naming the pods in churn and asking you to let
  them settle and re-run first. Measured during an ordinary rollout on a real cluster; the
  caveat covers roughly half the window, and the remaining half is recorded in the ledger
  (LR-062).

- **The CRD is now published as a release asset** (`littlered-crds.yaml`), and the
  release notes say to apply it first. `helm upgrade` never updates a chart's
  `crds/` directory, so upgrading users have always had to apply the CRD
  themselves — but the only published copy of it was inside the chart tarball, so
  the instruction could not be followed without cloning the repository. Found by
  upgrading a 0.3.0 install to 0.4.0-rc1 by chart: the operator upgraded, the CRD
  did not, and `spec.sentinel.masterName` was pruned without an error on apply.

- **The generated release notes printed a `helm install` command that fails.**
  `--version` was rendered from the tag (`v0.4.0`) while the chart is pushed with
  the leading `v` stripped (`0.4.0`), so the one command a new user is most likely
  to copy returned `chart not found`.

### Known issues

None of these is a regression. The rename and capture entries are gaps in functionality that
ships here for the first time; the authentication entries ship in v0.3.0 today and are
carried forward knowingly, documented rather than patched a week before a tag.

- **Renaming an instance whose Redis pods cannot become Ready wedges the rename**
  (LR-061). A pod still on the pre-rename template asks Sentinel for the old name, which
  the rename has correctly pruned, so it never starts — and the rolling update that would
  give it the new name advances only as pods become Ready. Renaming a degraded instance is
  already documented as out of scope; recover the instance first. The operation reports
  `Stalled` and will not clear itself.

- **Rotating an auth Secret still stops the operator managing the instance, and recovery
  requires rolling the pods.** With the credential mismatch above now classified rather than
  silent, the data-loss path is closed and the instance reports
  `OperatorCannotAuthenticate` — but the operator still cannot read Sentinel, so healing
  stays suspended until the pods are restarted onto the new credential. The consequence to
  plan for is that a master failure during that window promotes correctly in Sentinel while
  the operator cannot move the `role: master` label, so the `{name}` Service keeps selecting
  the old pod. Roll the pods as part of any rotation.

- **Any failure to read the auth Secret is treated as "auth is disabled".** A transient API
  error, a deleted or renamed Secret, an RBAC change or a mistyped key all make the operator
  present no credential to an auth-enabled fleet. The consequences are the two entries above,
  reached with no user intent at all. Watch for `OperatorCannotAuthenticate`.

- **Enabling authentication on a running sentinel instance can fail over a healthy master.**
  Turning `spec.auth.enabled` on changes container arguments, so both StatefulSets roll; during
  the roll an un-rolled Sentinel presenting no credential to an already-enforcing master is
  told `NOAUTH`, marks it subjectively down, and a quorum of un-rolled Sentinels can agree and
  fail over a master that is perfectly healthy. Availability only — no data loss — and it is
  Sentinel behaving correctly on a topology the rollout creates. Enable auth during a window
  where a failover is acceptable.

- **A capture victim that is holding data and cannot sync produces no verdict at all**
  (LR-054, ruled and accepted for this release). Such a pod is `link:down`, so the instance
  never looks settled, so the operator withholds the capture diagnosis entirely. **Nothing
  is deleted and the victim's own keys are untouched** — what is lost is that the operator
  stays silent while a neighbouring captor remains polluted. `lrctl verify` still reports
  the foreign contact, and the capture runbook in `docs/USAGE.md` flags that this shape
  produces no condition.

## [0.3.0] - 2026-08-11

Everything below has landed since `v0.2.2`. The headline is a restructure of
cluster mode — one StatefulSet per shard instead of a single striped one — which
unlocks per-shard failure-domain isolation. **Existing cluster instances migrate
automatically, online and without data loss; no action is required.** The
external contract is unchanged: same CRD API, same Services, service names,
ports and selectors, so client connection endpoints keep working untouched.
Standalone and sentinel mode are unaffected by the restructure.

### Added

- **Automatic in-place migration of pre-0.3 cluster instances** (ADR-013,
  LR-025). Instead of refusing to manage a legacy `{name}-cluster` StatefulSet,
  the operator migrates it to the per-shard layout online, on the same running
  Redis Cluster, without data loss and without changing client connection
  endpoints. The mechanism is **replicate-then-failover**: the new per-shard pods
  join as slot-less replicas of the legacy master owning their range, full-sync,
  and only then is `{name}-shard-K-0` promoted by a coordinated `CLUSTER
  FAILOVER` — an atomic handoff after which the legacy master demotes to a live
  replica. A new node therefore never owns slots without a redundant copy
  existing, which makes the migration restart-safe for every
  `replicasPerShard`, including `0`.
  - Phases, re-derived from live cluster state every reconcile (nothing
    load-bearing is persisted): `Standup` → `Meet` → `Replicate` → `Failover` →
    `Decommission` → `Complete`. The steady-state repair loop is suspended while
    a migration is in flight.
  - Entry is health-gated (`cluster_state:ok`, all 16384 slots assigned, all
    legacy pods `Ready`, master quorum) and **shape-preserving only** — the same
    `shards × (1 + replicasPerShard)`, which is what makes the 1:1
    range-for-range mapping valid. An unhealthy legacy cluster simply waits and
    migrates once it recovers. A legacy topology that does *not* match the
    declared shape is refused rather than guessed at: the instance reports
    `Phase=Failed` with a `LegacyClusterTopology` condition and needs operator
    attention. The legacy workload is never deleted in that case — with EmptyDir
    storage, deleting it would destroy data.
  - The legacy StatefulSet and its PDB are deleted only once no legacy node owns
    a single slot — i.e. provably holds no data.
  - Opt out per-CR with the annotation
    `redis.chuck-chuck-chuck.net/migrate-legacy-sts: hold`, which parks a
    non-mutating holding state for a maintenance window. Note the trade-off:
    while held, the repair loop stays suspended, so the instance is unmanaged.
  - Progress is observable via `status.cluster.migration`
    (`phase`, `shardsMoved`, `totalShards`, `startedAt`), and `lrctl status` /
    `lrctl verify` print a one-line migration banner while it is underway.
    Non-migrating output is unchanged, and `lrctl` remains read-only.
  - Legacy detection is deliberately narrow: a StatefulSet qualifies only if it
    is named `{name}-cluster`, carries `component=cluster`, **lacks** the
    per-shard label, is sized exactly `shards × (1 + replicasPerShard)`, and is
    controller-owned by this CR. A stray, mis-sized or half-formed StatefulSet
    never triggers a migration.

  Verified end-to-end on a live cluster for the Redis 8.4+ atomic-slot-migration
  engine and for the pre-8.4 path, including a restart-during-migration chaos
  tier.

- **`spec.placement.shardAntiAffinity` — per-shard failure-domain isolation as a
  one-line setting** (LR-022). The operator injects a `topologySpreadConstraint`
  (`maxSkew: 1`, selector scoped to that shard's pods) into each shard
  StatefulSet, appended after anything in
  `spec.podTemplate.topologySpreadConstraints`. Users could not write this
  themselves, because it has to select on the operator-owned shard label.
  Defaults are `topologyKey: kubernetes.io/hostname` and
  `whenUnsatisfiable: ScheduleAnyway` (soft, matching CloudNativePG/Strimzi
  convention); hard `DoNotSchedule` is opt-in. Cluster mode only. Enabling it
  triggers one serialized rollout that re-places the pods.

- **Cluster mode: recovery from a total-/partial-wipe deadlock** (LR-023,
  ADR-008) — the cluster analog of the sentinel leaderless deadlock. A mass
  container crash (`kill -9`, OOM) leaves `nodes.conf` on the EmptyDir, so every
  restarted master parks in the startup yield loop with no live replica to take
  over, lands in `CrashLoopBackOff`, and never becomes `Ready` — which meant the
  operator, gated on all pods being ready, never gathered state or acted. It now
  recycles exactly the stuck pods (redis container not ready, crash-looping, not
  `OOMKilled`) after a 120s cooldown tracked in
  `status.cluster.wipeDeadlockSince`, and their StatefulSets reschedule them
  fresh into the normal self-heal path. Data-safe by construction: the gate is
  the kubelet's *local* readiness probe rather than a remote dial, and a
  not-ready pod in a pure in-memory cluster holds no data. A `Ready` pod — a
  possible data holder — is never recycled, so a partial wipe keeps its
  survivor. Requires `delete` on pods (granted by the chart).

- **Sentinel mode: operator-led recovery from the ghost-master failover
  deadlock** (LR-024). A graceful failover followed by a crash could leave every
  Sentinel pinned to a dead master with an empty replica list —
  `-failover-abort-no-good-slave` forever, data safe but the instance never
  serving. Neither existing rule could help: ghost-master correction needs a
  living consensus master (every pod was a slave of the ghost) and Rule L needs
  bare Sentinels (these monitor the ghost). The operator now elects the
  most-complete survivor via `SENTINEL REMOVE` + `MONITOR` (+ `REPLICAOF NO
  ONE`), gated on `!HasHealthyKnownReplica` and a 30s cooldown
  (`status.ghostMasterStuckSince`) so a legitimate in-progress failover is never
  stolen. The safety gate keys on replication **lineage**, not holder count:
  same-lineage survivors are elected with no opt-in, while genuinely divergent
  histories still require `sentinel.allowUnsafeRebootstrapOnDeadlock`.

- **`lrctl verify`: shard-colocation checking and a `[DEGRADED]` tier**
  (LR-020). `verify` previously green-lit a cluster whose Redis shards were
  scrambled across StatefulSets, because it only checked Redis health. It now
  fails on any cross-StatefulSet master/replica pairing, and reports a new
  `[DEGRADED]` warning tier (exit 0) when a replica's replication link is down —
  reduced redundancy is not "healthy and consistent", but it is usually a
  transient resync, so it warns rather than fails.

### Changed

- **Cluster mode: one StatefulSet per shard** (LR-020, ADR-007). The
  single striped `{name}-cluster` StatefulSet is replaced by `{name}-shard-K`
  (one per shard, each sized `1 + replicasPerShard`), carrying a stable
  `redis.chuck-chuck-chuck.net/shard` label; shard K's intended master is pod
  `{name}-shard-K-0`, and each redundant shard gets its own
  `{name}-shard-K-pdb`. Pod enumeration and master identity now come from a
  single source of truth instead of the old `(i - shards) % shards` striping.
  The shared headless Service `{name}-cluster` is retained and governs every
  shard StatefulSet, so peer discovery, pod DNS and **client connection
  endpoints are unchanged**.

  This is what makes single-domain-loss survivability possible at all: a shard's
  master and replicas can only be placed in different failure domains if they
  live in separate StatefulSets. Because OSS Redis/Valkey Cluster has no
  failure-domain awareness, the operator is now the sole topology authority — an
  empty pod is reattached to the under-replicated master **in its own shard**
  (cross-shard only as a logged fallback), and
  `cluster-allow-replica-migration no` stops Redis from autonomously re-pairing
  replicas across shards.

  Two never-delete-data guards: the operator refuses to stand up per-shard
  StatefulSets beside a lingering legacy one (without deleting it), and refuses a
  decrease of `spec.cluster.shards` that would orphan high-index shards.

  **Upgrade note — no action required.** Existing instances are migrated
  automatically and online by the migration described under *Added*: no
  delete-and-recreate, no data loss, and no change to the client-facing contract
  (CRD API, Services, ports and selectors are all unchanged). The one visible
  difference is that **workload and pod names change**
  (`{name}-cluster-N` → `{name}-shard-K-M`, and `{name}-cluster-pdb` →
  `{name}-shard-K-pdb`), so anything referencing them *directly* — scripts,
  dashboards, NetworkPolicies, `kubectl rollout restart` invocations — needs
  updating. While a migration runs, the instance reports `Ready=False` with
  reason `MigrationInProgress` until it reaches `Complete`, which is worth
  knowing for anything that gates on readiness (CI checks, Argo CD health).

- **Rolling updates are serialized across shard StatefulSets** (LR-021).
  Splitting into per-shard StatefulSets lost the global one-pod-at-a-time
  restart ordering that a single StatefulSet provided for free: an
  operator-driven pod-template change rolled every shard in parallel and
  restarted all masters in one wave (the chaos e2e measured ~24% failed
  operations, no data loss). The operator now rolls one shard at a time,
  deferring the next until the current one has fully settled, detecting changes
  via a new `redis.chuck-chuck-chuck.net/pod-spec-hash` annotation on the
  operator-authored pod template. Creating missing shards stays immediate and
  parallel, so a fresh bootstrap is not slowed down. This governs
  operator-triggered rollouts only — a manual `kubectl rollout restart` bypasses
  the operator. On first upgrade, existing shard StatefulSets acquire the hash
  through one serialized, availability-safe roll.

- Event recording migrated to the `events.k8s.io/v1` API, replacing the
  deprecated core-`v1` recorder (`SA1019`, which 0.2.2 silenced with a scoped
  `//nolint`). The new broadcaster requires `events.k8s.io` `create`/`patch`
  permissions, added to the generated RBAC and to the shared chart RBAC helper
  used by both the cluster- and namespace-scoped roles.

- Upgrade and install documentation corrected. The cluster-mode upgrade note
  still described the superseded clean-slate, delete-and-recreate behavior,
  which would have led upgrading users to destroy data for an upgrade that is
  now seamless; it now documents the automatic migration, its phases and the
  `hold` opt-out with its trade-off. Also fixed: a false "exactly 3 shards /
  0 or 1 replica" claim (validation allows 3+ shards and 0+ replicas),
  rolling-restart commands that still targeted the no-longer-existing
  `{name}-cluster` StatefulSet, and a stale install section (OCI chart install,
  correct image/CRD paths; the Kustomize option is gone, since Helm is the
  distribution).

- `golangci-lint` is pinned to v2.12.2 via the `go.mod` tool directive, matching
  what CI runs, so local `make lint` and CI now surface the same findings. All 56
  newly-surfaced findings were resolved by introducing or reusing constants — no
  value or logic changes.

- Topology-aware master balancing (spreading *masters* across failure domains)
  is explicitly **declined** and recorded as a contestable decision with revisit
  conditions in ADR-009: reads commonly go to replicas so load is already
  spread, `replicasPerShard: 1` leaves no balancing freedom, there is no
  schedule-time master label to spread on, and active balancing would only add
  failover churn without improving uptime.

### Fixed

- **Sentinel seeding could silently no-op against a stale master.** The
  idempotency guard skipped any Sentinel that already knew *some* master, which
  during a ghost-master deadlock is the ghost — and a bare `SENTINEL MONITOR` is
  rejected while a same-named master is still configured, so the repoint did
  nothing and recovery oscillated. It now skips only Sentinels already
  monitoring the *target* master (preserving no-churn idempotency) and otherwise
  issues `SENTINEL REMOVE` before `MONITOR`, so the repoint actually reaches a
  ghost-pinned Sentinel. Bootstrap and leaderless seeding are unaffected.

- **A normal promotion chain was misread as divergent data.** When a node is
  promoted and its peers resync, Redis rotates `master_replid` and shifts the
  previous value into `master_replid2`. Divergence was computed from
  `master_replid` alone, so the survivors of a graceful-then-crash sequence
  looked like independent lineages and recovery refused to elect any of them.
  The gather now also captures `master_replid2`, and divergence is computed over
  each holder's `{replid, replid2}` with union-find, so holders connected through
  a shared replication id count as one lineage. Only genuinely independent
  histories are reported as divergent and still require the unsafe opt-in.

## [0.2.2] - 2026-08-11

Everything below has landed since `v0.2.1`.

### Added

- **Namespace-scoped operation** (ADR-014). The operator can now be restricted to
  a subset of namespaces instead of always reconciling every `LittleRed` CR in the
  cluster. Two mutually-exclusive, opt-in modes:
  - `WATCH_NAMESPACE` — allow-list. The manager cache watches only those
    namespaces, and the Helm chart renders a per-namespace manager `Role` +
    `RoleBinding` instead of a `ClusterRole` (least privilege).
  - `IGNORE_NAMESPACE` — deny-list. Watches everything except those namespaces
    (cache field selector); keeps the `ClusterRole`.

  Both unset means cluster-scoped, exactly as before; setting both is a fatal
  startup error. The leader-election lease ID is derived from the effective scope,
  so operators with disjoint scopes never contend for the same lease. Configure via
  the chart values `scope.watchNamespaces` / `scope.ignoreNamespaces`. The CRD stays
  cluster-installed. See the *Namespace Scoping* section in
  [`docs/USAGE.md`](docs/USAGE.md).

- **Cluster mode: self-healing of the consolidated-shard deadlock** (LR-018,
  ADR-006). An instance where one master owned two or more shard ranges while other
  masters sat slotless could stay in `phase: Initializing` indefinitely — no repair
  step could act. The operator now detects this (Step 3b) and relocates the surplus
  range onto an empty master, **preserving keys unconditionally**:
  - Redis 8.4+ (detected at gather time from `CLUSTER INFO`, nothing persisted):
    native atomic slot migration (`CLUSTER MIGRATION IMPORT`).
  - Older engines: the classic `IMPORTING`/`MIGRATING` → `MIGRATE` → `SETSLOT NODE`
    dance, made incremental across reconciles so it never blocks the reconcile
    worker, resuming from the cluster's own on-node markers (no operator state).

  The root cause is also closed: a missing shard range may now only be assigned to
  a reachable *empty* master, so recovery can never pile a second range onto a
  master that already owns one. New advanced tunables under `spec.cluster`:
  `reshardKeyBatchSize` (128), `reshardMaxKeysPerReconcile` (2000),
  `reshardMigrateTimeoutMillis` (5000) — ignored on the native 8.4+ path.

- **Sentinel mode: recovery from a leaderless bootstrap deadlock** (LR-015,
  ADR-005). After a mass pod restart, an already-initialized instance whose entire
  Sentinel quorum lost its master config was unrecoverable without manual
  `SENTINEL MONITOR` or a CR delete + redeploy: with no master, every healing rule
  short-circuited. The new Rule L fires only on that exact signature (all reachable
  Sentinels bare, reachable quorum, no disruption in flight, state persisted past a
  30s cooldown) and decides by how many reachable pods still hold keys:
  - **0 holders** → seed `redis-0` as master.
  - **exactly 1 holder** → promote it (the sole surviving copy of the data; nothing
    else can be lost). No opt-in required; reported as `ReseededFromSurvivor`.
  - **2 or more holders** → refuse and wait, because electing one discards the
    others. Set `spec.sentinel.allowUnsafeRebootstrapOnDeadlock: true` to force-elect
    the most complete pod (highest replication offset, keys as tiebreak).

  Observability: new `LeaderlessRecovery` status condition, `status.leaderlessSince`,
  Kubernetes Events (the operator now needs — and the chart grants — `events` RBAC),
  and per-pod key counts in `lrctl verify` / `lrctl status`.

- **Metrics for Sentinel pods and replicas** (#6). The `redis_exporter` sidecar was
  missing from the Sentinel pods, so they emitted no metrics at all. They now carry
  a sidecar pointed at port 26379 (`redis_sentinel_*` series), and the Sentinel
  headless service exposes the metrics port with the `prometheus.io` annotations.
  Additionally, in sentinel mode the metrics port moved from the role-scoped master
  service to the all-pods replicas headless service, so master *and* replicas are
  each scraped exactly once (previously only the master was scraped). The existing
  `ServiceMonitor` picks both services up automatically.

- **Admission-time rejection of mode-mismatched specs** (#61). CEL
  `x-kubernetes-validations` on `LittleRedSpec` now make the apiserver reject a CR
  that carries `spec.cluster` when `spec.mode` is not `cluster`, or `spec.sentinel`
  when the mode is not `sentinel`. Previously the mismatched block was silently
  ignored.

- **Scheduling: `topologySpreadConstraints` for the operator Deployment**, as a new
  Helm chart value (rendered only when set), so a multi-replica operator can be
  spread across nodes and zones. `docs/USAGE.md` gained a *Spreading pods across
  nodes and failure domains* section covering the `spec.podTemplate` passthrough for
  managed instances.

- **Release tooling.** `scripts/prepare-release.sh` promotes `[Unreleased]` to a
  versioned section and bumps the Helm chart version; [`RELEASING.md`](RELEASING.md)
  documents the process; the publish workflow now generates the GitHub Release notes
  from the matching changelog section instead of a hardcoded body, and verifies the
  chart version matches the tag before publishing.

- **Licensing artifacts.** Apache-2.0 `LICENSE`, `AUTHORS`, `NOTICE` with upstream
  attributions, a generated `THIRD_PARTY_LICENSES` inventory with a `make licenses`
  target, and the collective `Copyright <year> The littlered Authors.` header across
  all Go sources.

- **E2E test selection.** Heavy or opt-in specs carry a shared Ginkgo `extended`
  label: `make test-e2e` runs everything except those, `make test-e2e-all` (or
  `E2E_ALL=true`) runs the lot, and `LABEL_FILTER` accepts any Ginkgo label
  expression. New coverage for reshard recovery (native and pre-8.4 paths) and for
  leaderless recovery.

### Changed

- Leader election is now always enabled and is no longer configurable. The
  operator manager hardcodes `LeaderElection: true`, the `--leader-elect`
  command-line flag has been removed, and the Helm chart's
  `leaderElection.enabled` value has been removed. This guarantees that only
  one controller manager reconciles at a time regardless of how the deployment
  is scaled (via Helm or directly with `kubectl`/`k9s`), preventing concurrent
  reconcilers from racing over Sentinel master/failover state. The
  leader-election RBAC (Role/RoleBinding) is now rendered unconditionally.

  **Upgrade note:** installs that previously set `leaderElection.enabled: false`
  will have leader election forced on at the next `helm upgrade`. This is the
  intended hardening and requires no action, but the now-unknown
  `leaderElection.enabled` value should be removed from custom `values.yaml`
  files to avoid confusion.

- **BREAKING: PodDisruptionBudgets for managed instances are created by default**
  (#69). `spec.podDisruptionBudget.create` is now a `*bool` with a CRD-level
  `default: true`, so the default lives in the OpenAPI schema and is materialized on
  the object at admission. Opt out per-CR with `create: false`. The
  `--pdb-create-default` operator flag and its chart wiring are **removed** — with a
  CRD default they were redundant. The chart's top-level `podDisruptionBudget` values
  block now governs only the operator Deployment's own PDB.

  **Upgrade note:** the new CRD must be applied for the new default to take effect.
  Upgrading only the operator image against an old CRD is safe but leaves the feature
  inert for CRs that omit `create`. `helm upgrade` does not upgrade CRDs in the
  chart's `crds/` directory, so Helm users must apply the CRD manually.

- Default `redis_exporter` sidecar image is now **v1.88.0**. The version has a single
  Dependabot-tracked source of truth (`api/v1alpha1/redis-exporter.Dockerfile`,
  `go:embed`-ed and parsed) instead of being hardcoded across Go constants, the
  kubebuilder default marker and the generated CRD, with a drift guard test that
  fails CI if a bump is not mirrored into the marker + `make manifests`.

- **Documentation now matches the project's actual resource conventions.** Example
  manifests no longer set CPU *limits* (they contradicted both the operator's
  convention and production practice), and the rationale was rewritten: Redis's CPU
  use is bounded by its thread budget (main thread + `io-threads`), so the CPU
  *request* should be sized to that budget while a CPU *limit* can only throttle
  Redis into rising latency and cascading timeouts. Claims of "Guaranteed QoS by
  default" corrected to Burstable (memory limit = request, no CPU limit). Also adds
  PodDisruptionBudget coverage (`docs/USAGE.md` example, `docs/API_SPEC.md` field
  reference, production samples) and stops presenting `sentinel.quorum: 2` as
  something to set — it is the default.

- **Makefile targets follow kubebuilder conventions.** `install`/`uninstall` now
  mean apply/delete the CRDs (the old lrctl-installing `install` is
  `install-lrctl`); canonical `docker-build`/`docker-push` driven by a new `IMG`
  variable, which `deploy` also honors; `img-buildx` renamed `docker-buildx`; default
  `CONTAINER_TOOL` is `docker` (override via env). The Helm `deploy`/`undeploy` path
  and the two-image e2e build set are kept as deliberate divergences. Makefile tool
  versions (controller-gen, golangci-lint, go-licenses) are now derived from `go.mod`
  `tool` directives so Dependabot can see them.

- Helm chart: namespaced resources now carry an explicit `namespace` in their
  templates, and the repository URL in `Chart.yaml` was corrected. Manager RBAC rules
  are generated into a single shared template helper so the `ClusterRole` and the
  namespaced `Role` cannot drift.

- Dependency updates: Kubernetes libraries 0.36.2 with controller-runtime 0.24.1,
  `go-redis` 9.22.0, prometheus-operator apis 0.93.0, Ginkgo 2.32.0 / Gomega 1.42.1,
  `logr` 1.4.4, Go 1.26.5 and Alpine 3.24.1 base images, plus `golang.org/x/text`
  0.39.0 and `google.golang.org/grpc` 1.82.1 to pick up security fixes.

- Internal maintenance: migrated off deprecated APIs (`client.Apply` (#32),
  controller-runtime's `scheme.Builder`), adopted Go 1.26's `new(expr)` builtin in
  place of pointer helpers, and cleaned up lint findings across the operator, `lrctl`
  and tests. No behavior change.

### Fixed

- **Sentinel mode: a reconcile could stall ~146s on dead pod IPs** (LR-017), on
  clouds where a killed pod's IP blackholes rather than refusing connections. The
  stall froze status at a stale value and starved the leaderless-recovery rule, which
  surfaced as apparent data loss in recovery testing. The sentinel gather now probes
  all Redis and Sentinel pods concurrently (as cluster mode already did), and every
  Sentinel read-path address loop plus the gatherer's Redis probe is bounded by a
  per-address `ProbeTimeout` (3s), so a dead address fails in ≤3s regardless of
  client retries. This is the sentinel-mode completion of LR-012.

- **Sentinel mode: the Redis liveness probe could wipe the last surviving copy of
  the data** (LR-016). The probe restarted any replica whose master was unreachable,
  intending to self-heal replicas following a ghost master — but a pod's local `INFO`
  cannot distinguish that case from a leaderless survivor, and since storage is
  EmptyDir the restart destroyed exactly the data leaderless recovery exists to
  preserve. The probe is now a plain local health check (bootstrap guard + local
  `PING`), matching standalone and cluster mode; topology repair stays
  operator-owned (`SLAVEOF` redirect without a restart). Readiness is unchanged and
  still gated on `link:up`, so a masterless replica is pulled from traffic without
  being killed.

- **Sentinel mode: ghost-replica `SENTINEL RESET` could deadlock a failover**
  (LR-013). After a force-deleted master, a broadcast `RESET` wiped Sentinel's
  replica list, which can only be rebuilt from the (now permanently dead) master's
  `INFO` — leaving Sentinel monitoring a dead IP with no known replicas and aborting
  failover indefinitely. The destructive ghost-replica `RESET` is now additionally
  gated on cluster wholeness (every expected Redis pod reachable, computed from
  already-gathered ground truth); when not whole the operator defers, since a stale
  ghost entry is harmless and is pruned on a later reconcile. Ghost-master
  correction, divergent-master correction and replica rescue still run during
  disruption.

- **Cluster mode: a restarted pod returning as an empty master could take minutes to
  reattach** (LR-014). Health was computed from slot-owning masters only, so an
  empty master still read "healthy": the operator dropped to the 30s steady cadence
  right when it needed the 2s healing cadence, and transient `ERR Unknown node`
  failures compounded. Empty masters now count as unhealthy, and `CLUSTER REPLICATE`
  is deferred until the empty master actually knows the target's node ID (using
  adjacency data already gathered — no extra round-trips).

- **Cluster mode: one stale pod IP could stall the whole reconcile loop** (LR-012).
  Ground-truth gathering dialed every pod IP serially with a 5s dial timeout and
  client-side retries, so each dead IP blocked ~25s and rapid pod churn starved the
  loop of the iterations needed to forget ghosts, re-`MEET` survivors and reassign
  replicas. Gathering now fans out concurrently with a hard 3s per-probe deadline,
  and `CLUSTER FORGET` skips unreachable nodes.

- **Cluster mode: `CLUSTER NODES` parsing counted migration markers as owned slots**
  (LR-018). The `[slot->-id]` / `[slot-<-id]` notations that appear mid-migration
  were parsed as slots, which made a range unparseable on the source and made an
  importing destination look like a slot-owning master — enough for the operator to
  declare the cluster healthy and abandon a reshard halfway, stranding keys on a node
  that did not own the slots. Latent until the reshard dance became the first code
  path to mark slots.

- **No PodDisruptionBudget is created for single-pod workloads** (#92). A PDB over a
  single pod can only ever block node drains and never protect availability. This
  now applies to standalone instances, cluster instances with
  `replicasPerShard: 0`, and — in the chart — the operator's own Deployment while it
  runs a single replica. Reconciliation also cleans up a PDB left behind by an
  earlier default-on version.

- The operator's own PDB template rendered both `minAvailable` and `maxUnavailable`,
  which is an invalid PDB spec. It now renders `minAvailable` when set (taking
  precedence, matching the operator's own resolution) and otherwise `maxUnavailable`,
  defaulting to 1.

- The Sentinel-monitor StatefulSet silently dropped `priorityClassName` and
  `topologySpreadConstraints` while the other three builders honored them; all four
  now propagate the full `spec.podTemplate` scheduling surface. The production sample's
  topology-spread `labelSelector` was also fixed — it selected on
  `app.kubernetes.io/name`, which is the constant `littlered`, so it matched no pods;
  the per-instance discriminator is `app.kubernetes.io/instance`.

- `.gitignore` matched the bare path component `littlered`, so new files under
  `charts/littlered/` and `cmd/littlered/` were silently ignored (existing files
  stayed tracked only because they predated the pattern). The pattern is now anchored
  to the repository root build binary.
