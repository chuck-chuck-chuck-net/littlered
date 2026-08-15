# In-cluster e2e work-list harness (M1)

Runs e2e work items **inside a cluster** as a Kubernetes **Indexed Job**: one item
per index, a concurrency cap, always `exit 0`, a recorded per-item verdict,
failed items parked for forensics, and a one-glance rollup.

Design: `docs/E2E_SOAK_HARNESS_DESIGN.md` (§4 the sketch, §9.1 the work-list
generalization, §9.2 what the matrix forces, §11 the milestone plan).
**This is M1** — the array mechanics, proven on a ~2-minute work item against a
pre-existing cluster-scoped operator. Per-item, namespace-scoped operators are
**M2** and are not implemented here.

## Why a work LIST and not a repeat counter

The two workloads this harness serves differ only in their iteration variable:

- **Soak** — `for i in 1..N: run(the whole suite)`, for flake statistics.
- **Matrix** — `for cell in cells: run(cell)`, for coverage.

So the driver takes a **work list**, and the soak workload is the degenerate case
where every item is identical (`manifests/worklist-soak.yaml`). Retrofitting a
work list onto a repeat counter later is the one avoidable rework the design
calls out (§11).

## Layout

| Path | What |
|---|---|
| `worklist/` | The **pure seam**: parse, resolve index→item, classify a verdict, fold a rollup, render the manifests. No I/O; unit-tested. |
| `cmd/lrharness/` | Thin CLI over that seam. Lives in the driver image and builds to `bin/lrharness` on the host. |
| `driver/Dockerfile` | The driver image: Go toolchain + make + kubectl + helm + the repo, **alive at runtime**. |
| `driver/harness-entrypoint.sh` | The per-index driver. Deliberately only plumbing — every decision is delegated to `lrharness`. |
| `manifests/worklist-*.yaml` | Sample work lists. |

## The work-list schema

```yaml
array: m1smoke              # required, DNS label. Names the Job, the result
                            # ConfigMaps and the per-item namespaces.

defaults:                   # optional; layered UNDER every item
  timeout: 8m               # Go duration
  operatorImage: ...        # pinned refs for the whole array
  chaosClientImage: ...
  env:                      # passed through to the suite
    E2E_TOPOLOGY: single

items:                      # required, >= 1
  - name: pdb-standalone-a  # required, DNS label, unique within the array
    labelFilter: pdb        # Ginkgo label selector  -> make LABEL_FILTER=
    focus: should never ... # Ginkgo focus regex     -> make FOCUS=
    timeout: 45s            # optional, overrides defaults.timeout
    operatorImage: ...      # optional per-item override (design §9.2.6:
    chaosClientImage: ...   #   matrix cell G9 starts on an older operator)
    env:                    # optional, merged over defaults.env
      SOME_KNOB: "value"
```

Every item needs **at least one of `labelFilter` / `focus`** — an item with
neither would silently select the entire suite. The whole list is validated at
launch, so a typo fails immediately rather than at item 37 of an overnight run.

**Quote your `env` values.** The list is parsed as YAML 1.1, where bare `yes`,
`no`, `on`, `off` are booleans: `EXTRA: yes` becomes the string `"true"`. Write
`EXTRA: "yes"`.

Each item gets its own namespace, derived deterministically as
`lr-harness-<array>-<index>-<name>`, truncated to the 63-char DNS-label limit.
The index precedes the name so truncating a long name can never collide.

## Prerequisites

- A cluster with the LittleRed **CRD installed and an operator running** (M1 uses
  `SKIP_OPERATOR_DEPLOY=true`).
- The `littlered-chaos-client` image pushed at the array's tag — the suite's
  `BeforeSuite` preflight refuses to start without it.
- A node path for artifacts (default `hostPath: /var/lib/lr-e2e-harness`), or a
  PVC via `lrharness render --artifacts-pvc`.

### Use a local private registry (strongly recommended)

Point the existing `LITTLERED_REGISTRY` variable at a registry on your own
network for all harness work:

```bash
make e2e-harness-images LITTLERED_REGISTRY=registry.example.lan
```

- **No pull rate limits.** A long array plus its build loop is exactly the
  workload that trips anonymous public-registry quotas — and it fails in the
  least helpful way, mid-run, as an `ImagePullBackOff` on item 47.
- **LAN-speed push/pull**, which dominates the build→test loop while the driver
  image is still changing.
- **No external dependency** for a substrate that is otherwise entirely local.

Requirements are modest: DNS-resolvable, a valid certificate, and reachable
**from inside the target cluster's pods** — verify that last one *from a pod*,
not from your workstation, because those are different network positions.
Anonymous/no-auth is fine and appropriate for a LAN-only registry; confirm it
accepts *pushes*, not only pulls.

**The committed default stays `ghcr.io/littlered-operator`.** littlered is public
OSS, so a LAN hostname as the shipped default would break every other user. This
is an environment variable you set, never a checked-in value.

## Running an array

```bash
# 1. Build and push the driver image (tag = GIT_TAG, i.e. the git short hash).
make e2e-harness-images LITTLERED_REGISTRY=registry.example.lan CONTAINER_TOOL=podman

# 2. Sanity-check the work list without touching the cluster.
make e2e-harness-validate E2E_HARNESS_WORKLIST=test/harness/manifests/worklist-m1-smoke.yaml

# 3. Launch. Renders namespace + RBAC + work-list ConfigMap + Job, and applies it.
make e2e-harness-launch \
     LITTLERED_REGISTRY=registry.example.lan \
     KUBECONTEXT=<your-context>

# 4. Watch, then read the rollup.
make e2e-harness-watch  KUBECONTEXT=<your-context>
make e2e-harness-rollup KUBECONTEXT=<your-context>

# 5. Tear down the array, its results and every namespace it parked.
make e2e-harness-clean  KUBECONTEXT=<your-context>
```

`make e2e-harness-render` prints the manifest without applying it — use it to
review the RBAC and the Job before launching.

### Image pull policy — the stale-tag trap

The driver image tag is the git short hash, which is **stable across a dirty
tree**. That is convenient for pushing (no commit needed to rebuild) and a trap
for pulling: re-pushing the same tag while a node already has it cached means
`IfNotPresent` keeps serving the **stale** image, and your change silently does
not take effect.

- **Iterating on the driver**: `E2E_HARNESS_PULL_POLICY=Always`, or give the dev
  build a distinct tag.
- **A real array**: the shipped default `IfNotPresent` is right — the array pins
  one pre-built ref and every item reuses it, so re-pulling per item is waste.

## What the driver does per index

1. `lrharness plan --index $JOB_COMPLETION_INDEX` resolves the item.
2. Deletes any leftover namespace from an earlier run of the **same array**.
3. Runs, under the item's own `timeout`:
   `make run-test-e2e SKIP_KIND_SETUP=true SKIP_OPERATOR_DEPLOY=true
   DEBUG_ON_FAILURE=true TEST_NAMESPACE=… LABEL_FILTER=… FOCUS=…`
4. Moves the suite's `debug-artifacts-*` directories onto the artifact volume.
5. `lrharness record` classifies the outcome and writes `result.json`; the
   result is published as a labelled ConfigMap.
6. **Green** → deletes the namespace. **Not green** → leaves it standing and
   labels it `lr-e2e-harness/parked=true`.
7. **Always exits 0.**

Step 7 is the load-bearing one (§4.2). A driver that propagated a failing exit
code would burn the Job's `backoffLimit` and stop the array — the opposite of
what a soak run wants. `backoffLimit: 0` therefore means "the driver itself
crashed", never "a work item failed".

`DEBUG_ON_FAILURE=true` is always set: it makes the suite collect artifacts *and*
skip its own cleanup on failure, which is exactly the parking behaviour wanted.

### Artifacts

`CollectDebugArtifacts` writes `./debug-artifacts-<ts>-<spec>` relative to the
test binary's working directory — which `go test` sets to the **package**
directory — and offers no override. Rather than patch the suite, the driver
**moves** those directories onto the artifact volume after each item, into:

```
/var/lib/lr-e2e-harness/<array>/<NNN>-<item>/
  run.log          # the item's full stdout/stderr
  result.json      # the verdict record
  driver.log       # the driver's own kubectl calls
  debug-artifacts-*/   # relocated, on failure
```

### Reading results

Each item writes **its own** result ConfigMap (`lrh-<array>-<NNN>`), never a
shared one that every item patches: at `parallelism > 1` (which §9.2.2 wants for
Phase P) concurrent patches of a single object would conflict and lose results.

The rollup counts against the **work-list length**, not the number of results
found, so an index whose pod died is reported `MISSING` rather than quietly
shrinking the denominator.

## Known limitations (M1)

- **One shared, cluster-scoped operator.** Parked namespaces are still watched
  and reconciled by it, so a parked run is not fully frozen. Per-item
  namespace-scoped operators are M2 (design §3, §5.3).
- **`FOCUS` cannot contain a single quote.** `run-test-e2e` interpolates it into
  `-ginkgo.focus='$(FOCUS)'` inside a shell recipe. That is pre-existing
  Makefile behaviour, not a harness constraint.
- **Re-running the same array name clobbers its own parked namespaces.** The
  array name is the uniqueness knob — use a fresh one, or
  `make e2e-harness-clean` first.
- **No garbage budget.** N parked failures = N namespaces of Redis pods resident
  (design §6 flags this as open).
- **`hostPath` artifacts** assume a single-node cluster, or that you only ever
  read artifacts from the node that ran the item. Use `--artifacts-pvc` where a
  StorageClass exists.
