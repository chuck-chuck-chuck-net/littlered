# In-cluster e2e work-list harness

Runs e2e work items **inside a cluster** as a Kubernetes **Indexed Job**: one item
per index, a concurrency cap, always `exit 0`, a recorded per-item verdict,
failed items parked for forensics, and a one-glance rollup.

Design: `docs/E2E_SOAK_HARNESS_DESIGN.md` (§4 the sketch, §9.1 the work-list
generalization, §9.2 what the matrix forces, §11 the milestone plan).
**M1** built the array mechanics; **M2** gave every item its own test namespace
*and* its own namespace-scoped operator, deployed from the array's pre-built
image. That pairing is the point (§3): N parked failures can only sit side by
side if each operator is blind to the others' namespaces — otherwise they
double-reconcile each other's CRs and corrupt the very forensic state parking
exists to preserve. **M3** moved the driver to the **hub** and made a work item
stand up a stretched **multi-site** Redis Cluster across real workload clusters
(see "Multi-site arrays" below).

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
| `fixture/` | The named multi-site topologies of matrix §3 (`F2-K4`, …) as data, plus the arithmetic derived from them: pods, the initial master split, the quorum, LR-031's two thresholds. |
| `lab/` | The multi-site **substrate** seam: the lab inventory, the harness's per-site credential, the in-pod kubeconfig, the per-item `MS_CONFIG`. |
| `cmd/lrharness/` | Thin CLI over those seams. Lives in the driver image and builds to `bin/lrharness` on the host. |
| `driver/Dockerfile` | The driver image: Go toolchain + make + kubectl + helm + the repo, **alive at runtime**. |
| `driver/harness-entrypoint.sh` | The driver. Three phases (`item`, `setup`, `teardown`), selected by `LRH_PHASE`. Deliberately only plumbing — every decision is delegated to `lrharness`. |
| `manifests/worklist-*.yaml` | Sample work lists. `worklist-m2-classic.yaml` is the M2 acceptance list (per-item operators, a deliberate spec-level failure); `worklist-m3-multisite.yaml` is the M3 one (a stretched cluster, twice); `worklist-soak.yaml` is the classic "N runs of the suite". |
| `manifests/lab.sample.yaml` | Template for the **lab inventory** (`E2E_HARNESS_LAB`). Copy it; never commit yours. |

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
    redisImage: ...         # optional -> E2E_REDIS_IMAGE: the redis image the
                            #   CRs AND the preflight probe use
    fixture: F2-K4          # optional: a MULTI-SITE topology from matrix §3.
                            #   Setting it makes the item multi-site.
    env:                    # optional, merged over defaults.env
      SOME_KNOB: "value"
```

Every item needs **at least one of `labelFilter` / `focus`** — an item with
neither would silently select the entire suite. The whole list is validated at
launch, so a typo fails immediately rather than at item 37 of an overnight run.

**Quote your `env` values.** The list is parsed as YAML 1.1, where bare `yes`,
`no`, `on`, `off` are booleans: `EXTRA: yes` becomes the string `"true"`. Write
`EXTRA: "yes"`.

Each item gets TWO namespaces, both derived deterministically and truncated to
the 63-char DNS-label limit:

| | Holds |
|---|---|
| `lr-harness-<array>-<index>-<name>` | the test namespace: CRs, Redis pods |
| `lr-harness-op-<array>-<index>-<name>` | that item's own operator, watching only the namespace above |

The index precedes the name so truncating a long name can never collide, and the
two families differ in a segment truncation never reaches.

### `redisImage` — why an array should set it

The suite's image preflight probes with `imagePullPolicy: Always` **on purpose**
(it is checking the image is genuinely pullable, not cached), so it is the one
guaranteed-uncacheable public pull *per work item*. Over a long array that is
exactly the anonymous-Docker-Hub rate-limit exposure. Point `redisImage` at a
copy in your own registry and the run pulls nothing public — the CRs use it too,
so it is one knob, not two. The committed manifests keep the operator default;
a mirror hostname is never a shipped default.

**Mirror the metrics exporter as well.** `ExporterSpec.Registry` *inherits* the
main image's registry (`api/v1alpha1` `FullImage`), so pinning `redisImage` to
`myreg.lan/library/redis:8.4.2` also makes every pod pull
`myreg.lan/oliver006/redis_exporter:<tag>`. Mirror both or the pods sit in
`ImagePullBackOff` with `manifest unknown` (found the hard way on the first M2
array):

```bash
skopeo copy docker://docker.io/library/redis:8.4.2 \
            docker://myreg.lan/library/redis:8.4.2
skopeo copy docker://docker.io/oliver006/redis_exporter:v1.88.0 \
            docker://myreg.lan/oliver006/redis_exporter:v1.88.0
```

### Namespace teardown order is load-bearing

A `LittleRed` CR carries the operator's finalizer, so a test namespace can only
finish terminating **while its own operator is still running**. Both cleanup
paths (the driver's green-path cleanup and `make e2e-harness-clean`) therefore
delete the test namespace first, waiting, and only then the operator namespace.

**Multi-site extends that rule one cluster further out.** A multi-site item also
creates an instance namespace on every hosting **site**, and the same operator
holds the `multisite-teardown` finalizer that sweeps those spoke workloads. So
the operator has to outlive *everything* it owns, on both sides, and
`e2e-harness-clean` runs an ordered plan (decided in `List.CleanupPlan`, not in
the recipe):

| # | Where | What | Wait? |
|---|---|---|---|
| 1 | hub | the item **instance** namespaces (`array=X,!role`) | yes — their CRs hold the finalizer, and deleting the CR is what triggers the spoke sweep |
| 2 | each site | the item instance namespaces, **by exact name** | yes — and this is what *unblocks* a hub namespace stuck because a site was unreachable: once the site namespace is gone the sweep succeeds trivially |
| 3 | hub | the per-item **operator** namespaces (`role=operator`) | no — nothing holds a finalizer on them |

Step 3 is **conditional, not merely last**. If the instance namespaces did not
finish terminating, it is SKIPPED and the operator is left running — because it
is the only process that can release their finalizers, and deleting it would
strand them in `Terminating` forever:

```
==> hub: the per-item operator namespaces, last of all: ...
    SKIPPED — these namespaces have NOT finished terminating:
        namespace/lr-harness-guardtest-0-x
    ...
    Left running on purpose. Wait, then re-run this target; or release the
    finalizer by hand.
```

Re-running the target once the instance namespaces are gone completes the
cleanup. (Ordering alone was not enough — this was found by cleaning a real
parked multi-site array and stranding its namespace.)

Site steps delete by exact **name**, never by prefix: those namespaces are
created by the e2e suite and carry no harness labels, and a `lr-harness-*` glob
on a shared lab site would match another array's parked evidence — the opposite
of the rule this target is the sanctioned exception to.

Cleaning a multi-site array therefore needs the lab inventory, and **refuses
without it** rather than silently leaving the sites dirty:

```bash
make e2e-harness-clean E2E_HARNESS_WORKLIST=<list> E2E_HARNESS_LAB=~/my-lab.yaml
```

If one does get stranded in `Terminating`, the escape hatch is the usual one:

```bash
kubectl -n <test-ns> patch littlered <name> --type=json \
    -p='[{"op":"remove","path":"/metadata/finalizers"}]'
```

## Prerequisites

- A cluster reachable from the driver pod. **No operator needs to be running** —
  each item installs its own. The CRD is applied by each item's `make deploy`
  (`install-crd`, no codegen); it is cluster-scoped and shared, deliberately.
- The **operator** and **chaos-client** images pushed at the array's tag: the
  driver deploys the operator from that pinned ref and builds nothing, and the
  suite's `BeforeSuite` preflight refuses to start without the chaos client.
- A **cluster-scoped operator running beside the array will fight it** — it
  watches every namespace, including the items'. Scale it to zero first.
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
# 1. Build and push ALL THREE images the array needs — operator, chaos client
#    and driver (tag = GIT_TAG, i.e. the git short hash).
make e2e-harness-images LITTLERED_REGISTRY=registry.example.lan CONTAINER_TOOL=podman

# 2. Sanity-check the work list without touching the cluster.
make e2e-harness-validate E2E_HARNESS_WORKLIST=test/harness/manifests/worklist-m2-classic.yaml

# 3. Launch. Verifies every pinned image resolves, then renders namespace + RBAC
#    + work-list ConfigMap + Job and applies them.
make e2e-harness-launch \
     LITTLERED_REGISTRY=registry.example.lan \
     KUBECONTEXT=<your-context>

# 4. Follow it. Prints each verdict as it lands, exits when the array ends, and
#    finishes with the rollup (non-zero unless every item is green).
make e2e-harness-watch KUBECONTEXT=<your-context>

# 5. Tear down the array, its results and every namespace it parked.
make e2e-harness-clean KUBECONTEXT=<your-context>
```

Every step takes `E2E_HARNESS_WORKLIST=` — the default is the M1 smoke list.
`make e2e-harness-rollup` re-prints the summary at any time and always exits 0.

### Step 1 builds three images, not one

An array pulls **the driver** (the Job), **the operator** (each item
helm-installs its own from the pinned ref) and **the chaos client** (the suite's
preflight refuses to start without it). `e2e-harness-images` covers all three;
`make e2e-harness-image e2e-harness-push` is the driver-only fast path while you
are iterating on the driver itself.

### Step 3 refuses to launch on a missing image

A pinned ref that does not exist is otherwise invisible until each item has
deployed, waited out its own readiness timeout and failed — the design note's
"ImagePullBackOff on item 47", except every item, identically. So the launch
first resolves every ref the array pins:

```
image preflight (via skopeo):
  ok       registry.example.lan/library/redis:8.4.2
  MISSING  registry.example.lan/littlered:708ebce
  ok       registry.example.lan/littlered-e2e-driver:708ebce
  ok       registry.example.lan/oliver006/redis_exporter:v1.88.0

REFUSING TO LAUNCH: the array pins image(s) that do not resolve:
    registry.example.lan/littlered:708ebce
...
    make e2e-harness-images LITTLERED_REGISTRY=registry.example.lan CONTAINER_TOOL=podman
```

The redis and exporter refs are included whenever the work list pins
`redisImage` — including the exporter's *inherited* registry, which is the trap
described above. The lookup uses `skopeo` when present, otherwise
`$(CONTAINER_TOOL) manifest inspect`; a plain `curl` is not enough, because
`ghcr.io` demands a token even for public reads.

`E2E_HARNESS_SKIP_IMAGE_CHECK=true` disables it for a genuine ad-hoc case (an
air-gapped host, a registry that refuses anonymous manifest reads). **No shipped
work list needs it** — a fixture that only launches with the safety check off
would teach the escape hatch as normal practice, so both lists' deliberate
failures use a *pullable* image that simply is not redis.

### Step 3 also refuses to launch beside a conflicting operator

Every work item deploys its **own** namespace-scoped operator. A second operator
watching the same namespaces double-reconciles the items' CRs — exactly the
failure mode the per-item operator exists to prevent (§3) — and it keeps acting
on a **parked** namespace long after the item stopped, so what you eventually
inspect is not what failed. It corrupts the evidence rather than failing
cleanly, which is why this is a refusal:

```
REFUSING TO LAUNCH: array "mscleanup" would run beside 1 operator(s) that reconcile its namespaces:

    littlered-system/littlered
        cluster-scoped (no WATCH_NAMESPACE / IGNORE_NAMESPACE), so it watches every namespace this array creates
...
  1. Scale it to zero for the run, and back afterwards:

         kubectl -n littlered-system scale deploy littlered --replicas=0
```

The check reads each operator's `WATCH_NAMESPACE` / `IGNORE_NAMESPACE` and
intersects its scope with the namespaces this array will create — **both**
families, test and operator. A **scaled-to-zero** operator is not a conflict
(that is the documented fix, so the gate has to be satisfiable), and neither is
a parked operator of *this* array, whose namespace the driver clears anyway.
An operator with **both** scope variables set is refused rather than reasoned
about: it cannot start, but "it is crash-looping" is a guess about another
binary, and this gate fails closed.

**What it cannot see** — a guard with unstated blind spots is worse than one
with known limits:

- only Deployments labelled `control-plane=controller-manager` (what the chart
  stamps). An operator deployed by other means, relabelled, or running as a bare
  Pod/StatefulSet — or out-of-cluster from a workstation — is invisible.
- only literal env **values**; scope supplied through `envFrom` or a ConfigMap
  reference is not resolved.
- only the cluster the array launches on. That is the right scope: multi-site
  CRs live on the hub, so an operator on a *site* has no `LittleRed` to
  reconcile there and cannot fight the array.
- if `kubectl` cannot list Deployments cluster-wide the list is empty, which
  reads as "no conflict" and the launch proceeds.

`E2E_HARNESS_ALLOW_OPERATOR_CONFLICT=true` overrides it.

### Step 3 also refuses to launch over parked evidence

A parked namespace pair *is* the product of a failed item: debug artifacts on
the volume, and a live operator kept for inspection. Re-launching the same array
name used to delete them as "leftovers from an earlier run" — silently, with the
only warning inside a per-item driver log nobody reads until after the fact. One
run lost five parked failures that way.

That is the opposite of how parking itself behaves (a namespace is deleted only
on an explicit `"parked":false` — an unknown outcome keeps the evidence), so the
launch now applies the same rule:

```
REFUSING TO LAUNCH: array "parkguard" has 2 PARKED namespace(s) from an earlier run:

    lr-harness-op-parkguard-0-doomed                item doomed (FAIL), operator
    lr-harness-parkguard-0-doomed                   item doomed (FAIL), test namespace
...
  1. Read them, then discard them deliberately:

         make e2e-harness-clean E2E_HARNESS_WORKLIST=<your work list>

  2. Keep them and run under a different name: change 'array:' in the work list.
```

`E2E_HARNESS_ALLOW_PARKED_CLOBBER=true` means "yes, clobber my parked evidence".
It is passed through to the driver, so the launch gate and the per-item
clean-slate step always agree.

**Defence in depth:** the driver refuses the same thing independently, because a
Job applied by hand bypasses the Makefile. A blocked item is then **not run at
all** — it records a `FAIL` naming the namespaces that blocked it (and writes
`blocked.txt` beside its artifacts) rather than handing a parked namespace to the
suite, whose own clean-slate guard would delete a CR-less one and start anyway.
Ordinary (non-parked) leftovers are still cleared, as before.

### Step 4 follows verdicts, not pod phases

**A driver pod always says `Completed`, even for a failed item** — it exits 0 by
design (§4.2) so the array continues. Watching pod phase therefore shows a
five-failure array as five clean successes, and `kubectl get -w` never returns.
So `e2e-harness-watch` polls the result ConfigMaps instead, reprints the rollup
whenever an item lands, stops when the Job does, and exits non-zero unless every
item is green:

```
watching array 'm2watch' (2 items) — verdicts appear as items finish

--- 12:42:14Z  1/2 items reported ---
e2e harness array "m2watch": 2 items, 1 green
  IDX  ITEM             VERDICT    DURATION  NAMESPACE
  0    pdb-ok           PASS            59s  lr-harness-m2watch-0-pdb-ok
  1    -                MISSING           -

--- 12:44:57Z  2/2 items reported ---
e2e harness array "m2watch": 2 items, 1 green
  IDX  ITEM             VERDICT    DURATION  NAMESPACE
  0    pdb-ok           PASS            59s  lr-harness-m2watch-0-pdb-ok
  1    pdb-doomed       FAIL           151s  lr-harness-m2watch-1-pdb-doomed  [PARKED]

=== array 'm2watch' finished ===
exit 1: not every item is green (parked namespaces hold the evidence)
```

`E2E_HARNESS_WATCH_TIMEOUT` (seconds, default 86400) bounds it — deliberately
long, since a 20-item soak of the full suite runs for hours and a watch that
gave up first would report a false "incomplete". Hitting it exits **2** and says
the array is still running; re-attaching is just running the target again.

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
2. Deletes any leftover namespaces from an earlier run of the **same array**,
   then creates the operator namespace *labelled* (so a driver pod that dies
   mid-item still leaves something `e2e-harness-clean` can reap).
3. Runs, under the item's own `timeout`:
   `make run-test-e2e SKIP_KIND_SETUP=true DEBUG_ON_FAILURE=true
   TEST_NAMESPACE=… E2E_OPERATOR_DEPLOY_MODE=pinned E2E_OPERATOR_NAMESPACE=…
   E2E_OPERATOR_SCOPED=true LABEL_FILTER=… FOCUS=…`
   The suite then helm-installs the operator itself, from the pinned image,
   scoped to this item's test namespace. Nothing is rebuilt.
4. Moves the suite's `debug-artifacts-*` directories onto the artifact volume.
5. `lrharness record` classifies the outcome and writes `result.json`; the
   result is published as a labelled ConfigMap.
6. **Green** → deletes both namespaces. **Not green** → leaves both standing
   (operator still running) labelled `lr-e2e-harness/parked=true`.
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

## Multi-site arrays (M3)

A work item that names a **fixture** stands up a stretched Redis Cluster across
several workload Kubernetes clusters, driven from a Job on the **hub**. Nothing
about the array mechanics changes — same Indexed Job, same always-`exit 0`, same
parking, same rollup.

```yaml
items:
  - name: f2k4-a
    fixture: F2-K4                       # matrix §3
    labelFilter: cluster-behaviour
    focus: Bring-up and basic operations
```

The fixtures are the whole matrix §3 table (`lrharness fixture --name ...` prints
one). `F2-K4` is 2 sites, `shards: 4`, `sitesPerShard: 2`, `replicasPerShard: 0`
⇒ **8 pods**, initial master split **2+2**, quorum 3 — the recommended dual-site,
whose even split sits exactly on the takeover floor in both directions.

The fixture is a **name**, not four numbers, on purpose: an item that stands up
the wrong topology is not a failing test but a passing test that measured
something else. An unknown name is refused at launch, before any cluster is
touched.

### Bootstrap the lab once

```bash
cp test/harness/manifests/lab.sample.yaml ~/my-lab.yaml   # fill in, do NOT commit
make e2e-harness-lab-validate E2E_HARNESS_LAB=~/my-lab.yaml
make e2e-harness-sites-setup  E2E_HARNESS_LAB=~/my-lab.yaml
```

That applies the harness's own per-site credential to each site (using your
admin kubecontexts), harvests the ServiceAccount tokens and CAs, and publishes
them on the hub as a Secret plus the inventory as a ConfigMap. Idempotent:
re-run it after adding a site or rotating a token. `make e2e-harness-sites-clean`
removes it all again.

Each site's `endpoint` must be routable **from hub pods**, not from your
workstation — different network positions. Verify from a pod.

### Why the harness site credential is a `ClusterRole`

Because `namespaces` is a **cluster-scoped resource**: a namespaced `Role`
cannot grant `create` on it at any rule content, and the harness creates one
fresh namespace per work item on every site. Pre-provisioning cannot serve that
either — the names are derived from array+index+item, and the e2e suite refuses
to start if its test namespace already exists.

**This is not the operator's credential and must never be merged with it.**
ADR-012's "multi-site keeps the namespaced per-site Role — no cluster-scoped
RBAC" is a property of the **operator**, whose per-site `littlered-site` Role is
untouched and stays namespaced. The harness is a *test driver*: it execs into
redis pods (the operator never does — it reaches Redis over TCP) and it creates
namespaces (the operator never does). Separate objects, separate names
(`lr-e2e-harness-driver`), so a later reader cannot "simplify" the two together.

### The run-scoped setup / teardown phases

The same image runs three phases, selected by `LRH_PHASE`:

| Phase | When | What |
|---|---|---|
| `setup` | once, before any item | assembles the kubeconfig and reaches **every** site. Every item waits for it. |
| `item` | per index | the work item (always `exit 0`). |
| `teardown` | once, after the array | reports what the run left on each site. **Deletes nothing.** |

The ordering is enforced **in-cluster** — the driver blocks on the setup Job's
completion — rather than by whoever launched the array, because a workstation in
the run path is exactly what §9.3 exists to remove. A failed setup blocks every
item with a recorded `FAIL` that names it, and no namespaces are created:

```
[harness 20:54:36]   ok       site scm-s1 reachable (8 namespaces)
[harness 20:54:36]   UNREACHABLE  site scm-s2: ... Client.Timeout exceeded ...
[harness 20:54:36] REFUSING TO RUN THE ARRAY: at least one site is unreachable from this pod.
...
[harness 20:54:39] SKIPPING item 'f2k4-a': the run-scoped setup Job 'msgate-setup' FAILED
```

Teardown stays **report-only**, deliberately, and it is worth saying why it was
not promoted to a deleting phase once `e2e-harness-clean` learned to reach the
sites. Three reasons, in increasing sharpness:

1. A Kubernetes Job cannot schedule a successor, so this phase is fired by
   `e2e-harness-watch` (or by hand) and that trigger is **not guaranteed**. A
   phase that deletes would be a phase that sometimes silently does not — the
   worst property a cleanup can have, because you stop checking.
2. It runs **immediately after the array**, when a parked namespace is the
   freshest evidence there is. Deleting there would race the very inspection
   parking exists for.
3. There is now a deliberate path that does reach the sites —
   `e2e-harness-clean` — so nothing is lost by keeping this one honest.

So the division is: the teardown phase **names** what the run left behind on
each site; `e2e-harness-clean` is the explicit instruction that removes it. The
standing rule is unchanged — the harness never destroys forensic evidence unless
explicitly told to.

Nothing needs the phases until M5's resident split-brain monitor; they exist now
so that monitor slots in without restructuring.

### `MS_CONFIG` is rendered per item, not mounted whole

The **lab inventory** is mounted (a ConfigMap). Each item's `MS_CONFIG` is
rendered from it at run time, into the item's artifact directory. That is forced
rather than chosen: the operator resolves its site kubeconfig Secrets from its
**own** namespace, and every item deploys its own operator into its own
namespace — so `operatorNamespace` cannot be a launch-time constant. The suite
re-reads `MS_CONFIG` on every call, so a rendered file needs no suite change.

The rendered file deliberately omits `vms` and `hubPodCIDR`: those arm the
whole-site power-off and the data-plane cut respectively, and **M3 injects no
faults**.

### A multi-site item watches TWO namespaces

A scoped operator's cache is restricted to its watched namespaces, but the site
kubeconfig Secrets live in the operator's **own** namespace — outside that
cache. So a multi-site item passes `E2E_WATCH_NAMESPACES=<test>,<operator>`
instead of the single-site `E2E_OPERATOR_SCOPED=true` shorthand. Leaving the
operator namespace out is a silent deadlock, not an error: the operator simply
never reads a site credential and the stretched cluster never comes up.

### Running one

```bash
make e2e-harness-images LITTLERED_REGISTRY=<your registry> CONTAINER_TOOL=podman
make e2e-harness-launch E2E_HARNESS_WORKLIST=test/harness/manifests/worklist-m3-multisite.yaml \
     LITTLERED_REGISTRY=<your registry> KUBECONTEXT=<hub>
make e2e-harness-watch  E2E_HARNESS_WORKLIST=test/harness/manifests/worklist-m3-multisite.yaml \
     KUBECONTEXT=<hub>
```

**Scale any cluster-scoped operator on the hub to zero first** — it watches every
namespace, including each item's, and would double-reconcile the CRs.

## Known limitations

- **`FOCUS` cannot contain a single quote.** `run-test-e2e` interpolates it into
  `-ginkgo.focus='$(FOCUS)'` inside a shell recipe. That is pre-existing
  Makefile behaviour, not a harness constraint.
- **Re-running an array name with parked namespaces is refused, not silently
  clobbered** (see "Step 3 also refuses to launch over parked evidence" above). The
  array name is still the uniqueness knob — use a fresh one, or
  `make e2e-harness-clean` first.
- **No garbage budget.** N parked failures = N namespaces of Redis pods *plus* N
  idle operators resident (design §6 flags this as open).
- **`hostPath` artifacts** assume a single-node cluster, or that you only ever
  read artifacts from the node that ran the item. Use `--artifacts-pvc` where a
  StorageClass exists.
- **A parked multi-site item leaves namespaces on the SITES too.** The
  report-only teardown phase names them; `make e2e-harness-clean
  E2E_HARNESS_LAB=<inventory>` removes them, in the order above. Without the
  inventory that target now refuses rather than half-cleaning.
- **The run-scoped teardown only fires if something fires it** —
  `e2e-harness-watch`, `e2e-harness-teardown`, or a human. A Job cannot schedule
  a successor. Harmless today (the phase is report-only); a phase that must run
  unattended — M5's monitor shutdown — will need a small controller or a
  wrapping workflow rather than this hook.
- **The conflicting-operator gate is a Deployment check**, with the blind spots
  listed under "Step 3 also refuses to launch beside a conflicting operator". It
  cannot see an operator that is not a labelled Deployment on the target
  cluster — an out-of-cluster one run from a workstation being the realistic
  case.
- **`e2e-harness-clean` reaches every site in the inventory**, not only the ones
  a fixture actually used. Deleting a namespace that was never created is a
  harmless no-op, and it is the safer direction: an array whose fixture list
  changed still gets fully cleaned.
