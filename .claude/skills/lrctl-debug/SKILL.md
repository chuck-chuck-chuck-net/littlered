---
name: lrctl-debug
description: >-
  Get ground truth about a LittleRed/Redis instance with the lrctl CLI when
  debugging. Use whenever investigating an e2e test failure, a stuck
  reconciliation, a suspected split-brain / ghost-master / ghost-node, a wrong
  master label, or any "what is the actual Redis/Sentinel/Cluster topology right
  now" question — instead of hand-rolling kubectl exec + redis-cli loops. Covers
  the read-only verbs: status, verify, inspect, debug-dump.
---

# lrctl-debug — ground truth for LittleRed instances

When debugging, **do not** hand-roll `kubectl exec ... redis-cli` loops to discover
topology. `lrctl` already gathers operator-side ground truth and cross-checks it.
Reach for it first.

`lrctl` is on PATH (installed via `make install-lrctl`). If it's missing, build it:
`make lrctl` → `./bin/lrctl`. It's also a kubectl plugin (`kubectl lr ...`) after
`make install-plugin`.

## Global flags (apply to every verb)

- `-n <ns>` / `--namespace <ns>` — target namespace (defaults to current context ns)
- `-A` / `--all-namespaces` — operate on every LittleRed in the cluster (no name arg)
- `--json` — machine-readable output; **prefer this when parsing/reasoning**, the
  text form when showing a human
- `--kubeconfig <path>` — for e2e Kind clusters, point at the test kubeconfig
- `--unmanaged --kind <standalone|sentinel|cluster>` — inspect a Redis instance
  that has **no** LittleRed CR (heuristic pod discovery); requires an explicit name

Name arg is optional for `status`/`verify`/`inspect` — omit it to act on **all**
instances in the namespace.

## Which verb for which question

| Symptom / question | Verb | Why |
|---|---|---|
| "Is the CR healthy? what does the operator *think*?" | `status` | Reads CR `.status` only (Phase, Master, Ready counts, bootstrapRequired). Fast, no exec. |
| "Is the actual topology consistent? split-brain? ghost?" | `verify` | Gathers live ground truth, computes the authority master, flags ghosts/partitions, and prints **recommended healing actions**. This is the workhorse for failover bugs. |
| "Show me the raw redis-cli output from every pod" | `inspect` | Deep dive: per-pod `INFO replication` (sentinel) or `CLUSTER NODES`+`CLUSTER INFO` (cluster), plus `SENTINEL MASTER` from each sentinel. Use when `verify` flags something and you need the raw evidence. |
| "Is a declared heavy operation stuck?" | `status`, then `verify` | `status` shows `status.operation` (name, reason, how long). `verify` prints a **Declared Operation** block and is the one that judges it: `Blocked` and `Stalled` **fail** and set a non-zero exit, `Running` and `Quarantined` do not. See below. |
| "Capture everything for a bug report / later analysis" | `debug-dump` | Writes CR YAML, operator logs (last 10m), all pod logs (incl. previous for crashloops), live redis state, and k8s resources to a timestamped dir. Secrets auto-redacted. |

## Recommended debugging order

1. `lrctl status <name> -n <ns>` — what the operator believes.
2. `lrctl verify <name> -n <ns>` — what's actually true + heal actions. **Start here
   for any failover / master-label / consistency bug.**
3. `lrctl inspect <name> -n <ns>` — raw per-pod evidence when `verify` flags an issue.
4. `lrctl debug-dump <name> -n <ns>` — when you want to preserve the whole state
   (e.g. an e2e failure you can't reproduce on demand).

## Reading `verify` output

- **Sentinel mode:** look at the *Authority Master* line. `[OK] Authority Master: <pod>`
  is good; `GHOST(<ip>)` means the consensus master IP has no live pod (the LR-008
  ghost-master case); `NONE` means split-brain or uninitialized. `[!] failover in
  progress` and the `Recommended Healing Actions` list tell you what the operator
  would do next.
- **Failover mode:** two layers — *Assignment Intent* (what the operator stamped:
  intended master + max assignment epoch, from the pod annotations) and
  `Authority Master` (intent ∩ observation: the intended pod, reachable and
  `role:master`). Per-pod lines show `assigned:<role>@<epoch>` next to the observed
  role/label, so intent-vs-reality disagreement is visible at a glance. Findings:
  `straggler` (unintended `role:master` — Rule-R repoint pending), `PARKED`
  (not-Ready + restarted + consumed epoch — the kill-9 yield, waiting for an operator
  epoch bump), label↔authority disagreement, lineage divergence across data holders.
  Mid-transition captures legitimately show `[FAIL]` for a few seconds (epoch already
  bumped, promotion not settled) — re-run before concluding it's stuck.
- **Declared operations (any mode, ADR-020):** a `Declared Operation` block appears only while
  one is in flight. `[OK] Running` means the operator is deliberately standing down the healing
  that **assigns authority** (Rule L and the LR-024 recovery in sentinel mode) until the declared
  change completes — **this is normal and is not a fault**, and `verify` will not fail for it.
  `[WARN] Quarantined` means a heavy change is pending but held because the instance has no pods;
  the real problem is the quarantine (see `Forsaken`), not the operation. `[FAIL] Blocked` and
  `[FAIL] Stalled` **never clear themselves** — no timer will rescue either, by design (ADR-017:
  a timer would be the defect with a delay), so they mean a human has to act. Note that `verify`
  can still exit non-zero for an ordinary topology reason *while* an operation reads `[OK]`; the
  two verdicts are deliberately independent, and `--json` carries `operation.needsAction` so a
  script agrees with the exit code.
- **Cluster mode:** `Cluster State`, `Total Slots Assigned: N / 16384` (anything <16384
  = gaps), `Ghost Nodes` (in cluster but not in K8s → expect `CLUSTER FORGET`),
  `Network Partitions`, and the per-master topology tree with replica link status.

## e2e specifics

E2e tests run either against a throwaway Kind cluster (default `make test-e2e`) or
against an existing external cluster (`make test-e2e SKIP_KIND_SETUP=true
SKIP_OPERATOR_DEPLOY=true`). Both are normal.

**Which cluster does `lrctl` talk to?**

- If the session context gives no explicit cluster/context, **use the current
  default kubectl context** — just run `lrctl ...` with no `--kubeconfig`. Don't
  assume Kind.
- If the session context introduced an explicit context (a named kube-context, a
  `KUBECONFIG` path, a Kind cluster name), **use that one** — pass `--kubeconfig
  <path>` or set the right context first.

When a test fails mid-run, `lrctl verify -A` is the fastest way to see which
instance is unhealthy across all the test namespaces at once.

A nonzero exit from `verify` means "not healthy/consistent" — useful as a signal,
but read the printed detail (or `--json`) to know *why*.
