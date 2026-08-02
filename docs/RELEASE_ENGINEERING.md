# Release Engineering & Git Workflow — Split-by-Altitude

This document defines how LittleRed branches, integrates, and releases. It is the
canonical working mode; sessions (human or LLM) should follow it and keep it current.

---

## The model in one sentence

**Rebase *inside* a branch; merge `--no-ff` *at integration*; read the trunk with
`git log --first-parent`.**

The two operations live at different *altitudes* and optimize different things. Do not
apply one altitude's rule at the other.

| Altitude | Operation | Why |
|----------|-----------|-----|
| **Inside a branch** (WIP on `feature/*`, `fix/*`) | rebase / squash | Make each branch read as one tidy story. Kill accidental pull-merges. A branch's internal history is a draft — clean it up freely. |
| **At integration** (`feature/fix → e2e-* → release/* → main`) | merge `--no-ff` | The integration boundary is a *fact* worth recording: which named branch went in, its tip commit, and how inter-feature conflicts were actually resolved. A merge records this once and honestly. |

`--first-parent` on the trunk still reads as one clean line per feature — you get the
linear-reading experience without manufacturing a fiction via conflict re-resolution.

---

## Why merge (not rebase) at integration

- **Provenance is native.** A `--no-ff` merge commit permanently records the branch name
  and its tip SHA (as the second parent). Rebase-flattening loses this the moment the
  `feature/*` branch is deleted. This is the whole reason the model exists: we want "which
  branches are in this release" answerable *from git*.
- **The clean-apply guarantee is structural, not observed.** Cut `release/*` **from `main`**
  (or from the prior release when releases stack — see below) and only ever *add* to it
  (feature merges, plus merges of the base if it advances). Then the base is always an
  ancestor of `release/*`, so it merges/fast-forwards **by construction** — a stronger
  guarantee than "I rebased it clean once" (which goes stale the moment the base moves).
  Verify with `git merge-base --is-ancestor`.
- **Honest history.** Linearizing parallel work that conflicts is *not* conflict-free: rebase
  re-resolves conflicts commit-by-commit and can leave intermediate commits that don't build
  (silently defeating `git bisect`). A merge resolves once, in the merge commit, and every
  commit on the branches stays buildable.
- **Whole-feature operations are cheap.** Roll back an entire feature with
  `git revert -m 1 <merge>`; find a bad *feature* with `git bisect --first-parent`, then dive
  into its handful of commits.

**The one thing we give up:** the full DAG of `main` has merge bubbles. `--first-parent`
hides them by default, so the trunk still *reads* linear.

---

## Branch types

| Branch | Base | Lifetime | Integration style |
|--------|------|----------|-------------------|
| `feature/*`, `fix/*` | **`main`** | until merged, then deleted | internal history rebased/squashed clean |
| `e2e-*` | `main` | disposable, rebuilt freely | `--no-ff` merge **everything** — the "latest & greatest" superset for e2e testing |
| `release/<ver>` | **`main`** (or the prior `release/*` when stacking) | until landed | `--no-ff` merge the **curated subset** agreed for the release |
| `main` | — | forever | `release/*` lands via `--no-ff` merge + tag (or `--ff-only` if a strictly linear `main` is wanted) |

- Base `feature/*` / `fix/*` on **`main`**, not on `e2e-*` — keeps them independently
  mergeable and their SHAs stable across worktrees.
- `e2e-*` (superset, "does it all work together") and `release/*` (curated cut, "what we
  ship") are genuinely different sets — keeping both families is intentional, not redundant.
- **Stacked releases.** When a batch of releases is staged ahead of `main` (i.e. `main` is
  many commits behind and releases build on each other cumulatively), cut the next
  `release/<ver>` from the **prior release**, not from stale `main`. The ancestry guarantee
  still holds — just against the prior release as the base. This is the *only* sanctioned
  deviation from "base on `main`", and it exists because a fast-moving doc/code line (e.g.
  `CLAUDE.md`) far ahead of `main` cannot be edited coherently on top of `main`. Once the
  backlog lands on `main`, the deviation collapses back to the default.
- Experimental work stays **local/private**. Push only `main` and tags to the public repo.
  PRs are a review UI, orthogonal to this topology — not required by it.

---

## Cheat sheet

### Start work
```bash
git switch -c feature/foo main        # branch off main (stable base)
# ... rebase/squash internal WIP freely before integrating ...
```

### Build / refresh the e2e superset
```bash
git switch -c e2e-0802 main            # or reuse; it is disposable
git merge --no-ff feature/foo fix/bar  # pull in everything; rebuild any time
```

### Cut and populate a release
```bash
git switch main
git switch -c release/0.3.0                          # cut FROM main (or from the prior release when stacking)
git merge --no-ff feature/foo -m "Merge feature/foo into release/0.3.0 (LR-0XX)"
git merge --no-ff fix/bar    -m "Merge fix/bar into release/0.3.0 (LR-0YY)"
```

### Confirm it will land on its base cleanly (structural, anytime)
```bash
git merge-base --is-ancestor main release/0.3.0 && echo "ff-clean onto main"
# if the base advanced and you want release current:
git switch release/0.3.0 && git merge --no-ff main
```

### Land the release
```bash
git switch main
git merge --no-ff release/0.3.0 -m "Release 0.3.0"   # bubble on main (richest provenance)
#   or: git merge --ff-only release/0.3.0            # strictly linear main
git tag -a v0.3.0 -m "Release 0.3.0"
```

### Provenance queries
```bash
git log --first-parent --oneline release/0.3.0   # one line per feature merged (trunk view)
git log --merges --oneline release/0.3.0          # the integration merges, with branch names
git branch --merged release/0.3.0                 # which branches are contained
git log --oneline --graph --first-parent main     # linear-reading trunk history
git log --oneline --graph main                    # full DAG, bubbles visible
```

### Whole-feature rollback
```bash
git revert -m 1 <merge-commit>       # revert an entire merged feature (mainline = parent 1)
```

### Bisect across features, then within
```bash
git bisect start --first-parent      # find the bad *feature* on the trunk
# then bisect normally inside that feature's commits
```

### Make conflict resolutions replay (set once, global)
```bash
git config --global rerere.enabled true   # reuse recorded resolutions across rebuilds/merges
```

---

## Glossary

- **DAG** — Directed Acyclic Graph. Git history: commits point to parent(s) ("directed"),
  never looping ("acyclic"). A merge commit has two parents — the point where the graph
  splits and rejoins.
- **`--first-parent`** — walk only the trunk side of each merge (parent 1), skipping the
  merged-in branch's commits. Makes a merge-based trunk read as one line per feature.
- **`--no-ff`** — force a merge commit even when a fast-forward is possible, so the
  integration is recorded as a distinct node rather than silently flattened.
