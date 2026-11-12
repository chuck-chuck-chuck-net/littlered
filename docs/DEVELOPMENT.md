# Development Guide

This document covers building LittleRed from source, working with a custom registry, and running the full test suite locally.

## Prerequisites

- **Go** 1.24+
- **buildx** and a BuildKit worker, for images (see *Getting a BuildKit worker* below)
- **Helm** 3.13+
- **kubectl** 1.28+
- **Kind** (for local e2e testing)
- **make**

## Building

Build the operator and chaos-client binaries:

```bash
make build
```

Build container images (without pushing):

```bash
make build-images
```

The image build graph is `docker-bake.hcl`; CI runs it with `docker buildx bake`, and the
Makefile's image targets (`build-images`, `images`, `docker-build`, `kind-load`, …) are aliases
onto it. `buildx bake --print` shows what a run would build, with every tag resolved.

### Getting a BuildKit worker

`buildx` is a client; the build happens in a worker. The graph is identical on all of them:

- **Docker's built-in worker.** Nothing to set up: `BUILDX="docker buildx"`, the Makefile's
  default, already addresses it.
- **A rootless `buildkitd` of your own**, with no Docker on the machine. Most distributions'
  `buildkit` package ships a systemd user unit for it:

  ```bash
  grep "^$USER:" /etc/subuid /etc/subgid   # the daemon needs a subordinate id range
  systemctl --user enable --now buildkit.service
  loginctl enable-linger "$USER"           # else the worker stops with your last session
  buildx create --driver remote --name local-rootless --use \
      "unix://$XDG_RUNTIME_DIR/buildkit/rootless"
  buildx inspect --bootstrap local-rootless
  ```

  Then `export BUILDX=buildx` — without Docker, the default `docker buildx` fails on a missing
  `/var/run/docker.sock`.
- **A remote worker**: `buildx create --driver remote --name <n> tcp://<host>:<port>`.

A worker with the `remote` driver has no local image store, so a build without `--push` stays in
its cache. `make kind-load` has the builder write image archives instead; `BAKE_FLAGS=--load`
keeps a build in a local Docker store where there is one.

## Using a Custom Registry

All registry-aware targets accept `LITTLERED_REGISTRY` as an override:

```bash
export LITTLERED_REGISTRY=registry.example.com/myorg

make images           # build + push both images
make helm-push        # package + push Helm chart to registry.example.com/myorg/charts/littlered
make push-latest      # re-tag as :latest (only runs on release tags vX.Y.Z)
```

The images land at:

```
registry.example.com/myorg/littlered:<tag>
registry.example.com/myorg/littlered-chaos-client:<tag>
```

The Helm chart lands at:

```
oci://registry.example.com/myorg/charts/littlered:<version>
```

### Authenticate before pushing

```bash
docker login registry.example.com   # buildx reads Docker's credential store
helm registry login registry.example.com
```

buildx and Helm maintain separate credential stores, so both logins are needed.

## Local Development with Kind

### 1. Create a Kind cluster

```bash
make setup-test-e2e
```

This creates a Kind cluster named `littlered-test-e2e`. Skip if you already have one:

```bash
SKIP_KIND_SETUP=true make test-e2e
```

### 2. Build and load images

```bash
make build-images
make kind-load
```

`kind-load` has the builder write both images as archives under `.build/` and loads them into the cluster — no registry push needed. It builds as well, so `build-images` beforehand only warms the cache.

### 3. Deploy the operator

```bash
make deploy
```

This runs `helm upgrade --install` with the local chart and the current image tag. To deploy with your custom registry instead:

```bash
LITTLERED_REGISTRY=registry.example.com/myorg make deploy
```

Or install from the published OCI chart directly:

```bash
helm install littlered oci://registry.example.com/myorg/charts/littlered \
  --version <version> \
  -n littlered-system --create-namespace \
  --set image.repository=registry.example.com/myorg/littlered \
  --set image.tag=<tag>
```

### 4. Tear down

```bash
make cleanup-test-e2e
```

## Running Tests

### Unit tests

```bash
make test
```

### End-to-end tests

```bash
make test-e2e
```

The e2e suite creates a Kind cluster, builds and loads images, deploys the operator, runs Ginkgo specs, and tears down the cluster. See [E2E Testing](E2E_TESTING.md) for options (`FOCUS`, `DEBUG_ON_FAILURE`, `SKIP_KIND_SETUP`, etc.).

## Release Workflow

```bash
git tag v0.x.y
make images           # build + push versioned images to ghcr.io
make helm-push        # package + push versioned Helm chart
make push-latest      # re-tag images as :latest, in the registry
```

`push-latest` is a no-op on untagged commits — it checks that `GIT_TAG` matches `vX.Y.Z` before doing anything.

There is no `:latest` tag for the Helm chart: Helm treats the OCI tag (and `--version`) as a semver constraint, so `latest` is rejected (`improper constraint: latest`). To get the newest chart, omit `--version` — Helm resolves it to the highest semver tag.
