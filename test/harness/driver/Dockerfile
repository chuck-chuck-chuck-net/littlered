# The e2e work-list driver image (docs/E2E_SOAK_HARNESS_DESIGN.md §4.3).
#
# Unlike the operator and chaos-client images, this one CANNOT discard its
# builder stage: the driver's job is to run `make run-test-e2e`, so it needs the
# Go toolchain, make, kubectl, helm and the repository ALIVE at runtime.
#
# It is therefore built from a staged context (`make e2e-harness-context`)
# rather than the repo directly: the root .dockerignore excludes `**/*_test.go`
# and `.git/`, and this image needs the test sources.
#
# Consequence of dropping `.git/`: `git rev-parse` cannot run in-pod, so the
# Makefile's GIT_TAG is injected on the command line instead (LRH_GIT_TAG).

ARG GO_VERSION=1.26.5

# --- stage 1: the lrharness helper, built statically -------------------------
FROM golang:${GO_VERSION}-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY test/harness/ test/harness/
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/lrharness ./test/harness/cmd/lrharness

# --- stage 2: the runtime, which keeps the toolchain -------------------------
FROM golang:${GO_VERSION}-alpine
ARG TARGETARCH=amd64
ARG KUBECTL_VERSION=v1.34.10
ARG HELM_VERSION=v3.19.0

# coreutils gives GNU `timeout` (busybox's lacks --kill-after), which is how a
# work item's per-item deadline is enforced.
RUN apk add --no-cache bash make git curl tar ca-certificates coreutils

RUN curl -fsSLo /usr/local/bin/kubectl \
        "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" \
    && chmod 0755 /usr/local/bin/kubectl \
    && kubectl version --client=true

RUN curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-${TARGETARCH}.tar.gz" \
        | tar -xz -C /tmp \
    && mv "/tmp/linux-${TARGETARCH}/helm" /usr/local/bin/helm \
    && rm -rf "/tmp/linux-${TARGETARCH}" \
    && helm version --short

ENV LRH_REPO=/workspace/littlered \
    CGO_ENABLED=0 \
    GOFLAGS=-mod=mod
WORKDIR /workspace/littlered

COPY . .
COPY --from=builder /out/lrharness /usr/local/bin/lrharness
RUN install -m 0755 test/harness/driver/harness-entrypoint.sh \
        /usr/local/bin/harness-entrypoint.sh

# Warm the module and build caches. Without this every work item re-downloads
# and re-compiles the whole dependency tree before it can run a single spec —
# which is exactly the "every harness iteration costs 40 minutes" trap M1 exists
# to avoid. It also fails the BUILD, not item 1, if the suite does not compile.
RUN go mod download \
    && go test -tags=e2e -c -o /tmp/e2e.test ./test/e2e/ \
    && rm -f /tmp/e2e.test

ENTRYPOINT ["/usr/local/bin/harness-entrypoint.sh"]
