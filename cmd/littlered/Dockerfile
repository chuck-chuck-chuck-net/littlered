# Build the manager binary
FROM golang:1.26.11 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# Download the modules into a layer of their own, so a source change reuses it.
RUN go mod download

# Copy the Go source: only the packages the manager imports, so a change to
# lrctl or the chaos client does not invalidate this build.
COPY api/ api/
COPY cmd/littlered/ cmd/littlered/
COPY internal/ internal/

# Build
# The GOARCH has no default value so the binary is built for the platform the build
# targets: the builder sets TARGETARCH from --platform, or from its own platform otherwise.
# The compiler cache is a cache mount, not a layer: it survives between builds on the
# same worker and never ends up in the image or in an exported layer cache.
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -o manager ./cmd/littlered

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
