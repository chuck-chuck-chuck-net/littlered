FROM golang:1.26.10-alpine AS builder

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY api/ api/
COPY test/chaos/ test/chaos/
COPY cmd/littlered-chaos-client/ cmd/littlered-chaos-client/

# Build
# The compiler cache is a cache mount, not a layer: it survives between builds on the
# same worker and never ends up in the image or in an exported layer cache.
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o chaos-client ./cmd/littlered-chaos-client

# Runtime image
FROM alpine:3.24.3

RUN apk --no-cache add ca-certificates

WORKDIR /
COPY --from=builder /workspace/chaos-client .

ENTRYPOINT ["/chaos-client"]
