FROM golang:1.26.10-alpine@sha256:87c65a688975c0b1d4585d204874c45ca78354670e3a5f11368c5a492303d696 AS builder

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
FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

RUN apk --no-cache add ca-certificates

WORKDIR /
COPY --from=builder /workspace/chaos-client .

ENTRYPOINT ["/chaos-client"]
