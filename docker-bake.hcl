# Every image this repository publishes, as one BuildKit graph. CI runs it
# directly (publish*.yml); the Makefile's image targets are aliases onto it.
#
#   make images                                   # build and push, tags from git
#   REGISTRY=... TAG=... buildx bake --push       # the same, by hand
#   buildx bake --print                           # show the resolved graph
#
# Tags and labels: locally each image gets one tag, ${REGISTRY}/<image>:${TAG},
# from the metadata targets below. In CI, docker/metadata-action writes a bake
# file that redefines those same targets with the published tag set and the
# OCI labels; passed after this one (-f), it overrides them.

variable "REGISTRY" {
  default = "ghcr.io/chuck-chuck-chuck-net"
}

# The Makefile passes IMAGE_TAG: X.Y.Z on a release tag, sha-<short> otherwise.
variable "TAG" {
  default = "dev"
}

# CI sets GHA_CACHE=1 to keep each image's layers in the GitHub Actions cache,
# in a scope of its own. Outside Actions there is no cache service to reach.
variable "GHA_CACHE" {
  default = "0"
}

function "cache_from" {
  params = [scope]
  result = GHA_CACHE == "1" ? ["type=gha,scope=${scope}"] : []
}

# A failed cache upload must not fail a build whose image was pushed.
function "cache_to" {
  params = [scope]
  result = GHA_CACHE == "1" ? ["type=gha,scope=${scope},mode=max,ignore-error=true"] : []
}

group "default" {
  targets = ["littlered", "littlered-chaos-client"]
}

# Pinned rather than detected: BuildKit builds for the worker's platform, so an
# arm64 laptop would otherwise build images an amd64 cluster cannot run.
target "_common" {
  context   = "."
  platforms = ["linux/amd64"]
}

# Overridden in CI by docker/metadata-action (bake-target input).
target "meta-littlered" {
  tags = ["${REGISTRY}/littlered:${TAG}"]
}

target "meta-littlered-chaos-client" {
  tags = ["${REGISTRY}/littlered-chaos-client:${TAG}"]
}

target "littlered" {
  inherits   = ["_common", "meta-littlered"]
  dockerfile = "cmd/littlered/Dockerfile"
  cache-from = cache_from("littlered")
  cache-to   = cache_to("littlered")
}

# Used by the e2e suite's chaos tiers.
target "littlered-chaos-client" {
  inherits   = ["_common", "meta-littlered-chaos-client"]
  dockerfile = "cmd/littlered-chaos-client/Dockerfile"
  cache-from = cache_from("littlered-chaos-client")
  cache-to   = cache_to("littlered-chaos-client")
}
