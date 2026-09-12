# Single source of truth for the default redis_exporter sidecar image.
#
# This file is NOT built into an image. It exists so that Dependabot's docker
# ecosystem can track and bump the redis_exporter version in one place. The
# reference below is embedded (//go:embed) and parsed at init time to populate
# DefaultExporterPath / DefaultExporterTag in littlered_defaults.go.
#
# Do not add build stages or other FROM lines — the parser reads the first FROM.
FROM docker.io/oliver006/redis_exporter:v1.91.1
