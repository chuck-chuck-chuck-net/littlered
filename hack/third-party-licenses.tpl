littlered — Third-Party License Inventory
==========================================

GENERATED FILE — do not edit by hand. Regenerate with `make licenses`.

This inventory lists every third-party Go module linked into the littlered
binaries (operator, chaos client, lrctl), together with its version, detected
license, and full license text. Required attribution notices are additionally
reproduced in NOTICE (Apache-2.0 §4(d)); littlered's own license is in LICENSE.
{{range .}}{{if ne .Name "github.com/chuck-chuck-chuck-net/littlered"}}
================================================================================
Module:  {{.Name}}
Version: {{.Version}}
License: {{.LicenseName}}
URL:     {{.LicenseURL}}
================================================================================

{{.LicenseText}}
{{end}}{{end}}
