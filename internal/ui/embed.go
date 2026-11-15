//go:build !nodist

package ui

import "embed"

// FS is the built SPA (make build-ui → internal/ui/dist), embedded into
// bin/clavesa. The hosted Lambda (cmd/clavesa-hosted, ADR-032) builds
// with -tags nodist and gets the empty FS in embed_nodist.go instead:
// CloudFront serves the SPA from S3 there.
//
//go:embed all:dist
var FS embed.FS
