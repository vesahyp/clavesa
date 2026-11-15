//go:build nodist

package ui

import "embed"

// FS is empty in a -tags nodist build: the hosted Lambda
// (cmd/clavesa-hosted) serves only the API; the SPA is in S3 behind
// CloudFront (ADR-032 section 3).
var FS embed.FS
