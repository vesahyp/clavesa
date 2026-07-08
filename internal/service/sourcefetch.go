package service

import (
	"net/url"
	"path/filepath"
	"strings"
)

// File survives ADR-017 slice 4 only as the home for two pure helpers
// — `inferFormatFromFilename` (used by source-registry registration to
// guess `format` from a URL filename) and `nameFromURL` (used to derive
// a default registry name from a URL when the caller omits one). The
// old `AddSourceFromURL` method that emitted inline source modules
// went away with the slice 4 cutover; the replacement is
// `service.Service.AddSource` plus `AttachSource`.

func inferFormatFromFilename(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".parquet"):
		return "parquet"
	case strings.HasSuffix(lower, ".csv"), strings.HasSuffix(lower, ".csv.gz"):
		return "csv"
	case strings.HasSuffix(lower, ".tsv"), strings.HasSuffix(lower, ".tsv.gz"):
		return "tsv"
	case strings.HasSuffix(lower, ".json"), strings.HasSuffix(lower, ".ndjson"):
		return "json"
	}
	return ""
}

// nameFromURL derives a Terraform-friendly node name from a URL filename.
// Returns "" if it can't produce a usable identifier.
func nameFromURL(urlStr string) string {
	u, err := url.Parse(urlStr)
	if err != nil {
		return ""
	}
	switch u.Scheme {
	case "http", "https", "s3", "file":
	default:
		return ""
	}
	base := strings.ToLower(filepath.Base(u.Path))
	if base == "" || base == "/" || base == "." {
		return ""
	}
	for _, ext := range []string{".gz", ".parquet", ".csv", ".json", ".ndjson"} {
		base = strings.TrimSuffix(base, ext)
	}
	var b strings.Builder
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_', r == '-', r == '.':
			b.WriteByte('_')
		}
	}
	out := b.String()
	for strings.Contains(out, "__") {
		out = strings.ReplaceAll(out, "__", "_")
	}
	out = strings.TrimLeft(out, "_0123456789")
	out = strings.Trim(out, "_")
	if out == "" {
		return ""
	}
	return out
}
