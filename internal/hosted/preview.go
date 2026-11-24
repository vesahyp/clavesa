package hosted

import "strings"

// Previews read a raw source or run a node over raw input: they belong to
// the Mac, for every workspace (ADR-033; slice 4 review, finding 2).
// What a viewer may read of a workspace's tables is Lake Formation's
// (column tags, docs/ideas/column-tags.md).
const previewRefusal = "previews read raw sources; they run in clavesa ui on the Mac, not in the hosted UI"

// refusesPreview reports whether a request (path relative to the
// workspace's /api) is a preview: /preview/* or /sources/<name>/preview.
func refusesPreview(rest string) bool {
	return rest == "/preview" || strings.HasPrefix(rest, "/preview/") ||
		(strings.HasPrefix(rest, "/sources/") && strings.HasSuffix(rest, "/preview"))
}
