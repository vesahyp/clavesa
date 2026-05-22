package api

import "testing"

// TestDBBelongsToWorkspace covers the slice-1g filter: cross-workspace
// bleed in the Catalog page is closed because each workspace's view
// scopes to its own catalog identifier.
func TestDBBelongsToWorkspace(t *testing.T) {
	cases := []struct {
		name             string
		dbName           string
		workspaceCatalog string
		want             bool
	}{
		// New (post-ADR-016) workspace — encoded `<catalog>__<schema>` form.
		{
			name:             "new workspace sees its own DB",
			dbName:           "clavesa_demo_ws__cloudfront",
			workspaceCatalog: "clavesa_demo_ws",
			want:             true,
		},
		{
			name:             "new workspace sees its own observability DB",
			dbName:           "clavesa_demo_ws__marketing",
			workspaceCatalog: "clavesa_demo_ws",
			want:             true,
		},
		{
			name:             "new workspace does NOT see another workspace's DB",
			dbName:           "clavesa_cloudfront_analytics__cloudfront",
			workspaceCatalog: "clavesa_demo_ws",
			want:             false,
		},
		{
			name:             "new workspace does NOT see legacy account-shared DBs",
			dbName:           "clavesa_legacypipe",
			workspaceCatalog: "clavesa_demo_ws",
			want:             false,
		},
		{
			name:             "shared catalog prefix isn't matched without `__` boundary",
			dbName:           "clavesa_demo_ws_other",
			workspaceCatalog: "clavesa_demo_ws",
			want:             false,
		},
		// Explicit `--catalog clavesa` override (legacy literal as the
		// new-style catalog identifier).
		{
			name:             "explicit `clavesa` catalog matches its encoded DBs",
			dbName:           "clavesa__cloudfront",
			workspaceCatalog: "clavesa",
			want:             true,
		},
		{
			name:             "explicit `clavesa` catalog rejects pre-ADR DBs (single underscore)",
			dbName:           "clavesa_legacypipe",
			workspaceCatalog: "clavesa",
			want:             false,
		},
		// Empty workspace catalog — should never reach this filter
		// post-v0.18 (Manifest.Load auto-migrates), but if it does the
		// safe answer is "match nothing" so the user sees an empty
		// Catalog page rather than an account-wide bleed.
		{
			name:             "empty catalog matches nothing — safe default after legacy removal",
			dbName:           "clavesa_legacypipe",
			workspaceCatalog: "",
			want:             false,
		},
		{
			name:             "empty catalog still rejects encoded DBs",
			dbName:           "clavesa_demo_ws__marketing",
			workspaceCatalog: "",
			want:             false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := dbBelongsToWorkspace(c.dbName, c.workspaceCatalog); got != c.want {
				t.Errorf("dbBelongsToWorkspace(%q, %q) = %v, want %v",
					c.dbName, c.workspaceCatalog, got, c.want)
			}
		})
	}
}
