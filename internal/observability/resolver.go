package observability

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/pathutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// Resolver picks a Provider per request from the workspace's
// environment mode: mode = local routes every pipeline to the local
// provider, mode = cloud routes to the cloud provider. The mode is the
// sole switch — the per-node `compute` attr is a deploy target and no
// longer influences dispatch (TODO bucket 16; the old
// `compute = "local"` fallback is gone). The host's AWS availability is
// irrelevant either way (ADR-014).
type Resolver struct {
	workspace string
	cloud     Provider
	local     Provider
}

// NewResolver wires a resolver against the workspace root. cloud may be nil
// when AWS credentials are unavailable; resolver returns a typed error in that
// case rather than panicking, so the UI can render the missing-credentials
// case for cloud pipelines without affecting local ones.
func NewResolver(workspace string, cloud, local Provider) *Resolver {
	return &Resolver{workspace: workspace, cloud: cloud, local: local}
}

// For returns the Provider matching the workspace environment mode.
// dir must be non-empty (the observability surfaces always inspect a
// named pipeline) but is not otherwise consulted — dispatch is purely
// the workspace mode.
func (r *Resolver) For(dir string) (Provider, error) {
	if dir == "" {
		return nil, fmt.Errorf("observability: dir is required to dispatch provider")
	}
	if r.IsLocal() {
		if r.local == nil {
			return nil, fmt.Errorf("observability: local provider not configured")
		}
		return r.local, nil
	}
	if r.cloud == nil {
		return nil, fmt.Errorf("observability: cloud provider unavailable (no AWS credentials)")
	}
	return r.cloud, nil
}

// IsLocal reports whether the workspace dispatches to the local
// provider — true iff the environment mode is local. Used by surfaces
// that need a local-vs-cloud decision without instantiating a provider
// — e.g. POST /pipeline/run, which routes to service.RunPipeline
// locally vs SFN StartExecution on cloud.
func (r *Resolver) IsLocal() bool {
	return workspace.LoadEnvironmentMode(r.workspace) == workspace.ModeLocal
}

// PipelineName returns the conventional pipeline name (the directory's
// basename). Used for the `pipeline = '...'` row filter in observability
// queries — the Glue DB name encoding (post-ADR-016) is computed
// separately via GlueDBFor below, since the DB no longer derives from
// pipeline name alone.
func (r *Resolver) PipelineName(dir string) string {
	abs := pathutil.ResolveDir(r.workspace, dir)
	return filepath.Base(abs)
}

// GlueDBFor returns the encoded Glue DB / Iceberg namespace name the
// runner writes into for the pipeline at dir. Mirrors the runner's
// `_glue_db()` and `internal/identutil.EncodeGlueDatabase`: catalog
// from clavesa.json (empty for legacy / pre-ADR-016 workspaces);
// schema from the pipeline's `variable "schema"` default with
// `sanitize(pipeline_name)` as fallback. Three encoders must stay
// byte-identical or the catalog handler reads from a different place
// than the runner writes to.
//
// dir may be relative to the workspace root or absolute.
func (r *Resolver) GlueDBFor(dir string) string {
	abs := pathutil.ResolveDir(r.workspace, dir)
	catalog := ""
	if m, _ := workspace.Load(r.workspace); m != nil {
		catalog = m.CatalogIdentifier()
	}
	schema := readSchemaDefault(abs)
	if schema == "" {
		schema = filepath.Base(abs)
	}
	return identutil.EncodeGlueDatabase(catalog, schema)
}

// SystemGlueDB returns the encoded Glue DB for the workspace's
// observability tables (ADR-016 v0.20.0 "Workspace system catalog"):
// `<system_catalog>__pipelines`. Workspace-wide — every pipeline's
// runs / node_runs / tables land in this DB, distinguished by the
// `pipeline` column. Empty when no workspace manifest is loadable;
// callers fall back to today's per-pipeline DB.
func (r *Resolver) SystemGlueDB() string {
	m, _ := workspace.Load(r.workspace)
	if m == nil || m.SystemCatalogIdentifier() == "" {
		return ""
	}
	return identutil.EncodeGlueDatabase(m.SystemCatalogIdentifier(), "pipelines")
}

// readSchemaDefault parses the pipeline's variables.tf for the default
// value of `variable "schema"`. Naive line scan, same shape used by
// readVariableDecls in api/workspace.go and resolvePipelineSchema in
// service/run.go. Returns "" when not declared (legacy pipelines) so
// callers can fall back to the sanitized pipeline name.
func readSchemaDefault(dir string) string {
	data, err := os.ReadFile(filepath.Join(dir, "variables.tf"))
	if err != nil {
		return ""
	}
	inSchemaBlock := false
	for _, line := range strings.Split(string(data), "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, `variable "schema"`) {
			inSchemaBlock = true
			continue
		}
		if inSchemaBlock {
			if strings.HasPrefix(t, "}") {
				return ""
			}
			if strings.HasPrefix(t, "default") {
				_, val, ok := strings.Cut(t, "=")
				if !ok {
					continue
				}
				v := strings.TrimSpace(val)
				if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
					return v[1 : len(v)-1]
				}
			}
		}
	}
	return ""
}

