// Package service provides direct (non-HTTP) access to Clavesa pipeline
// operations, used by the TUI and CLI subcommands.
package service

import (
	"context"
	"path/filepath"
	"sync"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/fileops"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/modules"
	"github.com/vesahyp/clavesa/internal/observability"
)

// PipelineGraph is an alias for graph.PipelineGraph used in method signatures.
type PipelineGraph = graph.PipelineGraph

// PipelineInfo describes a discovered pipeline directory. JSON tags
// match the HTTP `GET /pipelines` shape — the api handler serves this
// type directly and the CLI's `pipeline list --json` emits it, so both
// surfaces report identical keys (ADR-015).
type PipelineInfo struct {
	Name      string `json:"name"`
	Dir       string `json:"dir"` // relative to workspace root
	NodeCount int    `json:"node_count"`
	// Cloud is "aws" when any node's module source is an AWS module,
	// else empty. Compute reflects the pipeline's primary compute target
	// — "local" when any transform carries compute = "local", else the
	// first transform's compute (default "lambda"). The UI uses Compute
	// to choose dir-based vs ARN-based observability addressing (ADR-014).
	Cloud   string `json:"cloud,omitempty"`
	Compute string `json:"compute,omitempty"`
	// Schema is the ADR-016 schema identifier this pipeline writes into
	// (the middle level of catalog.schema.table). Resolved from the
	// pipeline's `schema` tfvar / variable default, falling back to the
	// sanitized pipeline name. A schema is owned by exactly one pipeline,
	// so this is the pipeline≡schema identity the UI surfaces.
	Schema string `json:"schema,omitempty"`
	// Sources are the registered source names (ADR-017) this pipeline's
	// transforms consume, sorted and deduplicated. Empty for pipelines
	// that read only inline modules or upstream transform outputs.
	Sources []string `json:"sources,omitempty"`
}

// NodeTypeDef describes a node type that can be added to a pipeline.
// ModuleRel is the path under the extracted modules tree (e.g. "transform/aws")
// — Service.ModuleSource turns it into a workspace-relative `source = "..."`.
type NodeTypeDef struct {
	Label         string
	Type          string
	ModuleRel     string
	BlockPrefix   string
	DefaultConfig map[string]interface{}
}

// Module-relative paths under the extracted modules tree. Service.ModuleSource
// converts these into the depth-aware relative `source = "..."` string a
// pipeline .tf file gets written with.
const (
	OrchestrationModuleRel = "orchestration/aws"
	SourceModuleRel        = "source/aws"
	TransformModuleRel     = "transform/aws"
	DestinationModuleRel   = "destination/aws"
)

// NodeTypes is the catalogue of supported node types.
// Type matches what hclparser.nodeTypeFromSource returns from the module path.
//
// ADR-017 slice 4 dropped "source" from this catalogue — sources are
// workspace-level registry entries now (see internal/sources). Existing
// inline `module "src_X"` blocks still parse via hclparser for backward
// compatibility; the catalogue just stops the UI palette and `node add
// --type source` from creating new ones.
var NodeTypes = []NodeTypeDef{
	{
		Label:       "SQL Transform",
		Type:        "transform",
		ModuleRel:   TransformModuleRel,
		BlockPrefix: "transform",
		DefaultConfig: map[string]interface{}{
			"language": "sql",
			"sql":      "",
		},
	},
	{
		Label:       "Python Transform",
		Type:        "transform",
		ModuleRel:   TransformModuleRel,
		BlockPrefix: "transform",
		DefaultConfig: map[string]interface{}{
			"language": "python",
			"compute":  "lambda",
			"python":   "",
		},
	},
	{
		Label:       "S3 Dest",
		Type:        "destination",
		ModuleRel:   DestinationModuleRel,
		BlockPrefix: "destination",
		DefaultConfig: map[string]interface{}{
			"bucket": "", "prefix": "", "format": "parquet", "write_mode": "append",
		},
	},
}

// ModuleSource returns the value for a pipeline .tf `source = "..."`
// attribute pointing at the embedded module at moduleRel (e.g.
// "transform/aws") under this workspace's extracted modules tree for
// the current ModuleVersion. The result is workspace-relative so the
// pipeline directory can be moved with the workspace.
func (s *Service) ModuleSource(pipelineDir, moduleRel string) string {
	src, err := modules.RelativeSource(pipelineDir, s.workspace, ModuleVersion, moduleRel)
	if err != nil {
		// Defensive fallback — RelativeSource only fails on bad paths
		// (filepath.Abs against unresolvable input). Caller has already
		// produced a usable pipelineDir, so this branch shouldn't fire
		// in practice; return the absolute path as a last resort so
		// the user gets *something* terraform-resolvable.
		return filepath.ToSlash(filepath.Join(modules.ExtractRoot(s.workspace, ModuleVersion), moduleRel))
	}
	return src
}

// NoAWSCredsError is returned when AWS credentials are unavailable.
type NoAWSCredsError struct{ Err error }

func (e NoAWSCredsError) Error() string {
	return "AWS credentials not available: " + e.Err.Error()
}

// WarehouseEvictor releases the warm-Spark worker container for one
// warehouse so a `pipeline run` invocation has the Docker memory
// budget to itself. Implemented by
// `observability.persistentDockerQueryRunner.EvictWarehouse`. Wired in
// from cli/ui.go; nil-safe — `pipeline run` on a CLI-only invocation
// has no warm worker to evict.
type WarehouseEvictor interface {
	EvictWarehouse(warehouse string)
}

// Service provides direct access to pipeline operations without HTTP.
type Service struct {
	workspace string
	fo        *fileops.FileOps
	s3Client  dataquery.S3Client
	s3Once    sync.Once
	s3Err     error
	evictor   WarehouseEvictor

	// runsInFlight tracks pipelines with an async StartRun executing,
	// keyed by absolute dir. Guards against a double-dispatch the
	// synchronous RunPipeline used to prevent by blocking. runsMu
	// guards it.
	runsMu       sync.Mutex
	runsInFlight map[string]bool

	// dashResolver dispatches the dashboards system-table reads/writes
	// to the cloud or local observability provider. Wired via
	// WithResolver; nil for service instances that never touch
	// dashboards. dashMu guards the two lazy-init flags below:
	// dashTableReady (the CREATE TABLE has succeeded once) and
	// dashImported (the one-time legacy-file migration has run).
	dashResolver   *observability.Resolver
	dashMu         sync.Mutex
	dashTableReady bool
	dashImported   bool
}

// Ref wraps a bare HCL expression (e.g. file("path"), var.x) as a reference
// value that fileops writes without quotes.
func Ref(expr string) fileops.ModuleReference {
	return fileops.ModuleReference{Type: "reference", Expression: expr}
}

// New creates a new Service rooted at workspace.
func New(workspace string) *Service {
	return &Service{
		workspace:    workspace,
		fo:           fileops.New(),
		runsInFlight: make(map[string]bool),
	}
}

// WithEvictor registers a warm-worker evictor so `pipeline run` can
// release the Catalog/dashboard worker before spawning its own
// containers. Without it (CLI-only invocations, tests), `pipeline run`
// just spawns and Docker handles whatever memory pressure exists.
func (s *Service) WithEvictor(e WarehouseEvictor) *Service {
	s.evictor = e
	return s
}

func (s *Service) resolveDir(dir string) string {
	if filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(s.workspace, dir)
}

func (s *Service) ensureS3Client() error {
	s.s3Once.Do(func() {
		cfg, err := awsconfig.LoadDefaultConfig(context.Background())
		if err != nil {
			s.s3Err = NoAWSCredsError{Err: err}
			return
		}
		s.s3Client = s3.NewFromConfig(cfg)
	})
	return s.s3Err
}
