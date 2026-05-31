// Package service provides direct (non-HTTP) access to Clavesa pipeline
// operations, used by the TUI and CLI subcommands.
package service

import (
	"context"
	"errors"
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
//
// v1.1.5+ dropped OrchestrationModuleRel — the orchestration module was
// folded into Go-side TF emission (internal/orchestration/tfgen) so the
// generated orchestration.tf is self-contained standard Terraform with no
// module dependency. The bug forcing this was nested-fanout / multi-hop
// branch states going unreachable in the HCL-side ASL builder.
const (
	SourceModuleRel      = "source/aws"
	TransformModuleRel   = "transform/aws"
	DestinationModuleRel = "destination/aws"
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

// SQLParser parse-checks one SparkSQL statement against the warm Spark
// worker (Slice 3). Implementations route to the JVM-side Catalyst
// SqlParser via POST /parse — the seam that turns "any string" into a
// rejection-with-message at author time instead of after a Spark cold
// start. Returns *ParseError when the parser rejected the input; any
// other error means transport/runner failure (do not surface to the
// user as a parse error). Nil-safe at the service-method level:
// ValidateSQL no-ops when no parser is wired so CLI integration tests
// stay docker-free.
type SQLParser interface {
	Parse(ctx context.Context, sql string) error
}

// ParseError is the service-layer representation of a parser
// rejection. Mirrors observability.ParseError so callers in
// internal/cli + internal/api can `errors.As(&service.ParseError{})`
// without depending on the observability package.
type ParseError struct {
	Message string
}

func (e *ParseError) Error() string { return e.Message }

// Service provides direct access to pipeline operations without HTTP.
type Service struct {
	workspace string
	fo        *fileops.FileOps
	s3Client  dataquery.S3Client
	s3Once    sync.Once
	s3Err     error
	evictor   WarehouseEvictor
	sqlParser SQLParser

	// runsInFlight tracks pipelines with an async StartRun executing,
	// keyed by absolute dir. Guards against a double-dispatch the
	// synchronous RunPipeline used to prevent by blocking. runsMu
	// guards it.
	runsMu       sync.Mutex
	runsInFlight map[string]bool

	// dashResolver dispatches the widget-SQL *execution* path
	// (RenderDashboard) to the cloud or local observability provider
	// (ADR-014). Wired via WithResolver; nil for service instances that
	// never render dashboards. Definition CRUD is file-backed (ADR-021)
	// and does not use it. dashMu guards dashImported (the one-time
	// legacy-dashboard migration has run).
	dashResolver *observability.Resolver
	dashMu       sync.Mutex
	dashImported bool

	// notebookRunner is the REPL pool for notebook cell execution
	// (Slice 1). nil for CLI-only invocations that don't run cells;
	// `clavesa ui` wires the real per-workspace runner via
	// WithNotebookRunner. RunCell / CancelCell / StopNotebookSession
	// all early-return when this is nil.
	notebookRunner NotebookRunner

	// metastoreEnsure brings up (or reuses) the shared per-workspace
	// Derby metastore container and returns the (docker network, addr)
	// the run/transform/operation containers attach to. The default
	// (set in New) is a no-op returning ("", "") so a Service with no
	// injection — every unit test, via service.New(ws) — never touches
	// Docker. CLI and UI wire the real ensurer via WithMetastoreEnsurer;
	// empty results mean the containers fall back to embedded Derby.
	metastoreEnsure func(ctx context.Context, workspaceRoot, workspaceName string) (network, addr string)
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
		// No-op default: a Service with no injected ensurer never
		// touches Docker. Production wires the real one via
		// WithMetastoreEnsurer; empty results mean embedded fallback.
		metastoreEnsure: func(context.Context, string, string) (string, string) { return "", "" },
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

// WithMetastoreEnsurer wires the shared-metastore ensurer so
// `pipeline run` and the backfill/record paths attach their containers
// to the per-workspace Derby Network Server instead of opening the
// embedded single-writer DB. Without it (unit tests via service.New),
// the no-op default returns ("", "") and the containers fall back to
// embedded Derby — keeping test Service instances free of Docker.
func (s *Service) WithMetastoreEnsurer(fn func(ctx context.Context, workspaceRoot, workspaceName string) (network, addr string)) *Service {
	if fn != nil {
		s.metastoreEnsure = fn
	}
	return s
}

// WithSQLParser wires a parse-only SparkSQL validator (Slice 3) so
// authoring-time entry points (`node edit --set sql=…`, the UI's
// pipeline-node PUT, `dashboards apply`, `node preview`, `sql lint`)
// reject bad SQL before persisting or dispatching. Without it,
// ValidateSQL is a no-op — keeps unit-test Service instances free of
// docker / warm-worker dependencies.
func (s *Service) WithSQLParser(p SQLParser) *Service {
	s.sqlParser = p
	return s
}

// ValidateSQL parse-checks a single SparkSQL statement via the wired
// parser. Returns *ParseError when the parser rejected the input; any
// other error means transport/runner failure. Returns nil when no
// parser is wired (CLI integration tests, dry runs) — those paths
// already accept any string, and forcing a parser dependency here
// would block test runs that don't have docker.
func (s *Service) ValidateSQL(ctx context.Context, sql string) error {
	if s.sqlParser == nil {
		return nil
	}
	err := s.sqlParser.Parse(ctx, sql)
	if err == nil {
		return nil
	}
	// Translate the observability-layer parser rejection into the
	// service-layer one so callers in cli / api can detect it without
	// reaching into the observability package.
	var oe *observability.ParseError
	if errors.As(err, &oe) {
		return &ParseError{Message: oe.Message}
	}
	return err
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
