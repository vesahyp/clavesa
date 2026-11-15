package ui

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/fileops"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pipelinestatus"
	"github.com/vesahyp/clavesa/internal/preview"
	"github.com/vesahyp/clavesa/internal/runtiming"
	tuiservice "github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/wsdeps"
)

// WorkspaceDeps is what BuildWorkspaceMux needs for one workspace.
type WorkspaceDeps struct {
	// Deps is the workspace's service stack (wsdeps.New). Its Svc may
	// already carry a notebook runner: `clavesa ui` adds its REPL pool
	// before calling BuildWorkspaceMux; the multi-workspace router does not.
	*wsdeps.Deps
	// AWSIdentity is what GET /runtime/identity reports. Zero value is the
	// local-only answer.
	AWSIdentity api.AWSIdentity
	// Restart is the self-restart hook for PUT /workspace/aws-profile.
	// Nil when the process must not restart itself (multi-workspace mode).
	Restart func()
	// ReadOnly wraps the mux in ReadOnlyGuard.
	ReadOnly bool
}

// BuildWorkspaceMux returns the /api handler set for ONE workspace, with
// routes at their bare paths (/pipeline, /pipelines, /data/…). The caller
// mounts it behind http.StripPrefix: `clavesa ui` at /api/, the
// multi-workspace router at /w/<name>/api/, the hosted Lambda (ADR-032)
// the same way. Every handler constructor is cheap and lazy, so building
// a mux per workspace on first use costs a few allocations.
func BuildWorkspaceMux(d WorkspaceDeps) http.Handler {
	mux := http.NewServeMux()
	registerWorkspaceRoutes(d, mux)
	if d.ReadOnly {
		return ReadOnlyGuard(mux)
	}
	return mux
}

// registerWorkspaceRoutes registers every route of one workspace on r.
// Split from BuildWorkspaceMux so the read-only test can pass a recording
// router and walk the full route table.
func registerWorkspaceRoutes(d WorkspaceDeps, r httputil.Router) {
	workspace := d.Workspace
	svc := d.Svc
	clients := d.Clients
	athenaOutputBucket := clients.AthenaOutputBucket
	s3Client := clients.S3

	fo := fileops.New()
	pipelineHandler := api.New(fo, workspace).
		WithService(svc).
		WithSyncer(svc).
		WithLineage(lineageBridge{svc: svc}).
		WithNodeAdder(nodeAdderBridge{svc: svc}).
		WithExternalTableAttacher(svc).
		WithInputDetacher(svc)
	workspaceHandler := api.NewWorkspaceHandler(workspace).WithService(svc)
	if d.Restart != nil {
		workspaceHandler = workspaceHandler.WithRestart(d.Restart)
	}
	statusHandler := pipelinestatus.NewHandler(workspace).
		WithResolver(d.Resolver).
		WithLocalRunner(localPipelineRunnerBridge{svc: svc}).
		WithCloudRunner(cloudPipelineRunnerBridge{svc: svc}).
		WithCloudLocalRunner(cloudLocalPipelineRunnerBridge{svc: svc}).
		WithAthenaOutputBucket(athenaOutputBucket)
	// WithQueryService routes /data/query through the shared Service.Query
	// seam (provider dispatch + Spark→Trino transpile on cloud), the same
	// path `clavesa query` uses (ADR-015).
	dataHandler := dataquery.NewHandler(s3Client, clients.Athena, athenaOutputBucket).(*dataquery.Handler).
		WithResolver(d.Resolver).
		WithQueryService(func(ctx context.Context, sql, dir string, maxRows int) (*observability.QueryResult, error) {
			return svc.Query(ctx, sql, tuiservice.QueryOptions{Dir: dir, MaxRows: maxRows})
		}).
		// GET /data/run-timings reads through Service.RunTimings, the same
		// read as `clavesa pipeline timings` (ADR-015).
		WithRunTimingsService(func(ctx context.Context, dir string, limit int, since time.Duration) ([]runtiming.Record, error) {
			return svc.RunTimings(ctx, dir, tuiservice.RunTimingsQuery{Limit: limit, Since: since})
		})
	// nil-safe: the catalog handler renders an empty list in local-only mode.
	var catalogClient api.GlueClient
	if clients.Glue != nil {
		catalogClient = clients.Glue
	}
	// WithS3: the cloud catalog reads each Delta table's real schema from
	// its `_delta_log/`. Pass it only when the client exists, so a typed-nil
	// *s3.Client never reads as non-nil through the interface.
	catalogHandler := api.NewCatalogHandler(catalogClient).WithWorkspace(workspace)
	if s3Client != nil {
		catalogHandler = catalogHandler.WithS3(s3Client)
	}
	// Dashboards: CRUD against the `dashboards` system Delta table; the
	// widget-SQL query rides the same Service.Query seam as /data/query.
	dashboardsHandler := api.NewDashboardsHandler(dashboardStoreBridge{svc: svc}).
		WithQueryService(func(ctx context.Context, sql, dir string, maxRows int) (*observability.QueryResult, error) {
			return svc.Query(ctx, sql, tuiservice.QueryOptions{Dir: dir, MaxRows: maxRows})
		})
	sourcesHandler := api.NewSourcesHandler(sourceRegistryBridge{svc: svc})
	credentialsHandler := api.NewCredentialsHandler(credentialRegistryBridge{svc: svc})
	notebooksHandler := api.NewNotebooksHandler(notebookRegistryBridge{svc: svc})
	runnerHandler := api.NewRunnerHandler(svc)
	bundleHandler := api.NewBundleHandler(bundleBridge{svc: svc, root: workspace})
	backfillHandler := api.NewBackfillHandler(backfillBridge{svc: svc})
	resetHandler := api.NewResetHandler(svc)
	optimizeHandler := api.NewOptimizeHandler(svc)
	runtimeHandler := api.NewRuntimeHandler(d.Warm, d.AWSIdentity)

	hclParserFunc := func(dir string) (*graph.PipelineGraph, error) {
		g, err := hclparser.Parse(dir)
		if err != nil {
			return nil, err
		}
		return &g, nil
	}
	resolveDirFunc := func(dir string) string {
		if dir == "" || filepath.IsAbs(dir) {
			return dir
		}
		return filepath.Join(workspace, dir)
	}

	pipelineHandler.RegisterRoutes(r)
	workspaceHandler.RegisterRoutes(r)
	statusHandler.RegisterRoutes(r)
	catalogHandler.RegisterRoutes(r)
	dashboardsHandler.RegisterRoutes(r)
	sourcesHandler.RegisterRoutes(r)
	credentialsHandler.RegisterRoutes(r)
	notebooksHandler.RegisterRoutes(r)
	runnerHandler.RegisterRoutes(r)
	bundleHandler.RegisterRoutes(r)
	backfillHandler.RegisterRoutes(r)
	resetHandler.RegisterRoutes(r)
	optimizeHandler.RegisterRoutes(r)
	runtimeHandler.RegisterRoutes(r)
	dataHandler.RegisterRoutes(r)
	preview.RegisterRoutes(r, s3Client, hclParserFunc, resolveDirFunc)
}

// ReadOnlyGuard answers 405 to every method except GET and HEAD, with a
// JSON error body and `Allow: GET, HEAD`. It is the server half of
// read-only mode (ADR-032 section 1); the IAM role is the other. It also
// refuses the two read POSTs (POST /data/query, POST /dashboards/query):
// the hosted pages do not need them.
func ReadOnlyGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Allow", "GET, HEAD")
		httputil.WriteError(w, http.StatusMethodNotAllowed,
			"read-only: "+r.Method+" "+r.URL.Path+" is not allowed; this server only answers GET and HEAD")
	})
}

// ProbeAWSIdentity resolves the effective AWS account with one
// GetCallerIdentity, so the UI header can show "operating as account X".
// A short timeout keeps a hung credential provider from stalling startup.
// The zero value (Available=false) is the local-only answer.
func ProbeAWSIdentity(ctx context.Context, clients wsdeps.AWSClients, profile string) api.AWSIdentity {
	if clients.Err != nil {
		return api.AWSIdentity{}
	}
	idCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := sts.NewFromConfig(clients.Config).GetCallerIdentity(idCtx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return api.AWSIdentity{}
	}
	if profile == "" {
		profile = os.Getenv("AWS_PROFILE")
	}
	return api.AWSIdentity{
		Available: true,
		AccountID: aws.ToString(out.Account),
		ARN:       aws.ToString(out.Arn),
		Profile:   profile,
	}
}

// RoutePatterns returns every pattern BuildWorkspaceMux registers for d,
// as "METHOD /path". The hosted Lambda's tests walk it (ADR-032).
func RoutePatterns(d WorkspaceDeps) []string {
	rec := newRouteRecorder()
	registerWorkspaceRoutes(d, rec)
	return rec.patterns
}

// routeRecorder is an httputil.Router that registers on a ServeMux and
// remembers every pattern, so a test can walk the route table.
type routeRecorder struct {
	mux      *http.ServeMux
	patterns []string
}

func newRouteRecorder() *routeRecorder { return &routeRecorder{mux: http.NewServeMux()} }

func (r *routeRecorder) Handle(pattern string, h http.Handler) {
	r.patterns = append(r.patterns, pattern)
	r.mux.Handle(pattern, h)
}

func (r *routeRecorder) HandleFunc(pattern string, h func(http.ResponseWriter, *http.Request)) {
	r.patterns = append(r.patterns, pattern)
	r.mux.HandleFunc(pattern, h)
}

// splitPattern splits a ServeMux pattern into its method and path. A
// pattern with no method returns "".
func splitPattern(p string) (method, path string) {
	if m, rest, ok := strings.Cut(p, " "); ok {
		return m, strings.TrimSpace(rest)
	}
	return "", p
}
