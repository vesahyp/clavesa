package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/fileops"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/notebooks"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pipelinestatus"
	"github.com/vesahyp/clavesa/internal/preview"
	tuiservice "github.com/vesahyp/clavesa/internal/service"
	wspkg "github.com/vesahyp/clavesa/internal/workspace"
	"github.com/spf13/cobra"
)

// embeddedUI holds the embedded frontend filesystem, set from main.go.
var embeddedUI fs.FS

// SetEmbeddedUI injects the embedded UI filesystem before Execute is called.
func SetEmbeddedUI(f fs.FS) {
	embeddedUI = f
}

// lineageBridge converts service.LineageEdge values to api.LineageEdge so
// internal/api can stay free of an internal/service dependency. The two
// shapes mirror each other field-for-field; this adapter is the only place
// that knowledge lives.
type lineageBridge struct {
	svc *tuiservice.Service
}

// (sourceFetchBridge removed in ADR-017 slice 4 — the URL-to-inline-
// source flow is gone. Workspace source registry has its own bridge
// below.)

// nodeAdderBridge adapts service.Service onto api.NodeAdder. service.AddNode
// returns the updated graph; the typed-add HTTP handler re-parses
// itself, so we just discard it here and surface the error.
type nodeAdderBridge struct {
	svc *tuiservice.Service
}

func (b nodeAdderBridge) AddNode(dir, nodeType, name string) error {
	_, err := b.svc.AddNode(dir, nodeType, name)
	return err
}

// localPipelineRunnerBridge adapts service.Service onto
// pipelinestatus.LocalPipelineRunner. It maps service.ErrRunInFlight
// onto pipelinestatus.ErrRunInFlight so the handler can answer 409
// without importing internal/service.
type localPipelineRunnerBridge struct {
	svc *tuiservice.Service
}

func (b localPipelineRunnerBridge) StartRun(dir string) (string, error) {
	id, err := b.svc.StartRun(dir)
	if errors.Is(err, tuiservice.ErrRunInFlight) {
		return "", pipelinestatus.ErrRunInFlight
	}
	return id, err
}

// sourceRegistryBridge adapts service.Service onto api.SourceRegistry —
// translates SourceSpec / SourceUsage / ErrSourceInUse between the two
// packages so internal/api stays free of an internal/service import
// (same pattern as lineageBridge / sourceFetchBridge above).
type sourceRegistryBridge struct {
	svc *tuiservice.Service
}

func toAPISpec(s tuiservice.SourceSpec) api.SourceSpec {
	return api.SourceSpec{
		Name: s.Name, Kind: s.Kind, URL: s.URL,
		Bucket: s.Bucket, Prefix: s.Prefix,
		Format: s.Format, Credentials: s.Credentials,
		Partitions: s.Partitions, StartFrom: s.StartFrom,
		ManageBucketNotifications: s.ManageBucketNotifications,
	}
}

func toServiceSpec(s api.SourceSpec) tuiservice.SourceSpec {
	return tuiservice.SourceSpec{
		Name: s.Name, Kind: s.Kind, URL: s.URL,
		Bucket: s.Bucket, Prefix: s.Prefix,
		Format: s.Format, Credentials: s.Credentials,
		Partitions: s.Partitions, StartFrom: s.StartFrom,
		ManageBucketNotifications: s.ManageBucketNotifications,
	}
}

// inUseError bridges service.ErrSourceInUse onto api.inUseConflicter,
// translating the embedded SourceUsage slice into the api shape.
type inUseError struct{ inner *tuiservice.ErrSourceInUse }

func (e *inUseError) Error() string { return e.inner.Error() }
func (e *inUseError) InUseUsages() []api.SourceUsage {
	out := make([]api.SourceUsage, len(e.inner.Usages))
	for i, u := range e.inner.Usages {
		out[i] = api.SourceUsage{PipelineDir: u.PipelineDir, NodeIDs: u.NodeIDs}
	}
	return out
}

func (b sourceRegistryBridge) AddSource(spec api.SourceSpec) (api.SourceSpec, error) {
	stored, err := b.svc.AddSource(toServiceSpec(spec))
	if err != nil {
		return api.SourceSpec{}, err
	}
	return toAPISpec(stored), nil
}

func (b sourceRegistryBridge) PreviewRegistrySource(ctx context.Context, name string, offset, limit int) (*preview.PreviewResult, error) {
	return b.svc.PreviewRegistrySource(ctx, name, offset, limit)
}

func (b sourceRegistryBridge) UpdateSource(name string, spec api.SourceSpec) (api.SourceSpec, error) {
	stored, err := b.svc.UpdateSource(name, toServiceSpec(spec))
	if err != nil {
		return api.SourceSpec{}, err
	}
	return toAPISpec(stored), nil
}

func (b sourceRegistryBridge) ListSources() ([]api.SourceSpec, error) {
	src, err := b.svc.ListSources()
	if err != nil {
		return nil, err
	}
	out := make([]api.SourceSpec, len(src))
	for i, s := range src {
		out[i] = toAPISpec(s)
	}
	return out, nil
}

func (b sourceRegistryBridge) GetSource(name string) (api.SourceSpec, error) {
	s, err := b.svc.GetSource(name)
	if err != nil {
		return api.SourceSpec{}, err
	}
	return toAPISpec(s), nil
}

func (b sourceRegistryBridge) DeleteSource(name string, force bool) error {
	err := b.svc.DeleteSource(name, force)
	if err == nil {
		return nil
	}
	var inUse *tuiservice.ErrSourceInUse
	if errors.As(err, &inUse) {
		return &inUseError{inner: inUse}
	}
	return err
}

func (b sourceRegistryBridge) AttachSource(dir, name, toNode, alias string) error {
	return b.svc.AttachSource(dir, name, toNode, alias)
}

// dashboardStoreBridge adapts service.Service onto api.DashboardStore,
// translating the Dashboard / DashboardDataset / DashboardWidget shapes
// between the two packages so internal/api stays free of an
// internal/service import (same pattern as sourceRegistryBridge).
type dashboardStoreBridge struct {
	svc *tuiservice.Service
}

func toServiceDashboard(d api.Dashboard) tuiservice.Dashboard {
	ds := make([]tuiservice.DashboardDataset, len(d.Datasets))
	for i, x := range d.Datasets {
		ds[i] = tuiservice.DashboardDataset{Name: x.Name, Dir: x.Dir, SQL: x.SQL}
	}
	ws := make([]tuiservice.DashboardWidget, len(d.Widgets))
	for i, x := range d.Widgets {
		ws[i] = tuiservice.DashboardWidget{
			ID: x.ID, Type: x.Type, Title: x.Title, Dataset: x.Dataset,
			ValueField: x.ValueField, XField: x.XField, YField: x.YField,
			SeriesFields: x.SeriesFields, LineField: x.LineField,
			Layout: tuiservice.DashboardWidgetLayout{X: x.Layout.X, Y: x.Layout.Y, W: x.Layout.W, H: x.Layout.H},
		}
	}
	cs := make([]tuiservice.DashboardControl, len(d.Controls))
	for i, x := range d.Controls {
		cs[i] = tuiservice.DashboardControl{
			Name: x.Name, Type: x.Type, Label: x.Label, Default: x.Default,
			Dir: x.Dir, SQL: x.SQL, Options: x.Options,
		}
	}
	return tuiservice.Dashboard{Slug: d.Slug, Title: d.Title, Datasets: ds, Widgets: ws, Controls: cs}
}

func toAPIDashboard(d tuiservice.Dashboard) api.Dashboard {
	ds := make([]api.DashboardDataset, len(d.Datasets))
	for i, x := range d.Datasets {
		ds[i] = api.DashboardDataset{Name: x.Name, Dir: x.Dir, SQL: x.SQL}
	}
	ws := make([]api.DashboardWidget, len(d.Widgets))
	for i, x := range d.Widgets {
		ws[i] = api.DashboardWidget{
			ID: x.ID, Type: x.Type, Title: x.Title, Dataset: x.Dataset,
			ValueField: x.ValueField, XField: x.XField, YField: x.YField,
			SeriesFields: x.SeriesFields, LineField: x.LineField,
			Layout: api.DashboardWidgetLayout{X: x.Layout.X, Y: x.Layout.Y, W: x.Layout.W, H: x.Layout.H},
		}
	}
	cs := make([]api.DashboardControl, len(d.Controls))
	for i, x := range d.Controls {
		cs[i] = api.DashboardControl{
			Name: x.Name, Type: x.Type, Label: x.Label, Default: x.Default,
			Dir: x.Dir, SQL: x.SQL, Options: x.Options,
		}
	}
	return api.Dashboard{Slug: d.Slug, Title: d.Title, Datasets: ds, Widgets: ws, Controls: cs, UpdatedAt: d.UpdatedAt}
}

func (b dashboardStoreBridge) ListDashboards(ctx context.Context) ([]api.DashboardSummary, error) {
	list, err := b.svc.ListDashboards(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]api.DashboardSummary, len(list))
	for i, s := range list {
		out[i] = api.DashboardSummary{Slug: s.Slug, Title: s.Title}
	}
	return out, nil
}

func (b dashboardStoreBridge) GetDashboard(ctx context.Context, slug string) (api.Dashboard, error) {
	d, err := b.svc.GetDashboard(ctx, slug)
	if err != nil {
		return api.Dashboard{}, err
	}
	return toAPIDashboard(d), nil
}

func (b dashboardStoreBridge) SaveDashboard(ctx context.Context, d api.Dashboard) (api.Dashboard, error) {
	stored, err := b.svc.SaveDashboard(ctx, toServiceDashboard(d))
	if err != nil {
		return api.Dashboard{}, err
	}
	return toAPIDashboard(stored), nil
}

func (b dashboardStoreBridge) DeleteDashboard(ctx context.Context, slug string) error {
	return b.svc.DeleteDashboard(ctx, slug)
}

// credentialRegistryBridge adapts service.Service onto api.CredentialRegistry,
// translating CredentialSpec / CredentialUsage / ErrCredentialInUse the
// same way sourceRegistryBridge does for sources.
type credentialRegistryBridge struct {
	svc *tuiservice.Service
}

func toAPICred(c tuiservice.CredentialSpec) api.CredentialSpec {
	return api.CredentialSpec{
		Name:        c.Name,
		Kind:        c.Kind,
		HeaderName:  c.HeaderName,
		ValuePrefix: c.ValuePrefix,
		Secret:      c.Secret,
		Backend:     c.SecretBackend(),
	}
}

func toServiceCred(c api.CredentialSpec) tuiservice.CredentialSpec {
	return tuiservice.CredentialSpec{
		Name:        c.Name,
		Kind:        c.Kind,
		HeaderName:  c.HeaderName,
		ValuePrefix: c.ValuePrefix,
		Secret:      c.Secret,
	}
}

type credInUseError struct{ inner *tuiservice.ErrCredentialInUse }

func (e *credInUseError) Error() string { return e.inner.Error() }
func (e *credInUseError) InUseUsages() []api.CredentialUsage {
	out := make([]api.CredentialUsage, len(e.inner.Usages))
	for i, u := range e.inner.Usages {
		out[i] = api.CredentialUsage{SourceName: u.SourceName}
	}
	return out
}

func (b credentialRegistryBridge) AddCredential(spec api.CredentialSpec) (api.CredentialSpec, error) {
	stored, err := b.svc.AddCredential(toServiceCred(spec))
	if err != nil {
		return api.CredentialSpec{}, err
	}
	return toAPICred(stored), nil
}

func (b credentialRegistryBridge) ListCredentials() ([]api.CredentialSpec, error) {
	src, err := b.svc.ListCredentials()
	if err != nil {
		return nil, err
	}
	out := make([]api.CredentialSpec, len(src))
	for i, c := range src {
		out[i] = toAPICred(c)
	}
	return out, nil
}

func (b credentialRegistryBridge) GetCredential(name string) (api.CredentialSpec, error) {
	c, err := b.svc.GetCredential(name)
	if err != nil {
		return api.CredentialSpec{}, err
	}
	return toAPICred(c), nil
}

func (b credentialRegistryBridge) DeleteCredential(name string, force bool) error {
	err := b.svc.DeleteCredential(name, force)
	if err == nil {
		return nil
	}
	var inUse *tuiservice.ErrCredentialInUse
	if errors.As(err, &inUse) {
		return &credInUseError{inner: inUse}
	}
	return err
}

// notebookRegistryBridge adapts service.Service onto api.NotebookRegistry.
// The service returns *service.CellRunResult; api expects *api.CellRunResult.
// Both share the same field shape (cell + result) but the Go type system
// requires the bridge translation.
type notebookRegistryBridge struct {
	svc *tuiservice.Service
}

func (b notebookRegistryBridge) ListNotebooks() ([]notebooks.Summary, error) {
	return b.svc.ListNotebooks()
}

func (b notebookRegistryBridge) GetNotebook(name string) (*notebooks.Notebook, error) {
	return b.svc.GetNotebook(name)
}

func (b notebookRegistryBridge) CreateNotebook(name string) (*notebooks.Notebook, error) {
	return b.svc.CreateNotebook(name)
}

func (b notebookRegistryBridge) SaveNotebook(nb *notebooks.Notebook) (*notebooks.Notebook, error) {
	return b.svc.SaveNotebook(nb)
}

func (b notebookRegistryBridge) DeleteNotebook(name string) error {
	return b.svc.DeleteNotebook(name)
}

func (b notebookRegistryBridge) ClearOutputs(name string) (*notebooks.Notebook, error) {
	return b.svc.ClearOutputs(name)
}

func (b notebookRegistryBridge) RunCell(ctx context.Context, name, cellID string) (*api.CellRunResult, error) {
	res, err := b.svc.RunCell(ctx, name, cellID)
	if err != nil {
		return nil, err
	}
	return &api.CellRunResult{Cell: res.Cell, Result: res.Result}, nil
}

func (b notebookRegistryBridge) CancelCell(ctx context.Context, name, cellRunID string) error {
	return b.svc.CancelCell(ctx, name, cellRunID)
}

func (b notebookRegistryBridge) StopNotebookSession(ctx context.Context, name string) error {
	return b.svc.StopNotebookSession(ctx, name)
}

func (b notebookRegistryBridge) GraduateCell(notebookName, cellID, pipelineDir, transformName string) error {
	_, err := b.svc.GraduateCell(notebookName, cellID, pipelineDir, transformName)
	return err
}

// backfillBridge adapts service.Service onto api.Backfiller. The service
// types and API types mirror each other field-for-field; this bridge is
// the seam that keeps internal/api free of an internal/service import.
type backfillBridge struct {
	svc *tuiservice.Service
}

func (b backfillBridge) BackfillStage(ctx context.Context, req api.BackfillStageRequest) (*api.BackfillRun, error) {
	run, err := b.svc.BackfillStage(ctx, tuiservice.BackfillStageRequest{
		Dir:    req.Dir,
		Node:   req.Node,
		From:   req.From,
		To:     req.To,
		Direct: req.Direct,
	})
	// BackfillStage can return both a partial run AND an error when the
	// Lambda itself reported an error — surface both verbatim.
	return toAPIBackfillRun(run), err
}

func (b backfillBridge) BackfillList(ctx context.Context, dir string) ([]api.BackfillRun, error) {
	src, err := b.svc.BackfillList(ctx, dir)
	if err != nil {
		return nil, err
	}
	out := make([]api.BackfillRun, len(src))
	for i := range src {
		out[i] = *toAPIBackfillRun(&src[i])
	}
	return out, nil
}

func (b backfillBridge) BackfillDiff(ctx context.Context, dir, runID string) (*api.BackfillDiff, error) {
	d, err := b.svc.BackfillDiff(ctx, dir, runID)
	if err != nil {
		return nil, err
	}
	cols := make([]api.BackfillColumnInfo, len(d.StagingColumns))
	for i, c := range d.StagingColumns {
		cols[i] = api.BackfillColumnInfo{Name: c.Name, Type: c.Type}
	}
	return &api.BackfillDiff{
		RunID:           d.RunID,
		StagingTable:    d.StagingTable,
		CanonicalTable:  d.CanonicalTable,
		StagingRows:     d.StagingRows,
		CanonicalRows:   d.CanonicalRows,
		SchemaMatches:   d.SchemaMatches,
		SchemaDiff:      d.SchemaDiff,
		OutputMode:      d.OutputMode,
		MergeKeys:       d.MergeKeys,
		MatchingKeyRows: d.MatchingKeyRows,
		NewKeyRows:      d.NewKeyRows,
		StagingColumns:  cols,
	}, nil
}

func (b backfillBridge) BackfillDedupCheck(ctx context.Context, dir, runID, col string) (*api.BackfillDedupCheckResult, error) {
	r, err := b.svc.BackfillDedupCheck(ctx, dir, runID, col)
	if err != nil {
		return nil, err
	}
	return &api.BackfillDedupCheckResult{MatchingRows: r.MatchingRows, NewRows: r.NewRows}, nil
}

func (b backfillBridge) BackfillPromote(ctx context.Context, dir, runID string, opts api.BackfillPromoteOpts) (*api.BackfillPromoteResult, error) {
	r, err := b.svc.BackfillPromote(ctx, dir, runID, tuiservice.BackfillPromoteOpts{
		ForceDedup:      opts.ForceDedup,
		AllowDuplicates: opts.AllowDuplicates,
	})
	if err != nil {
		return nil, err
	}
	return &api.BackfillPromoteResult{ColumnsAdded: r.ColumnsAdded}, nil
}

func (b backfillBridge) BackfillDiscard(ctx context.Context, dir, runID string) error {
	return b.svc.BackfillDiscard(ctx, dir, runID)
}

func toAPIBackfillRun(r *tuiservice.BackfillRun) *api.BackfillRun {
	if r == nil {
		return nil
	}
	out := &api.BackfillRun{
		RunID:          r.RunID,
		Pipeline:       r.Pipeline,
		Node:           r.Node,
		OutputKey:      r.OutputKey,
		From:           r.From,
		To:             r.To,
		Direct:         r.Direct,
		TargetTable:    r.TargetTable,
		CanonicalTable: r.CanonicalTable,
		Status:         r.Status,
		RowsWritten:    r.RowsWritten,
		ErrorMsg:       r.ErrorMsg,
	}
	if !r.StartedAt.IsZero() {
		out.StartedAt = r.StartedAt.UTC().Format(time.RFC3339)
	}
	if !r.StoppedAt.IsZero() {
		out.StoppedAt = r.StoppedAt.UTC().Format(time.RFC3339)
	}
	return out
}

func (b lineageBridge) Lineage(dir string) (*api.LineageResponse, error) {
	src, err := b.svc.Lineage(dir)
	if err != nil {
		return nil, err
	}
	if src == nil {
		return &api.LineageResponse{}, nil
	}
	out := make([]api.LineageEdge, len(src.Edges))
	for i, e := range src.Edges {
		out[i] = api.LineageEdge{
			FromNode:     e.FromNode,
			FromType:     e.FromType,
			ToNode:       e.ToNode,
			ToType:       e.ToType,
			ViaTable:     e.ViaTable,
			FromPipeline: e.FromPipeline,
			ToPipeline:   e.ToPipeline,
			ToTable:      e.ToTable,
		}
	}
	return &api.LineageResponse{
		Edges:   out,
		Catalog: src.Catalog,
		Schema:  src.Schema,
	}, nil
}

func newUICmd() *cobra.Command {
	var noBrowser bool

	cmd := &cobra.Command{
		Use:   "ui",
		Short: "Start the visual editor in your browser",
		Long: `Start the UI server and open it in your browser.

The UI reads and writes .tf files in the workspace directory. It renders
pipelines as interactive DAGs and lets you edit nodes, connect edges,
and preview data visually.

Examples:
  clavesa ui
  clavesa ui --workspace /path/to/project
  clavesa ui --no-browser`,
		RunE: func(cmd *cobra.Command, args []string) error {
			workspace, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			workspace, err = filepath.Abs(workspace)
			if err != nil {
				return fmt.Errorf("resolve workspace: %w", err)
			}

			addr := ":8080"
			if a := os.Getenv("CLAVESA_ADDR"); a != "" {
				addr = a
			}

			// resolveWorkspace (above) has already applied the
			// workspace's persisted AWS profile to AWS_PROFILE, so the
			// AWS clients built below and the AWS_PROFILE forwarded into
			// the runner (service/run.go) agree on one profile.

			// Athena output bucket: explicit env var wins; otherwise
			// auto-derive from the workspace's terraform.tfstate
			// (`pipeline_bucket` output, the same bucket the Athena
			// workgroup uses for `athena-results/`). Empty in local-only
			// or pre-deploy mode — Athena calls would fail anyway, the
			// resolver routes those requests to the local provider.
			athenaOutputBucket := os.Getenv("ATHENA_OUTPUT_BUCKET")
			if athenaOutputBucket == "" {
				athenaOutputBucket = wspkg.PipelineBucket(workspace)
			}

			var s3Client *s3.Client
			var athenaClient *athena.Client
			var glueClient *glue.Client
			var sfnClient *sfn.Client
			var cwlClient *cloudwatchlogs.Client
			// awsIdentity: the server's effective AWS account/profile,
			// resolved once here so the UI header can show "operating as
			// account X" — the fast diagnosis for a creds-mismatch 403.
			// Zero value (Available=false) is the local-only answer.
			var awsIdentity api.AWSIdentity
			if awsCfg, err := awsconfig.LoadDefaultConfig(context.Background()); err != nil {
				fmt.Fprintf(os.Stderr, "clavesa: AWS config unavailable (local-only mode): %v\n", err)
			} else {
				s3Client = s3.NewFromConfig(awsCfg)
				athenaClient = athena.NewFromConfig(awsCfg)
				glueClient = glue.NewFromConfig(awsCfg)
				sfnClient = sfn.NewFromConfig(awsCfg)
				cwlClient = cloudwatchlogs.NewFromConfig(awsCfg)
				// One cached GetCallerIdentity. Short timeout so a hung
				// credential provider can't stall `clavesa ui` startup.
				idCtx, idCancel := context.WithTimeout(context.Background(), 3*time.Second)
				if out, idErr := sts.NewFromConfig(awsCfg).GetCallerIdentity(idCtx, &sts.GetCallerIdentityInput{}); idErr == nil {
					awsIdentity = api.AWSIdentity{
						Available: true,
						AccountID: derefStr(out.Account),
						ARN:       derefStr(out.Arn),
						Profile:   os.Getenv("AWS_PROFILE"),
					}
				}
				idCancel()
			}

			// Per-pipeline observability resolver: routes states/logs/snapshots/
			// node-runs/runs queries to either the cloud provider (Athena+SFN+
			// CloudWatch) or the local provider (filesystem progress channel +
			// runner-container Spark) based on the inspected pipeline's compute
			// attr. ADR-014 binds parity here.
			var cloudProv observability.Provider
			if athenaClient != nil {
				cloudProv = observability.NewCloudProvider(athenaClient, athenaOutputBucket, sfnClient, cwlClient)
			}
			// Warm-Spark-per-warehouse: the Catalog / dashboards / TableDetail
			// surfaces fire many read queries on load, each previously paying
			// the ~18-30s Spark JVM cold start. The persistent runner keeps
			// one warm container per pipeline warehouse and reuses it across
			// queries — first call still ~30s, every subsequent call <100ms.
			// SweepWarmWorkers cleans up any containers left behind by a
			// prior SIGKILL'd session before we spawn fresh.
			observability.SweepWarmWorkers(workspace)
			warmQuery := observability.NewPersistentQueryRunner(workspace)

			// Eager Spark warmup: the Catalog landing page itself doesn't
			// fire a Spark query (it reads from Glue + filesystem state),
			// so without this the runtime indicator stays "idle" until the
			// user clicks into a table — at which point Spark suddenly
			// starts a ~30s cold boot exactly when they wanted data.
			// Spawning here in the background flips the indicator to
			// "Starting Spark…" on the next 3s poll and is "Spark ready"
			// by the time the user navigates anywhere Spark-backed.
			// Gated on the workspace being initialized; otherwise the
			// runner image doesn't exist and `docker run` would fail
			// against the empty-name fallback tag.
			if m, _ := wspkg.Load(workspace); m != nil {
				go warmQuery.Warmup(context.Background(), wspkg.LocalWarehouseDir(workspace))
			}

			localProv := observability.NewLocalProvider(workspace).WithQueryRunner(warmQuery)
			resolver := observability.NewResolver(workspace, cloudProv, localProv)

			// Per-notebook REPL pool — shares the warm container's Spark
			// Connect plugin via per-notebook session_ids. Evict-on-pipeline-
			// run targets only notebook REPLs (not the warm container itself)
			// so catalog rendering stays alive during a `pipeline run`.
			nbRunner := observability.NewNotebookSessionRunner(warmQuery)

			fo := fileops.New()
			svc := tuiservice.New(workspace).
				WithEvictor(nbRunner).
				WithResolver(resolver).
				WithNotebookRunner(nbRunner)
			// lineageAdapter shims the JSON shape the api package owns onto the
			// derivation owned by service. Two shapes mirror each other field-
			// for-field; the adapter is the seam keeping api.Handler from
			// importing internal/service.
			lineageAdapter := lineageBridge{svc: svc}
			pipelineHandler := api.New(fo, workspace).
				WithSyncer(svc).
				WithLineage(lineageAdapter).
				WithNodeAdder(nodeAdderBridge{svc: svc}).
				WithExternalTableAttacher(svc).
				WithInputDetacher(svc)
			// Self-restart hook for PUT /workspace/aws-profile: the AWS
			// SDK clients are built once above and can't be hot-swapped,
			// so a profile change re-execs the process. syscall.Exec
			// keeps the PID, fds, and terminal — the browser just
			// reloads once the server is back.
			restartFn := func() {
				exe, err := os.Executable()
				if err != nil {
					fmt.Fprintf(os.Stderr, "clavesa: cannot locate own binary to restart: %v\n", err)
					return
				}
				// Brief pause so the HTTP response flushes before the
				// process image is replaced.
				time.Sleep(250 * time.Millisecond)
				if err := syscall.Exec(exe, os.Args, os.Environ()); err != nil {
					fmt.Fprintf(os.Stderr, "clavesa: self-restart failed: %v\n", err)
				}
			}
			workspaceHandler := api.NewWorkspaceHandler(workspace).WithRestart(restartFn)
			statusHandler := pipelinestatus.NewHandler(workspace).WithResolver(resolver).WithLocalRunner(localPipelineRunnerBridge{svc: svc})
			dataHandler := dataquery.NewHandler(s3Client, athenaClient, athenaOutputBucket).(*dataquery.Handler).WithResolver(resolver)
			// nil-safe: catalog handler renders an empty list in local-only mode.
			var catalogClient api.GlueClient
			if glueClient != nil {
				catalogClient = glueClient
			}
			catalogHandler := api.NewCatalogHandler(catalogClient).WithWorkspace(workspace)
			// Dashboards: CRUD against the `dashboards` system Iceberg table
			// via the service layer, plus a Provider-dispatched query route
			// for widget SQL. Cloud fallback lights up when the resolver
			// can't dispatch on a dir.
			dashboardsHandler := api.NewDashboardsHandler(dashboardStoreBridge{svc: svc}, cloudProv).WithResolver(resolver)
			sourcesHandler := api.NewSourcesHandler(sourceRegistryBridge{svc: svc})
			credentialsHandler := api.NewCredentialsHandler(credentialRegistryBridge{svc: svc})
			notebooksHandler := api.NewNotebooksHandler(notebookRegistryBridge{svc: svc})
			backfillHandler := api.NewBackfillHandler(backfillBridge{svc: svc})
			runtimeHandler := api.NewRuntimeHandler(warmQuery, awsIdentity)

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
			previewHandler := preview.NewHandler(s3Client, hclParserFunc, resolveDirFunc)

			// API mux — all backend routes mount under /api/* so the SPA at /
			// is free to use any path without colliding with the backend.
			apiMux := http.NewServeMux()
			pipelineHandler.RegisterRoutes(apiMux)
			workspaceHandler.RegisterRoutes(apiMux)
			statusHandler.RegisterRoutes(apiMux)
			catalogHandler.RegisterRoutes(apiMux)
			dashboardsHandler.RegisterRoutes(apiMux)
			sourcesHandler.RegisterRoutes(apiMux)
			credentialsHandler.RegisterRoutes(apiMux)
			notebooksHandler.RegisterRoutes(apiMux)
			backfillHandler.RegisterRoutes(apiMux)
			runtimeHandler.RegisterRoutes(apiMux)
			apiMux.Handle("/data/", dataHandler)
			apiMux.Handle("/preview/", previewHandler)

			mux := http.NewServeMux()
			mux.Handle("/api/", http.StripPrefix("/api", apiMux))

			if embeddedUI == nil {
				return fmt.Errorf("embedded UI not available")
			}
			sub, err := fs.Sub(embeddedUI, "dist")
			if err != nil {
				return fmt.Errorf("embed: %w", err)
			}
			mux.Handle("/", spaHandler{static: sub})

			fmt.Printf("clavesa listening on %s (workspace: %s)\n", addr, workspace)
			if !noBrowser {
				go openBrowser("http://localhost" + addr)
			}

			// Graceful shutdown: SIGINT/SIGTERM triggers http.Server.Shutdown
			// AND a docker stop on every warm Spark container we spawned.
			// Without this, Ctrl-C would leak ~1GB-resident containers per
			// pipeline; SweepWarmWorkers cleans that up on the next start
			// but the kill-and-restart cycle still wastes time and RAM.
			srv := &http.Server{Addr: addr, Handler: mux}
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			serveErr := make(chan error, 1)
			go func() { serveErr <- srv.ListenAndServe() }()

			select {
			case err := <-serveErr:
				warmQuery.Close()
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					return err
				}
				return nil
			case <-sigCh:
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = srv.Shutdown(shutdownCtx)
				warmQuery.Close()
				return nil
			}
		},
	}

	cmd.Flags().BoolVar(&noBrowser, "no-browser", false, "do not open the browser automatically")

	return cmd
}

type spaHandler struct{ static fs.FS }

func (h spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}
	_, err := h.static.Open(path)
	if err != nil {
		r2 := new(http.Request)
		*r2 = *r
		r2.URL = new(url.URL)
		*r2.URL = *r.URL
		r2.URL.Path = "/"
		http.FileServer(http.FS(h.static)).ServeHTTP(w, r2)
		return
	}
	http.FileServer(http.FS(h.static)).ServeHTTP(w, r)
}

func openBrowser(u string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", u)
	default:
		cmd = exec.Command("xdg-open", u)
	}
	_ = cmd.Start()
}
