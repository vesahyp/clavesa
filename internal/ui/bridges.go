package ui

// The bridges adapt service.Service onto the narrow interfaces the HTTP
// handlers in internal/api and internal/pipelinestatus declare, so those
// packages stay free of an internal/service import. Moved here from
// internal/cli/ui.go with BuildWorkspaceMux (ADR-032 slice 2).

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/lineagetype"
	"github.com/vesahyp/clavesa/internal/notebooks"
	"github.com/vesahyp/clavesa/internal/pipelinestatus"
	"github.com/vesahyp/clavesa/internal/preview"
	tuiservice "github.com/vesahyp/clavesa/internal/service"
)

// lineageBridge used to convert service.LineageEdge → api.LineageEdge
// field-for-field. Both sides now alias internal/lineagetype.Edge so the
// adapter is structurally a no-op; the type still exists to satisfy
// api.Lineager (service.Service can't directly because the receiver
// returns *LineageResult, which equals *lineagetype.Response now —
// pointer-aliasing works fine).
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
// pipelinestatus.LocalPipelineRunner. Both sentinels point at the same
// internal/errs.ErrRunInFlight value now (C10, 2026-05-24), so the
// errors.Is mapping that used to live here is no longer needed.
type localPipelineRunnerBridge struct {
	svc *tuiservice.Service
}

func (b localPipelineRunnerBridge) StartRunWithOpts(dir string, opts pipelinestatus.RunOpts) (string, error) {
	return b.svc.StartRunWithOpts(dir, tuiservice.RunOpts{
		Force:      opts.Force,
		ForceNodes: opts.ForceNodes,
	})
}

// cloudPipelineRunnerBridge adapts service.Service onto
// pipelinestatus.CloudPipelineRunner. Mirrors the local bridge above —
// translates RunOpts between the two packages so the handler stays free
// of an internal/service import.
type cloudPipelineRunnerBridge struct {
	svc *tuiservice.Service
}

func (b cloudPipelineRunnerBridge) RunPipelineCloud(ctx context.Context, dir string, opts pipelinestatus.RunOpts) (string, error) {
	return b.svc.RunPipelineCloud(ctx, dir, tuiservice.RunOpts{
		Force:      opts.Force,
		ForceNodes: opts.ForceNodes,
	})
}

// cloudLocalPipelineRunnerBridge adapts service.Service onto
// pipelinestatus.CloudLocalPipelineRunner (ADR-024 — cloud warehouse, local
// compute). Same RunOpts translation as the cloud bridge.
type cloudLocalPipelineRunnerBridge struct {
	svc *tuiservice.Service
}

func (b cloudLocalPipelineRunnerBridge) StartRunCloudLocal(ctx context.Context, dir string, opts pipelinestatus.RunOpts) (string, error) {
	// Async dispatch: the cloud-local run is a synchronous docker bundle that
	// takes the full pipeline duration, so the UI must NOT block the HTTP
	// request on it (the CLI's StartRunCloudLocal does, with --wait implicit).
	// The async variant prepares synchronously — a held lock still 409s — then
	// returns the local-<uuid> id immediately and runs the bundle in the
	// background; the browser polls the progress channel. ctx is unused (the
	// run is detached onto context.Background inside the async path).
	_ = ctx
	return b.svc.StartRunCloudLocalAsync(dir, tuiservice.RunOpts{
		Force:      opts.Force,
		ForceNodes: opts.ForceNodes,
	})
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
		NoTrigger:                 s.NoTrigger,
		Table:                     s.Table,
		Stream:                    s.Stream, Region: s.Region,
		StartingPosition: s.StartingPosition, Endpoint: s.Endpoint,
	}
}

func toServiceSpec(s api.SourceSpec) tuiservice.SourceSpec {
	return tuiservice.SourceSpec{
		Name: s.Name, Kind: s.Kind, URL: s.URL,
		Bucket: s.Bucket, Prefix: s.Prefix,
		Format: s.Format, Credentials: s.Credentials,
		Partitions: s.Partitions, StartFrom: s.StartFrom,
		ManageBucketNotifications: s.ManageBucketNotifications,
		NoTrigger:                 s.NoTrigger,
		Table:                     s.Table,
		Stream:                    s.Stream, Region: s.Region,
		StartingPosition: s.StartingPosition, Endpoint: s.Endpoint,
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
			RegionField: x.RegionField, TooltipField: x.TooltipField,
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
			RegionField: x.RegionField, TooltipField: x.TooltipField,
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
		ClientID:    c.ClientID,
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
		ClientID:    c.ClientID,
		Secret:      c.Secret,
	}
}

type credInUseError struct {
	inner *tuiservice.ErrCredentialInUse
}

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
	return &api.CellRunResult{Cell: res.Cell, Result: res.Result, Served: res.Served}, nil
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
		Dir:     req.Dir,
		Node:    req.Node,
		From:    req.From,
		To:      req.To,
		Direct:  req.Direct,
		Compute: req.Compute,
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
		Compute:         opts.Compute,
	})
	if err != nil {
		return nil, err
	}
	return &api.BackfillPromoteResult{ColumnsAdded: r.ColumnsAdded}, nil
}

func (b backfillBridge) BackfillDiscard(ctx context.Context, dir, runID, compute string) error {
	return b.svc.BackfillDiscard(ctx, dir, runID, compute)
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
		Compute:        r.Compute,
	}
	if !r.StartedAt.IsZero() {
		out.StartedAt = r.StartedAt.UTC().Format(time.RFC3339)
	}
	if !r.StoppedAt.IsZero() {
		out.StoppedAt = r.StoppedAt.UTC().Format(time.RFC3339)
	}
	return out
}

func (b lineageBridge) Lineage(dir string) (*lineagetype.Response, error) {
	return b.svc.Lineage(dir)
}

// bundleBridge adapts the service to the /api/bundle handler (the same
// calls as clavesa bundle plan and bundle list, ADR-015).
type bundleBridge struct {
	svc  *tuiservice.Service
	root string
}

func (b bundleBridge) PlanBundle(ctx context.Context, dir, target string) (any, error) {
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(b.root, dir)
	}
	return tuiservice.PlanBundle(ctx, tuiservice.BundleOptions{Dir: dir, Target: target})
}

func (b bundleBridge) BundlePipelines() (any, error) { return b.svc.BundlePipelines() }
