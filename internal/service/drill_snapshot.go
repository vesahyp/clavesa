package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// DrillSnapshot is what a local-warehouse workspace publishes beyond its
// runs (ADR-036): the catalog listing, each table's newest snapshots and
// each dashboard dataset's result with its controls' defaults. The hosted
// Lambda answers the Catalog and Dashboards pages from it and runs no
// SQL for the workspace.
type DrillSnapshot struct {
	// At is when the snapshot was taken (RFC 3339, UTC).
	At string `json:"at"`
	// Catalog is the GET /workspace/tables body as the Mac answers it.
	Catalog json.RawMessage `json:"catalog,omitempty"`
	// Snapshots is each table's newest SnapshotLimit Delta snapshots,
	// keyed by "<database>.<table>".
	Snapshots map[string]*observability.SnapshotsResult `json:"snapshots,omitempty"`
	// Widgets is each dashboard dataset's result, keyed by
	// DashboardWidgetKey(dir, dataset SQL).
	Widgets map[string]*observability.QueryResult `json:"widgets,omitempty"`
	// Notes say what was left out and why.
	Notes []string `json:"notes,omitempty"`
}

// SnapshotLimit is how many snapshots per table the drill keeps, the
// number the Catalog page asks for.
const SnapshotLimit = 20

// DashboardWidgetKey is the key of a dataset's stored result: its pipeline
// dir and its SQL as the spec stores it (with {{control}} placeholders),
// so a phone whose control defaults resolve to another day still finds
// the Mac's result.
func DashboardWidgetKey(dir, sql string) string {
	sum := sha256.Sum256([]byte(dir + "\n" + strings.TrimSpace(sql)))
	return hex.EncodeToString(sum[:])
}

// CatalogTableRef is one table of a catalog snapshot, for its snapshots.
type CatalogTableRef struct {
	Database string
	Table    string
	Dir      string
}

// CatalogSnapshotFunc returns the catalog listing (the GET
// /workspace/tables body) and its tables. The catalog code lives in the
// api package, which this package cannot import; wsdeps wires it.
type CatalogSnapshotFunc func(ctx context.Context) (json.RawMessage, []CatalogTableRef, error)

// WithCatalogSnapshot sets how the drill reads the catalog listing.
func (s *Service) WithCatalogSnapshot(fn CatalogSnapshotFunc) *Service {
	s.catalogSnapshotFn = fn
	return s
}

// SnapshotBudget bounds the snapshot's time, apart from the summary's:
// what is not done by then is left out with a note, and the summary is
// published anyway (slice 5 review, finding 3). A var for tests.
var SnapshotBudget = 15 * time.Second

// drillSnapshot builds the snapshot of a local-warehouse workspace, or nil
// when the workspace has not turned it on (hosted.snapshot, ADR-036: off
// unless set). Values (widget results) go only when clavesa.json says
// "personal_data": false explicitly; otherwise the snapshot holds names,
// types and counts only. Each part that fails or runs out of time is left
// out with a note; the publish never fails here.
func (s *Service) drillSnapshot(ctx context.Context, now time.Time) *DrillSnapshot {
	m, _ := workspace.Load(s.workspace)
	if m == nil || m.Hosted == nil || !m.Hosted.Snapshot {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), SnapshotBudget)
	defer cancel()
	snap := &DrillSnapshot{At: now.UTC().Format(time.RFC3339)}
	outOfTime := func(what string) bool {
		if ctx.Err() == nil {
			return false
		}
		snap.Notes = append(snap.Notes, what+" left out: the snapshot ran out of its time")
		return true
	}

	var tables []CatalogTableRef
	if s.catalogSnapshotFn != nil && !outOfTime("the catalog") {
		cat, refs, err := s.catalogSnapshotFn(ctx)
		if err != nil {
			snap.Notes = append(snap.Notes, "catalog not published: "+err.Error())
		} else if cat, err = scrubPaths(cat, s.workspace); err != nil {
			snap.Notes = append(snap.Notes, "catalog not published: "+err.Error())
		} else {
			snap.Catalog, tables = cat, refs
		}
	}
	if len(tables) > 0 && s.dashResolver != nil {
		if prov, err := s.dashResolver.ForWarehouse(workspace.WarehouseLocal); err == nil {
			snap.Snapshots = map[string]*observability.SnapshotsResult{}
			for _, t := range tables {
				if outOfTime("table snapshots") {
					break
				}
				res, err := prov.Snapshots(ctx, observability.SnapshotsQuery{Database: t.Database, Table: t.Table, PipelineDir: t.Dir, Limit: SnapshotLimit})
				if err != nil || res == nil {
					continue
				}
				snap.Snapshots[t.Database+"."+t.Table] = res
			}
		}
	}

	if !m.Hosted.FreeOfPersonalData() {
		snap.Notes = append(snap.Notes, "dashboard values not published: clavesa.json does not say \"personal_data\": false")
		return snap
	}
	list, err := s.ListDashboards(ctx)
	if err != nil {
		snap.Notes = append(snap.Notes, "dashboards not published: "+err.Error())
		return snap
	}
	for _, sum := range list {
		d, err := s.GetDashboard(ctx, sum.Slug)
		if err != nil {
			continue
		}
		params := map[string]string{}
		resolveControlDefaults(d.Controls, params, now)
		for _, ds := range d.Datasets {
			if outOfTime(fmt.Sprintf("dashboard %s dataset %s", sum.Slug, ds.Name)) {
				return snap
			}
			expanded, err := expandPlaceholders(ds.SQL, params)
			if err != nil {
				snap.Notes = append(snap.Notes, fmt.Sprintf("dashboard %s dataset %s: %v", sum.Slug, ds.Name, err))
				continue
			}
			res, err := s.Query(ctx, expanded, QueryOptions{Dir: ds.Dir, MaxRows: DrillWidgetRows})
			if err != nil {
				snap.Notes = append(snap.Notes, fmt.Sprintf("dashboard %s dataset %s: %v", sum.Slug, ds.Name, err))
				continue
			}
			if snap.Widgets == nil {
				snap.Widgets = map[string]*observability.QueryResult{}
			}
			snap.Widgets[DashboardWidgetKey(ds.Dir, ds.SQL)] = res
		}
	}
	return snap
}

// scrubPaths removes paths on the Mac from a catalog listing (slice 5
// review, finding 4): every "location" is dropped, and any other string
// under the workspace root becomes workspace-relative.
func scrubPaths(body json.RawMessage, root string) (json.RawMessage, error) {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, err
	}
	abs, _ := filepath.Abs(root)
	var walk func(any) any
	walk = func(x any) any {
		switch t := x.(type) {
		case map[string]any:
			delete(t, "location")
			for k, e := range t {
				t[k] = walk(e)
			}
			return t
		case []any:
			for i, e := range t {
				t[i] = walk(e)
			}
			return t
		case string:
			for _, r := range []string{abs, root} {
				if r != "" && strings.HasPrefix(t, r) {
					return strings.TrimPrefix(strings.TrimPrefix(t, r), string(filepath.Separator))
				}
			}
			return t
		}
		return x
	}
	return json.Marshal(walk(v))
}

// DrillWidgetRows caps each stored widget result, the hosted row cap.
const DrillWidgetRows = 1000
