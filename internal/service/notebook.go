package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/notebooks"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// Notebook mirrors notebooks.Notebook at the service boundary so callers
// don't need to import internal/notebooks directly.
type Notebook = notebooks.Notebook

// NotebookSummary is the list-page shape.
type NotebookSummary = notebooks.Summary

// CellRunResult bundles the runner CellResult with the freshly persisted
// per-cell metadata so the UI can update the cell badge without re-fetching
// the whole notebook.
type CellRunResult struct {
	Cell   notebooks.Cell       `json:"cell"`
	Result observability.CellResult `json:"result"`
}

// NotebookRunner is what the service layer needs from the REPL pool. Kept
// as an interface so tests can stub it without spinning a Docker container.
type NotebookRunner interface {
	RunCell(ctx context.Context, notebookID, warehouse, cellRunID, language, source string) (*observability.CellResult, error)
	CancelCell(ctx context.Context, notebookID, warehouse, cellRunID string) error
	StopSession(ctx context.Context, notebookID string) error
	Sessions() []observability.NotebookSessionStatus
}

// notebookStore returns the workspace-rooted notebooks store. Built per call
// so a workspace switch (`workspace use`) takes effect without re-creating
// the Service.
func (s *Service) notebookStore() *notebooks.Store {
	return notebooks.New(s.workspace)
}

// WithNotebookRunner registers the REPL pool. nil disables the runner-side
// ops (Run/Cancel/StopSession will error). CLI list/get/create/delete don't
// need a runner — leave it unset in pure-CLI processes.
func (s *Service) WithNotebookRunner(r NotebookRunner) *Service {
	s.notebookRunner = r
	return s
}

// ListNotebooks returns lightweight summaries (name + cell count + mtime).
func (s *Service) ListNotebooks() ([]NotebookSummary, error) {
	return s.notebookStore().List()
}

// GetNotebook reads one notebook by name. Returns os.ErrNotExist when absent.
func (s *Service) GetNotebook(name string) (*Notebook, error) {
	nb, err := s.notebookStore().Get(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &notRegisteredError{kind: "notebook", name: name}
		}
		return nil, err
	}
	return nb, nil
}

// CreateNotebook creates an empty notebook for `name`. Returns an error if
// it already exists.
func (s *Service) CreateNotebook(name string) (*Notebook, error) {
	return s.notebookStore().Create(name)
}

// SaveNotebook writes the full notebook back. The notebook's Name field
// determines the target file — callers that want a rename must Delete +
// Create then Save (no rename op; matches sources).
//
// Auto-assigns IDs to cells missing one (UI clients sometimes add a cell
// before deciding on an id — easier to fill in here than push the policy
// up to the caller).
func (s *Service) SaveNotebook(nb *Notebook) (*Notebook, error) {
	if nb == nil {
		return nil, fmt.Errorf("notebook is nil")
	}
	for i := range nb.Cells {
		if nb.Cells[i].ID == "" {
			nb.Cells[i].ID = newCellID()
		}
	}
	if err := s.notebookStore().Save(nb); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &notRegisteredError{kind: "notebook", name: nb.Name}
		}
		return nil, err
	}
	out, err := s.notebookStore().Get(nb.Name)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteNotebook removes the notebook from disk and stops its REPL if one
// is running. Pure user content — no pipeline-scan guard (notebooks aren't
// referenced from .tf files).
func (s *Service) DeleteNotebook(name string) error {
	if s.notebookRunner != nil {
		_ = s.notebookRunner.StopSession(context.Background(), name)
	}
	if err := s.notebookStore().Delete(name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &notRegisteredError{kind: "notebook", name: name}
		}
		return err
	}
	return nil
}

// ClearOutputs zeroes outputs[] and execution_count on every code cell —
// `jupyter nbconvert --clear-output` equivalent. For git-friendly commits.
func (s *Service) ClearOutputs(name string) (*Notebook, error) {
	return s.notebookStore().ClearOutputs(name)
}

// RunCell runs cell `cellID` of notebook `name`. The cell's source is taken
// from disk (no override) — callers that want to test edited-but-unsaved
// source must Save first. Returns the runner result AND the freshly updated
// cell so the UI can update its badge inline.
func (s *Service) RunCell(ctx context.Context, name, cellID string) (*CellRunResult, error) {
	if s.notebookRunner == nil {
		return nil, fmt.Errorf("notebook runner not configured — start `clavesa ui` to enable cell execution")
	}
	nb, err := s.GetNotebook(name)
	if err != nil {
		return nil, err
	}
	cellIdx := -1
	for i, c := range nb.Cells {
		if c.ID == cellID {
			cellIdx = i
			break
		}
	}
	if cellIdx < 0 {
		return nil, fmt.Errorf("notebook %q has no cell with id %q", name, cellID)
	}
	cell := &nb.Cells[cellIdx]
	if cell.CellType != notebooks.CellTypeCode {
		return nil, fmt.Errorf("cell %q is %s, not code — only code cells can run", cellID, cell.CellType)
	}

	source := strings.Join(cell.Source, "")
	language := detectCellLanguage(source)
	warehouse := workspace.LocalWarehouseDir(s.workspace)
	cellRunID := newCellRunID()

	res, err := s.notebookRunner.RunCell(ctx, name, warehouse, cellRunID, language, source)
	if err != nil {
		return nil, err
	}

	// Apply outputs + metadata back to the cell, then save.
	cell.Outputs = cellResultToOutputs(res)
	ec := nextExecutionCount(nb)
	cell.ExecutionCount = &ec
	if cell.Metadata.Clavesa == nil {
		cell.Metadata.Clavesa = &notebooks.CellClavesaMeta{}
	}
	cell.Metadata.Clavesa.LastRunAt = time.Now().UTC().Format(time.RFC3339)
	cell.Metadata.Clavesa.LastDurationMS = res.DurationMS
	cell.Metadata.Clavesa.LastStatus = res.Status

	if _, err := s.SaveNotebook(nb); err != nil {
		return nil, fmt.Errorf("save after run: %w", err)
	}
	return &CellRunResult{Cell: *cell, Result: *res}, nil
}

// CancelCell forwards a cancel request to the notebook's REPL. Best-effort;
// see notebookSessionRunner.CancelCell for semantics. Safe to call when no
// session exists (no-op).
func (s *Service) CancelCell(ctx context.Context, name, cellRunID string) error {
	if s.notebookRunner == nil {
		return nil
	}
	warehouse := workspace.LocalWarehouseDir(s.workspace)
	return s.notebookRunner.CancelCell(ctx, name, warehouse, cellRunID)
}

// StopNotebookSession kills the REPL subprocess for `name`. No-op when no
// session is active.
func (s *Service) StopNotebookSession(ctx context.Context, name string) error {
	if s.notebookRunner == nil {
		return nil
	}
	return s.notebookRunner.StopSession(ctx, name)
}

// NotebookSessions surfaces every active REPL for the runtime indicator.
func (s *Service) NotebookSessions() []observability.NotebookSessionStatus {
	if s.notebookRunner == nil {
		return []observability.NotebookSessionStatus{}
	}
	return s.notebookRunner.Sessions()
}

// GraduateCell turns a notebook cell into a transform node in the named
// pipeline — the explore → productionize loop. Writes the cell source to
// `<pipeline>/transforms/<transformName>.{sql,py}` (stripping the leading
// %%magic), then calls AddNode + UpdateNode to register the transform in
// the pipeline's main.tf with the right language / file() reference.
//
// Constraints:
//   - cell must be a code cell (markdown can't graduate)
//   - transformName must be a valid identifier and not collide with an
//     existing node in the pipeline
//   - source code is taken verbatim from disk; callers that want to
//     graduate an edited-but-unsaved cell should Save first
//
// The graduated transform has no inputs wired — the user is expected to
// attach sources / connect upstream nodes via the editor afterward, since
// the notebook side has no formal upstream concept (it just reads from
// `spark` directly). v1 limitation; future slice may infer inputs from
// `spark.table("…")` calls in the cell body.
func (s *Service) GraduateCell(notebookName, cellID, pipelineDir, transformName string) (PipelineGraph, error) {
	if transformName == "" {
		return PipelineGraph{}, fmt.Errorf("transform name is required")
	}
	if pipelineDir == "" {
		return PipelineGraph{}, fmt.Errorf("pipeline dir is required")
	}

	nb, err := s.GetNotebook(notebookName)
	if err != nil {
		return PipelineGraph{}, err
	}
	var cell *notebooks.Cell
	for i := range nb.Cells {
		if nb.Cells[i].ID == cellID {
			cell = &nb.Cells[i]
			break
		}
	}
	if cell == nil {
		return PipelineGraph{}, fmt.Errorf("notebook %q has no cell with id %q", notebookName, cellID)
	}
	if cell.CellType != notebooks.CellTypeCode {
		return PipelineGraph{}, fmt.Errorf("cell %q is %s, not code", cellID, cell.CellType)
	}

	source := strings.Join(cell.Source, "")
	language, body := stripCellMagic(source)
	ext := "sql"
	if language == "python" {
		ext = "py"
	}

	pipelineAbs := s.resolveDir(pipelineDir)
	transformsDir := filepath.Join(pipelineAbs, "transforms")
	if err := os.MkdirAll(transformsDir, 0o755); err != nil {
		return PipelineGraph{}, fmt.Errorf("create transforms dir: %w", err)
	}
	transformFile := filepath.Join(transformsDir, transformName+"."+ext)
	if _, err := os.Stat(transformFile); err == nil {
		return PipelineGraph{}, fmt.Errorf(
			"transform file %s already exists — pick a different name or delete it first",
			filepath.Join("transforms", transformName+"."+ext),
		)
	}
	if err := os.WriteFile(transformFile, []byte(ensureTrailingNewline(body)), 0o644); err != nil {
		return PipelineGraph{}, fmt.Errorf("write transform body: %w", err)
	}

	// AddNode writes the new module block. It rejects duplicate names so
	// no need to pre-check here; the error message is already user-readable.
	if _, err := s.AddNode(pipelineDir, "transform", transformName); err != nil {
		// Best-effort cleanup of the file we wrote — keeps the workspace
		// from accumulating orphaned transforms/foo.sql on a failed add.
		_ = os.Remove(transformFile)
		return PipelineGraph{}, fmt.Errorf("add transform node: %w", err)
	}

	// Wire the language + file() reference to the freshly created node.
	relRef := fmt.Sprintf(`file(%q)`, filepath.Join("transforms", transformName+"."+ext))
	attrs := map[string]interface{}{}
	if language == "python" {
		attrs["language"] = "python"
		attrs["python"] = Ref(relRef)
	} else {
		attrs["sql"] = Ref(relRef)
	}
	return s.UpdateNode(pipelineDir, transformName, attrs)
}

// stripCellMagic peels the leading %%sql / %%python directive off a cell's
// source and returns (language, body). Mirrors the runner's _parse_magic
// in notebook_repl.py.
func stripCellMagic(source string) (lang, body string) {
	trimmed := source
	// preserve the original indentation of body; only consume leading
	// blank lines + the magic line itself.
	stripped := strings.TrimLeft(trimmed, "\n\r ")
	if !strings.HasPrefix(stripped, "%%") {
		return "python", trimmed
	}
	firstLine, rest, _ := strings.Cut(stripped, "\n")
	magic := strings.TrimSpace(firstLine[2:])
	switch magic {
	case "sql":
		return "sql", rest
	case "python":
		return "python", rest
	default:
		// Unknown magic — treat as python with the magic line still in
		// the body. Lets the user spot + fix it after graduation.
		return "python", trimmed
	}
}

func ensureTrailingNewline(s string) string {
	if s == "" || strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}

// ---- helpers ----------------------------------------------------------------

// detectCellLanguage parses the cell magic the same way the runner does, so
// `clavesa notebook run --json` and the UI both report a consistent language
// tag in their CellResult envelope (the runner ignores this hint — the magic
// IS the language — so this is purely informational).
func detectCellLanguage(source string) string {
	trimmed := strings.TrimLeft(source, "\n\r ")
	if strings.HasPrefix(trimmed, "%%sql") {
		return "sql"
	}
	if strings.HasPrefix(trimmed, "%%python") {
		return "python"
	}
	return "python"
}

// newCellID returns a fresh hex ID for a cell. nbformat 4.5 accepts any
// non-empty string; we use 16 random bytes (128 bits, UUID-shaped without
// the dashes) which is more than enough collision resistance for cell IDs
// within a notebook.
func newCellID() string { return randHex(16) }

// newCellRunID is the tag attached to a cell's Spark ops so
// `spark.interruptTag(cell_run_id)` can target just this execution.
func newCellRunID() string { return randHex(16) }

func randHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand never errors on Linux/macOS in practice; if it does
		// we'd rather fall back to a coarse time-derived ID than panic.
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

// nextExecutionCount returns the largest existing execution_count + 1, so
// re-running cell 3 after cell 7 makes its execution_count = 8 (matches
// Jupyter convention — monotonic, scoped to the notebook).
func nextExecutionCount(nb *Notebook) int {
	max := 0
	for _, c := range nb.Cells {
		if c.ExecutionCount != nil && *c.ExecutionCount > max {
			max = *c.ExecutionCount
		}
	}
	return max + 1
}

// cellResultToOutputs translates the runner's CellResult into nbformat
// outputs[]. Mirrors what notebook_repl.py would emit if it spoke nbformat
// natively, but doing it here keeps the runner's wire format light.
func cellResultToOutputs(res *observability.CellResult) []notebooks.Output {
	var out []notebooks.Output
	if res.Stdout != "" {
		out = append(out, notebooks.Output{
			OutputType: notebooks.OutputTypeStream,
			Name:       "stdout",
			Text:       splitLinesKeepEnd(res.Stdout),
		})
	}
	if res.Stderr != "" {
		out = append(out, notebooks.Output{
			OutputType: notebooks.OutputTypeStream,
			Name:       "stderr",
			Text:       splitLinesKeepEnd(res.Stderr),
		})
	}
	if res.Status == "error" && res.Error != nil {
		out = append(out, notebooks.Output{
			OutputType: notebooks.OutputTypeError,
			EName:      res.Error.EName,
			EValue:     res.Error.EValue,
			Traceback:  res.Error.Traceback,
		})
	}
	if res.Status == "ok" && res.Display != nil && res.Display.Type != "none" {
		data := map[string]any{
			"text/plain": res.Display.TextRepr,
		}
		if res.Display.Type == "table" {
			data[notebooks.MIMEDataFrame] = map[string]any{
				"columns":      res.Display.Columns,
				"column_types": res.Display.ColumnTypes,
				"rows":         res.Display.Rows,
			}
		}
		meta := map[string]any{
			"clavesa": map[string]any{"truncated": res.Display.Truncated},
		}
		ec := lastExecutionCountOrZero(res)
		out = append(out, notebooks.Output{
			OutputType:     notebooks.OutputTypeExecuteResult,
			ExecutionCount: ec,
			Data:           data,
			Metadata:       meta,
		})
	}
	return out
}

// lastExecutionCountOrZero stubs the per-output execution_count to nil —
// we don't currently thread the notebook-wide count through the runner.
// The cell-level execution_count is set in RunCell; nbformat output-level
// execution_count is informational only and can stay null.
func lastExecutionCountOrZero(_ *observability.CellResult) *int { return nil }

// splitLinesKeepEnd preserves nbformat convention: each entry in stream
// output text[] should end in '\n' except possibly the last. We just split
// on newlines and re-add the separators on all but the final piece.
func splitLinesKeepEnd(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.SplitAfter(s, "\n")
	// strip empty trailing element produced when s ends in '\n'
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}
