// Package notebooks implements the workspace-level notebooks registry: one
// .ipynb file per notebook, multi-cell SQL + PySpark with shared SparkSession
// (via the warm worker's Spark Connect plugin) and persistent Python globals
// across cells in a long-lived per-notebook REPL subprocess.
//
// File format: Jupyter nbformat 4.5 stored at <workspace>/notebooks/<name>.ipynb.
// We pick .ipynb (not a custom shape) for the interop wins — GitHub renders
// these natively, JupyterLab / VSCode / PyCharm open them for offline analysis,
// nbdime diffs them, jupytext converts to git-friendly .py. Per-cell language
// switching uses the established `%%sql` / `%%python` magic convention
// (Databricks does the same) so the file stays valid Jupyter; no custom
// per-cell `language` field is needed.
//
// nbformat is permissive — this package only accepts a supported subset on
// load (no ipywidget outputs, no unknown MIME bundles), so users get a clear
// error at load time instead of a half-rendered cell.
package notebooks

import (
	"encoding/json"
	"fmt"
)

// FormatVersion is the value we stamp into metadata.clavesa.format_version
// when writing notebooks; future migrations branch on it.
const FormatVersion = 1

// SupportedNbformat / SupportedNbformatMinor enforce the nbformat 4.5
// baseline. 4.5 is where stable cell IDs landed — older notebooks lack the
// `id` field and can't be loaded by this package without a migration step
// (intentionally out of scope; the recommendation is `jupyter nbformat
// update`).
const (
	SupportedNbformat      = 4
	SupportedNbformatMinor = 5
)

// KernelSpecName is what we stamp into metadata.kernelspec.name. Lets
// JupyterLab match the notebook against a registered kernel if the user has
// one installed (we don't ship one). DisplayName is the human-readable label.
const (
	KernelSpecName        = "clavesa-pyspark"
	KernelSpecDisplayName = "Clavesa (PySpark)"
)

// Cell types — nbformat allows "raw" too, but clavesa notebooks have no
// downstream tool that consumes raw cells, so we reject them on load.
const (
	CellTypeCode     = "code"
	CellTypeMarkdown = "markdown"
)

// Output types per nbformat.
const (
	OutputTypeStream        = "stream"
	OutputTypeError         = "error"
	OutputTypeExecuteResult = "execute_result"
	OutputTypeDisplayData   = "display_data"
)

// MIMEDataFrame is the custom MIME bundle entry the runner produces for
// Spark DataFrame results. Namespaced so other Jupyter clients can fall
// back to the `text/plain` repr we ship alongside it.
const MIMEDataFrame = "application/vnd.clavesa.dataframe+json"

// supportedMIMEs is the closed set the validator accepts inside an
// execute_result / display_data `data` bundle. Anything else (ipywidget
// views, custom JS, ...) is rejected on load.
var supportedMIMEs = map[string]struct{}{
	"text/plain":      {},
	"text/html":       {},
	"text/markdown":   {},
	"application/json": {},
	"image/png":       {},
	"image/svg+xml":   {},
	MIMEDataFrame:     {},
}

// CellStatus is the value the runner reports back per cell and we persist
// in cell.metadata.clavesa.last_status. Used for the per-cell badge.
const (
	CellStatusOK        = "ok"
	CellStatusError     = "error"
	CellStatusCancelled = "cancelled"
)

// Notebook is the in-memory shape of one .ipynb file.
type Notebook struct {
	// Name is the registry identifier (= filename without .ipynb). Set on
	// Load from the filename; not persisted (filename is authoritative).
	Name string `json:"-"`

	Nbformat      int              `json:"nbformat"`
	NbformatMinor int              `json:"nbformat_minor"`
	Metadata      NotebookMetadata `json:"metadata"`
	Cells         []Cell           `json:"cells"`
}

// NotebookMetadata is the top-level `metadata` object.
type NotebookMetadata struct {
	KernelSpec   KernelSpec   `json:"kernelspec"`
	LanguageInfo LanguageInfo `json:"language_info"`
	Clavesa      ClavesaMeta  `json:"clavesa"`
}

// KernelSpec identifies which kernel a Jupyter client should attach. Clavesa
// doesn't ship a real kernel — JupyterLab will fall back to "no kernel" and
// the user can still view cells + outputs.
type KernelSpec struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
}

// LanguageInfo gives editors syntax highlighting hints. We always emit
// `python` even though SQL cells exist — the magic line (`%%sql`) is what
// signals SQL to the runner, and Python is the language of every non-magic
// cell.
type LanguageInfo struct {
	Name string `json:"name"`
}

// ClavesaMeta is our extension to nbformat metadata.
type ClavesaMeta struct {
	FormatVersion int `json:"format_version"`
}

// Cell is one notebook cell. Code and markdown cells share this struct; code-
// only fields (ExecutionCount, Outputs) are omitted on markdown via omitempty
// + pointer for ExecutionCount.
type Cell struct {
	CellType string   `json:"cell_type"`
	ID       string   `json:"id"`
	Source   []string `json:"source"`
	Metadata CellMeta `json:"metadata"`

	// Code-cell-only. ExecutionCount is *int so JSON `null` (the
	// nbformat-correct value for "cell never ran or outputs were cleared")
	// round-trips; an explicit `0` would be misleading.
	ExecutionCount *int     `json:"execution_count,omitempty"`
	Outputs        []Output `json:"outputs,omitempty"`
}

// CellMeta wraps our per-cell metadata. The container exists so future
// nbformat-standard fields (`collapsed`, `scrolled`, `tags`) can land here
// without breaking the JSON shape.
type CellMeta struct {
	Clavesa *CellClavesaMeta `json:"clavesa,omitempty"`
}

// CellClavesaMeta is what the per-cell run status badge in the UI reads.
// Not the cell's outputs — those live in nbformat's own Outputs[] so
// JupyterLab + GitHub can render them.
type CellClavesaMeta struct {
	LastRunAt      string `json:"last_run_at,omitempty"`
	LastDurationMS int64  `json:"last_duration_ms,omitempty"`
	LastStatus     string `json:"last_status,omitempty"`
}

// Output is one entry in cell.outputs[]. nbformat models outputs as a
// discriminated union by `output_type`; we flatten to a single Go type with
// omitempty so the wire shape matches nbformat exactly per variant.
type Output struct {
	OutputType string `json:"output_type"`

	// `stream` variant: name = "stdout" | "stderr", text = lines.
	Name string   `json:"name,omitempty"`
	Text []string `json:"text,omitempty"`

	// `error` variant: ename/evalue/traceback.
	EName     string   `json:"ename,omitempty"`
	EValue    string   `json:"evalue,omitempty"`
	Traceback []string `json:"traceback,omitempty"`

	// `execute_result` / `display_data` variants. Data is a MIME bundle.
	// Metadata is per-output metadata (we set metadata.clavesa.truncated
	// on capped DataFrame outputs).
	ExecutionCount *int                   `json:"execution_count,omitempty"`
	Data           map[string]any         `json:"data,omitempty"`
	Metadata       map[string]any         `json:"metadata,omitempty"`
}

// Parse decodes a .ipynb byte payload into a Notebook and runs the
// supported-subset validator. The name argument is the registry identifier
// (typically the filename without .ipynb) and is set on the returned struct;
// no validation of the name is done here (the Store layer owns name rules).
func Parse(data []byte, name string) (*Notebook, error) {
	var nb Notebook
	if err := json.Unmarshal(data, &nb); err != nil {
		return nil, fmt.Errorf("parse notebook %q: %w", name, err)
	}
	nb.Name = name
	if err := nb.Validate(); err != nil {
		return nil, fmt.Errorf("validate notebook %q: %w", name, err)
	}
	return &nb, nil
}

// Marshal encodes a Notebook to .ipynb bytes with 1-space indent (matches
// what `jupyter nbconvert` and `nbformat.write` emit by default), making
// human-readable diffs.
func (nb *Notebook) Marshal() ([]byte, error) {
	if err := nb.Validate(); err != nil {
		return nil, fmt.Errorf("validate notebook %q: %w", nb.Name, err)
	}
	data, err := json.MarshalIndent(nb, "", " ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// Validate runs the supported-subset checks. Returns the first error it
// finds; callers don't typically need to see multiple at once because most
// notebooks are user-edited one cell at a time.
func (nb *Notebook) Validate() error {
	if nb.Nbformat != SupportedNbformat {
		return fmt.Errorf("unsupported nbformat %d (want %d)", nb.Nbformat, SupportedNbformat)
	}
	if nb.NbformatMinor < SupportedNbformatMinor {
		return fmt.Errorf(
			"unsupported nbformat_minor %d (want >=%d — cell IDs landed in 4.5)",
			nb.NbformatMinor, SupportedNbformatMinor,
		)
	}
	if nb.Metadata.Clavesa.FormatVersion != 0 && nb.Metadata.Clavesa.FormatVersion != FormatVersion {
		return fmt.Errorf(
			"unsupported clavesa format_version %d (this binary supports %d)",
			nb.Metadata.Clavesa.FormatVersion, FormatVersion,
		)
	}

	seenIDs := make(map[string]struct{}, len(nb.Cells))
	for i, c := range nb.Cells {
		if err := c.validate(i, seenIDs); err != nil {
			return err
		}
	}
	return nil
}

func (c Cell) validate(index int, seenIDs map[string]struct{}) error {
	if c.ID == "" {
		return fmt.Errorf("cells[%d]: id is required (nbformat 4.5)", index)
	}
	if _, dup := seenIDs[c.ID]; dup {
		return fmt.Errorf("cells[%d]: duplicate id %q", index, c.ID)
	}
	seenIDs[c.ID] = struct{}{}

	switch c.CellType {
	case CellTypeCode:
		for j, o := range c.Outputs {
			if err := o.validate(index, j); err != nil {
				return err
			}
		}
	case CellTypeMarkdown:
		if len(c.Outputs) > 0 {
			return fmt.Errorf("cells[%d]: markdown cell may not carry outputs", index)
		}
		if c.ExecutionCount != nil {
			return fmt.Errorf("cells[%d]: markdown cell may not carry execution_count", index)
		}
	default:
		return fmt.Errorf("cells[%d]: unsupported cell_type %q (allowed: code, markdown)", index, c.CellType)
	}
	return nil
}

func (o Output) validate(cellIndex, outputIndex int) error {
	switch o.OutputType {
	case OutputTypeStream:
		if o.Name != "stdout" && o.Name != "stderr" {
			return fmt.Errorf(
				"cells[%d].outputs[%d]: stream name %q (allowed: stdout, stderr)",
				cellIndex, outputIndex, o.Name,
			)
		}
	case OutputTypeError:
		// ename/evalue may be empty for some Python errors; don't enforce.
	case OutputTypeExecuteResult, OutputTypeDisplayData:
		for mime := range o.Data {
			if _, ok := supportedMIMEs[mime]; !ok {
				return fmt.Errorf(
					"cells[%d].outputs[%d]: unsupported MIME %q (clavesa renders %v)",
					cellIndex, outputIndex, mime, supportedMIMEKeys(),
				)
			}
		}
	default:
		return fmt.Errorf(
			"cells[%d].outputs[%d]: unsupported output_type %q",
			cellIndex, outputIndex, o.OutputType,
		)
	}
	return nil
}

func supportedMIMEKeys() []string {
	out := make([]string, 0, len(supportedMIMEs))
	for k := range supportedMIMEs {
		out = append(out, k)
	}
	return out
}

// NewEmpty returns a fresh notebook with no cells, ready to be persisted.
// Used by `clavesa notebook create` and the UI's "New notebook" action.
func NewEmpty(name string) *Notebook {
	return &Notebook{
		Name:          name,
		Nbformat:      SupportedNbformat,
		NbformatMinor: SupportedNbformatMinor,
		Metadata: NotebookMetadata{
			KernelSpec:   KernelSpec{Name: KernelSpecName, DisplayName: KernelSpecDisplayName},
			LanguageInfo: LanguageInfo{Name: "python"},
			Clavesa:      ClavesaMeta{FormatVersion: FormatVersion},
		},
		Cells: []Cell{},
	}
}
