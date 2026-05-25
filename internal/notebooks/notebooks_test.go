package notebooks

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStoreCreateListGetDelete(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)

	if got, err := st.List(); err != nil || len(got) != 0 {
		t.Fatalf("empty workspace List = %v, %v; want []", got, err)
	}

	nb, err := st.Create("exploration")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if nb.Name != "exploration" {
		t.Errorf("Created notebook Name = %q; want %q", nb.Name, "exploration")
	}
	if got := nb.Nbformat; got != SupportedNbformat {
		t.Errorf("Nbformat = %d; want %d", got, SupportedNbformat)
	}
	if got := nb.NbformatMinor; got != SupportedNbformatMinor {
		t.Errorf("NbformatMinor = %d; want %d", got, SupportedNbformatMinor)
	}

	path := filepath.Join(ws, RelDir, "exploration.ipynb")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s: %v", path, err)
	}

	// Second Create with same name → error.
	if _, err := st.Create("exploration"); err == nil {
		t.Error("Create duplicate: want error, got nil")
	}

	list, err := st.List()
	if err != nil {
		t.Fatalf("List after create: %v", err)
	}
	if len(list) != 1 || list[0].Name != "exploration" || list[0].CellCount != 0 {
		t.Errorf("List = %+v; want one entry name=exploration cells=0", list)
	}

	got, err := st.Get("exploration")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "exploration" {
		t.Errorf("Get returned name %q", got.Name)
	}

	if err := st.Delete("exploration"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get("exploration"); !os.IsNotExist(err) {
		t.Errorf("Get after Delete: want os.ErrNotExist, got %v", err)
	}
}

func TestStoreSaveRoundtrip(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)

	nb, err := st.Create("rt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	ec := 1
	nb.Cells = []Cell{
		{
			CellType: CellTypeCode,
			ID:       "01HXZ_sql",
			Source:   []string{"%%sql\n", "SELECT 1"},
			Metadata: CellMeta{Clavesa: &CellClavesaMeta{
				LastRunAt:      "2026-05-24T17:00:00Z",
				LastDurationMS: 412,
				LastStatus:     CellStatusOK,
			}},
			ExecutionCount: &ec,
			Outputs: []Output{{
				OutputType:     OutputTypeExecuteResult,
				ExecutionCount: &ec,
				Data: map[string]any{
					"text/plain":  "1",
					MIMEDataFrame: map[string]any{"columns": []any{"n"}, "rows": []any{[]any{1.0}}},
				},
				Metadata: map[string]any{"clavesa": map[string]any{"truncated": false}},
			}},
		},
		{
			CellType: CellTypeMarkdown,
			ID:       "01HY0_md",
			Source:   []string{"# Title\n", "Some text"},
			Metadata: CellMeta{},
		},
	}
	if err := st.Save(nb); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := st.Get("rt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Cells) != 2 {
		t.Fatalf("Cells length = %d; want 2", len(got.Cells))
	}
	if got.Cells[0].Source[0] != "%%sql\n" {
		t.Errorf("source[0][0] = %q; want %%sql header", got.Cells[0].Source[0])
	}
	if got.Cells[0].Metadata.Clavesa == nil || got.Cells[0].Metadata.Clavesa.LastStatus != CellStatusOK {
		t.Errorf("LastStatus lost on roundtrip: %+v", got.Cells[0].Metadata.Clavesa)
	}
	if got.Cells[1].CellType != CellTypeMarkdown {
		t.Errorf("cell[1].CellType = %q; want markdown", got.Cells[1].CellType)
	}
	if got.Cells[1].ExecutionCount != nil {
		t.Errorf("markdown cell has execution_count: %v", got.Cells[1].ExecutionCount)
	}
}

func TestValidateRejectsBadFormatVersion(t *testing.T) {
	t.Parallel()
	cases := map[string]Notebook{
		"old nbformat 3": {Nbformat: 3, NbformatMinor: 5},
		"older minor 4":  {Nbformat: 4, NbformatMinor: 4},
	}
	for name, nb := range cases {
		t.Run(name, func(t *testing.T) {
			if err := nb.Validate(); err == nil {
				t.Errorf("Validate(%s): want error, got nil", name)
			}
		})
	}
}

func TestValidateRejectsUnsupportedMIME(t *testing.T) {
	t.Parallel()
	ec := 1
	nb := NewEmpty("u")
	nb.Cells = []Cell{{
		CellType:       CellTypeCode,
		ID:             "c",
		Source:         []string{"x"},
		ExecutionCount: &ec,
		Outputs: []Output{{
			OutputType: OutputTypeDisplayData,
			Data: map[string]any{
				"application/vnd.jupyter.widget-view+json": map[string]any{},
			},
		}},
	}}
	err := nb.Validate()
	if err == nil || !strings.Contains(err.Error(), "unsupported MIME") {
		t.Errorf("Validate ipywidget: want unsupported MIME error, got %v", err)
	}
}

func TestValidateRejectsDuplicateCellID(t *testing.T) {
	t.Parallel()
	nb := NewEmpty("d")
	nb.Cells = []Cell{
		{CellType: CellTypeMarkdown, ID: "same", Source: []string{"a"}},
		{CellType: CellTypeMarkdown, ID: "same", Source: []string{"b"}},
	}
	err := nb.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate id") {
		t.Errorf("Validate dup id: want duplicate id error, got %v", err)
	}
}

func TestValidateRejectsMarkdownWithOutputs(t *testing.T) {
	t.Parallel()
	nb := NewEmpty("m")
	nb.Cells = []Cell{{
		CellType: CellTypeMarkdown,
		ID:       "m1",
		Source:   []string{"# H"},
		Outputs:  []Output{{OutputType: OutputTypeStream, Name: "stdout", Text: []string{"x"}}},
	}}
	err := nb.Validate()
	if err == nil || !strings.Contains(err.Error(), "markdown") {
		t.Errorf("Validate markdown+output: want error mentioning markdown, got %v", err)
	}
}

func TestParseAcceptsValidJupyterFile(t *testing.T) {
	t.Parallel()
	// Smallest valid Jupyter 4.5 notebook (no cells, no kernelspec —
	// kernelspec is a soft requirement, our parser doesn't enforce its
	// presence so notebooks edited by hand without one still load).
	raw := []byte(`{
  "nbformat": 4,
  "nbformat_minor": 5,
  "metadata": {"kernelspec": {"name": "clavesa-pyspark", "display_name": "Clavesa (PySpark)"}, "language_info": {"name": "python"}, "clavesa": {"format_version": 1}},
  "cells": []
}`)
	nb, err := Parse(raw, "tiny")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if nb.Name != "tiny" {
		t.Errorf("Name = %q; want tiny", nb.Name)
	}
}

func TestClearOutputs(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	nb, err := st.Create("co")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	ec := 7
	nb.Cells = []Cell{{
		CellType:       CellTypeCode,
		ID:             "c1",
		Source:         []string{"SELECT 1"},
		ExecutionCount: &ec,
		Metadata:       CellMeta{Clavesa: &CellClavesaMeta{LastStatus: CellStatusOK, LastDurationMS: 12}},
		Outputs:        []Output{{OutputType: OutputTypeStream, Name: "stdout", Text: []string{"hi"}}},
	}}
	if err := st.Save(nb); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cleared, err := st.ClearOutputs("co")
	if err != nil {
		t.Fatalf("ClearOutputs: %v", err)
	}
	if len(cleared.Cells[0].Outputs) != 0 {
		t.Errorf("Outputs not cleared: %+v", cleared.Cells[0].Outputs)
	}
	if cleared.Cells[0].ExecutionCount != nil {
		t.Errorf("ExecutionCount not cleared: %v", cleared.Cells[0].ExecutionCount)
	}
	if cleared.Cells[0].Metadata.Clavesa != nil {
		t.Errorf("Clavesa metadata not cleared: %+v", cleared.Cells[0].Metadata.Clavesa)
	}

	// Re-read from disk — confirm the cleared state was persisted.
	reread, err := st.Get("co")
	if err != nil {
		t.Fatalf("Get after ClearOutputs: %v", err)
	}
	if len(reread.Cells[0].Outputs) != 0 || reread.Cells[0].ExecutionCount != nil {
		t.Errorf("Re-read shows outputs not cleared: %+v", reread.Cells[0])
	}
}

func TestStoreNameRules(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	bad := []string{"", "1numfirst", "Has-Caps", "with space", "with/slash", "with..dots", strings.Repeat("a", 65)}
	for _, name := range bad {
		if _, err := st.Create(name); err == nil {
			t.Errorf("Create(%q): want validation error, got nil", name)
		}
	}
}

// TestMarshalIsValidJSON guards against a regression where omitempty + zero
// values produce malformed nbformat that JupyterLab would refuse to open.
func TestMarshalIsValidJSON(t *testing.T) {
	t.Parallel()
	nb := NewEmpty("v")
	data, err := nb.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var generic map[string]any
	if err := json.Unmarshal(data, &generic); err != nil {
		t.Fatalf("re-parse: %v\n%s", err, string(data))
	}
	if generic["nbformat"] != float64(4) {
		t.Errorf("nbformat in JSON = %v; want 4", generic["nbformat"])
	}
}
