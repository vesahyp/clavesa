package bundle

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Cell is one cell of a Databricks notebook.
type Cell struct {
	// Lang is the cell's language: python, sql, scala, r, md, or "magic"
	// for a line magic cell (%run, %pip, %sh, %fs, ...).
	Lang string
	// Magic is the magic the cell starts with, without the %: "sql", "md",
	// "run", "pip", "sh", ... Empty for a cell in the notebook's own
	// language with no magic.
	Magic string
	// Args is the rest of the magic line (for %run: the path; for %pip:
	// the arguments).
	Args string
	// Body is the cell's code with the magic line and the "# MAGIC "
	// prefixes removed.
	Body string
	// Line is the line of the cell's first body line in the file (1-based).
	Line int
	// MagicLine is the line of the magic itself.
	MagicLine int
}

// Notebook is a parsed Databricks notebook.
type Notebook struct {
	Path     string // bundle-root-relative
	Language string // python or sql: the notebook's default language
	Cells    []Cell
}

const (
	pySourceHeader  = "# Databricks notebook source"
	sqlSourceHeader = "-- Databricks notebook source"
)

// ParseNotebook reads a notebook in one of the three formats a bundle
// holds: a .py source file, a .sql source file, or .ipynb. A .py file with
// no Databricks header is one Python cell (a plain file).
func ParseNotebook(root, rel string) (*Notebook, error) {
	b, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(filepath.Ext(rel)) {
	case ".ipynb":
		return parseIpynb(rel, b)
	case ".sql":
		return parseSource(rel, string(b), "sql", "--"), nil
	case ".py":
		return parseSource(rel, string(b), "python", "#"), nil
	case ".scala", ".r":
		return &Notebook{Path: rel, Language: strings.TrimPrefix(strings.ToLower(filepath.Ext(rel)), ".")}, nil
	}
	return nil, fmt.Errorf("%s: not a notebook (.py, .sql or .ipynb)", rel)
}

// parseSource reads the source format: cells split at "<c> COMMAND ----------",
// magic lines written as "<c> MAGIC %sql".
func parseSource(rel, src, lang, c string) *Notebook {
	nb := &Notebook{Path: rel, Language: lang}
	lines := strings.Split(strings.ReplaceAll(src, "\r\n", "\n"), "\n")
	start := 0
	if len(lines) > 0 && strings.TrimSpace(lines[0]) == c+" Databricks notebook source" {
		start = 1
	}
	sep := c + " COMMAND ----------"
	magicPrefix := c + " MAGIC"
	var cur []string
	curStart := start + 1
	flush := func() {
		nb.Cells = append(nb.Cells, makeCell(lang, cur, curStart, magicPrefix))
		cur = nil
	}
	for i := start; i < len(lines); i++ {
		if strings.TrimRight(lines[i], " ") == sep {
			flush()
			curStart = i + 2
			continue
		}
		cur = append(cur, lines[i])
	}
	flush()
	// Drop cells that are empty.
	out := nb.Cells[:0]
	for _, cell := range nb.Cells {
		if strings.TrimSpace(cell.Body) != "" || cell.Magic != "" {
			out = append(out, cell)
		}
	}
	nb.Cells = out
	return nb
}

// makeCell builds a cell from its lines; first is the file line of lines[0].
func makeCell(lang string, lines []string, first int, magicPrefix string) Cell {
	// Skip leading blank lines.
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
		first++
	}
	allMagic := len(lines) > 0
	for _, l := range lines {
		if strings.TrimSpace(l) != "" && !strings.HasPrefix(l, magicPrefix) {
			allMagic = false
		}
	}
	if !allMagic {
		return Cell{Lang: lang, Body: strings.TrimRight(strings.Join(lines, "\n"), "\n "), Line: first}
	}
	body := make([]string, len(lines))
	for i, l := range lines {
		l = strings.TrimPrefix(l, magicPrefix)
		body[i] = strings.TrimPrefix(l, " ")
	}
	return magicCell(lang, body, first)
}

// magicCell reads a cell whose first line is a %magic.
func magicCell(lang string, lines []string, first int) Cell {
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
		first++
	}
	if len(lines) == 0 || !strings.HasPrefix(strings.TrimSpace(lines[0]), "%") {
		return Cell{Lang: lang, Body: strings.Join(lines, "\n"), Line: first}
	}
	head := strings.TrimSpace(lines[0])
	name, args, _ := strings.Cut(strings.TrimPrefix(head, "%"), " ")
	name = strings.TrimPrefix(name, "%") // %%sql (ipynb style)
	cell := Cell{Magic: name, Args: strings.TrimSpace(args), MagicLine: first, Line: first + 1,
		Body: strings.TrimRight(strings.Join(lines[1:], "\n"), "\n ")}
	switch name {
	case "sql", "python", "scala", "r", "md":
		cell.Lang = name
		// "%sql SELECT 1" on one line
		if cell.Args != "" {
			cell.Body = strings.TrimSpace(cell.Args + "\n" + cell.Body)
			cell.Line = first
		}
	case "md-sandbox":
		cell.Lang = "md"
	default:
		cell.Lang = "magic"
	}
	return cell
}

func parseIpynb(rel string, b []byte) (*Notebook, error) {
	var doc struct {
		Metadata struct {
			Language string `json:"language"`
			Kernel   struct {
				Language string `json:"language"`
			} `json:"kernelspec"`
			DBX struct {
				Language string `json:"language"`
			} `json:"application/vnd.databricks.v1+notebook"`
		} `json:"metadata"`
		Cells []struct {
			CellType string          `json:"cell_type"`
			Source   json.RawMessage `json:"source"`
		} `json:"cells"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", rel, err)
	}
	lang := strings.ToLower(doc.Metadata.DBX.Language)
	if lang == "" {
		lang = "python"
	}
	nb := &Notebook{Path: rel, Language: lang}
	for i, c := range doc.Cells {
		var src string
		var parts []string
		if err := json.Unmarshal(c.Source, &parts); err == nil {
			src = strings.Join(parts, "")
		} else {
			_ = json.Unmarshal(c.Source, &src)
		}
		if c.CellType == "markdown" {
			nb.Cells = append(nb.Cells, Cell{Lang: "md", Magic: "md", Body: src, Line: i + 1})
			continue
		}
		if c.CellType != "code" || strings.TrimSpace(src) == "" {
			continue
		}
		// An ipynb has no file lines; Line is the cell number.
		cell := magicCell(lang, strings.Split(src, "\n"), i+1)
		cell.Line, cell.MagicLine = i+1, i+1
		nb.Cells = append(nb.Cells, cell)
	}
	return nb, nil
}

// RunTarget resolves a %run argument against the notebook's folder,
// putting back the notebook's file extension.
func RunTarget(root, notebookRel, arg string) (string, error) {
	arg = strings.Trim(strings.TrimSpace(arg), `"'`)
	if arg == "" {
		return "", fmt.Errorf("%%run without a path")
	}
	if strings.Contains(arg, "$") || strings.Contains(arg, "{") {
		return "", fmt.Errorf("%%run %s: the path must be static", arg)
	}
	if strings.HasPrefix(arg, "/") {
		return "", fmt.Errorf("%%run %s: a workspace path outside the bundle; use a path relative to the notebook", arg)
	}
	rel := path.Clean(path.Join(path.Dir(notebookRel), arg))
	if strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("%%run %s: the notebook is outside the bundle", arg)
	}
	found := notebookFile(root, rel)
	if _, err := os.Stat(filepath.Join(root, found)); err != nil {
		return "", fmt.Errorf("%%run %s: no notebook %s in the bundle", arg, rel)
	}
	return found, nil
}
