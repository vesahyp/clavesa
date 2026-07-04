package hclparser

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/vesahyp/clavesa/internal/graph"
)

// moduleRefRE matches a literal module reference: module.<node>.outputs["<output>"]
// Capture groups: 1 = node name, 2 = output name.
var moduleRefRE = regexp.MustCompile(`module\.([A-Za-z0-9_-]+)\.outputs\["([^"]+)"\]`)

// clavesaSourceRE matches clavesa/<type>/<cloud>.
// Capture groups: 1 = type, 2 = cloud.
var clavesaSourceRE = regexp.MustCompile(`^clavesa/([^/]+)/([^/]+)$`)

// githubSourceRE matches github.com/<org>/<repo>//<path>/modules/<type>/<cloud>
// with an optional ?ref= query, e.g.:
//
//	github.com/vesahyp/clavesa//modules/source/aws?ref=v0.1.0
//
// Capture groups: 1 = type, 2 = cloud.
var githubSourceRE = regexp.MustCompile(`^github\.com/[^/]+/[^/]+//modules/([^/]+)/([^/?]+)`)

// localModuleSourceRE matches local relative paths ending in modules/<type>/<cloud>,
// e.g. ../../modules/source/aws used by fixture pipelines during local development.
// Capture groups: 1 = type, 2 = cloud.
var localModuleSourceRE = regexp.MustCompile(`(?:^|/)modules/([^/]+)/([^/]+)$`)

// embeddedModuleSourceRE matches the v0.30.0+ embedded-modules path form:
// `<prefix>/.clavesa/modules/v<X.Y.Z>/<type>/<cloud>` (and the v1.0.0+
// `.clavesa` equivalent — kept flexible so the same regex spans the rename).
// Capture groups: 1 = type, 2 = cloud.
var embeddedModuleSourceRE = regexp.MustCompile(`(?:^|/)\.[A-Za-z0-9_-]+/modules/v[^/]+/([^/]+)/([^/]+)$`)

// schemaTableRE matches an ADR-016 cross-pipeline / external-table
// reference: `<schema>.<table>` where each segment is a valid Glue
// identifier (`[A-Za-z_][A-Za-z0-9_]*`). The pipeline-`schema` always
// sanitizes to that shape (sanitize() folds dashes to underscores);
// table names follow the same `<node>__<key>` rule. Catalog is implicit
// — it's the workspace's own (single catalog per workspace, ADR-016).
//
// `isSchemaTableRef` is intentionally strict: it rejects anything with
// >1 dot, a leading digit, or characters outside the Glue charset,
// because the orchestration emitter and the lineage walker both treat
// matches as a guaranteed-resolvable Iceberg table reference.
var schemaTableRefRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$`)

func isSchemaTableRef(s string) bool {
	return schemaTableRefRE.MatchString(s)
}

// structuralAttrs are the module attributes that are NOT placed into config;
// they are consumed by the parser to build node identity, edges, and outputs.
//
// output_definitions used to live here, but the orchestration emitter needs
// per-output `mode` (replace|append) since v0.12. Easier to surface it via
// Config than to add a typed field to graph.Node — the emitter reads
// Config["output_definitions"] as map[string]any when present.
var structuralAttrs = map[string]bool{
	"name":          true,
	"source":        true,
	"input":         true,
	"inputs":        true,
	"pipeline_name": true,
}

// Parse reads all .tf files in the given directory and returns a PipelineGraph.
//
// Identification: Clavesa modules have source starting with "clavesa/".
// Non-Clavesa blocks are silently ignored.
// Edge extraction: only literal module.<name>.outputs["<output>"] references.
// config excludes: name, source, input, inputs, output_definitions.
func Parse(directory string) (graph.PipelineGraph, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return graph.PipelineGraph{}, fmt.Errorf("hclparser.Parse: read dir %q: %w", directory, err)
	}

	var tfFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tf") {
			// orchestration.tf is emitter-managed. As of v0.23.0 it also
			// materialises `module "src_<name>"` blocks for kind=s3
			// registered sources, so parsing them back here would
			// double-count them as authored source nodes alongside the
			// transform that attached them. Skip the file outright; the
			// `module "orchestration"` block was already a no-op for the
			// graph (nodeTypeFromSource bails on it).
			if e.Name() == "orchestration.tf" {
				continue
			}
			tfFiles = append(tfFiles, e.Name())
		}
	}

	var nodes []graph.Node
	var edges []graph.Edge

	for _, fname := range tfFiles {
		fpath := filepath.Join(directory, fname)
		data, err := os.ReadFile(fpath)
		if err != nil {
			return graph.PipelineGraph{}, fmt.Errorf("hclparser.Parse: read %q: %w", fpath, err)
		}

		fileNodes, fileEdges, err := parseFile(string(data))
		if err != nil {
			return graph.PipelineGraph{}, fmt.Errorf("hclparser.Parse: parse %q: %w", fname, err)
		}
		nodes = append(nodes, fileNodes...)
		edges = append(edges, fileEdges...)
	}

	// Resolve var.foo references using terraform.tfvars / *.auto.tfvars
	if tfvars, err := parseTFVars(directory); err == nil && len(tfvars) > 0 {
		for i := range nodes {
			for k, v := range nodes[i].Config {
				if s, ok := v.(string); ok && strings.HasPrefix(s, "var.") {
					if resolved, found := tfvars[s[4:]]; found {
						nodes[i].Config[k] = resolved
					}
				}
			}
		}
	}

	if nodes == nil {
		nodes = []graph.Node{}
	}
	if edges == nil {
		edges = []graph.Edge{}
	}

	g := graph.PipelineGraph{
		Pipeline: graph.PipelineMeta{
			Directory: directory,
			Files:     tfFiles,
		},
		Nodes: nodes,
		Edges: edges,
		Validation: graph.Validation{
			Errors:   []graph.ValidationMessage{},
			Warnings: []graph.ValidationMessage{},
		},
	}

	msgs := Validate(g)
	for _, m := range msgs {
		switch m.Code {
		case graph.CodeCycleDetected,
			graph.CodeDanglingReference,
			graph.CodeMissingRequiredConfig:
			g.Validation.Errors = append(g.Validation.Errors, m)
		default:
			g.Validation.Warnings = append(g.Validation.Warnings, m)
		}
	}

	return g, nil
}

// parseFile extracts Clavesa nodes and edges from a single .tf file content.
func parseFile(src string) ([]graph.Node, []graph.Edge, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, nil, err
	}

	p := &parser{toks: toks}
	return p.parse()
}

// parser is a recursive-descent parser over the token stream.
type parser struct {
	toks []token
	pos  int
}

// peek returns the next non-newline token without consuming it.
func (p *parser) peek() token {
	i := p.pos
	for i < len(p.toks) && p.toks[i].kind == tokNewline {
		i++
	}
	if i >= len(p.toks) {
		return token{kind: tokEOF}
	}
	return p.toks[i]
}

// next returns the next non-newline token and advances the position.
func (p *parser) next() token {
	for p.pos < len(p.toks) && p.toks[p.pos].kind == tokNewline {
		p.pos++
	}
	if p.pos >= len(p.toks) {
		return token{kind: tokEOF}
	}
	t := p.toks[p.pos]
	p.pos++
	return t
}

// rawPeek returns the next token without consuming it and without skipping newlines.
func (p *parser) rawPeek() token {
	if p.pos >= len(p.toks) {
		return token{kind: tokEOF}
	}
	return p.toks[p.pos]
}

// expect consumes the next token and returns an error if it doesn't match kind.
func (p *parser) expect(kind tokenKind) (token, error) {
	t := p.next()
	if t.kind != kind {
		return t, fmt.Errorf("expected token kind %d, got %d (%q) at line %d",
			kind, t.kind, t.value, t.line)
	}
	return t, nil
}

// parse processes the entire file and returns nodes + edges.
func (p *parser) parse() ([]graph.Node, []graph.Edge, error) {
	var nodes []graph.Node
	var edges []graph.Edge

	for p.peek().kind != tokEOF {
		t := p.peek()
		if t.kind != tokIdent {
			p.next() // skip unexpected tokens at top level
			continue
		}

		blockType := t.value
		if blockType == "module" {
			node, nodeEdges, err := p.parseModuleBlock()
			if err != nil {
				return nil, nil, err
			}
			if node != nil {
				nodes = append(nodes, *node)
				edges = append(edges, nodeEdges...)
			}
		} else {
			// Non-module block (terraform, provider, variable, locals, resource, etc.)
			// Consume the block and discard it.
			p.next() // consume block type ident
			if err := p.skipBlock(); err != nil {
				return nil, nil, err
			}
		}
	}
	return nodes, edges, nil
}

// parseModuleBlock parses a module "name" { ... } block.
// Returns nil node if the module is not a Clavesa module.
func (p *parser) parseModuleBlock() (*graph.Node, []graph.Edge, error) {
	p.next() // consume "module"

	// module label
	labelTok := p.next()
	if labelTok.kind != tokIdent && labelTok.kind != tokString {
		return nil, nil, fmt.Errorf("expected module label, got %q at line %d", labelTok.value, labelTok.line)
	}
	moduleName := labelTok.value

	// opening brace
	if _, err := p.expect(tokLBrace); err != nil {
		return nil, nil, fmt.Errorf("module %q: %w", moduleName, err)
	}

	// Parse all attributes and nested blocks into a raw map.
	attrs, err := p.parseBody()
	if err != nil {
		return nil, nil, fmt.Errorf("module %q: %w", moduleName, err)
	}

	// Check if this is a Clavesa module.
	sourceRaw, ok := attrs["source"]
	if !ok {
		return nil, nil, nil // no source attribute — skip
	}
	source, ok := sourceRaw.(string)
	if !ok {
		return nil, nil, nil // source is not a string literal — skip
	}
	// One predicate for all four recognised source forms (clavesa/,
	// github ?ref=, embedded, local) — keep this in sync with
	// nodeTypeFromSource by never inlining a subset here: the github
	// form was twice admitted only by localModuleSourceRE's tail
	// coincidence when this check drifted (2026-05-10 P2-5, 2026-07-02
	// session D P2-2).
	if !IsRecognisedModuleSource(source) {
		return nil, nil, nil // not a Clavesa module
	}

	// Determine node type from source pattern.
	nodeType := nodeTypeFromSource(source)
	switch nodeType {
	case "source", "transform", "destination":
		// known pipeline node type — continue parsing
	default:
		return nil, nil, nil // not a pipeline node (e.g. orchestration module)
	}

	// Build config (exclude structural attributes).
	//
	// Both scalar and complex (list/map) attributes are kept. Scalars are
	// what the UI form fields edit; complex attributes are what the
	// orchestration emitter inspects (partitions, output_definitions, etc.
	// per v0.12). The UI is responsible for filtering to scalars when
	// rendering forms — having extra entries in Config doesn't break it.
	config := make(map[string]interface{})
	for k, v := range attrs {
		if structuralAttrs[k] {
			continue
		}
		switch tv := v.(type) {
		case string, bool, int64, float64, []interface{}, map[string]interface{}:
			config[k] = tv
		}
	}

	// Extract edges from input attribute.
	var edges []graph.Edge
	if inputRaw, ok := attrs["input"]; ok {
		if inputStr, ok := inputRaw.(string); ok {
			// inputStr is the raw expression text like module.foo.outputs["bar"]
			if e, ok := parseModuleRef(inputStr, moduleName, "default"); ok {
				edges = append(edges, e)
			}
		}
	}

	// Extract edges from inputs map (multi-input).
	//
	// Per ADR-017 slice 1, an inputs entry can be a literal
	// `"sources.<name>"` string referencing a workspace-level source
	// registry entry. Per ADR-016 slice 2 (v0.20.x), it can also be a
	// `"<schema>.<table>"` reference to an Iceberg table produced by
	// another pipeline in the workspace (or an external Glue table in
	// the same catalog). Module references become edges; the other two
	// forms stay in Config under synthetic `source_inputs` /
	// `external_inputs` keys so the orchestration emitter, the
	// lineage walker, and the deletion-guard scan can resolve them at
	// sync time. Other unresolved expressions are silently dropped —
	// same behavior as before.
	if inputsRaw, ok := attrs["inputs"]; ok {
		if inputsMap, ok := inputsRaw.(map[string]interface{}); ok {
			sourceInputs := map[string]interface{}{}
			externalInputs := map[string]interface{}{}
			for inputKey, refRaw := range inputsMap {
				if refStr, ok := refRaw.(string); ok {
					if e, ok := parseModuleRef(refStr, moduleName, inputKey); ok {
						edges = append(edges, e)
						continue
					}
					if strings.HasPrefix(refStr, "sources.") {
						sourceInputs[inputKey] = refStr
						continue
					}
					if isSchemaTableRef(refStr) {
						externalInputs[inputKey] = refStr
					}
				}
			}
			if len(sourceInputs) > 0 {
				// v0.22.0: source_inputs can also appear as its own
				// typed HCL attribute (kind=s3 attachments). Merge —
				// typed entries win, so a legacy `inputs = "sources.X"`
				// string and a typed source_inputs[X] = {...} block
				// referencing the same alias resolve to the typed one.
				if existing, ok := config["source_inputs"].(map[string]interface{}); ok {
					for k, v := range sourceInputs {
						if _, present := existing[k]; !present {
							existing[k] = v
						}
					}
					config["source_inputs"] = existing
				} else {
					config["source_inputs"] = sourceInputs
				}
			}
			if len(externalInputs) > 0 {
				config["external_inputs"] = externalInputs
			}
		}
	}

	// Extract PreviewSQL for transform nodes from config["sql"].
	previewSQL := ""
	if s, ok := config["sql"].(string); ok {
		previewSQL = s
	}

	node := &graph.Node{
		ID:           moduleName,
		Type:         nodeType,
		ModuleSource: source,
		Config:       config,
		PreviewSQL:   previewSQL,
	}

	return node, edges, nil
}

// IsRecognisedModuleSource reports whether source matches one of the
// four forms the parser knows how to read.
func IsRecognisedModuleSource(source string) bool {
	return clavesaSourceRE.MatchString(source) ||
		githubSourceRE.MatchString(source) ||
		embeddedModuleSourceRE.MatchString(source) ||
		localModuleSourceRE.MatchString(source)
}

// nodeTypeFromSource returns "source", "transform", "destination", or "" from
// a source string like "clavesa/source/aws", "../../modules/source/aws",
// "github.com/vesahyp/clavesa//modules/source/aws?ref=v0.1.0", or the
// v0.30.0+ embedded form "../.clavesa/modules/v0.30.0/source/aws".
func nodeTypeFromSource(source string) string {
	if m := clavesaSourceRE.FindStringSubmatch(source); m != nil {
		return m[1]
	}
	if m := githubSourceRE.FindStringSubmatch(source); m != nil {
		return m[1]
	}
	if m := embeddedModuleSourceRE.FindStringSubmatch(source); m != nil {
		return m[1]
	}
	if m := localModuleSourceRE.FindStringSubmatch(source); m != nil {
		return m[1]
	}
	// Fallback: extract second path segment.
	parts := strings.SplitN(source, "/", 3)
	if len(parts) >= 2 {
		return parts[1]
	}
	return ""
}

// parseModuleRef tries to parse a module reference expression string and build
// an edge. toInput is the input key on the destination node.
func parseModuleRef(expr, toNode, toInput string) (graph.Edge, bool) {
	m := moduleRefRE.FindStringSubmatch(expr)
	if m == nil {
		return graph.Edge{}, false
	}
	return graph.Edge{
		FromNode:   m[1],
		ToNode:     toNode,
		ToInput:    toInput,
		FromOutput: m[2],
	}, true
}

// parseBody parses a block body (after the opening brace) until the matching
// closing brace. Returns a map of attribute name → value.
// Values are: string, bool, int64, float64, map[string]interface{}, []interface{},
// or a raw expression string (for references like module.foo.outputs["bar"]).
func (p *parser) parseBody() (map[string]interface{}, error) {
	attrs := make(map[string]interface{})
	for {
		t := p.peek()
		if t.kind == tokEOF {
			return nil, fmt.Errorf("unexpected EOF in block body")
		}
		if t.kind == tokRBrace {
			p.next() // consume }
			return attrs, nil
		}

		// Attribute or nested block.
		keyTok := p.next()
		if keyTok.kind != tokIdent && keyTok.kind != tokString {
			// Skip unexpected token.
			continue
		}
		key := keyTok.value

		next := p.peek()
		switch next.kind {
		case tokEquals:
			// Attribute assignment: key = value
			p.next() // consume =
			val, err := p.parseValue()
			if err != nil {
				return nil, fmt.Errorf("attribute %q: %w", key, err)
			}
			attrs[key] = val

		case tokIdent, tokString:
			// Nested block with additional label(s): e.g. "resource" "aws_s3_bucket" "name" { ... }
			// or just a block with no label. Collect labels then parse body.
			var labels []string
			for p.peek().kind == tokIdent || p.peek().kind == tokString {
				labels = append(labels, p.next().value)
			}
			if p.peek().kind == tokLBrace {
				p.next() // consume {
				nested, err := p.parseBody()
				if err != nil {
					return nil, err
				}
				// Store as nested map under combined key. For our purposes
				// (ignoring non-Clavesa blocks), we don't need to be precise.
				_ = labels
				_ = nested
			}

		case tokLBrace:
			// Nested block without label (e.g. lifecycle { ... })
			p.next() // consume {
			nested, err := p.parseBody()
			if err != nil {
				return nil, err
			}
			attrs[key] = nested

		default:
			// Skip unexpected content.
		}
	}
}

// parseValue parses a single HCL value (string, number, bool, null, object, list,
// or reference expression).
func (p *parser) parseValue() (interface{}, error) {
	t := p.peek()
	switch t.kind {
	case tokString:
		p.next()
		return t.value, nil

	case tokBool:
		p.next()
		return t.value == "true", nil

	case tokNull:
		p.next()
		return nil, nil

	case tokNumber:
		p.next()
		return parseNumber(t.value), nil

	case tokLBrace:
		p.next() // consume {
		return p.parseObjectValue()

	case tokLBrack:
		p.next() // consume [
		return p.parseListValue()

	case tokIdent:
		// Could be a reference expression (var.foo, module.foo.outputs["bar"], etc.)
		// Consume the entire expression as a raw string.
		return p.parseExpression()

	default:
		p.next()
		return nil, nil
	}
}

// parseObjectValue parses an HCL object value { key = val, ... } after the
// opening { has been consumed. Returns map[string]interface{}.
func (p *parser) parseObjectValue() (map[string]interface{}, error) {
	m := make(map[string]interface{})
	for {
		t := p.peek()
		if t.kind == tokRBrace || t.kind == tokEOF {
			if t.kind == tokRBrace {
				p.next()
			}
			return m, nil
		}
		if t.kind == tokComma {
			p.next()
			continue
		}

		// Key (ident or string).
		keyTok := p.next()
		if keyTok.kind != tokIdent && keyTok.kind != tokString {
			continue
		}
		key := keyTok.value

		// Optional = (HCL objects can use both = and no = for different styles,
		// but in module attribute objects we always have =).
		if p.peek().kind == tokEquals {
			p.next()
		}

		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		m[key] = val

		// Optional trailing comma.
		if p.peek().kind == tokComma {
			p.next()
		}
	}
}

// parseListValue parses an HCL list value [ val, ... ] after the opening [
// has been consumed. Returns []interface{}.
func (p *parser) parseListValue() ([]interface{}, error) {
	var items []interface{}
	for {
		t := p.peek()
		if t.kind == tokRBrack || t.kind == tokEOF {
			if t.kind == tokRBrack {
				p.next()
			}
			return items, nil
		}
		if t.kind == tokComma {
			p.next()
			continue
		}

		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		if val != nil {
			items = append(items, val)
		}

		if p.peek().kind == tokComma {
			p.next()
		}
	}
}

// parseExpression consumes an identifier-based expression (like var.foo or
// module.foo.outputs["bar"]) and returns its raw string form.
// It stops at newlines (using rawPeek) so it never crosses a line boundary.
func (p *parser) parseExpression() (interface{}, error) {
	var parts []string
	for {
		// Use rawPeek — expressions end at newlines; do not cross line boundaries.
		t := p.rawPeek()
		switch t.kind {
		case tokNewline, tokEOF:
			return strings.Join(parts, ""), nil
		case tokIdent:
			parts = append(parts, p.next().value)
		case tokDot:
			p.next()
			parts = append(parts, ".")
		case tokLBrack:
			p.next()
			// Could be outputs["name"]
			inner, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			if _, err2 := p.expect(tokRBrack); err2 != nil {
				// best-effort: ignore error
			}
			parts = append(parts, fmt.Sprintf("[%q]", inner))
		case tokLParen:
			// Function call — consume the whole call.
			p.next()
			for depth := 1; depth > 0; {
				nt := p.next()
				if nt.kind == tokLParen {
					depth++
				} else if nt.kind == tokRParen {
					depth--
				} else if nt.kind == tokEOF {
					break
				}
			}
			return strings.Join(parts, ""), nil
		default:
			return strings.Join(parts, ""), nil
		}
	}
}

// skipBlock consumes an entire block (including any labels and the body braces).
// Called when we encounter a non-module top-level block.
func (p *parser) skipBlock() error {
	// Consume any labels (ident or string tokens before the opening brace).
	for {
		t := p.peek()
		if t.kind == tokIdent || t.kind == tokString {
			p.next()
			continue
		}
		break
	}
	if p.peek().kind != tokLBrace {
		// No body — just skip to end of line.
		return nil
	}
	p.next() // consume {
	depth := 1
	for depth > 0 {
		t := p.next()
		switch t.kind {
		case tokLBrace:
			depth++
		case tokRBrace:
			depth--
		case tokEOF:
			return fmt.Errorf("unexpected EOF skipping block")
		}
	}
	return nil
}

// parseNumber attempts to parse a number string into int64 or float64.
func parseNumber(s string) interface{} {
	// Try int first.
	var i int64
	_, err := fmt.Sscanf(s, "%d", &i)
	if err == nil && fmt.Sprintf("%d", i) == s {
		return i
	}
	var f float64
	_, err = fmt.Sscanf(s, "%g", &f)
	if err == nil {
		return f
	}
	return s
}
