package service

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/fileops"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/hclutil"
	"github.com/vesahyp/clavesa/internal/sources"
)

// SourceSpec mirrors sources.Spec at the service boundary so callers (CLI,
// HTTP, UI) don't need to import the storage package directly.
type SourceSpec = sources.Spec

// SourceUsage is one entry in the deletion-guard scan: a pipeline that
// references the source by name from one or more transform input keys.
type SourceUsage struct {
	PipelineDir string   `json:"pipeline_dir"`
	NodeIDs     []string `json:"node_ids"`
}

// notRegisteredError keeps the CLI/UI message terse ("source X not
// registered") while still satisfying errors.Is(err, os.ErrNotExist) so
// the HTTP layer can dispatch 404. Plain fmt.Errorf("...%w", os.ErrNotExist)
// leaks the wrapped "file does not exist" text into user-visible
// messages, which is both noisy and a small workspace-internal leak.
type notRegisteredError struct {
	kind string // "source" or "credential"
	name string
}

func (e *notRegisteredError) Error() string {
	return fmt.Sprintf("%s %q not registered", e.kind, e.name)
}

// Is satisfies errors.Is(err, os.ErrNotExist) so callers (HTTP handlers,
// the CLI, sibling service methods) can dispatch the same way they do
// against a raw not-found.
func (e *notRegisteredError) Is(target error) bool {
	return target == os.ErrNotExist
}

// ErrSourceInUse is returned by DeleteSource when one or more pipelines
// still reference the source. The Usages slice names them so callers can
// surface "used by N pipelines" inline.
type ErrSourceInUse struct {
	Name   string
	Usages []SourceUsage
}

// InUseUsages returns the structured usage list. Defined as a method so
// the api package can pull the list through an interface without importing
// internal/service.
func (e *ErrSourceInUse) InUseUsages() []SourceUsage { return e.Usages }

func (e *ErrSourceInUse) Error() string {
	parts := make([]string, 0, len(e.Usages))
	for _, u := range e.Usages {
		parts = append(parts, fmt.Sprintf("%s (%s)", u.PipelineDir, strings.Join(u.NodeIDs, ", ")))
	}
	return fmt.Sprintf("source %q is in use by: %s", e.Name, strings.Join(parts, "; "))
}

// sourceStore returns the workspace-rooted storage instance. Sources live
// at the workspace level (ADR-017), not per-pipeline.
func (s *Service) sourceStore() *sources.Store {
	return sources.New(s.workspace)
}

// AddSource registers a new source in the workspace registry. Returns the
// stored spec — useful for callers that pass empty optional fields and
// want to see what was inferred.
//
// `s3://bucket/key` URLs in the URL field auto-promote to kind=s3 with
// bucket+prefix derived. This is the slice 3 shorthand the CLI's
// `source register --from s3://...` rides on, so users don't have to
// hand-split the URL when registering.
func (s *Service) AddSource(spec SourceSpec) (SourceSpec, error) {
	spec, err := s.normalizeSourceSpec(spec)
	if err != nil {
		return SourceSpec{}, err
	}
	if err := s.sourceStore().Add(spec); err != nil {
		return SourceSpec{}, err
	}
	out, err := s.sourceStore().Get(spec.Name)
	if err != nil {
		return SourceSpec{}, fmt.Errorf("read back source: %w", err)
	}
	return out, nil
}

// UpdateSource overwrites an existing source's spec. The name is fixed —
// it is the registry key and pipelines reference the source by it, so a
// rename is a delete + re-register, not an edit. Applies the same URL
// inference and credential validation as AddSource.
//
// Note: editing a kind=s3 source does not re-sync pipelines it is already
// attached to — the typed `source_inputs` payload is written at attach
// time. Re-run `source attach` to propagate. kind=http edits flow through
// automatically (the `sources.<name>` sentinel resolves at run time).
func (s *Service) UpdateSource(name string, spec SourceSpec) (SourceSpec, error) {
	if _, err := s.sourceStore().Get(name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return SourceSpec{}, &notRegisteredError{kind: "source", name: name}
		}
		return SourceSpec{}, err
	}
	spec.Name = name
	spec, err := s.normalizeSourceSpec(spec)
	if err != nil {
		return SourceSpec{}, err
	}
	if err := s.sourceStore().Update(spec); err != nil {
		return SourceSpec{}, err
	}
	out, err := s.sourceStore().Get(name)
	if err != nil {
		return SourceSpec{}, fmt.Errorf("read back source: %w", err)
	}
	return out, nil
}

// normalizeSourceSpec applies the register-time inference and validation
// shared by AddSource and UpdateSource: `s3://` and `http(s)://` URL
// promotion, format inference from the URL/key filename, and
// credential-reference validation against the registry.
func (s *Service) normalizeSourceSpec(spec SourceSpec) (SourceSpec, error) {
	// s3:// URL shortcut — derive kind/bucket/prefix/format from the
	// URL when the caller didn't fill those in explicitly. Bucket and
	// prefix derivation is greedy: everything up to the last "/" is
	// the prefix, the basename feeds format inference. Whole-key
	// references (e.g. s3://bucket/path/file.parquet) become a prefix
	// of `path/file.parquet/` after normalization, which Spark reads
	// as a single-file dataset just fine.
	if strings.HasPrefix(spec.URL, "s3://") {
		bucket, key, _ := strings.Cut(spec.URL[len("s3://"):], "/")
		// An s3:// URL always means kind=s3 — overwrite any
		// caller-provided "http" default (the UI used to do this
		// before the slice 3 fix; left as belt-and-braces). Kind=""
		// gets the same answer.
		spec.Kind = "s3"
		if spec.Bucket == "" {
			spec.Bucket = bucket
		}
		if spec.Prefix == "" {
			spec.Prefix = key
		}
		if spec.Format == "" && key != "" {
			spec.Format = inferFormatFromFilename(filepath.Base(key))
		}
		// URL stops mattering once kind=s3 derivation is done.
		spec.URL = ""
	}
	// Same inference for http(s):// URLs — the CLI used to do this
	// before delegating, the UI registerSource has no way to know
	// kind a priori. Lifts the http/https-→-kind=http rule into
	// service.AddSource so both surfaces (ADR-015) get identical
	// behavior from `{name, url}` input alone.
	if spec.Kind == "" && (strings.HasPrefix(spec.URL, "http://") || strings.HasPrefix(spec.URL, "https://")) {
		spec.Kind = "http"
	}
	// Infer format from URL filename when caller left it blank — same
	// inference the legacy `node add --from` path used so users get the
	// same feel from `source register --from`.
	if spec.Kind == "http" && spec.Format == "" {
		spec.Format = inferFormatFromFilename(filepath.Base(stripURLPath(spec.URL)))
	}
	// Slice 2: a `--credentials <name>` reference must resolve in the
	// registry at register time. Without this the user discovers their
	// typo at `pipeline run` instead of at `source register`.
	if spec.Credentials != "" {
		if _, err := s.GetCredential(spec.Credentials); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return SourceSpec{}, fmt.Errorf("%w: %q (try `clavesa credential register`)", ErrCredentialNotFound, spec.Credentials)
			}
			return SourceSpec{}, fmt.Errorf("validate credential reference: %w", err)
		}
	}
	return spec, nil
}

// ListSources returns every registered source, sorted by name.
func (s *Service) ListSources() ([]SourceSpec, error) {
	return s.sourceStore().List()
}

// GetSource reads one source by name.
func (s *Service) GetSource(name string) (SourceSpec, error) {
	return s.sourceStore().Get(name)
}

// DeleteSource removes a source after a pipeline-scan deletion guard.
// `force` skips the guard — used by scripted teardown (`source delete --force`).
//
// The guard scans every pipeline directory in the workspace for
// `"sources.<name>"` references in any transform's `inputs` block. It uses
// the HCL parser, not a substring grep, so a comment mentioning the source
// name doesn't count as a reference.
//
// A delete of an unregistered name returns a wrapped os.ErrNotExist —
// CLI / HTTP layers translate it to "source X not registered" / 404 so
// the raw filesystem path doesn't leak.
func (s *Service) DeleteSource(name string, force bool) error {
	if _, err := s.GetSource(name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &notRegisteredError{kind: "source", name: name}
		}
		return err
	}
	if !force {
		usages, err := s.findSourceUsages(name)
		if err != nil {
			return fmt.Errorf("scan pipelines for source usage: %w", err)
		}
		if len(usages) > 0 {
			return &ErrSourceInUse{Name: name, Usages: usages}
		}
	}
	return s.sourceStore().Delete(name)
}

// AttachSource wires a registered workspace source into a transform's
// inputs map. kind=s3 attachments go into the new `source_inputs` HCL
// attribute (ADR-017 v0.22.0) — typed against `var.source_inputs` on
// the transform module so `terraform plan` validates and the module's
// IAM scope sees the source bucket. kind=http attachments keep the
// legacy `inputs = { x = "sources.<name>" }` sentinel — http sources
// have no bucket and don't grant IAM scope, and the orchestration
// emit resolves URL + credentials at SFN execution time.
//
// Existing kind=s3 entries get refreshed from the registry on every
// AttachSource call, so renaming a bucket / changing partitions / etc.
// flows into the pipeline .tf without an explicit resync command.
func (s *Service) AttachSource(dir, name, toNode, alias string) error {
	if alias == "" {
		alias = name
	}
	if _, err := s.GetSource(name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("source %q is not registered (try `clavesa source register`)", name)
		}
		return err
	}
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return fmt.Errorf("parse pipeline: %w", err)
	}
	var node *graph.Node
	for i := range g.Nodes {
		if g.Nodes[i].ID == toNode {
			n := g.Nodes[i]
			node = &n
			if n.Type != "transform" {
				return fmt.Errorf("node %q is %s; sources can only attach to transforms", toNode, n.Type)
			}
			break
		}
	}
	if node == nil {
		return fmt.Errorf("node %q not found in %s", toNode, dir)
	}
	file, err := hclutil.FindNodeFile(s.fo, abs, toNode)
	if err != nil {
		return fmt.Errorf("locate node file: %w", err)
	}

	// Reconstruct the four attribute payloads we'll write back:
	//   inputs        — module-reference edges (transform→transform) + legacy
	//                   kind=http source sentinels.
	//   source_inputs — kind=s3 resolved descriptors (typed v0.22.0 shape).
	// Existing source attachments live under Config["source_inputs"] (the
	// parser surfaces them there whether they were authored as a typed
	// source_inputs HCL block or as legacy `inputs = "sources.X"` strings).
	inputs := map[string]interface{}{}
	sourceInputs := map[string]interface{}{}

	// 1) Edges: transform→transform → `inputs`.
	for _, e := range g.Edges {
		if e.ToNode != toNode {
			continue
		}
		key := e.ToInput
		if key == "" {
			key = "default"
		}
		inputs[key] = fileops.ModuleReference{
			Type:       "reference",
			Expression: fmt.Sprintf(`module.%s.outputs["default"]`, e.FromNode),
		}
	}

	// 2) Already-attached sources: re-resolve from the registry so a
	// rename / partition-change flows through on the next attach.
	if existing, ok := node.Config["source_inputs"].(map[string]interface{}); ok {
		for k, v := range existing {
			specName, _ := sourceInputSpecName(v)
			if specName == "" {
				continue
			}
			resolvedKind, resolved, sentinel, err := s.resolveSourceForAttach(specName)
			if err != nil {
				return fmt.Errorf("refresh attached source %q: %w", specName, err)
			}
			switch resolvedKind {
			case "s3":
				sourceInputs[k] = resolved
			default:
				inputs[k] = sentinel
			}
		}
	}

	// 3) Add (or overwrite) the new attachment.
	newKind, resolved, sentinel, err := s.resolveSourceForAttach(name)
	if err != nil {
		return fmt.Errorf("resolve source %q: %w", name, err)
	}
	switch newKind {
	case "s3":
		sourceInputs[alias] = resolved
	default:
		inputs[alias] = sentinel
	}

	attrs := map[string]fileops.AttributeValue{}
	if len(inputs) > 0 {
		attrs["inputs"] = inputs
	}
	// Always write source_inputs (even empty) when it was previously
	// non-empty, so removing the last s3 source actually clears the
	// block. Today AttachSource never removes — keep emit conditional
	// on len > 0; DetachSource (when it lands) will own clearing.
	if len(sourceInputs) > 0 {
		attrs["source_inputs"] = sourceInputs
	}
	if _, err := s.fo.UpdateBlock(file, "module."+toNode, attrs); err != nil {
		return fmt.Errorf("update node: %w", err)
	}
	if err := s.SyncOrchestration(dir, ""); err != nil {
		return fmt.Errorf("sync orchestration: %w", err)
	}
	return nil
}

// sourceInputSpecName extracts the registry name from either the
// resolved-object form ({spec_name = "X", bucket = …}) or the legacy
// string sentinel form ("sources.X"). Returns "" when neither matches.
func sourceInputSpecName(v interface{}) (string, bool) {
	switch val := v.(type) {
	case string:
		if strings.HasPrefix(val, "sources.") {
			return strings.TrimPrefix(val, "sources."), true
		}
		return "", false
	case map[string]interface{}:
		if sn, ok := val["spec_name"].(string); ok && sn != "" {
			return sn, true
		}
	}
	return "", false
}

// resolveSourceForAttach returns the kind plus the appropriate
// HCL-attribute payload for a registered source:
//   - kind=s3   → typed object for the `source_inputs` map.
//   - kind=http → legacy "sources.<name>" sentinel for the `inputs` map.
//
// One function so AttachSource handles both kinds with a single switch
// instead of two parallel resolution loops.
func (s *Service) resolveSourceForAttach(name string) (kind string, resolved map[string]interface{}, sentinel string, err error) {
	spec, e := s.GetSource(name)
	if e != nil {
		if errors.Is(e, os.ErrNotExist) {
			return "", nil, "", fmt.Errorf("source %q is not registered", name)
		}
		return "", nil, "", e
	}
	switch spec.Kind {
	case "s3":
		desc := map[string]interface{}{
			"spec_name": spec.Name,
			"bucket":    spec.Bucket,
			"prefix":    spec.Prefix,
			"format":    spec.Format,
		}
		if len(spec.Partitions) > 0 {
			parts := make([]interface{}, len(spec.Partitions))
			for i, p := range spec.Partitions {
				parts[i] = p
			}
			desc["partitions"] = parts
		}
		if spec.StartFrom != "" {
			desc["start_from"] = spec.StartFrom
		}
		return "s3", desc, "", nil
	case "http":
		return "http", nil, "sources." + spec.Name, nil
	default:
		return "", nil, "", fmt.Errorf("source %q kind %q not supported", name, spec.Kind)
	}
}

// AttachExternalTable writes `inputs = { <alias> = "<schema>.<table>" }`
// into the named transform's HCL block — the ADR-016 slice 2
// cross-pipeline / external-Glue-table authoring path. The string is a
// sentinel the orchestration emitter resolves at sync time (mirrors
// the `"sources.<name>"` pattern from ADR-017 slice 1).
//
// Validation here is intentionally light: we confirm the ref is the
// `<schema>.<table>` shape and that the target node is a transform.
// Whether the producing pipeline actually exists in the workspace is
// the lineage walker's concern — unresolved refs surface as
// "(external)" upstream rows in the UI rather than blocking authoring.
func (s *Service) AttachExternalTable(dir, ref, toNode, alias string) error {
	if !isSchemaTableRefSvc(ref) {
		return fmt.Errorf("table reference %q must be `<schema>.<table>` (Glue-identifier chars only)", ref)
	}
	if alias == "" {
		// Default the alias to the table-name portion so the SQL reads
		// naturally: `FROM dim_customers` over a `marketing.dim_customers`
		// reference, not `FROM marketing.dim_customers`.
		dot := strings.Index(ref, ".")
		alias = ref[dot+1:]
	}
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return fmt.Errorf("parse pipeline: %w", err)
	}
	var node *string
	for _, n := range g.Nodes {
		if n.ID == toNode {
			id := n.ID
			node = &id
			if n.Type != "transform" {
				return fmt.Errorf("node %q is %s; external tables can only attach to transforms", toNode, n.Type)
			}
			break
		}
	}
	if node == nil {
		return fmt.Errorf("node %q not found in %s", toNode, dir)
	}
	file, err := hclutil.FindNodeFile(s.fo, abs, toNode)
	if err != nil {
		return fmt.Errorf("locate node file: %w", err)
	}

	// Same merge pattern as AttachSource: preserve intra-pipeline edges,
	// source-registry refs, and any existing external-table refs.
	merged := map[string]interface{}{}
	for _, e := range g.Edges {
		if e.ToNode != toNode {
			continue
		}
		key := e.ToInput
		if key == "" {
			key = "default"
		}
		merged[key] = fileops.ModuleReference{
			Type:       "reference",
			Expression: fmt.Sprintf(`module.%s.outputs["default"]`, e.FromNode),
		}
	}
	for _, n := range g.Nodes {
		if n.ID != toNode {
			continue
		}
		if existing, ok := n.Config["source_inputs"].(map[string]interface{}); ok {
			for k, v := range existing {
				if str, ok := v.(string); ok {
					merged[k] = str
				}
			}
		}
		if existing, ok := n.Config["external_inputs"].(map[string]interface{}); ok {
			for k, v := range existing {
				if str, ok := v.(string); ok {
					merged[k] = str
				}
			}
		}
	}
	merged[alias] = ref

	attrs := map[string]fileops.AttributeValue{"inputs": merged}
	if _, err := s.fo.UpdateBlock(file, "module."+toNode, attrs); err != nil {
		return fmt.Errorf("update node: %w", err)
	}
	if err := s.SyncOrchestration(dir, ""); err != nil {
		return fmt.Errorf("sync orchestration: %w", err)
	}
	return nil
}

// DetachInput removes an aliased input from a transform's `inputs` / typed
// `source_inputs` HCL attributes. Mirrors AttachSource's rebuild pattern:
// re-parse the graph, walk every input attached to toNode, write back
// everything except the alias being removed. Covers all three kinds —
// transform→transform edges, registry sources (kind=s3 typed and kind=http
// sentinel), external `<schema>.<table>` references — so one UI affordance
// and one CLI command can detach any attachment without the caller having
// to discriminate first.
//
// Errors:
//   - node not found / not a transform.
//   - alias not attached (so the UI X button doesn't silently no-op on a
//     stale row).
func (s *Service) DetachInput(dir, toNode, alias string) error {
	if alias == "" {
		return fmt.Errorf("alias is required")
	}
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return fmt.Errorf("parse pipeline: %w", err)
	}
	var node *graph.Node
	for i := range g.Nodes {
		if g.Nodes[i].ID == toNode {
			n := g.Nodes[i]
			node = &n
			if n.Type != "transform" {
				return fmt.Errorf("node %q is %s; only transforms have detachable inputs", toNode, n.Type)
			}
			break
		}
	}
	if node == nil {
		return fmt.Errorf("node %q not found in %s", toNode, dir)
	}
	file, err := hclutil.FindNodeFile(s.fo, abs, toNode)
	if err != nil {
		return fmt.Errorf("locate node file: %w", err)
	}

	// Confirm the alias is actually attached somewhere before mutating.
	found := false
	for _, e := range g.Edges {
		if e.ToNode == toNode && e.ToInput == alias {
			found = true
			break
		}
	}
	if !found {
		if src, ok := node.Config["source_inputs"].(map[string]interface{}); ok {
			if _, present := src[alias]; present {
				found = true
			}
		}
	}
	if !found {
		if ext, ok := node.Config["external_inputs"].(map[string]interface{}); ok {
			if _, present := ext[alias]; present {
				found = true
			}
		}
	}
	if !found {
		return fmt.Errorf("alias %q is not attached to node %q", alias, toNode)
	}

	// Rebuild the three payloads from scratch, skipping the alias being
	// detached. Mirrors AttachSource's reconstruction so the file ends up
	// in the canonical shape regardless of how it was authored.
	inputs := map[string]interface{}{}
	sourceInputs := map[string]interface{}{}

	// 1) Edges minus the detached alias.
	for _, e := range g.Edges {
		if e.ToNode != toNode {
			continue
		}
		key := e.ToInput
		if key == "" {
			key = "default"
		}
		if key == alias {
			continue
		}
		inputs[key] = fileops.ModuleReference{
			Type:       "reference",
			Expression: fmt.Sprintf(`module.%s.outputs["default"]`, e.FromNode),
		}
	}

	// 2) Registry-source attachments minus the detached alias. Re-resolve
	// kind=s3 entries from the registry so rename/partition changes flow
	// through; kind=http stays as the legacy sentinel.
	if existing, ok := node.Config["source_inputs"].(map[string]interface{}); ok {
		for k, v := range existing {
			if k == alias {
				continue
			}
			specName, _ := sourceInputSpecName(v)
			if specName == "" {
				continue
			}
			resolvedKind, resolved, sentinel, err := s.resolveSourceForAttach(specName)
			if err != nil {
				return fmt.Errorf("refresh attached source %q: %w", specName, err)
			}
			switch resolvedKind {
			case "s3":
				sourceInputs[k] = resolved
			default:
				inputs[k] = sentinel
			}
		}
	}

	// 3) External `<schema>.<table>` refs minus the detached alias.
	if existing, ok := node.Config["external_inputs"].(map[string]interface{}); ok {
		for k, v := range existing {
			if k == alias {
				continue
			}
			if str, ok := v.(string); ok {
				inputs[k] = str
			}
		}
	}

	// Write back. Set both attrs every time so removing the last entry of
	// a kind actually clears the block (matching the cue at AttachSource
	// line 339-341 — "DetachSource (when it lands) will own clearing").
	attrs := map[string]fileops.AttributeValue{}
	if len(inputs) > 0 {
		attrs["inputs"] = inputs
	} else {
		attrs["inputs"] = nil
	}
	if len(sourceInputs) > 0 {
		attrs["source_inputs"] = sourceInputs
	} else {
		attrs["source_inputs"] = nil
	}
	if _, err := s.fo.UpdateBlock(file, "module."+toNode, attrs); err != nil {
		return fmt.Errorf("update node: %w", err)
	}
	if err := s.SyncOrchestration(dir, ""); err != nil {
		return fmt.Errorf("sync orchestration: %w", err)
	}
	return nil
}

// isSchemaTableRefSvc mirrors hclparser.isSchemaTableRef without
// exporting the regex out of the parser package. Same charset and
// shape: `<schema>.<table>` where each segment matches the Glue
// identifier rule `[A-Za-z_][A-Za-z0-9_]*`.
func isSchemaTableRefSvc(s string) bool {
	dot := strings.Index(s, ".")
	if dot <= 0 || dot == len(s)-1 {
		return false
	}
	if strings.Count(s, ".") != 1 {
		return false
	}
	return isGlueIdent(s[:dot]) && isGlueIdent(s[dot+1:])
}

func isGlueIdent(s string) bool {
	if len(s) == 0 {
		return false
	}
	first := s[0]
	if !(first == '_' || (first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z')) {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !(c == '_' || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
			return false
		}
	}
	return true
}

// findSourceUsages walks every directory under the workspace looking for
// pipelines (by presence of `module "<name>" { source = "...transform/aws"
// ... inputs = { ... = "sources.<name>" } }` in any `.tf`). The hclparser
// gives us the inputs map directly, so the scan is "parse, look at every
// transform's inputs values".
func (s *Service) findSourceUsages(name string) ([]SourceUsage, error) {
	ref := "sources." + name
	out := []SourceUsage{}
	entries, err := os.ReadDir(s.workspace)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// Skip the workspace's own infra dir + the registry/dashboards dirs.
		if e.Name() == "_workspace" || strings.HasPrefix(e.Name(), ".") || e.Name() == "runner" {
			continue
		}
		dir := filepath.Join(s.workspace, e.Name())
		usage := scanPipelineForSourceRef(dir, ref)
		if len(usage.NodeIDs) > 0 {
			usage.PipelineDir = e.Name()
			out = append(out, usage)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PipelineDir < out[j].PipelineDir })
	return out, nil
}

// scanPipelineForSourceRef returns the node IDs in `dir` whose
// `inputs` map references `ref` (e.g. "sources.foo"). Empty node list
// means "this dir has no references" (or isn't a pipeline at all — the
// hclparser tolerates non-pipeline dirs and returns an empty graph).
func scanPipelineForSourceRef(dir, ref string) SourceUsage {
	usage := SourceUsage{}
	g, err := hclparser.Parse(dir)
	if err != nil {
		return usage
	}
	for _, n := range g.Nodes {
		if n.Type != "transform" {
			continue
		}
		// hclparser surfaces unresolved inputs strings under
		// Config["source_inputs"] (added for ADR-017 slice 1) — module
		// references are stripped into edges, but `sources.<name>`
		// literals stay as strings so this scan can find them.
		raw, ok := n.Config["source_inputs"]
		if !ok {
			continue
		}
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		for _, v := range m {
			if s, ok := v.(string); ok && s == ref {
				usage.NodeIDs = append(usage.NodeIDs, n.ID)
				break
			}
		}
	}
	sort.Strings(usage.NodeIDs)
	return usage
}

// stripURLPath returns just the path portion of a URL (after the host) so
// inferFormatFromFilename can read its extension. Falls back to the input
// when parsing fails.
func stripURLPath(s string) string {
	// Cheap split — the inference logic only cares about the trailing
	// filename, and the existing inferFormatFromFilename normalises case.
	i := strings.LastIndex(s, "/")
	if i < 0 {
		return s
	}
	return s[i+1:]
}
