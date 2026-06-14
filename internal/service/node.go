package service

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/vesahyp/clavesa/internal/fileops"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/hclutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// nodeIdentifierRe is the allowed shape of a node id — a Terraform/HCL
// identifier, which is also a valid SQL alias and Glue-safe table-name stem.
var nodeIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// AddNode adds a new node of the given type to the pipeline.
// If name is non-empty it is used as the block name; otherwise a sequential
// name like "transform2" is generated automatically.
//
// ADR-017 slice 4: source nodes are no longer authored inline. Use
// `clavesa source register` (CLI) or `/sources` (UI) to put a source
// in the workspace registry, then attach it to a transform's inputs
// via `clavesa source attach`. Existing inline `module "src_X"`
// blocks in already-authored pipelines continue to parse and run for
// backward compatibility — only the *creation* path is gone.
func (s *Service) AddNode(dir, nodeType, name string) (PipelineGraph, error) {
	if nodeType == "source" {
		return PipelineGraph{}, fmt.Errorf("inline source nodes have been removed (ADR-017 slice 4); use `clavesa source register --from <url>` and `clavesa source attach <pipeline> <source> --to <transform>` instead")
	}
	var def *NodeTypeDef
	for i := range NodeTypes {
		if NodeTypes[i].Type == nodeType {
			def = &NodeTypes[i]
			break
		}
	}
	if def == nil {
		return PipelineGraph{}, fmt.Errorf("unknown node type: %s", nodeType)
	}

	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return PipelineGraph{}, err
	}

	blockName := name
	if blockName == "" {
		count := 0
		for _, n := range g.Nodes {
			if n.Type == nodeType {
				count++
			}
		}
		blockName = fmt.Sprintf("%s%d", def.BlockPrefix, count+1)
	}

	pipelineNameRef := fileops.ModuleReference{Type: "reference", Expression: "var.pipeline_name"}
	ws, _ := workspace.Load(s.workspace)
	bucketExprStr := "aws_s3_bucket.pipeline_bucket.bucket"
	if ws != nil {
		bucketExprStr = "data.terraform_remote_state.workspace.outputs.pipeline_bucket"
	}
	pipelineBucketRef := fileops.ModuleReference{Type: "reference", Expression: bucketExprStr}

	attrs := map[string]fileops.AttributeValue{
		"source":        s.ModuleSource(abs, def.ModuleRel),
		"pipeline_name": pipelineNameRef,
		"name":          blockName,
	}
	switch def.Type {
	case "source":
		// Sources are pass-through (no Glue, no compute) — no output_bucket needed.
	case "transform":
		attrs["bucket"] = pipelineBucketRef
		attrs["output_definitions"] = fileops.ModuleReference{Type: "reference", Expression: "{ default = {} }"}
		// No `compute` attribute — it defaults to the module's "lambda"
		// (compute is purely the cloud deploy target). A fresh transform
		// still runs on the laptop without `terraform
		// apply`: the workspace warehouse (default "local")
		// drives local-vs-cloud dispatch, not this attribute. Users
		// pick "fargate" / "emr-serverless" here only when the cloud
		// workload outgrows Lambda.
		// Three-level namespace (ADR-016): thread the workspace's catalog
		// identifier as a literal (manifest is the single source of truth)
		// and the pipeline schema as a var reference (pipeline owns its
		// schema; user can override per-pipeline). Legacy workspaces
		// (manifest catalog == "") get an empty literal — runner falls
		// through to today's `clavesa_<schema>` Glue DB form.
		catalog := ""
		systemCatalog := ""
		if ws != nil {
			catalog = ws.CatalogIdentifier()
			systemCatalog = ws.SystemCatalogIdentifier()
		}
		attrs["catalog"] = catalog
		attrs["schema"] = fileops.ModuleReference{Type: "reference", Expression: "var.schema"}
		// Workspace system catalog (ADR-016 v0.20.0). Literal because
		// the workspace owns it; every transform in this pipeline
		// writes node_runs / tables there alongside every other
		// pipeline's transforms.
		attrs["system_catalog"] = systemCatalog
		// No depends_on on module.orchestration — orchestration reads
		// module.<this>.lambda_function_arn, so the dependency goes the
		// other way. With every transform on Lambda (post PySpark-everywhere
		// rewrite) the old depends_on would always create a cycle.
		// runner_image was dropped in v2.2.1 (per-transform Lambda was
		// collapsed in v2.2.0; the pipeline Lambda emitted by
		// internal/orchestration/tfgen consumes the workspace
		// remote-state output directly).
	case "destination":
		// Destinations are pass-through path declarations — no compute, no pipeline_bucket needed.
	}
	for k, v := range def.DefaultConfig {
		attrs[k] = v
	}

	file := filepath.Join(abs, "main.tf")
	if _, err := s.fo.AddBlock(file, "module", blockName, attrs); err != nil {
		return PipelineGraph{}, err
	}
	updated, parseErr := hclparser.Parse(abs)
	if parseErr != nil {
		return PipelineGraph{}, parseErr
	}
	_ = s.SyncOrchestration(dir, "") // best-effort; user can re-run manually
	return updated, nil
}

// UpdateNode merges attrs into the given node's module block.
func (s *Service) UpdateNode(dir, nodeID string, attrs map[string]interface{}) (PipelineGraph, error) {
	// compute is strictly a cloud deploy target. "local" is no longer a
	// value — local execution is the workspace warehouse, not a
	// per-node attribute.
	if c, ok := attrs["compute"].(string); ok {
		switch c {
		case "lambda", "fargate", "emr-serverless":
			// a valid cloud deploy target
		case "local":
			return PipelineGraph{}, fmt.Errorf(`compute = "local" is no longer a value: compute is the cloud deploy target (lambda / fargate / emr-serverless). Pipelines run locally by default — no compute value is needed for local development`)
		default:
			return PipelineGraph{}, fmt.Errorf("compute %q is not a valid deploy target — use lambda, fargate, or emr-serverless", c)
		}
	}
	abs := s.resolveDir(dir)
	file, err := hclutil.FindNodeFile(s.fo, abs, nodeID)
	if err != nil {
		return PipelineGraph{}, err
	}
	normalized := make(map[string]fileops.AttributeValue, len(attrs))
	for k, v := range attrs {
		normalized[k] = v
	}
	// When switching to a non-SQL language, remove the sql attribute so it
	// doesn't linger as an empty string in the TF file.
	if lang, ok := attrs["language"].(string); ok && lang != "sql" {
		if _, alreadySet := normalized["sql"]; !alreadySet {
			normalized["sql"] = nil // nil → remove
		}
	}
	if _, err := s.fo.UpdateBlock(file, "module."+nodeID, normalized); err != nil {
		return PipelineGraph{}, err
	}
	updated, err := hclparser.Parse(abs)
	if err != nil {
		return PipelineGraph{}, err
	}
	_ = s.SyncOrchestration(dir, "") // best-effort; user can re-run manually
	return updated, nil
}

// DeleteNode removes the given node and all edges referencing it.
func (s *Service) DeleteNode(dir, nodeID string) (PipelineGraph, error) {
	abs := s.resolveDir(dir)
	file, err := hclutil.FindNodeFile(s.fo, abs, nodeID)
	if err != nil {
		return PipelineGraph{}, err
	}
	if _, err := s.fo.RemoveBlock(file, "module."+nodeID); err != nil {
		return PipelineGraph{}, err
	}
	if err := hclutil.RemoveEdgesReferencing(s.fo, abs, nodeID); err != nil {
		return PipelineGraph{}, fmt.Errorf("clean up edges: %w", err)
	}
	g, err := hclparser.Parse(abs)
	if err != nil {
		return PipelineGraph{}, err
	}
	_ = s.SyncOrchestration(dir, "")
	return g, nil
}

// RenameNode renames node oldID to newID: the module block label and its
// `name` attribute, every downstream `module.<oldID>.outputs` edge
// reference, and the transform's `<oldID>.sql`/`.py` script files.
//
// A node's id is also the stem of its Iceberg output table
// (`<node>__default`), so a rename changes that table's identity — data
// already materialised under the old name is not moved.
func (s *Service) RenameNode(dir, oldID, newID string) (PipelineGraph, error) {
	if newID == oldID {
		return PipelineGraph{}, fmt.Errorf("new name is the same as the current name")
	}
	if !nodeIdentifierRe.MatchString(newID) {
		return PipelineGraph{}, fmt.Errorf("%q is not a valid node name — use letters, digits and underscores, starting with a letter or underscore", newID)
	}
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return PipelineGraph{}, err
	}
	var oldConfig map[string]interface{}
	oldFound := false
	for _, n := range g.Nodes {
		if n.ID == newID {
			return PipelineGraph{}, fmt.Errorf("a node named %q already exists in %s", newID, dir)
		}
		if n.ID == oldID {
			oldFound = true
			oldConfig = n.Config
		}
	}
	if !oldFound {
		return PipelineGraph{}, fmt.Errorf("node %q not found in %s", oldID, dir)
	}
	file, err := hclutil.FindNodeFile(s.fo, abs, oldID)
	if err != nil {
		return PipelineGraph{}, err
	}

	// A transform's `sql`/`python` is stored as the string `file("<id>.<ext>")`
	// pointing at a sibling script file. Move any such file to the new id and
	// remember the attribute rewrite for after the block rename. A literal-SQL
	// node has no file reference and is skipped.
	scriptUpdates := map[string]fileops.AttributeValue{}
	for _, sc := range []struct{ attr, ext string }{{"sql", "sql"}, {"python", "py"}} {
		ref, _ := oldConfig[sc.attr].(string)
		if ref != fmt.Sprintf(`file("%s.%s")`, oldID, sc.ext) {
			continue
		}
		oldPath := filepath.Join(abs, oldID+"."+sc.ext)
		if _, statErr := os.Stat(oldPath); statErr != nil {
			continue
		}
		if err := os.Rename(oldPath, filepath.Join(abs, newID+"."+sc.ext)); err != nil {
			return PipelineGraph{}, fmt.Errorf("rename script %s.%s: %w", oldID, sc.ext, err)
		}
		scriptUpdates[sc.attr] = fmt.Sprintf(`file("%s.%s")`, newID, sc.ext)
	}

	if err := hclutil.RenameModuleBlock(s.fo, file, oldID, newID); err != nil {
		return PipelineGraph{}, err
	}
	if err := hclutil.RewriteEdgeReferences(s.fo, abs, oldID, newID); err != nil {
		return PipelineGraph{}, fmt.Errorf("rewrite edges: %w", err)
	}
	if len(scriptUpdates) > 0 {
		if _, err := s.fo.UpdateBlock(file, "module."+newID, scriptUpdates); err != nil {
			return PipelineGraph{}, fmt.Errorf("repoint script reference: %w", err)
		}
	}

	updated, err := hclparser.Parse(abs)
	if err != nil {
		return PipelineGraph{}, err
	}
	_ = s.SyncOrchestration(dir, "") // best-effort; user can re-run manually
	return updated, nil
}

// AddEdge creates an edge from fromNode.fromOutput to toNode.toInput.
func (s *Service) AddEdge(dir, fromNode, fromOutput, toNode, toInput string) (PipelineGraph, error) {
	if fromOutput == "" {
		fromOutput = "default"
	}
	// Default the SQL table alias to the from-node id. Node ids are valid
	// identifiers, so this reads naturally in SQL (`FROM <from-node>`) and —
	// crucially — gives each upstream a distinct key in the `inputs` map.
	// A blanket "default" alias meant a second edge into the same transform
	// silently overwrote the first. The CLI's `node connect` already
	// defaults `--input` this way; this keeps the service consistent for
	// the UI drag-to-connect path, which passes no alias.
	if toInput == "" {
		toInput = fromNode
	}
	abs := s.resolveDir(dir)
	file, err := hclutil.FindNodeFile(s.fo, abs, toNode)
	if err != nil {
		return PipelineGraph{}, fmt.Errorf("to_node %q not found: %w", toNode, err)
	}
	ref := fileops.ModuleReference{
		Type:       "reference",
		Expression: fmt.Sprintf(`module.%s.outputs["%s"]`, fromNode, fromOutput),
	}

	// Destination modules take a single `input` (a direct table reference).
	// Transform modules take `inputs` (a map keyed by SQL table alias).
	// Resolve the target node type to pick the right attribute name.
	g, err := hclparser.Parse(abs)
	if err != nil {
		return PipelineGraph{}, err
	}
	toNodeType := ""
	fromExists := false
	for _, n := range g.Nodes {
		if n.ID == toNode {
			toNodeType = n.Type
		}
		if n.ID == fromNode {
			fromExists = true
		}
	}
	// The from-node must be a real node in this pipeline. Without this
	// check a bogus id (e.g. the UI's read-only `source:<name>` synthetic
	// node) is written straight into `module.<id>.outputs[...]`, which
	// for an id containing a colon is invalid HCL and corrupts the file.
	if !fromExists {
		return PipelineGraph{}, fmt.Errorf("from_node %q not found in %s", fromNode, dir)
	}

	var edgeAttrs map[string]fileops.AttributeValue
	if toNodeType == "destination" {
		edgeAttrs = map[string]fileops.AttributeValue{"input": ref}
	} else {
		// Merge into existing inputs map so multiple connections are preserved.
		// Reconstruct from existing edges since the parser converts inputs to edges.
		existingInputs := make(map[string]interface{})
		for _, e := range g.Edges {
			if e.ToNode == toNode {
				existingInputs[e.ToInput] = fileops.ModuleReference{
					Type:       "reference",
					Expression: fmt.Sprintf(`module.%s.outputs["default"]`, e.FromNode),
				}
			}
		}
		existingInputs[toInput] = ref
		edgeAttrs = map[string]fileops.AttributeValue{"inputs": existingInputs}
	}
	if _, err := s.fo.UpdateBlock(file, "module."+toNode, edgeAttrs); err != nil {
		return PipelineGraph{}, err
	}
	updated, err := hclparser.Parse(abs)
	if err != nil {
		return PipelineGraph{}, err
	}
	_ = s.SyncOrchestration(dir, "")
	return updated, nil
}

// DeleteEdge removes the edge identified by edgeID.
func (s *Service) DeleteEdge(dir, edgeID string) (PipelineGraph, error) {
	fromNode, toNode, ok := hclutil.ParseEdgeID(edgeID)
	if !ok {
		return PipelineGraph{}, fmt.Errorf("invalid edge id: %s", edgeID)
	}
	abs := s.resolveDir(dir)
	if err := hclutil.RemoveEdge(s.fo, abs, fromNode, toNode); err != nil {
		return PipelineGraph{}, err
	}
	g, err := hclparser.Parse(abs)
	if err != nil {
		return PipelineGraph{}, err
	}
	_ = s.SyncOrchestration(dir, "")
	return g, nil
}
