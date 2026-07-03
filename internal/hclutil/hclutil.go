// Package hclutil provides shared HCL helpers used by api and service packages.
package hclutil

import (
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"

	"github.com/vesahyp/clavesa/internal/fileops"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
)

// InitialPos returns the starting source position for hclwrite.ParseConfig.
func InitialPos() hcl.Pos {
	return hcl.Pos{Line: 1, Column: 1}
}

// FindNodeFile scans all .tf files in dir (via fo.Read) for a module block
// labelled id. Returns the absolute file path, or an error if not found.
func FindNodeFile(fo *fileops.FileOps, dir, id string) (string, error) {
	result, err := fo.Read(dir)
	if err != nil {
		return "", err
	}
	for _, f := range result.Files {
		hf, diags := hclwrite.ParseConfig([]byte(f.Content), f.Path, InitialPos())
		if diags.HasErrors() {
			continue
		}
		for _, block := range hf.Body().Blocks() {
			if block.Type() == "module" && len(block.Labels()) == 1 && block.Labels()[0] == id {
				return f.Path, nil
			}
		}
	}
	return "", fmt.Errorf("module %q not found in %s", id, dir)
}

// RemoveEdgesReferencing removes every edge fed by fromNodeID from module
// blocks in dir. This cleans up dangling edges after a node is deleted.
//
//   - a singular `input = module.<fromNodeID>.outputs[…]` attribute
//     (destination modules) only ever holds the one reference, so the whole
//     attribute is cleared;
//   - an `inputs = { … }` map (transform modules) is rebuilt minus only the
//     deleted node's entries — a multi-input transform keeps its edges from
//     other producers (and any registry/external string entries) intact.
func RemoveEdgesReferencing(fo *fileops.FileOps, dir, fromNodeID string) error {
	result, err := fo.Read(dir)
	if err != nil {
		return err
	}
	refMarker := fmt.Sprintf("module.%s.outputs", fromNodeID)
	// Parsed lazily, at most once: only needed when a transform's inputs map
	// must be rebuilt, and the parse has to happen after fo.Read so it sees
	// the same on-disk state (the deleted node's block already removed).
	var g graph.PipelineGraph
	parsed := false
	for _, f := range result.Files {
		if !strings.Contains(f.Content, refMarker) {
			continue
		}
		hf, diags := hclwrite.ParseConfig([]byte(f.Content), f.Path, InitialPos())
		if diags.HasErrors() {
			continue
		}
		for _, block := range hf.Body().Blocks() {
			if block.Type() != "module" || len(block.Labels()) == 0 {
				continue
			}
			moduleName := block.Labels()[0]
			attrUpdates := make(map[string]fileops.AttributeValue)
			for attrName, attr := range block.Body().Attributes() {
				if attrName != "input" && attrName != "inputs" {
					continue
				}
				var sb strings.Builder
				for _, tok := range attr.BuildTokens(nil) {
					sb.Write(tok.Bytes)
				}
				if !strings.Contains(sb.String(), refMarker) {
					continue
				}
				if attrName == "input" {
					attrUpdates[attrName] = nil
					continue
				}
				if !parsed {
					g, err = hclparser.Parse(dir)
					if err != nil {
						return err
					}
					parsed = true
				}
				remaining := TransformInputsExcluding(g, moduleName, fromNodeID)
				if len(remaining) == 0 {
					attrUpdates[attrName] = nil
				} else {
					attrUpdates[attrName] = remaining
				}
			}
			if len(attrUpdates) > 0 {
				if _, err := fo.UpdateBlock(f.Path, "module."+moduleName, attrUpdates); err != nil {
					return fmt.Errorf("remove edges in %s: %w", f.Path, err)
				}
			}
		}
	}
	return nil
}

// TransformInputsExcluding rebuilds the `inputs` map of transform toNode from
// the parsed graph, dropping every edge from dropFrom into toNode (pass "" to
// keep all edges). Module-ref entries keep their parsed output key, so an
// authored `module.x.outputs["stats"]` reference survives the rewrite
// verbatim. String entries that also live in the inputs map — legacy
// `"sources.<name>"` registry references and `"<schema>.<table>"` external
// refs — are re-emitted too, so an edge rewrite never drops them.
//
// Typed source_inputs attachments (kind=s3 blocks) live in their own HCL
// attribute, not in `inputs`, and are represented as maps in Config —
// the string-only filter below keeps them out of the rebuilt inputs map.
func TransformInputsExcluding(g graph.PipelineGraph, toNode, dropFrom string) map[string]interface{} {
	remaining := make(map[string]interface{})
	for _, e := range g.Edges {
		if e.ToNode != toNode {
			continue
		}
		if dropFrom != "" && e.FromNode == dropFrom {
			continue
		}
		out := e.FromOutput
		if out == "" {
			out = "default"
		}
		remaining[e.ToInput] = fileops.ModuleReference{
			Type:       "reference",
			Expression: fmt.Sprintf(`module.%s.outputs["%s"]`, e.FromNode, out),
		}
	}
	for _, n := range g.Nodes {
		if n.ID != toNode {
			continue
		}
		for _, key := range []string{"source_inputs", "external_inputs"} {
			entries, _ := n.Config[key].(map[string]interface{})
			for alias, v := range entries {
				if s, ok := v.(string); ok {
					remaining[alias] = s
				}
			}
		}
		break
	}
	return remaining
}

// RemoveEdge deletes the edge from fromNode into toNode. The shape of the
// edit depends on the target node type:
//
//   - destination modules use a singular `input = …` attribute. The attr is
//     cleared.
//   - transform modules use an `inputs = { alias = … }` map. The map is
//     rebuilt from the parsed graph minus the (fromNode → toNode) entries,
//     since the edge id alone (`{from_node}->{to_node}`) doesn't carry the
//     SQL alias and a transform may have multiple inputs from other
//     producers.
//
// Returns an error if toNode can't be located or its type can't be
// determined. A no-op (no matching edge) is silent — callers parse the
// resulting graph to confirm.
func RemoveEdge(fo *fileops.FileOps, dir, fromNode, toNode string) error {
	file, err := FindNodeFile(fo, dir, toNode)
	if err != nil {
		return err
	}
	g, err := hclparser.Parse(dir)
	if err != nil {
		return err
	}
	toNodeType := ""
	for _, n := range g.Nodes {
		if n.ID == toNode {
			toNodeType = n.Type
			break
		}
	}

	if toNodeType == "destination" {
		_, err := fo.UpdateBlock(file, "module."+toNode, map[string]fileops.AttributeValue{"input": nil})
		return err
	}

	// Transform: rebuild the inputs map from existing edges, dropping every
	// edge from fromNode into toNode (multiple aliases from the same source
	// are unusual but legal).
	remaining := TransformInputsExcluding(g, toNode, fromNode)
	var attrs map[string]fileops.AttributeValue
	if len(remaining) == 0 {
		attrs = map[string]fileops.AttributeValue{"inputs": nil}
	} else {
		attrs = map[string]fileops.AttributeValue{"inputs": remaining}
	}
	_, err = fo.UpdateBlock(file, "module."+toNode, attrs)
	return err
}

// RenameModuleBlock renames the module block labelled oldID to newID in
// the given file. It changes the block label and keeps the block's `name`
// attribute in sync (modules use it for resource and table naming).
// Downstream edge references are handled by RewriteEdgeReferences;
// `sql`/`python` script-file references by the caller.
func RenameModuleBlock(fo *fileops.FileOps, file, oldID, newID string) error {
	src, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	hf, diags := hclwrite.ParseConfig(src, file, InitialPos())
	if diags.HasErrors() {
		return fmt.Errorf("parse %s: %s", file, diags.Error())
	}
	found := false
	for _, block := range hf.Body().Blocks() {
		if block.Type() != "module" || len(block.Labels()) != 1 || block.Labels()[0] != oldID {
			continue
		}
		block.SetLabels([]string{newID})
		if block.Body().GetAttribute("name") != nil {
			block.Body().SetAttributeValue("name", cty.StringVal(newID))
		}
		found = true
	}
	if !found {
		return fmt.Errorf("module %q not found in %s", oldID, file)
	}
	return fo.WriteFile(file, string(hf.Bytes()))
}

// RewriteEdgeReferences rewrites every `module.<oldID>.outputs` reference in
// dir's .tf files to point at newID, keeping downstream transforms and
// destinations wired after a node rename. The `.outputs` suffix anchors the
// match, so a node id that is a prefix of another (e.g. `orders` vs
// `orders_eu`) is never corrupted.
func RewriteEdgeReferences(fo *fileops.FileOps, dir, oldID, newID string) error {
	result, err := fo.Read(dir)
	if err != nil {
		return err
	}
	oldRef := fmt.Sprintf("module.%s.outputs", oldID)
	newRef := fmt.Sprintf("module.%s.outputs", newID)
	for _, f := range result.Files {
		if !strings.Contains(f.Content, oldRef) {
			continue
		}
		if err := fo.WriteFile(f.Path, strings.ReplaceAll(f.Content, oldRef, newRef)); err != nil {
			return fmt.Errorf("rewrite edges in %s: %w", f.Path, err)
		}
	}
	return nil
}

// ParseEdgeID parses a React Flow edge id of the form {from_node}->{to_node}.
// Returns the two node IDs and ok=true on success.
func ParseEdgeID(id string) (fromNode, toNode string, ok bool) {
	left, right, found := strings.Cut(id, "->")
	if !found || left == "" || right == "" {
		return "", "", false
	}
	return left, right, true
}
