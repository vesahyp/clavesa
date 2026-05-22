package service

import (
	"fmt"
	"path/filepath"
)

// ValidateSchemaOwnership checks the pipeline at `dir` (relative to the
// workspace, or absolute) against the ADR-016 §5 schema-ownership rule.
// Used as a `pipeline deploy` preflight: deploy does not re-emit
// orchestration.tf, so without this a hand-edited `variable "schema"`
// collision would only surface deep inside `terraform apply`.
func (s *Service) ValidateSchemaOwnership(dir string) error {
	abs := s.resolveDir(dir)
	name := filepath.Base(abs)
	return s.validateSchemaOwnership(name, resolvePipelineSchema(abs, name))
}

// validateSchemaOwnership refuses a configuration where `schema` is already
// the ADR-016 schema of a different pipeline in the workspace. ADR-016 §5:
// a `<catalog>.<schema>` has exactly one producing pipeline — cross-pipeline
// writes are forbidden, full-schema (not per-table).
//
// The workspace system catalog is exempt by construction: a pipeline's
// resolved (catalog, schema) always uses the workspace USER catalog, so the
// system catalog never appears as a scanned pipeline's schema.
//
// Best-effort: a workspace-scan failure returns nil rather than blocking
// authoring — the guard only adds a refusal on positive evidence of a
// conflict. Mirrors the lineage resolver's `siblings, _ := ...` reuse of the
// same scan.
func (s *Service) validateSchemaOwnership(pipelineName, schema string) error {
	siblings, err := s.workspacePipelineScan()
	if err != nil {
		return nil
	}
	for _, p := range siblings {
		if p.name == pipelineName {
			continue // self
		}
		if p.schema == schema {
			return fmt.Errorf("schema %q is already owned by pipeline %q — "+
				"every pipeline writes into its own <catalog>.<schema> (ADR-016); "+
				"choose a different schema (pass --schema on `pipeline create`, "+
				"or edit `variable \"schema\"` in variables.tf)", schema, p.name)
		}
	}
	return nil
}
