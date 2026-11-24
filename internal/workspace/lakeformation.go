package workspace

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/access"
)

// LakeFormationTFFile is the clavesa-owned file in the workspace root that
// calls the lake-formation module (docs/ideas/column-tags.md, slice 1a).
// It exists only while clavesa.json has an "access" block, and only for a
// cloud workspace (one with a stack).
const LakeFormationTFFile = "lakeformation.tf"

const lakeFormationTFHeader = `# clavesa-managed Lake Formation registration, generated from clavesa.json's
# "access" block (docs/ideas/column-tags.md). Do not edit by hand: clavesa
# overwrites this file on every deploy and removes it when the block goes.

`

// LakeFormationRoleName is the registration role:
// clavesa-lf-register-<workspace>, at most 64 characters.
func LakeFormationRoleName(workspaceName string) string {
	return hostedName("clavesa-lf-register-", workspaceName, 64)
}

// RenderLakeFormationTF renders lakeformation.tf.
func RenderLakeFormationTF(m *Manifest, moduleVersion string) string {
	return lakeFormationTFHeader + fmt.Sprintf(`module "lake_formation" {
  source          = "./.clavesa/modules/%s/lake-formation/aws"
  workspace_name  = var.workspace_name
  role_name       = %q
  pipeline_bucket = module.workspace.pipeline_bucket
  tag_key         = %q
  tag_values      = [%s]
  system_database = module.workspace.system_pipelines_db
}

output "lf_tag_key" {
  description = "The workspace's Lake Formation tag key (column tags)."
  value       = module.lake_formation.tag_key
}

output "lf_register_role_arn" {
  description = "Lake Formation registration role; each pipeline registers its <pipeline>/_warehouse/ prefix with it."
  value       = module.lake_formation.register_role_arn
}

output "lf_system_location_arn" {
  description = "The registered _system/ location of the system catalog."
  value       = module.lake_formation.system_location_arn
}
`, moduleVersion, LakeFormationRoleName(m.Name), access.TagKey(m.Name), quotedHCLList(lakeFormationTagValues(m)))
}

// lakeFormationTagValues are the tag key's values: the "access" block's
// tags and access.Unreviewed, sorted.
func lakeFormationTagValues(m *Manifest) []string {
	seen := map[string]bool{access.Unreviewed: true}
	vals := []string{access.Unreviewed}
	if m.Access != nil {
		for _, t := range m.Access.Tags {
			if t = strings.TrimSpace(t); t != "" && !seen[t] {
				seen[t] = true
				vals = append(vals, t)
			}
		}
	}
	sort.Strings(vals)
	return vals
}

// SyncLakeFormationTF makes lakeformation.tf match the manifest: written
// while m.Access is set, removed when it is not.
func SyncLakeFormationTF(root string, m *Manifest, moduleVersion string) error {
	path := filepath.Join(root, LakeFormationTFFile)
	if m.Access == nil {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if moduleVersion == "" {
		return fmt.Errorf("no module version for %s", LakeFormationTFFile)
	}
	return os.WriteFile(path, []byte(RenderLakeFormationTF(m, moduleVersion)), 0o644)
}
