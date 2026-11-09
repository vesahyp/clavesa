package workspace

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// UnityCatalogTFFile is the clavesa-owned file in the workspace root that
// carries the Databricks provider and the unity-catalog module (ADR-027).
// It exists only while clavesa.json has a `databricks` block.
const UnityCatalogTFFile = "unity_catalog.tf"

const unityCatalogTFHeader = `# clavesa-managed Unity Catalog publish target, generated from
# clavesa.json's "databricks" field (ADR-027). Do not edit by hand:
# clavesa overwrites this file on connect and on every upgrade, and
# removes it on disconnect.

`

// UnityCatalogNeedsRunnerSecret reports whether the workspace stack must
// create a Secrets Manager secret for the runner. An arn: reference is
// read as it is; env: and file: references are local only (ADR-017), so
// deploy copies the value into the clavesa-created secret.
func UnityCatalogNeedsRunnerSecret(secretRef string) bool {
	return !strings.HasPrefix(secretRef, "arn:aws:secretsmanager:")
}

// RenderUnityCatalogTF renders unity_catalog.tf. clientID is the service
// principal's application ID from the connection's credential;
// secretRef is that credential's secret reference. The OAuth secret
// itself never appears here: the provider reads DATABRICKS_CLIENT_SECRET,
// which `clavesa deploy` sets from the credential.
func RenderUnityCatalogTF(m *Manifest, moduleVersion, clientID, secretRef string) string {
	d := m.Databricks
	extLoc := "null"
	if d.ExternalLocation != "" {
		extLoc = fmt.Sprintf("%q", d.ExternalLocation)
	}
	return unityCatalogTFHeader + fmt.Sprintf(`# The module configures the databricks provider itself, because main.tf
# already declares this root's providers. It authenticates as the
# service principal; the client secret comes from DATABRICKS_CLIENT_SECRET,
# set by clavesa deploy from the %q credential, and is never written to a
# file or the state.
module "unity_catalog" {
  source               = "./.clavesa/modules/%s/unity-catalog/aws"
  databricks_host      = %q
  databricks_client_id = %q
  workspace_name       = var.workspace_name
  bucket               = module.workspace.pipeline_bucket
  catalog              = %q
  create_catalog       = %t
  external_location    = %s
  create_runner_secret = %t
  readers              = [%s]
}

output "unity_catalog_external_location" {
  description = "External location over the workspace bucket (ADR-027)."
  value       = module.unity_catalog.external_location
}

output "unity_catalog_catalog" {
  description = "Unity Catalog catalog this workspace writes into (ADR-027)."
  value       = module.unity_catalog.catalog
}

output "databricks_runner_secret_arn" {
  description = "Secrets Manager secret the runner reads the Databricks OAuth secret from; null when the credential is already an arn: reference."
  value       = module.unity_catalog.runner_secret_arn
}
`, d.Credential, moduleVersion, d.Host, clientID, d.Catalog, d.CreateCatalog, extLoc, UnityCatalogNeedsRunnerSecret(secretRef), quotedHCLList(d.Readers))
}

// SyncUnityCatalogTF makes unity_catalog.tf match the manifest: written
// when m.Databricks is set, removed when it is not. clientID and
// secretRef are ignored on removal.
func SyncUnityCatalogTF(root string, m *Manifest, moduleVersion, clientID, secretRef string) error {
	path := filepath.Join(root, UnityCatalogTFFile)
	if m.Databricks == nil {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if moduleVersion == "" {
		return fmt.Errorf("no module version: main.tf has no recognised workspace module source")
	}
	return os.WriteFile(path, []byte(RenderUnityCatalogTF(m, moduleVersion, clientID, secretRef)), 0o644)
}

// CurrentModuleVersion returns the module version main.tf pins the
// workspace module to, or "" when main.tf has no recognised source line.
func CurrentModuleVersion(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, "main.tf"))
	if err != nil {
		return "", err
	}
	if m := workspaceModuleSourceRE.FindSubmatch(data); m != nil {
		return string(m[3]), nil
	}
	return "", nil
}

// quotedHCLList renders strings as comma-separated HCL string literals.
func quotedHCLList(items []string) string {
	q := make([]string, len(items))
	for i, s := range items {
		q[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(q, ", ")
}
