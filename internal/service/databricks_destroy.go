package service

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/identutil"
)

// tableDropper is the part of the Databricks client the destroy sweep
// needs; tests replace it.
type tableDropper interface {
	ListTables(ctx context.Context, catalog, schema string) ([]databricks.Table, error)
	DeleteTable(ctx context.Context, fullName string) error
}

var newDatabricksDropper = func(host, clientID, secret string) tableDropper {
	return databricks.NewClient(host, clientID, secret)
}

// UnregisterPipelineDatabricksTables runs before `pipeline destroy`. With
// --create-catalog the pipeline's Unity Catalog schema is clavesa's (one
// pipeline, one schema), and Terraform cannot drop a schema that still
// holds the tables the runner registered: destroy failed with "Schema
// ... is not empty". So the tables in that schema are unregistered first;
// their files are the pipeline's own and go with the rest of it.
//
// Without --create-catalog the schema and the tables are the team's (a
// taken-over table is a production table), and nothing is touched.
func (s *Service) UnregisterPipelineDatabricksTables(ctx context.Context, dir string) ([]string, error) {
	m, err := s.loadManifest()
	if err != nil || m.Databricks == nil || !m.Databricks.CreateCatalog {
		return nil, err
	}
	abs := s.resolveDir(dir)
	name := filepath.Base(abs)
	schema := strings.ReplaceAll(identutil.Sanitize(resolvePipelineSchema(abs, name)), "-", "_")
	cred, err := s.databricksCredential(m.Databricks.Credential)
	if err != nil {
		return nil, err
	}
	secret, err := s.resolveSecret(ctx, cred.Secret)
	if err != nil {
		return nil, fmt.Errorf("credential %q: %w", cred.Name, err)
	}
	api := newDatabricksDropper(m.Databricks.Host, cred.ClientID, secret)
	tables, err := api.ListTables(ctx, m.Databricks.Catalog, schema)
	if databricks.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list %s.%s: %w", m.Databricks.Catalog, schema, err)
	}
	var dropped []string
	for _, t := range tables {
		if err := api.DeleteTable(ctx, t.FullName); err != nil {
			return dropped, fmt.Errorf("unregister %s: %w", t.FullName, err)
		}
		dropped = append(dropped, t.FullName)
	}
	return dropped, nil
}
