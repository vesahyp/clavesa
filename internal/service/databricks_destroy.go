package service

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/hclparser"
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

// lineageCleaner is the part of the Databricks client the lineage cleanup
// needs; tests replace it.
type lineageCleaner interface {
	ListExternalMetadata(ctx context.Context) ([]databricks.ExternalMetadata, error)
	DeleteExternalMetadata(ctx context.Context, name string) error
}

var newLineageCleaner = func(host, clientID, secret string) lineageCleaner {
	return databricks.NewClient(host, clientID, secret)
}

// RemovePipelineLineage deletes the external lineage objects the
// pipeline's runs created in Unity Catalog (ADR-027 section 5), and with
// each object its lineage relationships. An object is the pipeline's when
// its clavesa.pipeline property is the pipeline's name; a name prefix
// would also match a pipeline whose name starts with this one's. The
// calls run as clavesa's service principal, which created the objects
// and so owns them: a person, even a workspace admin, has no MANAGE on
// them (GH #108).
//
// With keep == nil every object of the pipeline goes (destroy). With a
// keep set only the "clavesa transform" objects of nodes that are not in
// it go (deploy: a task removed from the job leaves no step in the
// Lineage graph); input objects stay.
//
// A workspace with no Databricks connection has nothing to remove.
func (s *Service) RemovePipelineLineage(ctx context.Context, dir string, keep map[string]bool) ([]string, error) {
	m, err := s.loadManifest()
	if err != nil || m.Databricks == nil {
		return nil, err
	}
	pipeline := filepath.Base(s.resolveDir(dir))
	cred, err := s.databricksCredential(m.Databricks.Credential)
	if err != nil {
		return nil, err
	}
	secret, err := s.resolveSecret(ctx, cred.Secret)
	if err != nil {
		return nil, fmt.Errorf("credential %q: %w", cred.Name, err)
	}
	api := newLineageCleaner(m.Databricks.Host, cred.ClientID, secret)
	objects, err := api.ListExternalMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("list external lineage objects: %w", err)
	}
	var removed []string
	for _, o := range objects {
		if o.Properties["clavesa.pipeline"] != pipeline {
			continue
		}
		if keep != nil && (o.EntityType != "clavesa transform" || keep[o.Properties["clavesa.node"]]) {
			continue
		}
		if err := api.DeleteExternalMetadata(ctx, o.Name); err != nil {
			return removed, fmt.Errorf("delete external lineage object %s: %w", o.Name, err)
		}
		removed = append(removed, o.Name)
	}
	return removed, nil
}

// PruneStaleLineage runs after a pipeline deploy: it removes the
// "clavesa transform" lineage objects of nodes the pipeline no longer has.
func (s *Service) PruneStaleLineage(ctx context.Context, dir string) ([]string, error) {
	g, err := hclparser.Parse(s.resolveDir(dir))
	if err != nil {
		return nil, fmt.Errorf("parse pipeline: %w", err)
	}
	keep := map[string]bool{}
	for _, n := range g.Nodes {
		keep[n.ID] = true
	}
	return s.RemovePipelineLineage(ctx, dir, keep)
}
