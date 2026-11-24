package service

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/vesahyp/clavesa/internal/access"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/modules"
	"github.com/vesahyp/clavesa/internal/orchestration/tfgen"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// AccessReport is the column tags of a workspace
// (docs/ideas/column-tags.md): the "access" block of clavesa.json and the
// tags of every transform output. The twin of `clavesa workspace access
// show --json` and GET /workspace/access (ADR-015).
type AccessReport struct {
	// Enabled is true when clavesa.json has an "access" block.
	Enabled bool `json:"enabled"`
	// TagKey is the workspace's Lake Formation tag key.
	TagKey string `json:"tag_key,omitempty"`
	// Tags are the declared values; Unreviewed is the value of every table
	// and column that no declaration tags.
	Tags       []string `json:"tags"`
	Unreviewed string   `json:"unreviewed"`
	// Audiences map a named audience to the values it reads. The default
	// audience reads "open" only.
	Audiences map[string][]string `json:"audiences"`
	// Tables lists every transform output that is a table, tagged or not.
	Tables []AccessTable `json:"tables"`
	// Problems are declarations that the deploy would refuse. Empty when
	// the tags are valid.
	Problems []string `json:"problems"`
}

// AccessTable is one transform output and its tags.
type AccessTable struct {
	Pipeline string `json:"pipeline"`
	Node     string `json:"node"`
	Output   string `json:"output"`
	// Database and Table are the Glue names.
	Database string `json:"database"`
	Table    string `json:"table"`
	// TableTag is the table's value; Unreviewed when the output sets none.
	TableTag string `json:"table_tag"`
	// Columns are the tagged columns, sorted by name.
	Columns []AccessColumn `json:"columns"`
}

// AccessColumn is one tagged column.
type AccessColumn struct {
	Name string `json:"name"`
	Tag  string `json:"tag"`
}

// AccessTags reads the workspace's column tags. It never touches AWS.
func (s *Service) AccessTags() (*AccessReport, error) {
	m, err := workspace.Load(s.workspace)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("not a workspace: no clavesa.json in %s", s.workspace)
	}
	rep := &AccessReport{
		Unreviewed: access.Unreviewed,
		Tags:       []string{},
		Audiences:  map[string][]string{},
		Tables:     []AccessTable{},
		Problems:   []string{},
	}
	cfg := m.Access
	if cfg != nil {
		rep.Enabled = true
		rep.TagKey = access.TagKey(m.Name)
		rep.Tags = append(rep.Tags, cfg.Tags...)
		for _, a := range cfg.AudienceNames() {
			rep.Audiences[a] = append([]string{}, cfg.Audiences[a]...)
		}
		if err := cfg.Validate(); err != nil {
			rep.Problems = append(rep.Problems, err.Error())
			cfg = nil // its values cannot be trusted for the checks below
		}
	}
	pipes, err := s.ListPipelines()
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	for _, p := range pipes {
		g, err := hclparser.Parse(filepath.Join(s.workspace, p.Dir))
		if err != nil {
			return nil, fmt.Errorf("pipeline %s: %w", p.Name, err)
		}
		db := identutil.EncodeGlueDatabase(m.CatalogIdentifier(), p.Schema)
		for _, n := range g.Nodes {
			if n.Type != "transform" {
				continue
			}
			defs, _ := n.Config["output_definitions"].(map[string]interface{})
			for _, key := range outputKeyList(n) {
				if outputFormat(n, key) == "json" {
					continue // a file, not a table
				}
				def, _ := defs[key].(map[string]interface{})
				tags, err := access.ParseOutput(def)
				where := fmt.Sprintf("%s: %s output %q", p.Name, n.ID, key)
				if err != nil {
					rep.Problems = append(rep.Problems, where+": "+err.Error())
					continue
				}
				if m.Access == nil || cfg != nil {
					if err := tags.Check(cfg); err != nil {
						rep.Problems = append(rep.Problems, where+": "+err.Error())
					}
				}
				t := AccessTable{
					Pipeline: p.Name,
					Node:     n.ID,
					Output:   key,
					Database: db,
					Table:    canonicalTableSegment(n.ID, defs, key),
					TableTag: tags.TableTag,
					Columns:  []AccessColumn{},
				}
				if t.TableTag == "" {
					t.TableTag = access.Unreviewed
				}
				for col, v := range tags.Columns {
					t.Columns = append(t.Columns, AccessColumn{Name: col, Tag: v})
				}
				sort.Slice(t.Columns, func(i, j int) bool { return t.Columns[i].Name < t.Columns[j].Name })
				// A workspace with no block lists only its tagged outputs
				// (each one is a problem); with a block, every table.
				if m.Access != nil || !tags.Empty() {
					rep.Tables = append(rep.Tables, t)
				}
			}
		}
	}
	sort.Slice(rep.Tables, func(i, j int) bool {
		a, b := rep.Tables[i], rep.Tables[j]
		if a.Database != b.Database {
			return a.Database < b.Database
		}
		return a.Table < b.Table
	})
	return rep, nil
}

// SyncLakeFormationTF makes lakeformation.tf match clavesa.json: written
// while the workspace has an "access" block, removed otherwise
// (docs/ideas/column-tags.md, slice 1a). Deploy calls it before the
// workspace plan. version "" means the binary's ModuleVersion; the
// lake-formation module is self-contained, so its tree is extracted when
// it is not there yet, as for hosted.tf.
func (s *Service) SyncLakeFormationTF(version string) error {
	m, err := s.loadManifest()
	if err != nil {
		return err
	}
	if m.Access == nil {
		return workspace.SyncLakeFormationTF(s.workspace, m, "")
	}
	if version == "" {
		version = ModuleVersion
	}
	if err := modules.Extract(s.workspace, version); err != nil {
		return fmt.Errorf("extract modules %s: %w", version, err)
	}
	return workspace.SyncLakeFormationTF(s.workspace, m, version)
}

// lakeFormationConfig is the tfgen side of column tags: set for a
// workspace with an "access" block and a remote backend (the pipeline
// reads the registration role from the workspace's remote state).
func lakeFormationConfig(ws *workspace.Manifest) *tfgen.LakeFormationConfig {
	if ws == nil || ws.Access == nil {
		return nil
	}
	return &tfgen.LakeFormationConfig{
		RoleExpr:           "data.terraform_remote_state.workspace.outputs.lf_register_role_arn",
		SystemLocationExpr: "data.terraform_remote_state.workspace.outputs.lf_system_location_arn",
		TagKeyExpr:         "data.terraform_remote_state.workspace.outputs.lf_tag_key",
	}
}
