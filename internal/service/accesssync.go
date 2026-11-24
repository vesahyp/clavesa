package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	lftypes "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"

	"github.com/vesahyp/clavesa/internal/access"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// Column tags, applied (docs/ideas/column-tags.md, slice 1b). Terraform
// makes the tag key and tags the databases; the tables come from runs, so
// `clavesa deploy` and `clavesa workspace access sync` tag every table and
// column that exists, as the deploying principal (a data lake admin).

// systemHiddenTag is the value of the system catalog's columns that can
// quote row values: "raw" when the workspace declares it (its audience
// reads them), access.Unreviewed (no audience reads them) otherwise.
func systemHiddenTag(cfg *access.Config) string {
	if cfg != nil && cfg.Allows("raw") {
		return "raw"
	}
	return access.Unreviewed
}

// tagTarget is the wanted tags of one Glue table.
type tagTarget struct {
	Database, Table string
	// TableTag is the table's own value; "" means the table has none of
	// its own and inherits the database's.
	TableTag string
	// Columns maps a lower-case column name to its own value.
	Columns map[string]string
	// Declared is true for a transform output (a missing table is
	// reported); false for a system table (a missing one is normal).
	Declared bool
}

// AccessSyncResult says what a tag sync changed and what it could not do.
type AccessSyncResult struct {
	// Changes are the tag changes made, one line each.
	Changes []string `json:"changes"`
	// NotCreated are declared tables that do not exist yet: their first
	// run makes them, and they stay hidden (unreviewed) until the next
	// deploy or `clavesa workspace access sync`.
	NotCreated []string `json:"not_created"`
	// Untagged are tables in a pipeline database that no output declares;
	// they keep the database's "unreviewed".
	Untagged []string `json:"untagged"`
	// Warnings are column_tags that name a column the table does not have.
	Warnings []string `json:"warnings"`
	// ProgressScrubbed is how many _progress/ markers lost their error
	// text in the one-time scrub (0 after the first sync).
	ProgressScrubbed int `json:"progress_scrubbed"`
}

type accessSyncGlue interface {
	GetTable(context.Context, *glue.GetTableInput, ...func(*glue.Options)) (*glue.GetTableOutput, error)
	GetTables(context.Context, *glue.GetTablesInput, ...func(*glue.Options)) (*glue.GetTablesOutput, error)
}

type accessSyncLF interface {
	GetResourceLFTags(context.Context, *lakeformation.GetResourceLFTagsInput, ...func(*lakeformation.Options)) (*lakeformation.GetResourceLFTagsOutput, error)
	AddLFTagsToResource(context.Context, *lakeformation.AddLFTagsToResourceInput, ...func(*lakeformation.Options)) (*lakeformation.AddLFTagsToResourceOutput, error)
	RemoveLFTagsFromResource(context.Context, *lakeformation.RemoveLFTagsFromResourceInput, ...func(*lakeformation.Options)) (*lakeformation.RemoveLFTagsFromResourceOutput, error)
}

// AccessSync tags the workspace's tables and columns in Lake Formation
// from the transform outputs' table_tag and column_tags. A no-op (nil, nil)
// for a workspace with no "access" block. It refuses to run while the
// declarations have problems, so a bad value never reaches Lake Formation.
func (s *Service) AccessSync(ctx context.Context) (*AccessSyncResult, error) {
	m, err := s.loadManifest()
	if err != nil {
		return nil, err
	}
	if m == nil || m.Access == nil {
		return nil, nil
	}
	rep, err := s.AccessTags()
	if err != nil {
		return nil, err
	}
	if len(rep.Problems) > 0 {
		return nil, fmt.Errorf("column tags have problems; fix them first (clavesa workspace access show):\n  %s", strings.Join(rep.Problems, "\n  "))
	}
	sys := strings.ReplaceAll(m.SystemCatalogIdentifier(), "-", "_") + "__pipelines"
	targets := accessTargets(rep, sys, m.Access)
	pipes, err := s.ListPipelines()
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	dbs, err := s.deployedPipelineDatabases(pipes)
	if err != nil {
		return nil, err
	}
	cfg, err := s.loadAWSConfig(ctx)
	if err != nil {
		return nil, err
	}
	res, err := accessSync(ctx, glue.NewFromConfig(cfg), lakeformation.NewFromConfig(cfg), rep.TagKey, targets, dbs)
	if err != nil {
		return res, err
	}
	// Markers written before column tags can hold error text.
	if bucket := workspace.PipelineBucket(s.workspace); bucket != "" {
		n, err := scrubProgressErrors(ctx, s3.NewFromConfig(cfg), bucket)
		res.ProgressScrubbed = n
		if err != nil {
			return res, fmt.Errorf("scrub _progress/ error text: %w", err)
		}
	}
	return res, nil
}

// accessTargets turns the report into the wanted state: every declared
// output, plus the system tables whose columns can quote row values.
func accessTargets(rep *AccessReport, systemDB string, cfg *access.Config) []tagTarget {
	// The system database is "unreviewed" (fail closed): each system
	// table the hosted pages read gets "open" here, and a system table
	// that does not exist yet is reported like a declared one.
	hidden := systemHiddenTag(cfg)
	out := []tagTarget{
		{Database: systemDB, Table: "column_stats", TableTag: hidden, Columns: map[string]string{}, Declared: true},
		{Database: systemDB, Table: "node_runs", TableTag: access.Open, Columns: map[string]string{"error_msg": hidden}, Declared: true},
		{Database: systemDB, Table: "runs", TableTag: access.Open, Columns: map[string]string{"error_msg": hidden}, Declared: true},
		{Database: systemDB, Table: "tables", TableTag: access.Open, Columns: map[string]string{}, Declared: true},
	}
	for _, t := range rep.Tables {
		tt := tagTarget{Database: t.Database, Table: strings.ToLower(t.Table), Columns: map[string]string{}, Declared: true}
		if t.TableTag != access.Unreviewed {
			tt.TableTag = t.TableTag
		}
		for _, c := range t.Columns {
			tt.Columns[strings.ToLower(c.Name)] = c.Tag
		}
		out = append(out, tt)
	}
	return out
}

func accessSync(ctx context.Context, gc accessSyncGlue, lc accessSyncLF, key string, targets []tagTarget, pipelineDBs []string) (*AccessSyncResult, error) {
	res := &AccessSyncResult{Changes: []string{}, NotCreated: []string{}, Untagged: []string{}, Warnings: []string{}}
	declared := map[string]bool{}
	for _, t := range targets {
		name := t.Database + "." + t.Table
		declared[name] = true
		got, err := gc.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String(t.Database), Name: aws.String(t.Table)})
		if err != nil {
			var nf *gluetypes.EntityNotFoundException
			if errors.As(err, &nf) {
				if t.Declared {
					res.NotCreated = append(res.NotCreated, name)
				}
				continue
			}
			return res, fmt.Errorf("glue get-table %s: %w", name, err)
		}
		if err := syncTableTags(ctx, lc, key, t, glueColumns(got.Table), res); err != nil {
			return res, fmt.Errorf("tag %s: %w", name, err)
		}
	}
	for _, db := range pipelineDBs {
		var next *string
		for {
			tabs, err := gc.GetTables(ctx, &glue.GetTablesInput{DatabaseName: aws.String(db), NextToken: next})
			if err != nil {
				var nf *gluetypes.EntityNotFoundException
				if errors.As(err, &nf) {
					break
				}
				return res, fmt.Errorf("glue get-tables %s: %w", db, err)
			}
			for _, t := range tabs.TableList {
				if name := db + "." + aws.ToString(t.Name); !declared[name] {
					res.Untagged = append(res.Untagged, name)
				}
			}
			if tabs.NextToken == nil {
				break
			}
			next = tabs.NextToken
		}
	}
	sort.Strings(res.Untagged)
	return res, nil
}

// glueColumns are a table's column names, lower case, partition keys
// included, in Glue's order.
func glueColumns(t *gluetypes.Table) []string {
	var cols []string
	if t == nil {
		return cols
	}
	if t.StorageDescriptor != nil {
		for _, c := range t.StorageDescriptor.Columns {
			cols = append(cols, strings.ToLower(aws.ToString(c.Name)))
		}
	}
	for _, c := range t.PartitionKeys {
		cols = append(cols, strings.ToLower(aws.ToString(c.Name)))
	}
	return cols
}

// syncTableTags makes the table's own tag and its columns' own tags under
// key match t. Tags under other keys are left alone.
func syncTableTags(ctx context.Context, lc accessSyncLF, key string, t tagTarget, cols []string, res *AccessSyncResult) error {
	name := t.Database + "." + t.Table
	cur, err := lc.GetResourceLFTags(ctx, &lakeformation.GetResourceLFTagsInput{
		Resource:           &lftypes.Resource{Table: &lftypes.TableResource{DatabaseName: aws.String(t.Database), Name: aws.String(t.Table)}},
		ShowAssignedLFTags: aws.Bool(true),
	})
	if err != nil {
		return fmt.Errorf("get tags: %w", err)
	}
	haveTable := tagValue(cur.LFTagsOnTable, key)
	haveCols := map[string]string{}
	for _, c := range cur.LFTagsOnColumns {
		if v := tagValue(c.LFTags, key); v != "" {
			haveCols[strings.ToLower(aws.ToString(c.Name))] = v
		}
	}

	has := map[string]bool{}
	for _, c := range cols {
		has[c] = true
	}
	add := map[string][]string{} // value -> columns
	remove := map[string][]string{}
	for _, c := range cols {
		want, have := t.Columns[c], haveCols[c]
		switch {
		case want == have:
		case want != "":
			add[want] = append(add[want], c)
		default:
			remove[have] = append(remove[have], c)
		}
	}
	for _, v := range sortedTagKeys(add) {
		if err := addTag(ctx, lc, columnsRes(t, add[v]), key, v); err != nil {
			return err
		}
		res.Changes = append(res.Changes, fmt.Sprintf("%s: %s %s", name, strings.Join(add[v], ", "), v))
	}
	for _, v := range sortedTagKeys(remove) {
		if err := removeTag(ctx, lc, columnsRes(t, remove[v]), key, v); err != nil {
			return err
		}
		res.Changes = append(res.Changes, fmt.Sprintf("%s: %s %s removed", name, strings.Join(remove[v], ", "), v))
	}
	// The table's own tag goes last: if a column call fails, a new table
	// keeps the database's "unreviewed" and no column is left open.
	tableRes := &lftypes.Resource{Table: &lftypes.TableResource{DatabaseName: aws.String(t.Database), Name: aws.String(t.Table)}}
	switch {
	case t.TableTag != "" && haveTable != t.TableTag:
		if err := addTag(ctx, lc, tableRes, key, t.TableTag); err != nil {
			return err
		}
		res.Changes = append(res.Changes, fmt.Sprintf("%s: table %s", name, t.TableTag))
	case t.TableTag == "" && haveTable != "":
		if err := removeTag(ctx, lc, tableRes, key, haveTable); err != nil {
			return err
		}
		res.Changes = append(res.Changes, fmt.Sprintf("%s: table %s removed (inherits the database's value)", name, haveTable))
	}

	for _, c := range sortedTagKeys(t.Columns) {
		if !has[c] && t.Declared {
			res.Warnings = append(res.Warnings, fmt.Sprintf("%s: column_tags names %q, which the table does not have", name, c))
		}
	}
	return nil
}

func columnsRes(t tagTarget, cols []string) *lftypes.Resource {
	return &lftypes.Resource{TableWithColumns: &lftypes.TableWithColumnsResource{
		DatabaseName: aws.String(t.Database), Name: aws.String(t.Table), ColumnNames: cols,
	}}
}

func tagValue(tags []lftypes.LFTagPair, key string) string {
	for _, p := range tags {
		if aws.ToString(p.TagKey) == key && len(p.TagValues) > 0 {
			return p.TagValues[0]
		}
	}
	return ""
}

func addTag(ctx context.Context, lc accessSyncLF, r *lftypes.Resource, key, value string) error {
	out, err := lc.AddLFTagsToResource(ctx, &lakeformation.AddLFTagsToResourceInput{
		Resource: r,
		LFTags:   []lftypes.LFTagPair{{TagKey: aws.String(key), TagValues: []string{value}}},
	})
	if err != nil {
		return fmt.Errorf("add %s=%s: %w", key, value, err)
	}
	return lfFailures("add", out.Failures)
}

func removeTag(ctx context.Context, lc accessSyncLF, r *lftypes.Resource, key, value string) error {
	out, err := lc.RemoveLFTagsFromResource(ctx, &lakeformation.RemoveLFTagsFromResourceInput{
		Resource: r,
		LFTags:   []lftypes.LFTagPair{{TagKey: aws.String(key), TagValues: []string{value}}},
	})
	if err != nil {
		return fmt.Errorf("remove %s=%s: %w", key, value, err)
	}
	return lfFailures("remove", out.Failures)
}

// lfFailures turns the per-resource failures that the tag calls return
// (with no error) into one error.
func lfFailures(op string, fs []lftypes.LFTagError) error {
	var msgs []string
	for _, f := range fs {
		if f.Error != nil {
			msgs = append(msgs, aws.ToString(f.Error.ErrorMessage))
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	return fmt.Errorf("%s tag: %s", op, strings.Join(msgs, "; "))
}

func sortedTagKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
