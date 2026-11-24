package service

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation"
	lftypes "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"

	"github.com/vesahyp/clavesa/internal/access"
)

// fakeTagGlue holds tables as database -> table -> columns.
type fakeTagGlue struct {
	dbs map[string]map[string][]string
}

func (f *fakeTagGlue) GetTable(_ context.Context, in *glue.GetTableInput, _ ...func(*glue.Options)) (*glue.GetTableOutput, error) {
	cols, ok := f.dbs[aws.ToString(in.DatabaseName)][aws.ToString(in.Name)]
	if !ok {
		return nil, &gluetypes.EntityNotFoundException{Message: aws.String("no table")}
	}
	sd := &gluetypes.StorageDescriptor{}
	for _, c := range cols {
		sd.Columns = append(sd.Columns, gluetypes.Column{Name: aws.String(c)})
	}
	return &glue.GetTableOutput{Table: &gluetypes.Table{Name: in.Name, StorageDescriptor: sd}}, nil
}

func (f *fakeTagGlue) GetTables(_ context.Context, in *glue.GetTablesInput, _ ...func(*glue.Options)) (*glue.GetTablesOutput, error) {
	tabs, ok := f.dbs[aws.ToString(in.DatabaseName)]
	if !ok {
		return nil, &gluetypes.EntityNotFoundException{Message: aws.String("no database")}
	}
	out := &glue.GetTablesOutput{}
	for name := range tabs {
		out.TableList = append(out.TableList, gluetypes.Table{Name: aws.String(name)})
	}
	return out, nil
}

// fakeTagLF holds the assigned tags of one key: "db.table" and
// "db.table.column" -> value.
type fakeTagLF struct {
	tags  map[string]string
	calls []string
}

func (f *fakeTagLF) GetResourceLFTags(_ context.Context, in *lakeformation.GetResourceLFTagsInput, _ ...func(*lakeformation.Options)) (*lakeformation.GetResourceLFTagsOutput, error) {
	t := in.Resource.Table
	name := aws.ToString(t.DatabaseName) + "." + aws.ToString(t.Name)
	out := &lakeformation.GetResourceLFTagsOutput{}
	if v, ok := f.tags[name]; ok {
		out.LFTagsOnTable = []lftypes.LFTagPair{{TagKey: aws.String("k"), TagValues: []string{v}}}
	}
	for k, v := range f.tags {
		if col, ok := strings.CutPrefix(k, name+"."); ok {
			out.LFTagsOnColumns = append(out.LFTagsOnColumns, lftypes.ColumnLFTag{
				Name:   aws.String(col),
				LFTags: []lftypes.LFTagPair{{TagKey: aws.String("k"), TagValues: []string{v}}, {TagKey: aws.String("other"), TagValues: []string{"x"}}},
			})
		}
	}
	return out, nil
}

func (f *fakeTagLF) names(r *lftypes.Resource) []string {
	if r.Table != nil {
		return []string{aws.ToString(r.Table.DatabaseName) + "." + aws.ToString(r.Table.Name)}
	}
	var out []string
	for _, c := range r.TableWithColumns.ColumnNames {
		out = append(out, aws.ToString(r.TableWithColumns.DatabaseName)+"."+aws.ToString(r.TableWithColumns.Name)+"."+c)
	}
	return out
}

func (f *fakeTagLF) AddLFTagsToResource(_ context.Context, in *lakeformation.AddLFTagsToResourceInput, _ ...func(*lakeformation.Options)) (*lakeformation.AddLFTagsToResourceOutput, error) {
	for _, n := range f.names(in.Resource) {
		f.tags[n] = in.LFTags[0].TagValues[0]
		f.calls = append(f.calls, "add "+n+"="+in.LFTags[0].TagValues[0])
	}
	return &lakeformation.AddLFTagsToResourceOutput{}, nil
}

func (f *fakeTagLF) RemoveLFTagsFromResource(_ context.Context, in *lakeformation.RemoveLFTagsFromResourceInput, _ ...func(*lakeformation.Options)) (*lakeformation.RemoveLFTagsFromResourceOutput, error) {
	for _, n := range f.names(in.Resource) {
		delete(f.tags, n)
		f.calls = append(f.calls, "remove "+n)
	}
	return &lakeformation.RemoveLFTagsFromResourceOutput{}, nil
}

func TestAccessSync(t *testing.T) {
	gc := &fakeTagGlue{dbs: map[string]map[string][]string{
		"sys__pipelines": {"runs": {"run_id", "error_msg"}, "node_runs": {"run_id", "error_msg"}, "column_stats": {"min"}, "tables": {"name"}},
		"ws__taxi":       {"trips": {"pickup_ip", "fare", "old_col"}, "scratch": {"a"}},
	}}
	lc := &fakeTagLF{tags: map[string]string{
		"ws__taxi.trips.old_col": "ip",   // no longer declared: removed
		"ws__taxi.trips.fare":    "open", // already right: no call
	}}
	rep := &AccessReport{Tables: []AccessTable{
		{Database: "ws__taxi", Table: "trips", TableTag: "open", Columns: []AccessColumn{{Name: "Pickup_IP", Tag: "ip"}, {Name: "fare", Tag: "open"}, {Name: "gone", Tag: "ip"}}},
		{Database: "ws__taxi", Table: "revenue", TableTag: access.Unreviewed},
	}}
	targets := accessTargets(rep, "sys__pipelines", &access.Config{Tags: []string{"open", "ip"}})
	res, err := accessSync(context.Background(), gc, lc, "k", targets, []string{"ws__taxi"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"sys__pipelines.column_stats":        "unreviewed", // no "raw" declared
		"sys__pipelines.node_runs":           "open",
		"sys__pipelines.runs":                "open",
		"sys__pipelines.tables":              "open",
		"sys__pipelines.node_runs.error_msg": "unreviewed",
		"sys__pipelines.runs.error_msg":      "unreviewed",
		"ws__taxi.trips":                     "open",
		"ws__taxi.trips.pickup_ip":           "ip",
		"ws__taxi.trips.fare":                "open",
	}
	if !reflect.DeepEqual(lc.tags, want) {
		t.Errorf("tags\n got %v\nwant %v", lc.tags, want)
	}
	if !reflect.DeepEqual(res.NotCreated, []string{"ws__taxi.revenue"}) {
		t.Errorf("not created %v", res.NotCreated)
	}
	if !reflect.DeepEqual(res.Untagged, []string{"ws__taxi.scratch"}) {
		t.Errorf("untagged %v", res.Untagged)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], `"gone"`) {
		t.Errorf("warnings %v", res.Warnings)
	}

	// A second sync changes nothing.
	lc.calls = nil
	res, err = accessSync(context.Background(), gc, lc, "k", targets, []string{"ws__taxi"})
	if err != nil || len(lc.calls) != 0 || len(res.Changes) != 0 {
		t.Errorf("second sync: err %v, calls %v, changes %v", err, lc.calls, res.Changes)
	}

	// table_tag taken away: the table's own tag goes, it inherits again.
	rep.Tables[0].TableTag = access.Unreviewed
	targets = accessTargets(rep, "sys__pipelines", &access.Config{Tags: []string{"open", "ip", "raw"}})
	if _, err := accessSync(context.Background(), gc, lc, "k", targets, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := lc.tags["ws__taxi.trips"]; ok {
		t.Error("trips kept its table tag")
	}
	if lc.tags["sys__pipelines.column_stats"] != "raw" || lc.tags["sys__pipelines.runs.error_msg"] != "raw" {
		t.Errorf("with raw declared, the system columns get raw: %v", lc.tags)
	}
}

// failColumnsLF fails every column tag call.
type failColumnsLF struct{ fakeTagLF }

func (f *failColumnsLF) AddLFTagsToResource(ctx context.Context, in *lakeformation.AddLFTagsToResourceInput, o ...func(*lakeformation.Options)) (*lakeformation.AddLFTagsToResourceOutput, error) {
	if in.Resource.TableWithColumns != nil {
		return nil, errors.New("throttled")
	}
	return f.fakeTagLF.AddLFTagsToResource(ctx, in, o...)
}

// A failed column call leaves a new table without its "open" tag, so no
// column is left open (review finding 3).
func TestAccessSyncColumnsBeforeTable(t *testing.T) {
	gc := &fakeTagGlue{dbs: map[string]map[string][]string{"ws__taxi": {"trips": {"pickup_ip", "fare"}}}}
	lc := &failColumnsLF{fakeTagLF{tags: map[string]string{}}}
	targets := []tagTarget{{Database: "ws__taxi", Table: "trips", TableTag: "open", Columns: map[string]string{"pickup_ip": "ip"}, Declared: true}}
	if _, err := accessSync(context.Background(), gc, lc, "k", targets, nil); err == nil {
		t.Fatal("want the column error")
	}
	if v, ok := lc.tags["ws__taxi.trips"]; ok {
		t.Errorf("trips got its table tag %q although its columns failed", v)
	}
}
