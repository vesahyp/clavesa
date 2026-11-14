package service

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/bundle"
)

// BundleCompareTable is the comparison of one table a bundle job writes:
// the Databricks job's table against clavesa's shadow table.
type BundleCompareTable struct {
	Table   string                   `json:"table"`
	Shadow  string                   `json:"shadow"`
	Skipped string                   `json:"skipped,omitempty"` // why it was not compared
	Result  *DatabricksCompareResult `json:"result,omitempty"`
	Error   string                   `json:"error,omitempty"`
}

// BundleCompareResult is `clavesa bundle compare` for one job: PASS when
// every compared table passes.
type BundleCompareResult struct {
	Job      string               `json:"job"`
	Pipeline string               `json:"pipeline"`
	Pass     bool                 `json:"pass"`
	Tables   []BundleCompareTable `json:"tables"`
}

// compareTables is the per-table comparison; a seam for tests.
var compareTables = func(s *Service, ctx context.Context, pipeline string, in DatabricksCompareInput) (DatabricksCompareResult, error) {
	return s.CompareDatabricksTables(ctx, pipeline, in)
}

// CompareBundleJob runs the ADR-027 comparison for every table a job
// writes (ADR-031 section 9), with the partition and sums from
// clavesa.compare; days > 0 limits it to the last days of the partition.
// It costs no DBUs: the deployed pipeline's runner reads both tables.
func (s *Service) CompareBundleJob(ctx context.Context, o BundleOptions, job string, days int) (*BundleCompareResult, error) {
	b, err := ResolveBundle(ctx, o)
	if err != nil {
		return nil, err
	}
	r, err := bundle.Plan(b)
	if err != nil {
		return nil, err
	}
	set := r.Settings()
	if set.Catalog != "unity" || !set.Shadow {
		return nil, fmt.Errorf("bundle compare needs a target with catalog: unity in shadow mode (target %s has catalog: %s, shadow: %t)", b.Target, set.Catalog, set.Shadow)
	}
	jf, err := bundle.BuildJobFile(b, r, job)
	if err != nil {
		return nil, err
	}
	out := &BundleCompareResult{Job: job, Pipeline: bundle.PipelineName(b.Name, job), Pass: true}
	tables := append([]string{}, jf.Writes...)
	sort.Strings(tables)
	if len(tables) == 0 {
		return nil, fmt.Errorf("job %s writes no tables clavesa knows: nothing to compare", job)
	}
	for _, t := range tables {
		row := BundleCompareTable{Table: t, Shadow: t + "_clavesa"}
		cs, ok := set.Compare[t]
		if !ok || cs.Partition == "" {
			row.Skipped = "no partition column: set clavesa.compare." + t + ".partition (and sums) in the target"
			out.Tables = append(out.Tables, row)
			continue
		}
		in := DatabricksCompareInput{Existing: t, Shadow: row.Shadow, Partition: cs.Partition, Sums: cs.Sums}
		if days > 0 {
			in.Where = fmt.Sprintf("%s >= date_sub(current_date(), %d)", cs.Partition, days)
		}
		res, err := compareTables(s, ctx, out.Pipeline, in)
		if err != nil {
			row.Error = err.Error()
			out.Pass = false
		} else {
			row.Result = &res
			out.Pass = out.Pass && res.Pass
		}
		out.Tables = append(out.Tables, row)
	}
	compared := 0
	for _, t := range out.Tables {
		if t.Skipped == "" {
			compared++
		}
	}
	if compared == 0 {
		out.Pass = false
	}
	return out, nil
}

// BundleSwitchSteps is what `clavesa bundle switch` prints: the switch of
// ADR-027 (getting-started steps 10 and 11, cookbook
// replace-a-databricks-step steps 3 to 5) for every table of the job, in
// order. It runs nothing.
func BundleSwitchSteps(ctx context.Context, o BundleOptions, job, dbxTarget string) (string, error) {
	b, err := ResolveBundle(ctx, o)
	if err != nil {
		return "", err
	}
	r, err := bundle.Plan(b)
	if err != nil {
		return "", err
	}
	if r.Settings().Catalog != "unity" {
		return "", fmt.Errorf("bundle switch is for a target with catalog: unity (target %s has %s)", b.Target, r.Settings().Catalog)
	}
	jf, err := bundle.BuildJobFile(b, r, job)
	if err != nil {
		return "", err
	}
	if dbxTarget == "" {
		dbxTarget = "<the Databricks target, for example prod>"
	}
	tables := append([]string{}, jf.Writes...)
	sort.Strings(tables)
	var sb strings.Builder
	w := func(f string, a ...any) { fmt.Fprintf(&sb, f, a...) }
	w("Switch of job %s (bundle %s) from Databricks to clavesa. Nothing here runs by itself.\n", job, b.Name)
	w("Do it right after a clavesa run, with `clavesa bundle compare -t %s --job %s` passing every day for 7 days.\n\n", b.Target, job)
	w("1. History. A person who can run SQL in Databricks gets MODIFY and MANAGE on the shadow tables, then copies\n")
	w("   the days before shadow mode began into them, between two clavesa runs:\n\n")
	for _, t := range tables {
		cs := r.Settings().Compare[t]
		part := cs.Partition
		if part == "" {
			part = "<partition column>"
		}
		w("   GRANT MODIFY, MANAGE, SELECT ON TABLE %s_clavesa TO `<you>`;\n", t)
		w("   INSERT INTO %s_clavesa SELECT * FROM %s WHERE %s < DATE'<first day of shadow mode>';\n\n", t, t, part)
	}
	w("   Then compare the whole tables: clavesa bundle compare -t %s --job %s\n\n", b.Target, job)
	w("2. Pause the Databricks job: set pause_status: PAUSED on its schedule or trigger in the %s target\n", dbxTarget)
	w("   (or presets: { trigger_pause_status: PAUSED }), run databricks bundle deploy -t %s, and wait until the\n", dbxTarget)
	w("   job has no active run. Compare once more; it must pass.\n\n")
	w("3. Names. For each MANAGED table, swap the names (for an EXTERNAL table skip this: clavesa takes it over in place,\n")
	w("   and the shadow table can be dropped after the switch):\n\n")
	for _, t := range tables {
		w("   ALTER TABLE %s RENAME TO %s_before_clavesa;\n", t, t)
		w("   ALTER TABLE %s_clavesa RENAME TO %s;\n\n", t, t)
	}
	w("4. Grants. Copy each old table's grants to the new one (the owner stays clavesa's service principal):\n\n")
	for _, t := range tables {
		w("   databricks grants get table %s_before_clavesa -o json \\\n", t)
		w("     | python3 -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps({\"changes\": [{\"principal\": a[\"principal\"], \"add\": a[\"privileges\"]} for a in d.get(\"privilege_assignments\", [])]}))' \\\n")
		w("     > grants.json\n")
		w("   databricks grants update table %s --json @grants.json\n\n", t)
	}
	w("5. Point clavesa at the real names. In databricks.yml, target %s, set in the clavesa variable:\n\n", b.Target)
	w("     shadow: false\n")
	w("     replaces_job: <the Databricks job ID of %s>\n\n", job)
	w("   so a run refuses to write while that job is unpaused or running. Then:\n\n")
	w("   clavesa bundle deploy -t %s --job %s --yes\n", b.Target, job)
	w("   clavesa bundle run -t %s --job %s --wait\n\n", b.Target, job)
	w("6. Check: a SQL warehouse query as one of the readers, each table's Lineage tab (the clavesa task upstream),\n")
	w("   and the jobs that come next. Keep the <table>_before_clavesa tables about a month, then drop them.\n\n")
	w("Roll-back: swap the names back (ALTER TABLE ... RENAME in reverse), set shadow: true and remove replaces_job,\n")
	w("deploy, and unpause the Databricks job. An external table taken over in place goes back with RESTORE TABLE\n")
	w("(docs/cookbook/replace-a-databricks-step.md, step 5).\n")
	return sb.String(), nil
}
