package bundle

import (
	"fmt"
	"sort"
	"strings"
)

// Settings is the typed `clavesa` variable of a target (ADR-031 section 1).
type Settings struct {
	Workspace string
	// Catalog is "unity" or "glue".
	Catalog  string
	Catalogs map[string]string
	// Place maps a task ("task" or "job.task") to "clavesa" or "databricks".
	Place   map[string]string
	Compute map[string]ComputeSetting
	Compare map[string]CompareSetting
	Shadow  bool
	Tables  map[string]TableLists
	// ReplacesJob is the Databricks job ID the target replaces after the
	// switch (ADR-031 section 9); 0 before it.
	ReplacesJob int64
}

type ComputeSetting struct {
	Kind     string  // lambda | fargate
	CPU      float64 // vCPU
	MemoryGB float64
}

type CompareSetting struct {
	Partition string
	Sums      []string
}

type TableLists struct {
	Reads  []string
	Writes []string
}

var settingsKeys = []string{"workspace", "catalog", "catalogs", "place", "compute", "compare", "shadow", "tables", "replaces_job"}

// parseSettings validates the clavesa variable. Every problem is a refuse
// finding (an unknown key or task name is an error, section 1).
func parseSettings(b *Bundle, jobs []*Job) (*Settings, []Finding) {
	var fs []Finding
	bad := func(format string, a ...any) {
		fs = append(fs, Finding{Severity: Refuse, Message: "clavesa variable: " + fmt.Sprintf(format, a...)})
	}
	s := &Settings{
		Catalogs: map[string]string{}, Place: map[string]string{}, Compute: map[string]ComputeSetting{},
		Compare: map[string]CompareSetting{}, Tables: map[string]TableLists{},
	}
	c := b.Clavesa
	for k := range c {
		if !contains(settingsKeys, k) {
			bad("unknown key %q (known: %s)", k, strings.Join(settingsKeys, ", "))
		}
	}
	s.Workspace, _ = c["workspace"].(string)
	if v, ok := c["catalog"]; ok {
		s.Catalog, _ = v.(string)
		if s.Catalog != "unity" && s.Catalog != "glue" {
			bad("catalog is %v; it must be unity or glue", v)
		}
	} else if b.Host != "" {
		s.Catalog = "unity"
	} else {
		s.Catalog = "glue"
	}
	if s.Catalog == "unity" && b.Host == "" {
		bad("catalog: unity needs a Databricks workspace host in the target")
	}
	if v, ok := c["shadow"]; ok {
		bv, isBool := v.(bool)
		if !isBool {
			bad("shadow must be true or false")
		}
		s.Shadow = bv
	} else {
		s.Shadow = s.Catalog == "unity"
	}

	if v, ok := c["replaces_job"]; ok {
		n, isNum := numOf(v)
		if !isNum || n < 0 || n != float64(int64(n)) {
			bad("replaces_job must be a Databricks job ID (a number)")
		}
		s.ReplacesJob = int64(n)
		if s.Catalog != "unity" {
			bad("replaces_job needs catalog: unity")
		}
		if s.Shadow && n > 0 {
			bad("replaces_job is for after the switch: set shadow: false with it")
		}
	}
	if m, ok := mapOf(c, "catalogs", bad); ok {
		if s.Catalog != "glue" && len(m) > 0 {
			bad("catalogs maps Unity Catalog names to Glue and needs catalog: glue")
		}
		for k, v := range m {
			sv, ok := v.(string)
			if !ok || sv == "" {
				bad("catalogs.%s must be a catalog name", k)
				continue
			}
			s.Catalogs[k] = sv
		}
	}

	taskRef := func(section, ref string) bool {
		if _, err := findTask(jobs, ref); err != nil {
			bad("%s.%s: %v", section, ref, err)
			return false
		}
		return true
	}
	if m, ok := mapOf(c, "place", bad); ok {
		for k, v := range m {
			if !taskRef("place", k) {
				continue
			}
			sv, _ := v.(string)
			if sv != "clavesa" && sv != "databricks" {
				bad("place.%s is %v; it must be clavesa or databricks", k, v)
				continue
			}
			if sv == "databricks" && b.Host == "" {
				bad("place.%s: databricks needs a Databricks workspace host in the target", k)
			}
			s.Place[k] = sv
		}
	}
	if m, ok := mapOf(c, "compute", bad); ok {
		for k, v := range m {
			if !taskRef("compute", k) {
				continue
			}
			vm, ok := v.(map[string]any)
			if !ok {
				bad("compute.%s must be a map with kind, cpu and memory_gb", k)
				continue
			}
			cs := ComputeSetting{}
			for ck := range vm {
				if !contains([]string{"kind", "cpu", "memory_gb"}, ck) {
					bad("compute.%s: unknown key %q (known: kind, cpu, memory_gb)", k, ck)
				}
			}
			cs.Kind, _ = vm["kind"].(string)
			if cs.Kind != "lambda" && cs.Kind != "fargate" {
				bad("compute.%s.kind must be lambda or fargate", k)
			}
			if n, ok := numOf(vm["cpu"]); ok {
				cs.CPU = n
			}
			if n, ok := numOf(vm["memory_gb"]); ok {
				cs.MemoryGB = n
			}
			s.Compute[k] = cs
		}
	}
	if m, ok := mapOf(c, "compare", bad); ok {
		for table, v := range m {
			if strings.Count(table, ".") != 2 {
				bad("compare.%s: use the three-level name catalog.schema.table", table)
			}
			vm, _ := v.(map[string]any)
			cs := CompareSetting{}
			for ck := range vm {
				if !contains([]string{"partition", "sums"}, ck) {
					bad("compare.%s: unknown key %q (known: partition, sums)", table, ck)
				}
			}
			cs.Partition, _ = vm["partition"].(string)
			cs.Sums = stringsOf(vm["sums"])
			s.Compare[table] = cs
		}
	}
	if m, ok := mapOf(c, "tables", bad); ok {
		for k, v := range m {
			if !taskRef("tables", k) {
				continue
			}
			vm, _ := v.(map[string]any)
			for ck := range vm {
				if !contains([]string{"reads", "writes"}, ck) {
					bad("tables.%s: unknown key %q (known: reads, writes)", k, ck)
				}
			}
			tl := TableLists{Reads: stringsOf(vm["reads"]), Writes: stringsOf(vm["writes"])}
			for _, t := range append(append([]string{}, tl.Reads...), tl.Writes...) {
				if strings.Count(t, ".") != 2 {
					bad("tables.%s: %q is not a three-level name catalog.schema.table", k, t)
				}
			}
			s.Tables[k] = tl
		}
	}
	sort.Slice(fs, func(i, j int) bool { return fs[i].Message < fs[j].Message })
	return s, fs
}

// findTask finds a task by "task_key" or "job.task_key".
func findTask(jobs []*Job, ref string) (*Task, error) {
	jobKey, taskKey := "", ref
	if i := strings.Index(ref, "."); i >= 0 {
		jobKey, taskKey = ref[:i], ref[i+1:]
	}
	var found []*Task
	var where []string
	for _, j := range jobs {
		if jobKey != "" && j.Key != jobKey {
			continue
		}
		for i := range j.Tasks {
			if j.Tasks[i].TaskKey == taskKey {
				found = append(found, &j.Tasks[i])
				where = append(where, j.Key)
			}
		}
	}
	switch len(found) {
	case 0:
		return nil, fmt.Errorf("no task %q in the bundle", ref)
	case 1:
		return found[0], nil
	}
	return nil, fmt.Errorf("task %q is in several jobs (%s): write job.task", ref, strings.Join(where, ", "))
}

// lookup returns the setting for a task, by "job.task" first, then "task".
func lookupTask[T any](m map[string]T, job, task string) (T, bool) {
	if v, ok := m[job+"."+task]; ok {
		return v, true
	}
	v, ok := m[task]
	return v, ok
}

func mapOf(c map[string]any, key string, bad func(string, ...any)) (map[string]any, bool) {
	v, ok := c[key]
	if !ok || v == nil {
		return nil, false
	}
	m, ok := v.(map[string]any)
	if !ok {
		bad("%s must be a map", key)
		return nil, false
	}
	return m, true
}

func stringsOf(v any) []string {
	l, _ := v.([]any)
	out := make([]string, 0, len(l))
	for _, e := range l {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func numOf(v any) (float64, bool) {
	switch t := v.(type) {
	case int:
		return float64(t), true
	case float64:
		return t, true
	}
	return 0, false
}

func contains(l []string, s string) bool {
	for _, e := range l {
		if e == s {
			return true
		}
	}
	return false
}
