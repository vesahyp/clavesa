package service

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/hclparser"
)

// maxClusterCols mirrors the runner's _MAX_CLUSTER_COLS: only the first four
// clustering columns are clustered, so only they can prune.
const maxClusterCols = 4

// idLikeCol is a column name that usually holds a random id or a hash. Its
// values spread over every file of a table, so a MERGE bounded on it reads
// the whole target: x_edge_request_id, session_id, uuid, event_hash.
var idLikeCol = regexp.MustCompile(`(?i)(^|_)(id|uuid|guid|hash)$`)

// timeLikeCol is a column name that usually holds a date or a time, which
// new rows share and which prunes files.
var timeLikeCol = regexp.MustCompile(`(?i)(^|_)(date|day|time|ts|timestamp|hour|month|year|week|at)($|_)`)

// mergeBoundCols is the runner's _merge_bound_cols: the merge_keys that are
// clustering columns (cluster_by, else merge_keys, first four), then
// bound_by. Keep the two in step.
func mergeBoundCols(mergeKeys, clusterBy, boundBy []string) []string {
	skip := clusterBy
	if len(skip) == 0 {
		skip = mergeKeys
	}
	if len(skip) > maxClusterCols {
		skip = skip[:maxClusterCols]
	}
	in := map[string]bool{}
	for _, c := range skip {
		in[c] = true
	}
	seen := map[string]bool{}
	var out []string
	for _, k := range mergeKeys {
		if in[k] && !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for _, b := range boundBy {
		if !seen[b] {
			seen[b] = true
			out = append(out, b)
		}
	}
	return out
}

// mergeBoundWarning says why a merge output's target scan cannot prune, or
// "" when it can or when the names do not tell. It is a name heuristic:
// it warns when no column bounds the scan, or when every bound column looks
// like an id and none like a date or a time.
//
// partitioned outputs get no cluster_by advice: Delta allows one layout.
func mergeBoundWarning(node, key string, mergeKeys, clusterBy, boundBy []string, partitioned bool) string {
	bound := mergeBoundCols(mergeKeys, clusterBy, boundBy)
	where := fmt.Sprintf("node %s, output %s", node, key)
	layout := ", and cluster_by on it"
	if partitioned {
		layout = ", and partition_by on it"
	}
	if len(bound) == 0 {
		return fmt.Sprintf("%s: the MERGE scan has no bound (no merge key is a clustering column and bound_by is empty), so every run reads the whole target table. Add bound_by = [\"<date column>\"] with a column the merge keys determine.", where)
	}
	for _, c := range bound {
		if !idLikeCol.MatchString(c) || timeLikeCol.MatchString(c) {
			return ""
		}
	}
	return fmt.Sprintf("%s: the MERGE scan is bounded only on %s, an id whose values spread over every file, so the bound prunes nothing and every run reads the whole target table. Add bound_by = [\"<date column>\"] with a column the merge keys determine (for example the event date)%s.", where, strings.Join(bound, ", "), layout)
}

// MergeBoundWarnings lists the merge outputs of the pipeline in dir whose
// MERGE target scan cannot prune (mergeBoundWarning), one line each, sorted.
// plan and deploy print them; they never fail.
func (s *Service) MergeBoundWarnings(dir string) ([]string, error) {
	g, err := hclparser.Parse(s.resolveDir(dir))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, n := range g.Nodes {
		if n.Type != "transform" {
			continue
		}
		for _, key := range outputKeyList(n) {
			if outputFormat(n, key) == "json" || outputMode(n, key) != "merge" {
				continue
			}
			if w := mergeBoundWarning(n.ID, key, outputMergeKeys(n, key), outputClusterBy(n, key), outputBoundBy(n, key), len(outputPartitionBy(n, key)) > 0); w != "" {
				out = append(out, w)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}
