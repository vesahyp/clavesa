package service

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/credentials"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/sources"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// SyncOrchestration generates (or overwrites) orchestration.tf in dir from the
// current pipeline graph. Trigger configuration is read from pipeline variables
// (trigger_schedule, trigger_batch_window) automatically. Pass schedule as a
// non-empty string to override; pass empty string to read from pipeline vars.
//
// The orchestration graph is transform-only: each transform becomes one Step
// Functions Task state that invokes its runner Lambda with a {inputs, outputs}
// payload. Source and destination nodes contribute paths via the inputs map but
// are not invoked directly. Same payload shape as `clavesa pipeline run`.
func (s *Service) SyncOrchestration(dir, schedule string) error {
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return err
	}

	// Workspace catalog identifier — both the inputs-resolution loop
	// (cross-pipeline references, ADR-016 slice 2) and the trailing
	// namespace attrs block need it. Standalone-pipeline fallback uses
	// the literal "clavesa" so the encoded Glue DB lands at
	// `clavesa__<schema>` when no workspace context is available.
	ws, _ := workspace.Load(s.workspace)
	catalog := "clavesa"
	if ws != nil {
		catalog = ws.CatalogIdentifier()
	}

	// ADR-016 §5: refuse to emit if this pipeline's schema is already owned
	// by another pipeline in the workspace. Catches hand-edits to
	// `variable "schema"` that the create-time check can't see; `pipeline
	// upgrade` is covered transitively (it re-runs SyncOrchestration).
	pipelineName := filepath.Base(abs)
	if err := s.validateSchemaOwnership(pipelineName, resolvePipelineSchema(abs, pipelineName)); err != nil {
		return err
	}

	transformIDs := make([]string, 0)
	nodeByID := make(map[string]graph.Node, len(g.Nodes))
	for _, n := range g.Nodes {
		nodeByID[n.ID] = n
		if n.Type == "transform" {
			transformIDs = append(transformIDs, n.ID)
		}
	}
	sort.Strings(transformIDs)

	edgesByToNode := make(map[string][]graph.Edge)
	edgesByFromNode := make(map[string][]graph.Edge)
	for _, e := range g.Edges {
		edgesByToNode[e.ToNode] = append(edgesByToNode[e.ToNode], e)
		edgesByFromNode[e.FromNode] = append(edgesByFromNode[e.FromNode], e)
	}

	var nodeLines strings.Builder
	for _, id := range transformIDs {
		fmt.Fprintf(&nodeLines, "    %s = {\n", id)
		fmt.Fprintf(&nodeLines, "      lambda_function_arn = module.%s.lambda_function_arn\n", id)

		// Inputs: alias → upstream output reference. Sorted by alias for
		// deterministic output across runs.
		//
		// Source upstreams are pass-through Parquet at table_path; runner reads
		// via spark.read.parquet(). Transform upstreams write Iceberg tables to
		// the shared warehouse (per ADR-013); runner reads via spark.table()
		// using the catalog identifier "clavesa.<catalog_db>.<catalog_table>"
		// composed from the transform module's outputs. Passing table_path for
		// transform upstreams is wrong — no Parquet is written there, only the
		// Iceberg table at the warehouse path under <db>.db/<table>__<key>.
		incoming := edgesByToNode[id]
		sort.Slice(incoming, func(i, j int) bool {
			return inputAlias(incoming[i]) < inputAlias(incoming[j])
		})
		fmt.Fprintf(&nodeLines, "      inputs              = {\n")
		// ADR-016 slice 2: cross-pipeline / external-table references.
		// `<schema>.<table>` literals in the transform's HCL inputs map
		// resolve against the workspace catalog at emit time. Runner
		// reads via spark.table() against the encoded Glue DB, same
		// shape as intra-pipeline transform→transform edges below — so
		// the runtime contract is unchanged.
		if extInputs, ok := nodeByID[id].Config["external_inputs"].(map[string]interface{}); ok {
			aliases := make([]string, 0, len(extInputs))
			for a := range extInputs {
				aliases = append(aliases, a)
			}
			sort.Strings(aliases)
			for _, alias := range aliases {
				ref, _ := extInputs[alias].(string)
				tableID, err := identutil.EncodeExternalTableRef(catalog, ref)
				if err != nil {
					return fmt.Errorf("transform %q input %q: %w", id, alias, err)
				}
				fmt.Fprintf(&nodeLines, "        %s = %q\n", alias, tableID)
			}
		}
		// ADR-017 slice 1: workspace-source-registry references next.
		// `sources.<name>` literals in the transform's HCL inputs map
		// resolve against the workspace registry; emit kind-specific
		// runner descriptors directly (slice 1: only http).
		if srcInputs, ok := nodeByID[id].Config["source_inputs"].(map[string]interface{}); ok {
			aliases := make([]string, 0, len(srcInputs))
			for a := range srcInputs {
				aliases = append(aliases, a)
			}
			sort.Strings(aliases)
			store := sources.New(s.workspace)
			credStore := credentials.New(s.workspace)
			isCloud := isCloudCompute(nodeByID[id])
			for _, alias := range aliases {
				name := ""
				switch v := srcInputs[alias].(type) {
				case string:
					// Legacy `inputs = { x = "sources.X" }` shape, or
					// kind=http sentinel from v0.22.0 AttachSource.
					name = strings.TrimPrefix(v, "sources.")
				case map[string]interface{}:
					// v0.22.0 typed source_inputs[alias] = {spec_name=…,…}.
					if sn, ok := v["spec_name"].(string); ok {
						name = sn
					}
				}
				if name == "" {
					return fmt.Errorf("transform %q input %q: malformed source_inputs entry %v", id, alias, srcInputs[alias])
				}
				spec, err := store.Get(name)
				if err != nil {
					return fmt.Errorf("transform %q input %q: source %q not registered (workspace registry)", id, alias, name)
				}
				credBlock := ""
				if spec.Credentials != "" {
					cred, err := credStore.Get(spec.Credentials)
					if err != nil {
						return fmt.Errorf("source %q references credential %q which is not registered", name, spec.Credentials)
					}
					// Cloud deploys can't resolve env: / file:
					// backends — Lambda runtime has neither the
					// developer's shell env nor the workspace
					// directory. Reject loudly at emit time so
					// users learn before `terraform apply`.
					if isCloud {
						switch cred.SecretBackend() {
						case "env", "file":
							return fmt.Errorf("source %q credential %q uses local-only backend %q; cloud-deployed transforms require an arn:aws:secretsmanager:... reference",
								name, cred.Name, cred.SecretBackend())
						}
					}
					credBlock = formatCredentialHCL(cred)
				}
				switch spec.Kind {
				case "http":
					fmt.Fprintf(&nodeLines,
						"        %s = { kind = \"http\", url = %q, format = %q%s }\n",
						alias, spec.URL, spec.Format, credBlock)
				case "s3":
					if len(spec.Partitions) > 0 {
						// Incremental S3 read — runner walks partition
						// tree under the prefix, advances watermark
						// each run. Same descriptor shape inline-module
						// partitioned sources emit (orchestration.go
						// line ~180), so the runner branch is unchanged.
						startFrom := spec.StartFrom
						if startFrom == "" {
							startFrom = "all"
						}
						quoted := make([]string, len(spec.Partitions))
						for i, p := range spec.Partitions {
							quoted[i] = fmt.Sprintf("%q", p)
						}
						fmt.Fprintf(&nodeLines,
							"        %s = { kind = \"partitioned_path\", path = \"s3://%s/%s\", partitions = [%s], start_from = %q }\n",
							alias, spec.Bucket, spec.Prefix, strings.Join(quoted, ", "), startFrom)
						break
					}
					// Slice 3: same-account reads use the deploy
					// role; the credBlock is unused for kind=s3
					// today (cross-account with assume-role lands in
					// a later slice). Emit it anyway so the runner
					// can branch on it once we wire that path; today
					// the runner ignores credentials on kind=s3.
					fmt.Fprintf(&nodeLines,
						"        %s = { kind = \"s3\", bucket = %q, prefix = %q, format = %q%s }\n",
						alias, spec.Bucket, spec.Prefix, spec.Format, credBlock)
				default:
					return fmt.Errorf("source %q kind %q not supported", name, spec.Kind)
				}
			}
		}
		incrementalAliases := incrementalInputAliases(nodeByID[id])
		for _, e := range incoming {
			alias := inputAlias(e)
			fromOutput := "default"
			from := nodeByID[e.FromNode]
			switch {
			case from.Type == "transform" && incrementalAliases[alias]:
				// v0.24.0: snapshot-bounded Iceberg read. Runner stores
				// a watermark per (consumer_node, alias) pair and reads
				// only the snapshot range committed since the last
				// successful run. Without this branch, downstream
				// transforms full-read their upstream on every run —
				// fine for nightly aggregates over small data, wasteful
				// for high-throughput pipelines where bronze accumulates
				// millions of rows.
				watermarkAlias := id + "__" + alias
				fmt.Fprintf(&nodeLines,
					"        %s = { kind = \"iceberg_table_incremental\", table = \"clavesa.${module.%s.outputs[%q].catalog_db}.${module.%s.outputs[%q].catalog_table}\", alias = %q }\n",
					alias,
					e.FromNode, fromOutput,
					e.FromNode, fromOutput,
					watermarkAlias)
			case from.Type == "transform":
				// Iceberg upstream — read via spark.table().
				fmt.Fprintf(&nodeLines,
					"        %s = \"clavesa.${module.%s.outputs[%q].catalog_db}.${module.%s.outputs[%q].catalog_table}\"\n",
					alias, e.FromNode, fromOutput, e.FromNode, fromOutput)
			case from.Type == "source" && sourceHasPartitions(from):
				// Partitioned S3 source — runner walks the partition tree,
				// filters by stored watermark, reads only new partitions.
				fmt.Fprintf(&nodeLines,
					"        %s = {\n          kind       = \"partitioned_path\"\n          path       = module.%s.outputs[%q].table_path\n          partitions = module.%s.outputs[%q].partitions\n          start_from = module.%s.outputs[%q].start_from\n        }\n",
					alias,
					e.FromNode, fromOutput,
					e.FromNode, fromOutput,
					e.FromNode, fromOutput)
			default:
				// Pass-through S3 source — runner reads via spark.read.parquet().
				fmt.Fprintf(&nodeLines, "        %s = module.%s.outputs[%q].table_path\n",
					alias, e.FromNode, fromOutput)
			}
		}
		fmt.Fprintf(&nodeLines, "      }\n")

		// Outputs: one entry per declared output_definitions key (or
		// just "default" when nothing's declared). Each entry is either
		// a bare string (Iceberg auto-table or destination path) or a
		// dict (when mode != replace or merge_keys is set, so the
		// runner has the per-key write semantics it needs). The
		// orchestration module's `nodes` variable is typed `any`, so
		// the outputs map can mix string and dict values across keys.
		//
		// Routing: any outgoing edge to a destination consumes the
		// transform's "default" output today. Edges don't carry
		// from_output yet (graph.Edge has no FromOutput field), so
		// non-default outputs always land as Iceberg auto-tables; the
		// per-output destination routing slice gets its own commit.
		defaultDest := `""`
		for _, out := range edgesByFromNode[id] {
			if nodeByID[out.ToNode].Type == "destination" {
				defaultDest = fmt.Sprintf("module.%s.target_path", out.ToNode)
				break
			}
		}
		outputKeys := outputKeyList(nodeByID[id])
		// Single "default" replace + no merge_keys: keep the legacy
		// bare-string form for back-compat with pipelines whose
		// orchestration.tf was emitted before v0.24.0. The runner reads
		// it identically; emitter consistency is the only thing this
		// short branch buys.
		if len(outputKeys) == 1 && outputKeys[0] == "default" &&
			outputMode(nodeByID[id], "default") == "replace" &&
			len(outputMergeKeys(nodeByID[id], "default")) == 0 &&
			!outputStats(nodeByID[id], "default") {
			fmt.Fprintf(&nodeLines, "      outputs             = { default = %s }\n", defaultDest)
		} else {
			entries := make([]string, 0, len(outputKeys))
			for _, key := range outputKeys {
				dest := `""`
				if key == "default" {
					dest = defaultDest
				}
				mode := outputMode(nodeByID[id], key)
				mergeKeys := outputMergeKeys(nodeByID[id], key)
				stats := outputStats(nodeByID[id], key)
				if mode == "replace" && len(mergeKeys) == 0 && !stats {
					entries = append(entries, fmt.Sprintf("%s = %s", key, dest))
					continue
				}
				quoted := make([]string, 0, len(mergeKeys))
				for _, k := range mergeKeys {
					quoted = append(quoted, fmt.Sprintf("%q", k))
				}
				statsAttr := ""
				if stats {
					statsAttr = ", stats = true"
				}
				entries = append(entries, fmt.Sprintf(
					"%s = { kind = \"iceberg_table\", table_id = %s, mode = %q, merge_keys = [%s]%s }",
					key, dest, mode, strings.Join(quoted, ", "), statsAttr))
			}
			fmt.Fprintf(&nodeLines, "      outputs             = { %s }\n", strings.Join(entries, ", "))
		}
		fmt.Fprintf(&nodeLines, "      timeout_seconds     = 300\n")
		fmt.Fprintf(&nodeLines, "    }\n")
	}

	// Edges: only transform-to-transform.
	var edgeLines strings.Builder
	for _, e := range g.Edges {
		if nodeByID[e.FromNode].Type != "transform" || nodeByID[e.ToNode].Type != "transform" {
			continue
		}
		fmt.Fprintf(&edgeLines, "    { from = %q, from_output = %q, to = %q },\n",
			e.FromNode, "default", e.ToNode)
	}

	// Trigger queue ARNs from source modules — sources push new-data
	// signals to SQS even though the orchestration doesn't run them.
	// Two flavours, summed:
	//   - Legacy `module "src_*"` inline-authored sources (graph node
	//     of type=source). Pre-ADR-017 shape; still parsed for backcompat.
	//   - Registered kind=s3 sources attached via `source attach`. Their
	//     `module "src_<name>"` blocks are emitter-materialised below,
	//     so the queue ARN reference points there.
	var queueARNs []string
	sourceIDs := make([]string, 0)
	for _, n := range g.Nodes {
		if n.Type == "source" {
			sourceIDs = append(sourceIDs, n.ID)
		}
	}
	sort.Strings(sourceIDs)
	for _, id := range sourceIDs {
		queueARNs = append(queueARNs, fmt.Sprintf("module.%s.trigger_queue_arn", id))
	}

	// Registered s3 sources attached to any transform get materialised
	// as `module "src_<name>" { source = "...source/aws..." }` at the top
	// of orchestration.tf. The block carries bucket / prefix / format /
	// partitions / start_from from the registry — same descriptor the
	// transform's source_inputs already encodes, plus the SQS queue and
	// EventBridge rule that make S3 → pipeline triggering work. Without
	// this, registered-source pipelines have no way to fire on new data;
	// only the cron schedule and manual runs work.
	registeredS3, err := s.collectRegisteredS3Sources(g, nodeByID)
	if err != nil {
		return err
	}
	for _, src := range registeredS3 {
		queueARNs = append(queueARNs, fmt.Sprintf("module.%s.trigger_queue_arn", srcModuleName(src.Name)))
	}

	var queueArnsAttr string
	if len(queueARNs) > 0 {
		queueArnsAttr = fmt.Sprintf("\n  trigger_queue_arns   = [%s]", strings.Join(queueARNs, ", "))
	} else {
		queueArnsAttr = "\n  trigger_queue_arns   = []"
	}

	// Bucket for the runs-writer Lambda (Athena results + the runs Iceberg
	// table). Same expression the transform emitter uses — workspace flow
	// reads from remote_state, legacy reads from the in-pipeline resource.
	bucketExpr := "aws_s3_bucket.pipeline_bucket.bucket"
	// system_catalog falls back to "<catalog>_system" when the standalone
	// fallback path is taken — same shape DefaultSystemCatalog produces
	// for real workspaces, so the runs_writer Lambda always lands the row
	// in a sensible-named DB even without a manifest.
	systemCatalog := workspace.DefaultSystemCatalog(catalog)
	if ws != nil {
		bucketExpr = "data.terraform_remote_state.workspace.outputs.pipeline_bucket"
		systemCatalog = ws.SystemCatalogIdentifier()
	}

	triggerAttrs := queueArnsAttr +
		"\n  trigger_batch_window = var.trigger_batch_window" +
		"\n  schedule             = var.trigger_schedule" +
		fmt.Sprintf("\n  bucket               = %s", bucketExpr)

	// Three-level namespace (ADR-016 v0.18.1): orchestration creates the
	// per-pipeline Glue DB at the encoded `<catalog>__<schema>` name. As
	// of v0.20.0 the runs_writer Lambda points at the workspace system
	// catalog instead (workspace-wide observability DB), so we pass that
	// through as a third literal alongside catalog + schema.
	namespaceAttrs := fmt.Sprintf("\n  catalog              = %q\n  schema               = var.schema\n  system_catalog       = %q", catalog, systemCatalog)

	// Source materialisation: one `module "src_<name>"` block per
	// registered kind=s3 source attached to a transform. Emitted before
	// the orchestration module so terraform's dependency order is
	// natural (orchestration references module.src_X.trigger_queue_arn).
	sourceModuleSrc := s.ModuleSource(abs, SourceModuleRel)
	var sourceBlocks strings.Builder
	for _, src := range registeredS3 {
		emitS3SourceModule(&sourceBlocks, sourceModuleSrc, src)
	}

	content := fmt.Sprintf(`# clavesa orchestration — managed by clavesa, do not edit by hand
# Re-generated automatically when nodes or edges change.
%smodule "orchestration" {
  source = %q

  pipeline_name = var.pipeline_name
%s%s

  nodes = {
%s  }

  edges = [
%s  ]
}
`, sourceBlocks.String(), s.ModuleSource(abs, OrchestrationModuleRel), triggerAttrs, namespaceAttrs, nodeLines.String(), edgeLines.String())

	return os.WriteFile(filepath.Join(abs, "orchestration.tf"), []byte(content), 0o644)
}

// collectRegisteredS3Sources walks the pipeline graph for transforms with
// kind=s3 source_inputs attachments, resolves each against the workspace
// source registry, and returns one entry per unique source name. Order is
// lexicographic so the emitted orchestration.tf is deterministic. An s3
// source attached to multiple transforms in the same pipeline yields one
// entry — they share a single SQS queue + EventBridge rule per pipeline.
//
// Unregistered names surface as an error here (same as in the input-
// descriptor loop above), but only one error per missing name to keep the
// message terse.
func (s *Service) collectRegisteredS3Sources(g graph.PipelineGraph, nodeByID map[string]graph.Node) ([]sources.Spec, error) {
	store := sources.New(s.workspace)
	seen := make(map[string]bool)
	names := make([]string, 0)
	for _, n := range g.Nodes {
		if n.Type != "transform" {
			continue
		}
		srcInputs, ok := nodeByID[n.ID].Config["source_inputs"].(map[string]interface{})
		if !ok {
			continue
		}
		for _, raw := range srcInputs {
			name := ""
			switch v := raw.(type) {
			case string:
				name = strings.TrimPrefix(v, "sources.")
			case map[string]interface{}:
				if sn, ok := v["spec_name"].(string); ok {
					name = sn
				}
			}
			if name == "" || seen[name] {
				continue
			}
			seen[name] = true
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make([]sources.Spec, 0, len(names))
	for _, name := range names {
		spec, err := store.Get(name)
		if err != nil {
			return nil, fmt.Errorf("materialise source %q: %w", name, err)
		}
		if spec.Kind != "s3" {
			continue
		}
		out = append(out, spec)
	}
	return out, nil
}

// srcModuleName returns the Terraform module identifier for a registered
// s3 source. Same `src_<name>` convention legacy inline-authored sources
// used so existing orchestration code paths (and the user-visible block
// name in `terraform plan`) match.
func srcModuleName(specName string) string { return "src_" + specName }

// emitS3SourceModule writes one `module "src_<name>" { ... }` block to b
// for a kind=s3 source. Mirrors the variables modules/source/aws/variables.tf
// declares, including the v0.23.0 `manage_bucket_notifications` flag.
// sourceModuleSrc is the pre-computed source = "..." string (depth-aware
// relative to the pipeline directory).
func emitS3SourceModule(b *strings.Builder, sourceModuleSrc string, spec sources.Spec) {
	fmt.Fprintf(b, "module %q {\n", srcModuleName(spec.Name))
	fmt.Fprintf(b, "  source = %q\n\n", sourceModuleSrc)
	fmt.Fprintf(b, "  pipeline_name = var.pipeline_name\n")
	fmt.Fprintf(b, "  name          = %q\n", spec.Name)
	fmt.Fprintf(b, "  bucket        = %q\n", spec.Bucket)
	fmt.Fprintf(b, "  prefix        = %q\n", spec.Prefix)
	fmt.Fprintf(b, "  format        = %q\n", spec.Format)
	if len(spec.Partitions) > 0 {
		quoted := make([]string, len(spec.Partitions))
		for i, p := range spec.Partitions {
			quoted[i] = fmt.Sprintf("%q", p)
		}
		fmt.Fprintf(b, "  partitions    = [%s]\n", strings.Join(quoted, ", "))
		startFrom := spec.StartFrom
		if startFrom == "" {
			startFrom = "all"
		}
		fmt.Fprintf(b, "  start_from    = %q\n", startFrom)
	}
	if spec.ManageBucketNotifications {
		fmt.Fprint(b, "\n  manage_bucket_notifications = true\n")
	}
	fmt.Fprint(b, "}\n\n")
}

// inputAlias returns the SQL table alias for an incoming edge — defaults to
// the from-node ID when ToInput is empty or "default".
func inputAlias(e graph.Edge) string {
	if e.ToInput == "" || e.ToInput == "default" {
		return e.FromNode
	}
	return e.ToInput
}

// sourceHasPartitions reports whether a source node declares a non-empty
// partitions list — the v0.12 signal that downstream transforms should read
// it incrementally via partition cursors.
func sourceHasPartitions(n graph.Node) bool {
	parts, ok := n.Config["partitions"].([]interface{})
	return ok && len(parts) > 0
}

// formatCredentialHCL emits the inline credential descriptor the runner
// reads in `_resolve_http_headers`. Format mirrors the runtime shape
// expected on the Python side (kind / header_name / value_prefix /
// secret) and stays a single-line `, credentials = { ... }` suffix so it
// composes with the outer source descriptor without breaking HCL.
func formatCredentialHCL(c credentials.Spec) string {
	return fmt.Sprintf(`, credentials = { kind = %q, header_name = %q, value_prefix = %q, secret = %q }`,
		c.Kind, c.HeaderName, c.ValuePrefix, c.Secret)
}

// isCloudCompute reports whether a transform's `compute` config selects
// a cloud target (lambda / fargate / emr-serverless). Defaults to true
// when the attribute is absent — the transform module's own default is
// `lambda`, so unspecified means cloud, which is the safer assumption
// when validating credential backends.
func isCloudCompute(n graph.Node) bool {
	v, ok := n.Config["compute"].(string)
	if !ok || v == "" {
		return true
	}
	return v != "local"
}

// incrementalInputAliases returns the set of input aliases on a
// transform that should read their upstream incrementally (snapshot-
// bounded Iceberg scan plus a per-(consumer, alias) watermark). The
// authoring shape is `incremental_inputs = ["<alias>", ...]` on the
// transform's HCL; CLI / UI use `node edit --incremental-input
// <alias>` to add to that list. Returns an empty set when the
// attribute is absent so the default behaviour stays "full read every
// run".
func incrementalInputAliases(n graph.Node) map[string]bool {
	raw, ok := n.Config["incremental_inputs"]
	if !ok {
		return nil
	}
	out := map[string]bool{}
	switch v := raw.(type) {
	case []interface{}:
		for _, x := range v {
			if s, ok := x.(string); ok && s != "" {
				out[s] = true
			}
		}
	case []string:
		for _, s := range v {
			if s != "" {
				out[s] = true
			}
		}
	}
	return out
}

// outputKeyList returns the declared output keys for a transform, in
// sorted order so emit is deterministic. Falls back to ["default"]
// when output_definitions is missing or empty — historical shape for
// simple SQL transforms that never declared the block. The transform
// module itself defaults to a single-key map seeded with "default", so
// this only affects orchestration.tf emit, not the runtime contract.
func outputKeyList(n graph.Node) []string {
	defs, ok := n.Config["output_definitions"].(map[string]interface{})
	if !ok || len(defs) == 0 {
		return []string{"default"}
	}
	keys := make([]string, 0, len(defs))
	for k := range defs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// outputMode returns the configured write mode for a transform's output key.
// Defaults to "replace" (current Iceberg createOrReplace semantics) when
// unset. "append" switches the runner to an Iceberg append write. "merge"
// (Gate 4) runs MERGE INTO target USING staging ON <merge_keys>.
//
// When merge_keys is declared and mode is unset, defaults to "merge" — the
// runner's _resolve_output applies the same default, so this stays in sync.
func outputMode(n graph.Node, key string) string {
	defs, ok := n.Config["output_definitions"].(map[string]interface{})
	if !ok {
		return "replace"
	}
	def, ok := defs[key].(map[string]interface{})
	if !ok {
		return "replace"
	}
	mode, _ := def["mode"].(string)
	if mode == "" {
		if len(outputMergeKeys(n, key)) > 0 {
			return "merge"
		}
		return "replace"
	}
	return mode
}

// outputStats reports whether opt-in per-column stats computation is set
// on a transform's output. Defaults to false — stats are paid only when
// the user explicitly turns them on per output_definitions[<key>].stats.
func outputStats(n graph.Node, key string) bool {
	defs, ok := n.Config["output_definitions"].(map[string]interface{})
	if !ok {
		return false
	}
	def, ok := defs[key].(map[string]interface{})
	if !ok {
		return false
	}
	v, _ := def["stats"].(bool)
	return v
}

// outputMergeKeys returns the merge_keys list configured for an output, in
// declared order. Empty when unset.
func outputMergeKeys(n graph.Node, key string) []string {
	defs, ok := n.Config["output_definitions"].(map[string]interface{})
	if !ok {
		return nil
	}
	def, ok := defs[key].(map[string]interface{})
	if !ok {
		return nil
	}
	raw, ok := def["merge_keys"].([]interface{})
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			keys = append(keys, s)
		}
	}
	return keys
}
