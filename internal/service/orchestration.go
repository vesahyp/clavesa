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
	"github.com/vesahyp/clavesa/internal/orchestration/sidecar"
	"github.com/vesahyp/clavesa/internal/orchestration/tfgen"
	"github.com/vesahyp/clavesa/internal/sources"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// SyncOrchestration generates (or overwrites) orchestration.tf in dir from
// the current pipeline graph. As of v1.1.5 the file is self-contained
// standard Terraform — the Step Functions state machine + IAM + log group +
// Glue DB + runs_writer + optional schedule + optional poller, all spelled
// out as direct resources rather than wrapped behind a clavesa module. The
// `module "src_*"` blocks for kind=s3 sources still get materialised at the
// top of the file (sources stay as modules; this slice scopes orchestration
// only).
//
// As of v2.2.0 the ASL collapses to a single Task that invokes the
// per-pipeline runner Lambda; the runner's pipeline_handler loops the
// transforms list in topo order, sharing one Spark session across them
// (local-cloud parity per ADR-014). tfgen owns the emit; the
// per-transform Lambda function and the multi-state ASL machinery
// (aslgen package, dropped in v2.2.1) are gone.
//
// Sidecar Python (poller.py) is copied from embedded FS into
// <pipeline>/_clavesa_sidecar/ so the generated archive_file blocks
// resolve locally and the pipeline directory is detach-complete: a user
// dropping clavesa keeps idiomatic Terraform with no module dependency.
// runs_writer used to ship here too as a zip Lambda but ADR-018
// moved it into the runner image (Athena's Delta support is read-only
// so the Iceberg INSERT path is gone).
//
// Trigger configuration is read from pipeline variables (trigger_schedule,
// trigger_batch_window) automatically. Pass schedule as a non-empty string
// to override; pass empty string to read from pipeline vars.
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
	// by another pipeline in the workspace.
	pipelineName := filepath.Base(abs)
	pipelineSchema := resolvePipelineSchema(abs, pipelineName)
	if err := s.validateSchemaOwnership(pipelineName, pipelineSchema); err != nil {
		return err
	}

	// GH #6: rewrite string-form intra-pipeline input refs
	// ("<own-schema>.<sibling-table>") into real graph edges before the topo
	// sort and before upstreamProducerPipelines runs. This keeps the emitted
	// transforms list topo-ordered with populated parents, resolves the ref as
	// an intra-pipeline read, and prevents a spurious cross-pipeline
	// EventBridge trigger / Lake Formation grant for what is really a sibling.
	reclassifyIntraPipelineEdges(&g, pipelineSchema)

	nodeByID := make(map[string]graph.Node, len(g.Nodes))
	for _, n := range g.Nodes {
		nodeByID[n.ID] = n
	}
	// v2.2.0 bundle execution: pipeline_handler iterates the emitted
	// transforms list in order, so the order MUST be topological — a
	// downstream transform cannot precede its parents in the slice.
	// (Alphabetical was fine for the v2.1.x multi-state ASL because the
	// state transitions encoded dependency order independently of slice
	// position.)
	topoOrder, err := topoSort(&g)
	if err != nil {
		return fmt.Errorf("orchestration: %w", err)
	}
	transformIDs := make([]string, 0, len(topoOrder))
	for _, id := range topoOrder {
		if nodeByID[id].Type == "transform" && nodeEnabled(nodeByID[id]) {
			transformIDs = append(transformIDs, id)
		}
	}

	edgesByToNode := make(map[string][]graph.Edge)
	edgesByFromNode := make(map[string][]graph.Edge)
	for _, e := range g.Edges {
		edgesByToNode[e.ToNode] = append(edgesByToNode[e.ToNode], e)
		edgesByFromNode[e.FromNode] = append(edgesByFromNode[e.FromNode], e)
	}

	// Trigger queue ARN expressions from source modules (legacy inline +
	// registered s3 sources materialised below).
	queueExprs := make([]string, 0)
	sourceIDs := make([]string, 0)
	for _, n := range g.Nodes {
		if n.Type == "source" {
			sourceIDs = append(sourceIDs, n.ID)
		}
	}
	sort.Strings(sourceIDs)
	for _, id := range sourceIDs {
		queueExprs = append(queueExprs, fmt.Sprintf("module.%s.trigger_queue_arn", id))
	}
	registeredS3, err := s.collectRegisteredS3Sources(g, nodeByID)
	if err != nil {
		return err
	}
	for _, src := range registeredS3 {
		queueExprs = append(queueExprs, fmt.Sprintf("module.%s.trigger_queue_arn", srcModuleName(src.Name)))
	}

	// External S3 buckets — union of inline source-module buckets and
	// registered s3 source attachments. Pre-v2.2.0 the per-transform IAM
	// summed every transform's input_buckets into its own role; collapsing
	// to a single per-pipeline Lambda dropped that grant, so pipelines
	// reading from any non-workspace bucket (cross-account CloudFront logs,
	// public datasets, …) started 403'ing on upgrade. tfgen emits an
	// `S3ReadExternal` IAM Statement listing these buckets only when the
	// list is non-empty.
	externalBucketSet := map[string]struct{}{}
	for _, n := range g.Nodes {
		if n.Type != "source" {
			continue
		}
		if bucket, ok := n.Config["bucket"].(string); ok && bucket != "" {
			externalBucketSet[bucket] = struct{}{}
		}
	}
	for _, src := range registeredS3 {
		if src.Bucket != "" {
			externalBucketSet[src.Bucket] = struct{}{}
		}
	}
	externalBuckets := make([]string, 0, len(externalBucketSet))
	for b := range externalBucketSet {
		externalBuckets = append(externalBuckets, b)
	}
	sort.Strings(externalBuckets)

	// Cross-pipeline auto-trigger producers (ADR-016 §6, this slice). For
	// each transform reading another pipeline's table via
	// `external_inputs`, find the producer pipeline so tfgen can emit one
	// EventBridge rule per producer that starts this pipeline's state
	// machine on the producer's SUCCEEDED execution event. Unresolved
	// refs (external Glue tables, typos) are skipped — there's no state
	// machine to listen to.
	upstreamProducers := s.upstreamProducerPipelines(g, pipelineName, resolvePipelineSchema(abs, pipelineName))

	// Producer-grain lookup for cross-pipeline CDF reads (GH: dim CDF
	// crash). Keyed schema → table → the producing node's output
	// merge_keys, so buildNodeInputsExpr stamps the upstream's grain on
	// each delta_table_cdf descriptor rather than the consumer's own
	// output keys (which name the consumer's columns, not the upstream's).
	producerMergeKeys := s.crossPipelineProducerMergeKeys(pipelineName)

	// Lake Formation read grants on upstream schemas (GH #4). Distinct
	// sanitized schemas this pipeline reads cross-pipeline, own schema
	// excluded — runs after edge reclassification so only genuine
	// cross-pipeline refs remain.
	upstreamSchemas := upstreamReadSchemas(g, pipelineSchema)

	// Bucket for runs_writer / Athena results. Workspace-rooted pipelines
	// read it from remote state; the standalone fallback path points at
	// the in-pipeline aws_s3_bucket resource the user wires up by hand.
	bucketExpr := "aws_s3_bucket.pipeline_bucket.bucket"
	systemCatalog := workspace.DefaultSystemCatalog(catalog)
	if ws != nil {
		bucketExpr = "data.terraform_remote_state.workspace.outputs.pipeline_bucket"
		systemCatalog = ws.SystemCatalogIdentifier()
	}
	// ADR-018: runs_writer deploys as the runner image (Athena INSERT
	// is gone with the Iceberg→Delta swap). Same expression every
	// transform node uses for `runner_image`.
	runnerImageExpr := "data.terraform_remote_state.workspace.outputs.runner_image"

	// Materialise registered s3 source modules at the top of the file.
	sourceModuleSrc := s.ModuleSource(abs, SourceModuleRel)
	var sourceBlocks strings.Builder
	for _, src := range registeredS3 {
		emitS3SourceModule(&sourceBlocks, sourceModuleSrc, src)
	}

	// Per-transform invocation payloads — tfgen renders one entry per
	// transform into the pipeline Lambda's event payload. Order matches
	// transformIDs (topo-sorted upstream), and Parents drives the runner's
	// cascade-skip on upstream failure.
	transforms := make([]tfgen.TransformConfig, 0, len(transformIDs))
	for _, id := range transformIDs {
		n := nodeByID[id]
		language, _ := n.Config["language"].(string)
		if language == "" {
			language = "sql"
		}
		ext := "sql"
		if language == "python" {
			ext = "py"
		}
		// Logic key mirrors modules/transform/aws/main.tf's _logic_key:
		//   "${var.pipeline_name}/${var.name}/_runtime/logic.${_logic_ext}"
		// The per-transform module still emits aws_s3_object "logic" at
		// this exact path; the pipeline Lambda reads via _read_text("s3://...").
		logicURI := fmt.Sprintf(
			`"s3://${%s}/${var.pipeline_name}/%s/_runtime/logic.%s"`,
			bucketExpr, id, ext,
		)

		inputsExpr, err := s.buildNodeInputsExpr(id, catalog, nodeByID, edgesByToNode, producerMergeKeys)
		if err != nil {
			return err
		}
		outputsExpr := buildNodeOutputsExpr(id, nodeByID, edgesByFromNode)

		// Parents = intra-pipeline upstream transform node ids (cascade-
		// skip input for pipeline_handler). Disabled upstreams are excluded:
		// they never run, so they're never in the runner's skipped_set, and
		// leaving them in would block legitimate cascade-skip (the "all
		// parents skipped" test can never pass with a parent that can't
		// skip). Downstream still reads the disabled node's materialized
		// table via its inputs descriptor — the module block stays in .tf.
		var parents []string
		for _, e := range edgesByToNode[id] {
			from := nodeByID[e.FromNode]
			if from.Type == "transform" && nodeEnabled(from) {
				parents = append(parents, e.FromNode)
			}
		}
		sort.Strings(parents)

		transforms = append(transforms, tfgen.TransformConfig{
			NodeID:      id,
			Language:    language,
			LogicS3URI:  logicURI,
			InputsExpr:  inputsExpr,
			OutputsExpr: outputsExpr,
			Parents:     parents,
		})
	}

	tfBody, err := tfgen.Emit(tfgen.Pipeline{
		PipelineNameExpr:  "var.pipeline_name",
		Catalog:           catalog,
		SchemaExpr:        "var.schema",
		SystemCatalog:     systemCatalog,
		BucketExpr:        bucketExpr,
		RunnerImageExpr:   runnerImageExpr,
		ScheduleExpr:      "var.trigger_schedule",
		BatchWindowExpr:   "var.trigger_batch_window",
		TriggerQueueExprs: queueExprs,
		UpstreamPipelines: upstreamProducers,
		Transforms:        transforms,
		ExternalBuckets:   externalBuckets,
		UpstreamSchemas:   upstreamSchemas,
	})
	if err != nil {
		return fmt.Errorf("orchestration: emit tf: %w", err)
	}

	// Sidecar Python is required for runs_writer (always emitted) and
	// poller (when queues present). Materialise unconditionally — cheap
	// and keeps the pipeline dir detach-complete.
	if err := sidecar.Materialise(abs); err != nil {
		return fmt.Errorf("orchestration: materialise sidecar: %w", err)
	}

	header := `# clavesa orchestration — managed by clavesa, do not edit by hand.
# Re-generated automatically when nodes or edges change.
#
# To detach from clavesa: delete this header, remove the
# _clavesa_sidecar/ directory's clavesa-only Python lambdas if you
# replace them, and own the file as standard Terraform.

`
	content := header + sourceBlocks.String() + tfBody
	return os.WriteFile(filepath.Join(abs, "orchestration.tf"), []byte(content), 0o644)
}

// buildNodeInputsExpr returns the inputs map for a transform node as a
// single-line HCL map literal `{ alias = value, … }` suitable for inlining
// inside the Lambda Payload. Each entry is one of:
//   - "<table_id>" (Delta upstream from another transform)
//   - { kind = "http", … } (registered http source)
//   - { kind = "s3", … } / { kind = "partitioned_path", … } (s3 sources)
//   - { kind = "delta_table_cdf", … } (CDF-bounded incremental read)
//   - "<table_path>" (pass-through S3 source)
//   - module.X.outputs[...].table_path (destination passthrough)
//
// Lifted verbatim from the v1.1.4 inline emitter — the per-input branching
// is unchanged because the runner reads the same shapes; only the
// surrounding container moved from `nodes = { id = { inputs = … } }` to
// `Payload = { inputs = … }`.
func (s *Service) buildNodeInputsExpr(id, catalog string, nodeByID map[string]graph.Node, edgesByToNode map[string][]graph.Edge, producerMergeKeys map[string]map[string][]string) (string, error) {
	entries := make([]string, 0)
	incrementalAliases := incrementalInputAliases(nodeByID[id])

	// Cross-pipeline / external table refs (ADR-016 slice 2).
	if extInputs, ok := nodeByID[id].Config["external_inputs"].(map[string]interface{}); ok {
		aliases := sortedKeys(extInputs)
		for _, alias := range aliases {
			ref, _ := extInputs[alias].(string)
			tableID, err := identutil.EncodeExternalTableRef(catalog, ref)
			if err != nil {
				return "", fmt.Errorf("transform %q input %q: %w", id, alias, err)
			}
			if incrementalAliases[alias] {
				// Cross-pipeline incremental read: the upstream Delta table
				// lives in another pipeline, so there's no module.X.outputs to
				// reference — point the CDF descriptor straight at the resolved
				// table id. The runner reads the upstream's Change Data Feed
				// over (watermark, current] and dedups the range to the latest
				// row per key by `_commit_version DESC`. That dedup MUST key on
				// the PRODUCER's grain — the columns that physically exist on
				// the upstream Delta table — not this consumer's output
				// merge_keys. The consumer renames freely (`SELECT cs_User_Agent
				// AS user_agent`), so its output keys need not exist upstream;
				// keying the range dedup on them throws "column not found" and
				// kills the transform. See crossPipelineProducerMergeKeys.
				// Watermark is per (consumer, alias), same as same-pipeline CDF.
				watermarkAlias := id + "__" + alias
				mergeKeysAttr := ""
				if mk := producerGrainForRef(producerMergeKeys, ref); len(mk) > 0 {
					mergeKeysAttr = fmt.Sprintf(", merge_keys = [%s]", quoteList(mk))
				}
				entries = append(entries, fmt.Sprintf(
					`%s = { kind = "delta_table_cdf", table = %q, alias = %q%s }`,
					alias, tableID, watermarkAlias, mergeKeysAttr))
				continue
			}
			entries = append(entries, fmt.Sprintf("%s = %q", alias, tableID))
		}
	}

	// Workspace source-registry refs (ADR-017).
	if srcInputs, ok := nodeByID[id].Config["source_inputs"].(map[string]interface{}); ok {
		aliases := sortedKeys(srcInputs)
		store := sources.New(s.workspace)
		credStore := credentials.New(s.workspace)
		isCloud := isCloudCompute(nodeByID[id])
		for _, alias := range aliases {
			name := ""
			switch v := srcInputs[alias].(type) {
			case string:
				name = strings.TrimPrefix(v, "sources.")
			case map[string]interface{}:
				if sn, ok := v["spec_name"].(string); ok {
					name = sn
				}
			}
			if name == "" {
				return "", fmt.Errorf("transform %q input %q: malformed source_inputs entry %v", id, alias, srcInputs[alias])
			}
			spec, err := store.Get(name)
			if err != nil {
				return "", fmt.Errorf("transform %q input %q: source %q not registered (workspace registry)", id, alias, name)
			}
			credBlock := ""
			if spec.Credentials != "" {
				cred, err := credStore.Get(spec.Credentials)
				if err != nil {
					return "", fmt.Errorf("source %q references credential %q which is not registered", name, spec.Credentials)
				}
				if isCloud {
					switch cred.SecretBackend() {
					case "env", "file":
						return "", fmt.Errorf("source %q credential %q uses local-only backend %q; cloud-deployed transforms require an arn:aws:secretsmanager:... reference",
							name, cred.Name, cred.SecretBackend())
					}
				}
				credBlock = formatCredentialHCL(cred)
			}
			switch spec.Kind {
			case "http":
				entries = append(entries, fmt.Sprintf(`%s = { kind = "http", url = %q, format = %q%s }`,
					alias, spec.URL, spec.Format, credBlock))
			case "s3":
				// Notification-drain ingest: the runner drains this source's
				// SQS queue (S3 ObjectCreated events) for new keys when the
				// descriptor carries a non-empty queue_url. The value is an
				// unquoted TF reference to the source module's output, same
				// module-instance name emitS3SourceModule uses.
				queueURLRef := fmt.Sprintf("module.%s.trigger_queue_url", srcModuleName(name))
				if len(spec.Partitions) > 0 {
					startFrom := spec.StartFrom
					if startFrom == "" {
						startFrom = "all"
					}
					quoted := quoteList(spec.Partitions)
					entries = append(entries, fmt.Sprintf(`%s = { kind = "partitioned_path", path = "s3://%s/%s", partitions = [%s], start_from = %q, queue_url = %s }`,
						alias, spec.Bucket, spec.Prefix, quoted, startFrom, queueURLRef))
					break
				}
				readOptsAttr := ""
				if len(spec.ReadOptions) > 0 {
					keys := make([]string, 0, len(spec.ReadOptions))
					for k := range spec.ReadOptions {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					pairs := make([]string, 0, len(keys))
					for _, k := range keys {
						pairs = append(pairs, fmt.Sprintf("%q = %q", k, spec.ReadOptions[k]))
					}
					readOptsAttr = fmt.Sprintf(", read_options = { %s }", strings.Join(pairs, ", "))
				}
				entries = append(entries, fmt.Sprintf(`%s = { kind = "s3", bucket = %q, prefix = %q, format = %q, queue_url = %s%s%s }`,
					alias, spec.Bucket, spec.Prefix, spec.Format, queueURLRef, readOptsAttr, credBlock))
			default:
				return "", fmt.Errorf("source %q kind %q not supported", name, spec.Kind)
			}
		}
	}

	incoming := edgesByToNode[id]
	sort.Slice(incoming, func(i, j int) bool {
		return inputAlias(incoming[i]) < inputAlias(incoming[j])
	})
	for _, e := range incoming {
		alias := inputAlias(e)
		fromOutput := "default"
		from := nodeByID[e.FromNode]
		switch {
		case from.Type == "transform" && incrementalAliases[alias]:
			watermarkAlias := id + "__" + alias
			// Stamp upstream's merge_keys onto the CDF descriptor when the
			// producer declares them (mode=merge or explicit merge_keys
			// list). The runner dedupes the CDF range to the latest row
			// per key by `_commit_version DESC`; without merge_keys the
			// range is read raw and downstream sees one row per change
			// event rather than the latest state per business key.
			mergeKeys := outputMergeKeys(from, fromOutput)
			mergeKeysAttr := ""
			if len(mergeKeys) > 0 {
				mergeKeysAttr = fmt.Sprintf(", merge_keys = [%s]", quoteList(mergeKeys))
			}
			entries = append(entries, fmt.Sprintf(
				`%s = { kind = "delta_table_cdf", table = "${module.%s.outputs[%q].catalog_db}.${module.%s.outputs[%q].catalog_table}", alias = %q%s }`,
				alias, e.FromNode, fromOutput, e.FromNode, fromOutput, watermarkAlias, mergeKeysAttr))
		case from.Type == "transform":
			entries = append(entries, fmt.Sprintf(
				`%s = "${module.%s.outputs[%q].catalog_db}.${module.%s.outputs[%q].catalog_table}"`,
				alias, e.FromNode, fromOutput, e.FromNode, fromOutput))
		case from.Type == "source" && sourceHasPartitions(from):
			entries = append(entries, fmt.Sprintf(
				`%s = { kind = "partitioned_path", path = module.%s.outputs[%q].table_path, partitions = module.%s.outputs[%q].partitions, start_from = module.%s.outputs[%q].start_from }`,
				alias, e.FromNode, fromOutput, e.FromNode, fromOutput, e.FromNode, fromOutput))
		default:
			entries = append(entries, fmt.Sprintf(`%s = module.%s.outputs[%q].table_path`,
				alias, e.FromNode, fromOutput))
		}
	}

	if len(entries) == 0 {
		return "{}", nil
	}
	return "{ " + strings.Join(entries, ", ") + " }", nil
}

// buildNodeOutputsExpr returns the outputs map for a transform node as a
// single-line HCL map literal `{ key = value, … }`. Lifted from v1.1.4
// with one cosmetic change: no fixed-width column alignment (the value
// goes inline in jsonencode's argument, where alignment buys nothing).
//
// The single-key-default-replace shortcut is preserved so simple SQL
// transforms keep their compact `{ default = "" }` shape — no semantic
// change on the runner side either way.
func buildNodeOutputsExpr(id string, nodeByID map[string]graph.Node, edgesByFromNode map[string][]graph.Edge) string {
	defaultDest := `""`
	for _, out := range edgesByFromNode[id] {
		if nodeByID[out.ToNode].Type == "destination" {
			defaultDest = fmt.Sprintf("module.%s.target_path", out.ToNode)
			break
		}
	}
	outputKeys := outputKeyList(nodeByID[id])
	if len(outputKeys) == 1 && outputKeys[0] == "default" &&
		outputFormat(nodeByID[id], "default") != "json" &&
		outputMode(nodeByID[id], "default") == "replace" &&
		len(outputMergeKeys(nodeByID[id], "default")) == 0 &&
		len(outputClusterBy(nodeByID[id], "default")) == 0 &&
		len(outputBoundBy(nodeByID[id], "default")) == 0 &&
		len(outputMergeUpdate(nodeByID[id], "default")) == 0 &&
		!outputStats(nodeByID[id], "default") {
		return fmt.Sprintf(`{ default = %s }`, defaultDest)
	}
	entries := make([]string, 0, len(outputKeys))
	for _, key := range outputKeys {
		dest := `""`
		if key == "default" {
			dest = defaultDest
		}
		// JSON-file outputs emit a json_object descriptor keyed on path,
		// bypassing both the delta_table descriptor and the destination
		// routing below.
		if outputFormat(nodeByID[id], key) == "json" {
			path := outputPath(nodeByID[id], key)
			contentType := outputContentType(nodeByID[id], key)
			if contentType == "" {
				contentType = "application/json"
			}
			cacheAttr := ""
			if cc := outputCacheControl(nodeByID[id], key); cc != "" {
				cacheAttr = fmt.Sprintf(", cache_control = %q", cc)
			}
			entries = append(entries, fmt.Sprintf(
				`%s = { kind = "json_object", path = %q, content_type = %q%s }`,
				key, path, contentType, cacheAttr))
			continue
		}
		mode := outputMode(nodeByID[id], key)
		mergeKeys := outputMergeKeys(nodeByID[id], key)
		clusterBy := outputClusterBy(nodeByID[id], key)
		boundBy := outputBoundBy(nodeByID[id], key)
		mergeUpdate := outputMergeUpdate(nodeByID[id], key)
		stats := outputStats(nodeByID[id], key)
		if mode == "replace" && len(mergeKeys) == 0 && len(clusterBy) == 0 && len(boundBy) == 0 && len(mergeUpdate) == 0 && !stats {
			entries = append(entries, fmt.Sprintf("%s = %s", key, dest))
			continue
		}
		statsAttr := ""
		if stats {
			statsAttr = ", stats = true"
		}
		clusterAttr := ""
		if len(clusterBy) > 0 {
			clusterAttr = fmt.Sprintf(", cluster_by = [%s]", quoteList(clusterBy))
		}
		boundAttr := ""
		if len(boundBy) > 0 {
			boundAttr = fmt.Sprintf(", bound_by = [%s]", quoteList(boundBy))
		}
		mergeUpdateAttr := ""
		if len(mergeUpdate) > 0 {
			mergeUpdateAttr = fmt.Sprintf(", merge_update = { %s }", joinMergeUpdate(mergeUpdate))
		}
		entries = append(entries, fmt.Sprintf(
			`%s = { kind = "delta_table", table_id = %s, mode = %q, merge_keys = [%s]%s%s%s%s }`,
			key, dest, mode, quoteList(mergeKeys), clusterAttr, boundAttr, statsAttr, mergeUpdateAttr))
	}
	return "{ " + strings.Join(entries, ", ") + " }"
}

// sortedKeys returns the keys of an interface map in sorted order — used
// to make per-node emit deterministic regardless of HCL parse order.
func sortedKeys(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// quoteList renders a string slice as `"a", "b", "c"` for use inside HCL
// list literals. Empty slice returns "".
func quoteList(items []string) string {
	if len(items) == 0 {
		return ""
	}
	quoted := make([]string, len(items))
	for i, s := range items {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(quoted, ", ")
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
		fmt.Fprintf(b, "  partitions    = [%s]\n", quoteList(spec.Partitions))
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
// nodeEnabled reports whether a transform participates in pipeline runs. A
// node with `enabled = false` in its HCL is omitted from the emitted
// transforms list — it isn't executed, but its module block and its
// last-materialized output table remain, so downstream consumers read the
// existing (no-longer-refreshed) table rather than failing. Default (attribute
// absent) is enabled. Used to pause a node — e.g. a derivation being moved to
// a view — without deleting it.
func nodeEnabled(n graph.Node) bool {
	if v, ok := n.Config["enabled"].(bool); ok {
		return v
	}
	return true
}

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
// when output_definitions is missing or empty.
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
// Defaults to "replace". When merge_keys is declared and mode is unset,
// defaults to "merge" — matches runner._resolve_output.
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
// on a transform's output.
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

// outputStringField returns a string attribute from a transform output's
// definition, or "" when the field (or the output/def map) is absent.
func outputStringField(n graph.Node, key, field string) string {
	defs, ok := n.Config["output_definitions"].(map[string]interface{})
	if !ok {
		return ""
	}
	def, ok := defs[key].(map[string]interface{})
	if !ok {
		return ""
	}
	v, _ := def[field].(string)
	return v
}

// outputFormat returns the configured output shape ("delta" default, or
// "json" for single-file JSON output).
func outputFormat(n graph.Node, key string) string {
	f := outputStringField(n, key, "format")
	if f == "" {
		return "delta"
	}
	return f
}

// outputPath returns the destination path for a format="json" output.
func outputPath(n graph.Node, key string) string {
	return outputStringField(n, key, "path")
}

// outputContentType returns the MIME type for a format="json" output.
func outputContentType(n graph.Node, key string) string {
	return outputStringField(n, key, "content_type")
}

// outputCacheControl returns the Cache-Control header for a format="json"
// output, or "" when unset.
func outputCacheControl(n graph.Node, key string) string {
	return outputStringField(n, key, "cache_control")
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

// outputClusterBy returns the cluster_by list configured for an output, in
// declared order. Empty when unset.
func outputClusterBy(n graph.Node, key string) []string {
	defs, ok := n.Config["output_definitions"].(map[string]interface{})
	if !ok {
		return nil
	}
	def, ok := defs[key].(map[string]interface{})
	if !ok {
		return nil
	}
	raw, ok := def["cluster_by"].([]interface{})
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

// outputBoundBy returns the bound_by list configured for an output, in
// declared order. Empty when unset.
func outputBoundBy(n graph.Node, key string) []string {
	defs, ok := n.Config["output_definitions"].(map[string]interface{})
	if !ok {
		return nil
	}
	def, ok := defs[key].(map[string]interface{})
	if !ok {
		return nil
	}
	raw, ok := def["bound_by"].([]interface{})
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

// outputMergeUpdate returns the per-column merge expressions configured for
// an output (column name -> spec). Non-string values are skipped. Returns
// nil when unset or empty.
func outputMergeUpdate(n graph.Node, key string) map[string]string {
	defs, ok := n.Config["output_definitions"].(map[string]interface{})
	if !ok {
		return nil
	}
	def, ok := defs[key].(map[string]interface{})
	if !ok {
		return nil
	}
	raw, ok := def["merge_update"].(map[string]interface{})
	if !ok {
		return nil
	}
	out := make(map[string]string, len(raw))
	for col, v := range raw {
		if s, ok := v.(string); ok {
			out[col] = s
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// joinMergeUpdate formats a merge_update map as the body of an HCL map
// literal: sorted `"col" = "spec"` pairs joined by ", ". Both key and
// value are quoted so column names that aren't valid HCL identifiers stay
// safe.
func joinMergeUpdate(mu map[string]string) string {
	cols := make([]string, 0, len(mu))
	for col := range mu {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	pairs := make([]string, 0, len(cols))
	for _, col := range cols {
		pairs = append(pairs, fmt.Sprintf("%q = %q", col, mu[col]))
	}
	return strings.Join(pairs, ", ")
}

// upstreamProducerPipelines returns the deduplicated, sorted list of
// sibling pipeline names that produce any table this pipeline reads via
// `external_inputs`. Best-effort: a workspace-scan failure returns an
// empty list rather than blocking the emit (mirrors the lineage and
// schema-ownership reuse of `workspacePipelineScan`).
//
// References to tables in the same schema (`refSchema == thisSchema`)
// are skipped — those are intra-pipeline edges already covered by the
// regular module-ref path. References that resolve to no producer
// (external Glue tables, typos) are also skipped: nothing to listen to.
//
// Producer resolution accepts both the bare `<node>` form (ADR-019
// single-default-output) and the legacy `<node>__<key>` form so old
// pipelines authored before the rename still resolve.
func (s *Service) upstreamProducerPipelines(g graph.PipelineGraph, thisName, thisSchema string) []string {
	siblings, err := s.workspacePipelineScan()
	if err != nil || len(siblings) == 0 {
		return nil
	}

	// schema → table-name → producer pipeline name. Each transform
	// contributes both the bare and `__<key>` forms (default-only
	// transforms write the bare form per ADR-019; multi-output
	// transforms write the suffixed form per output).
	bySchemaTable := map[string]map[string]string{}
	for _, p := range siblings {
		if p.name == thisName {
			continue
		}
		tbl, ok := bySchemaTable[p.schema]
		if !ok {
			tbl = map[string]string{}
			bySchemaTable[p.schema] = tbl
		}
		for _, n := range p.graph.Nodes {
			if n.Type != "transform" {
				continue
			}
			bare := identutil.Sanitize(n.ID)
			outs, _ := n.Config["output_definitions"].(map[string]interface{})
			if len(outs) == 0 {
				// default-only, implicit
				tbl[bare] = p.name
				tbl[bare+"__default"] = p.name
				continue
			}
			if len(outs) == 1 {
				if _, defaultOnly := outs["default"]; defaultOnly {
					tbl[bare] = p.name
					tbl[bare+"__default"] = p.name
					continue
				}
			}
			for k := range outs {
				tbl[bare+"__"+k] = p.name
			}
		}
	}

	seen := map[string]struct{}{}
	for _, n := range g.Nodes {
		if n.Type != "transform" {
			continue
		}
		ext, _ := n.Config["external_inputs"].(map[string]interface{})
		for _, refRaw := range ext {
			refStr, ok := refRaw.(string)
			if !ok {
				continue
			}
			dot := strings.Index(refStr, ".")
			if dot < 0 {
				continue
			}
			refSchema := refStr[:dot]
			refTable := refStr[dot+1:]
			if refSchema == thisSchema {
				continue
			}
			tbl, ok := bySchemaTable[refSchema]
			if !ok {
				continue
			}
			producer, ok := tbl[refTable]
			if !ok {
				continue
			}
			seen[producer] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// crossPipelineProducerMergeKeys scans sibling pipelines and returns a
// schema → table → producer-output-merge_keys map. The CDF input
// descriptor for a cross-pipeline incremental read keys its range-dedup
// on the PRODUCING node's grain (the columns that physically exist on the
// upstream Delta table), never on the consumer's own output merge_keys.
//
// The distinction is load-bearing: a dim doing
// `SELECT cs_User_Agent AS user_agent ... merge_keys = ["user_agent"]`
// outputs a `user_agent` column, but the upstream `enriched` change feed
// only has `cs_User_Agent` / `x_edge_request_id`. Keying the runner's
// `partitionBy(...)` range-dedup on `user_agent` throws "column not found"
// and kills the transform. The producer's grain (`x_edge_request_id`) is
// the only correct key — it's the column on the feed and the grain the
// upstream upserts on. The consumer's own SQL (DISTINCT / its own MERGE)
// does the output-side collapse; the input dedup must not pre-empt it.
//
// Best-effort, mirroring upstreamProducerPipelines: a scan failure or a
// ref that resolves to no in-workspace producer (external Glue table,
// typo) yields no keys, so the descriptor is emitted without merge_keys
// and the runner reads the CDF range raw. We can't dedup on a key we
// can't prove exists upstream.
//
// Table-name resolution accepts both the bare `<node>` (ADR-019
// single-default-output) and legacy `<node>__<key>` forms, matching
// upstreamProducerPipelines.
func (s *Service) crossPipelineProducerMergeKeys(thisName string) map[string]map[string][]string {
	siblings, err := s.workspacePipelineScan()
	if err != nil || len(siblings) == 0 {
		return nil
	}
	out := map[string]map[string][]string{}
	for _, p := range siblings {
		if p.name == thisName {
			continue
		}
		tbl, ok := out[p.schema]
		if !ok {
			tbl = map[string][]string{}
			out[p.schema] = tbl
		}
		for _, n := range p.graph.Nodes {
			if n.Type != "transform" {
				continue
			}
			bare := identutil.Sanitize(n.ID)
			outs, _ := n.Config["output_definitions"].(map[string]interface{})
			if len(outs) == 0 {
				mk := outputMergeKeys(n, "default")
				tbl[bare] = mk
				tbl[bare+"__default"] = mk
				continue
			}
			if len(outs) == 1 {
				if _, defaultOnly := outs["default"]; defaultOnly {
					mk := outputMergeKeys(n, "default")
					tbl[bare] = mk
					tbl[bare+"__default"] = mk
					continue
				}
			}
			for k := range outs {
				tbl[bare+"__"+k] = outputMergeKeys(n, k)
			}
		}
	}
	return out
}

// producerGrainForRef looks up the producing node's output merge_keys for
// a cross-pipeline `<schema>.<table>` reference. Empty when the ref is
// malformed or the producer isn't in the workspace scan.
func producerGrainForRef(m map[string]map[string][]string, ref string) []string {
	dot := strings.Index(ref, ".")
	if dot < 0 {
		return nil
	}
	if tbl, ok := m[ref[:dot]]; ok {
		return tbl[ref[dot+1:]]
	}
	return nil
}

// upstreamReadSchemas returns the deduplicated, sorted set of sanitized
// schema identifiers this pipeline reads CROSS-pipeline via `external_inputs`
// (GH #4). The own schema (`refSchema == thisSchema`) is excluded — the
// pipeline_runner_db / pipeline_runner_tables LF grants already cover it.
// These feed tfgen.Pipeline.UpstreamSchemas so the consumer's
// orchestration.tf grants the runner role Lake Formation DESCRIBE on each
// upstream Glue DB + SELECT/DESCRIBE on its tables.
//
// Run AFTER reclassifyIntraPipelineEdges so string-form intra-pipeline refs
// have already been rewritten into graph edges and no longer appear as
// external_inputs; what remains here is genuine cross-pipeline reads.
//
// Unlike upstreamProducerPipelines this needs no workspace scan: the schema
// segment of the ref is enough to name the Glue DB (one catalog per
// workspace, ADR-016), and the grant is harmless even if the schema has no
// in-workspace producer (external Glue tables registered out-of-band still
// want the read grant).
func upstreamReadSchemas(g graph.PipelineGraph, thisSchema string) []string {
	thisSanitized := identutil.Sanitize(thisSchema)
	seen := map[string]struct{}{}
	for _, n := range g.Nodes {
		if n.Type != "transform" {
			continue
		}
		ext, _ := n.Config["external_inputs"].(map[string]interface{})
		for _, refRaw := range ext {
			refStr, ok := refRaw.(string)
			if !ok {
				continue
			}
			dot := strings.Index(refStr, ".")
			if dot < 0 {
				continue
			}
			refSchema := identutil.Sanitize(refStr[:dot])
			if refSchema == "" || refSchema == thisSanitized {
				continue
			}
			seen[refSchema] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
