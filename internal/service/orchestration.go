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
	"github.com/vesahyp/clavesa/internal/orchestration/aslgen"
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
// The ASL state machine definition is built by internal/orchestration/aslgen
// (graph traversal in Go — fixes the v1.1.4 nested-fanout / multi-hop
// branch bug that HCL couldn't represent) and inlined via jsonencode({...})
// in the resource definition by internal/orchestration/tfgen.
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

	// Per-node Lambda payload pieces — inputs + outputs as HCL map literals
	// inlined into each Task's Payload by tfgen.
	nodeMeta := make(map[string]tfgen.NodeMeta, len(transformIDs))
	for _, id := range transformIDs {
		inputsExpr, err := s.buildNodeInputsExpr(id, catalog, nodeByID, edgesByToNode)
		if err != nil {
			return err
		}
		outputsExpr := buildNodeOutputsExpr(id, nodeByID, edgesByFromNode)
		nodeMeta[id] = tfgen.NodeMeta{
			LambdaARNExpr:  fmt.Sprintf("module.%s.lambda_function_arn", id),
			InputsExpr:     inputsExpr,
			OutputsExpr:    outputsExpr,
			TimeoutSeconds: 300,
		}
	}

	// Build the transform-only edge list for aslgen.
	aslEdges := make([]aslgen.Edge, 0)
	for _, e := range g.Edges {
		if nodeByID[e.FromNode].Type != "transform" || nodeByID[e.ToNode].Type != "transform" {
			continue
		}
		aslEdges = append(aslEdges, aslgen.Edge{From: e.FromNode, To: e.ToNode})
	}

	sm, err := aslgen.Build(transformIDs, aslEdges)
	if err != nil {
		return fmt.Errorf("orchestration: build ASL: %w", err)
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
		StateMachine:      sm,
		NodeMeta:          nodeMeta,
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
func (s *Service) buildNodeInputsExpr(id, catalog string, nodeByID map[string]graph.Node, edgesByToNode map[string][]graph.Edge) (string, error) {
	entries := make([]string, 0)

	// Cross-pipeline / external table refs (ADR-016 slice 2).
	if extInputs, ok := nodeByID[id].Config["external_inputs"].(map[string]interface{}); ok {
		aliases := sortedKeys(extInputs)
		for _, alias := range aliases {
			ref, _ := extInputs[alias].(string)
			tableID, err := identutil.EncodeExternalTableRef(catalog, ref)
			if err != nil {
				return "", fmt.Errorf("transform %q input %q: %w", id, alias, err)
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
				if len(spec.Partitions) > 0 {
					startFrom := spec.StartFrom
					if startFrom == "" {
						startFrom = "all"
					}
					quoted := quoteList(spec.Partitions)
					entries = append(entries, fmt.Sprintf(`%s = { kind = "partitioned_path", path = "s3://%s/%s", partitions = [%s], start_from = %q }`,
						alias, spec.Bucket, spec.Prefix, quoted, startFrom))
					break
				}
				entries = append(entries, fmt.Sprintf(`%s = { kind = "s3", bucket = %q, prefix = %q, format = %q%s }`,
					alias, spec.Bucket, spec.Prefix, spec.Format, credBlock))
			default:
				return "", fmt.Errorf("source %q kind %q not supported", name, spec.Kind)
			}
		}
	}

	incoming := edgesByToNode[id]
	sort.Slice(incoming, func(i, j int) bool {
		return inputAlias(incoming[i]) < inputAlias(incoming[j])
	})
	incrementalAliases := incrementalInputAliases(nodeByID[id])
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
		outputMode(nodeByID[id], "default") == "replace" &&
		len(outputMergeKeys(nodeByID[id], "default")) == 0 &&
		!outputStats(nodeByID[id], "default") {
		return fmt.Sprintf(`{ default = %s }`, defaultDest)
	}
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
		statsAttr := ""
		if stats {
			statsAttr = ", stats = true"
		}
		entries = append(entries, fmt.Sprintf(
			`%s = { kind = "delta_table", table_id = %s, mode = %q, merge_keys = [%s]%s }`,
			key, dest, mode, quoteList(mergeKeys), statsAttr))
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
