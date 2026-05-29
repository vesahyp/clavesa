package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/service"
)

func newNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Manage pipeline nodes and edges",
		Long: `Manage pipeline nodes and edges (list, show, add, edit, remove, connect,
disconnect, preview).

Nodes are transforms or destinations inside a pipeline. Sources live in
the workspace registry (see ` + "`clavesa source --help`" + `) and attach to a
transform's inputs map.

The pipeline directory is the first argument; omit it to use the current
directory once you have cd'd into the pipeline.

Examples:
  clavesa node list my-pipeline
  clavesa node add my-pipeline --type transform --name enrich
  clavesa source attach my-pipeline trips --to enrich --as trips
  clavesa node edit my-pipeline enrich --set sql="SELECT * FROM trips"
  clavesa node preview enrich              # from inside the pipeline dir`,
		RunE: requireSubcommand(),
	}

	cmd.AddCommand(
		newNodeListCmd(),
		newNodeShowCmd(),
		newNodeAddCmd(),
		newNodeEditCmd(),
		newNodeRenameCmd(),
		newNodeRemoveCmd(),
		newNodeConnectCmd(),
		newNodeDisconnectCmd(),
		newNodePreviewCmd(),
	)

	return cmd
}

func newNodeListCmd() *cobra.Command {
	var jsonOut bool

	cmd := &cobra.Command{
		Use:   "list [pipeline-dir]",
		Short: "List nodes in a pipeline",
		Long:  "List nodes in a pipeline.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			g, err := svc.GetPipeline(dir)
			if err != nil {
				return fmt.Errorf("get pipeline: %w", err)
			}
			if jsonOut {
				return printJSON(os.Stdout, g.Nodes)
			}
			if len(g.Nodes) == 0 {
				fmt.Println("No nodes.")
				return nil
			}
			rows := make([][]string, len(g.Nodes))
			for i, n := range g.Nodes {
				rows[i] = []string{n.ID, n.Type, n.ModuleSource}
			}
			printTable(os.Stdout, []string{"ID", "TYPE", "MODULE"}, rows)
			return nil
		},
	}

	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")

	return cmd
}

func newNodeShowCmd() *cobra.Command {
	var jsonOut bool

	cmd := &cobra.Command{
		Use:   "show [pipeline-dir] <node-id>",
		Short: "Show node details",
		Long:  "Show node details.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, rest, ws, err := resolvePipelineDir(cmd, args, 1)
			if err != nil {
				return err
			}
			nodeID := rest[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			g, err := svc.GetPipeline(dir)
			if err != nil {
				return fmt.Errorf("get pipeline: %w", err)
			}
			n := findNode(&g, nodeID)
			if n == nil {
				return fmt.Errorf("node %q not found in %s", nodeID, displayDir(ws, dir))
			}
			if jsonOut {
				return printJSON(os.Stdout, n)
			}
			fmt.Printf("ID:      %s\n", n.ID)
			fmt.Printf("Type:    %s\n", n.Type)
			fmt.Printf("Module:  %s\n", n.ModuleSource)
			display := filterInternalKeys(n.Config)
			if len(display) > 0 {
				fmt.Println()
				fmt.Println("Config:")
				keys := sortedKeys(display)
				for _, k := range keys {
					fmt.Printf("  %-12s %v\n", k, display[k])
				}
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")

	return cmd
}

func newNodeAddCmd() *cobra.Command {
	var nodeType string
	var name string

	cmd := &cobra.Command{
		Use:   "add <pipeline-dir>",
		Short: "Add a node to a pipeline",
		Long: `Add a new node to a pipeline.

Use --name to give the node a meaningful identifier (e.g. "enrich_logs"
or "warehouse"). If omitted, a sequential name like "transform2" is
generated.

ADR-017 slice 4: --type source and --from are gone — sources are
workspace-level registry entries now. Use:

  clavesa source register <name> --from <url> --attach <pipeline> --to <transform>

Examples:
  clavesa node add my-pipeline --type transform --name enrich_logs
  clavesa node add my-pipeline --type destination

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if nodeType == "source" {
				return fmt.Errorf("inline source nodes have been removed (ADR-017 slice 4); use `clavesa source register --from <url>` and `clavesa source attach <pipeline> <name> --to <transform>` instead")
			}
			if nodeType == "" {
				return fmt.Errorf("--type is required (transform, destination)")
			}
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			g, err := svc.AddNode(dir, nodeType, name)
			if err != nil {
				return fmt.Errorf("add node: %w", err)
			}
			var addedID string
			for _, n := range g.Nodes {
				if n.Type == nodeType {
					addedID = n.ID
				}
			}
			fmt.Printf("Added node: %s\n", addedID)
			return nil
		},
	}

	cmd.Flags().StringVar(&nodeType, "type", "", "node type: transform or destination (sources: see `clavesa source register`)")
	cmd.Flags().StringVar(&name, "name", "", "node name (e.g. enrich_logs); auto-generated if omitted")

	return cmd
}

func newNodeEditCmd() *cobra.Command {
	sets := make(setFlags)
	var outputMode string
	var outputMergeKeys []string
	var outputStats bool
	var addOutputs []string
	var removeOutputs []string
	var addIncrementalInputs []string
	var removeIncrementalInputs []string

	cmd := &cobra.Command{
		Use:   "edit <pipeline-dir> <node-id>",
		Short: "Edit node configuration",
		Long: `Edit node configuration using --set key=value flags.

Run without flags to see all settable config keys for a node.

Output shape (transforms): --output-mode and --output-merge-keys edit
the default output's writer behaviour without touching nested HCL.
Use --add-output / --remove-output (repeatable) to manage additional
output keys for multi-output Python transforms that return more than
one DataFrame. New outputs are seeded with replace mode; tune them
with --set output_definitions={...} or by hand-editing the .tf for
now. Pass --output-merge-keys with no value to clear the existing
keys; pass --output-mode "" likewise.

Examples:
  clavesa node edit my-pipeline source1 --set bucket=my-data
  clavesa node edit my-pipeline transform1 --set sql="SELECT * FROM source1"
  clavesa node edit my-pipeline transform1 --set python=file(transforms/enrich.py)
  clavesa node edit my-pipeline dim_customers --output-merge-keys customer_id
  clavesa node edit my-pipeline orders --output-mode append
  clavesa node edit my-pipeline enrich --add-output outliers
  clavesa node edit my-pipeline source1    # show settable keys

` + pipelineDirHelp,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, rest, ws, err := resolvePipelineDir(cmd, args, 1)
			if err != nil {
				return err
			}
			nodeID := rest[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			modeChanged := cmd.Flags().Changed("output-mode")
			keysChanged := cmd.Flags().Changed("output-merge-keys")
			statsChanged := cmd.Flags().Changed("output-stats")
			outputsChanged := len(addOutputs) > 0 || len(removeOutputs) > 0
			incrementalsChanged := len(addIncrementalInputs) > 0 || len(removeIncrementalInputs) > 0
			if len(sets) == 0 && !modeChanged && !keysChanged && !statsChanged && !outputsChanged && !incrementalsChanged {
				g, err := svc.GetPipeline(dir)
				if err != nil {
					return fmt.Errorf("get pipeline: %w", err)
				}
				n := findNode(&g, nodeID)
				if n == nil {
					return fmt.Errorf("node %q not found in %s", nodeID, displayDir(ws, dir))
				}
				fmt.Printf("Node %s (%s) — settable config:\n\n", n.ID, n.Type)
				display := mergeOptionalFields(n.Type, n.Config)
				keys := sortedKeys(display)
				for _, k := range keys {
					fmt.Printf("  --set %s=%v\n", k, display[k])
				}
				if len(keys) == 0 {
					fmt.Println("  (no config keys)")
				}
				if defs, ok := n.Config["output_definitions"].(map[string]interface{}); ok && len(defs) > 0 {
					fmt.Println()
					fmt.Println("Outputs:")
					for _, key := range sortedKeys(defs) {
						def, _ := defs[key].(map[string]interface{})
						mode, _ := def["mode"].(string)
						if mode == "" {
							mode = "(default)"
						}
						parts := []string{"mode=" + mode}
						if mk, ok := def["merge_keys"].([]interface{}); ok && len(mk) > 0 {
							ks := make([]string, len(mk))
							for i, v := range mk {
								ks[i] = fmt.Sprint(v)
							}
							parts = append(parts, "merge_keys="+joinComma(ks))
						}
						if stats, _ := def["stats"].(bool); stats {
							parts = append(parts, "stats=true")
						}
						fmt.Printf("  %s — %s\n", key, joinComma(parts))
					}
				}
				return nil
			}
			attrs := map[string]interface{}(sets)
			if modeChanged || keysChanged || statsChanged || outputsChanged {
				g, err := svc.GetPipeline(dir)
				if err != nil {
					return fmt.Errorf("get pipeline: %w", err)
				}
				n := findNode(&g, nodeID)
				if n == nil {
					return fmt.Errorf("node %q not found in %s", nodeID, displayDir(ws, dir))
				}
				existing, _ := n.Config["output_definitions"].(map[string]interface{})
				updated := updateOutputDefault(existing, modeChanged, outputMode, keysChanged, outputMergeKeys)
				for _, key := range removeOutputs {
					if key == "default" {
						return fmt.Errorf("cannot remove the 'default' output (every transform has one)")
					}
					delete(updated, key)
				}
				for _, key := range addOutputs {
					if err := validateOutputKey(key); err != nil {
						return err
					}
					if _, exists := updated[key]; !exists {
						updated[key] = map[string]interface{}{}
					}
				}
				if statsChanged {
					applyOutputStats(updated, outputStats)
				}
				if attrs == nil {
					attrs = map[string]interface{}{}
				}
				attrs["output_definitions"] = updated
			}
			if incrementalsChanged {
				g, err := svc.GetPipeline(dir)
				if err != nil {
					return fmt.Errorf("get pipeline: %w", err)
				}
				n := findNode(&g, nodeID)
				if n == nil {
					return fmt.Errorf("node %q not found in %s", nodeID, displayDir(ws, dir))
				}
				current := map[string]bool{}
				if existing, ok := n.Config["incremental_inputs"].([]interface{}); ok {
					for _, v := range existing {
						if s, ok := v.(string); ok && s != "" {
							current[s] = true
						}
					}
				}
				for _, alias := range removeIncrementalInputs {
					delete(current, alias)
				}
				for _, alias := range addIncrementalInputs {
					if alias == "" {
						return fmt.Errorf("--incremental-input requires a non-empty alias")
					}
					current[alias] = true
				}
				ordered := make([]interface{}, 0, len(current))
				for a := range current {
					ordered = append(ordered, a)
				}
				sort.Slice(ordered, func(i, j int) bool {
					return ordered[i].(string) < ordered[j].(string)
				})
				if attrs == nil {
					attrs = map[string]interface{}{}
				}
				attrs["incremental_inputs"] = ordered
			}
			// Parse-check inline SQL before persisting (Slice 3). file()-
			// referenced SQL is wrapped as a service.Ref above so the
			// type assertion misses it intentionally — only literal
			// strings flow through here. The check is a no-op when no
			// parser is wired (CLI integration tests). Transport
			// failures (no docker, missing image) become warnings —
			// the user can still author; the runner surfaces real
			// parse errors at dispatch time.
			if newSQL, ok := attrs["sql"].(string); ok && newSQL != "" {
				if err := svc.ValidateSQL(cmd.Context(), newSQL); err != nil {
					var pe *service.ParseError
					if errors.As(err, &pe) {
						return fmt.Errorf("SQL parse failed:\n  %s", pe.Message)
					}
					fmt.Fprintf(os.Stderr, "warn: SQL parse-check skipped: %v\n", err)
				}
			}
			if _, err := svc.UpdateNode(dir, nodeID, attrs); err != nil {
				return fmt.Errorf("update node: %w", err)
			}
			fmt.Printf("Updated node: %s\n", nodeID)
			return nil
		},
	}

	cmd.Flags().Var(sets, "set", "set key=value (repeatable)")
	cmd.Flags().StringVar(&outputMode, "output-mode", "", `default-output mode: "replace" (default; full overwrite), "append", or "merge"`)
	cmd.Flags().StringSliceVar(&outputMergeKeys, "output-merge-keys", nil, `comma-separated columns forming the natural key; sets mode=merge implicitly`)
	cmd.Flags().BoolVar(&outputStats, "output-stats", false, `opt this transform's outputs into per-column stats (null %, distinct, top-K, percentiles); pass --output-stats=false to turn off`)
	cmd.Flags().StringSliceVar(&addOutputs, "add-output", nil, "declare an additional output key on a multi-output transform (repeatable). Seeded with mode=replace; tune via direct .tf edit")
	cmd.Flags().StringSliceVar(&removeOutputs, "remove-output", nil, "remove a non-default output key from output_definitions (repeatable)")
	cmd.Flags().StringSliceVar(&addIncrementalInputs, "incremental-input", nil, "read this input alias incrementally (Iceberg snapshot range, watermark-tracked). Repeatable. Per-input opt-in; transforms full-read by default")
	cmd.Flags().StringSliceVar(&removeIncrementalInputs, "non-incremental-input", nil, "drop an alias from incremental_inputs so it reverts to full-read on every run (repeatable)")

	return cmd
}

// validateOutputKey enforces a minimal identifier rule on output-key
// names: must match Iceberg table-suffix conventions (the runner writes
// `<node>__<key>`) which is the same `[A-Za-z_][A-Za-z0-9_]*` Glue
// allows. Reject up-front so a typo doesn't land in .tf and surface as
// an opaque terraform error.
func validateOutputKey(key string) error {
	if key == "" {
		return fmt.Errorf("--add-output requires a non-empty key")
	}
	first := key[0]
	if !(first == '_' || (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')) {
		return fmt.Errorf("output key %q must start with a letter or underscore", key)
	}
	for _, c := range key {
		if !(c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return fmt.Errorf("output key %q contains invalid char %q (allowed: a-z A-Z 0-9 _)", key, c)
		}
	}
	return nil
}

// updateOutputDefault returns a new output_definitions map with the "default"
// entry's mode + merge_keys updated. Other outputs (rare; multi-output transforms)
// are preserved as-is. Passing an empty mode/keys clears the field.
func updateOutputDefault(existing map[string]interface{}, modeChanged bool, mode string, keysChanged bool, keys []string) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range existing {
		out[k] = v
	}
	def, _ := out["default"].(map[string]interface{})
	if def == nil {
		def = map[string]interface{}{}
	} else {
		// shallow-clone so callers can't mutate the parsed graph
		cloned := map[string]interface{}{}
		for k, v := range def {
			cloned[k] = v
		}
		def = cloned
	}
	if modeChanged {
		if mode == "" {
			delete(def, "mode")
		} else {
			def["mode"] = mode
		}
	}
	if keysChanged {
		if len(keys) == 0 {
			delete(def, "merge_keys")
		} else {
			items := make([]interface{}, len(keys))
			for i, k := range keys {
				items[i] = k
			}
			def["merge_keys"] = items
			// Recipe contract: setting merge_keys without an explicit
			// mode flips the output to merge automatically (the flag's
			// own help text says so). Without this, the HCL carries
			// merge_keys but no mode, and buildLocalOutputs defaults
			// mode → "replace" before reaching the runner — silently
			// turning the recipe's MERGE INTO into a full-table replace
			// that still keeps the row count flat but loses
			// snapshot-history semantics.
			if !modeChanged {
				if existingMode, _ := def["mode"].(string); existingMode == "" {
					def["mode"] = "merge"
				}
			}
		}
	}
	out["default"] = def
	return out
}

// applyOutputStats writes the stats flag onto every output_definitions
// entry. The CLI exposes one transform-wide knob (matching the editor
// checkbox); per-key splits are deferred until a real pipeline asks for
// the asymmetry. Mutates `defs` in place — `default` is materialised if
// absent so the toggle survives a fresh transform.
func applyOutputStats(defs map[string]interface{}, on bool) {
	if defs == nil {
		return
	}
	if _, ok := defs["default"]; !ok {
		defs["default"] = map[string]interface{}{}
	}
	for k, v := range defs {
		def, _ := v.(map[string]interface{})
		if def == nil {
			def = map[string]interface{}{}
		}
		if on {
			def["stats"] = true
		} else {
			delete(def, "stats")
		}
		defs[k] = def
	}
}

func joinComma(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += ", "
		}
		out += s
	}
	return out
}

func newNodeRenameCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rename [pipeline-dir] <old-id> <new-id>",
		Short: "Rename a node",
		Long: "Rename a node — the module block, every downstream edge that\n" +
			"reads it, and its SQL/PySpark script files all move to the new\n" +
			"name.\n\n" +
			"Note: a node's id is also the stem of its Iceberg output table\n" +
			"(<node>__default), so a rename changes that table's name. Data\n" +
			"already written under the old name is not moved.\n\n" +
			pipelineDirHelp,
		Args: cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, rest, _, err := resolvePipelineDir(cmd, args, 2)
			if err != nil {
				return err
			}
			oldID, newID := rest[0], rest[1]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if _, err := svc.RenameNode(dir, oldID, newID); err != nil {
				return fmt.Errorf("rename node: %w", err)
			}
			fmt.Printf("Renamed node: %s -> %s\n", oldID, newID)
			return nil
		},
	}
}

func newNodeRemoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "remove [pipeline-dir] <node-id>",
		Short: "Remove a node from a pipeline",
		Long:  "Remove a node from a pipeline.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, rest, _, err := resolvePipelineDir(cmd, args, 1)
			if err != nil {
				return err
			}
			nodeID := rest[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if _, err := svc.DeleteNode(dir, nodeID); err != nil {
				return fmt.Errorf("remove node: %w", err)
			}
			fmt.Printf("Removed node: %s\n", nodeID)
			return nil
		},
	}
}

func newNodeConnectCmd() *cobra.Command {
	var from, fromTable, to, output, input string

	cmd := &cobra.Command{
		Use:   "connect <pipeline-dir>",
		Short: "Connect a node, source, or external table to a transform's input",
		Long: `Wire a transform's inputs map. Three forms, mutually exclusive:

  --from <node>                 intra-pipeline edge (this pipeline's source/transform → this transform).
  --from-table <schema>.<table> cross-pipeline / external-table read (ADR-016 slice 2).
                                Resolved against the workspace catalog at orchestration-sync time.

The --input flag sets the SQL table alias for the connection. It defaults
to the from-node ID (--from) or the table-name portion (--from-table) so
the SQL reads naturally — e.g. ` + "`FROM dim_customers`" + ` over a
` + "`marketing.dim_customers`" + ` reference.

Examples:
  clavesa node connect my-pipeline --from source1 --to transform1
  clavesa node connect my-pipeline --from source1 --to transform1 --input raw
  clavesa node connect my-pipeline --from-table marketing.dim_customers --to enrich

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if to == "" {
				return fmt.Errorf("--to is required")
			}
			if (from == "") == (fromTable == "") {
				return fmt.Errorf("pass exactly one of --from or --from-table")
			}
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if fromTable != "" {
				if err := svc.AttachExternalTable(dir, fromTable, to, input); err != nil {
					return fmt.Errorf("attach external table: %w", err)
				}
				fmt.Printf("Connected: %s -> %s\n", fromTable, to)
				return nil
			}
			if input == "" {
				input = from
			}
			if _, err := svc.AddEdge(dir, from, output, to, input); err != nil {
				return fmt.Errorf("connect nodes: %w", err)
			}
			fmt.Printf("Connected: %s-%s->%s\n", from, output, to)
			return nil
		},
	}

	cmd.Flags().StringVar(&from, "from", "", "source node ID (intra-pipeline edge)")
	cmd.Flags().StringVar(&fromTable, "from-table", "", "cross-pipeline / external table reference (<schema>.<table>)")
	cmd.Flags().StringVar(&to, "to", "", "target node ID")
	cmd.Flags().StringVar(&output, "output", "default", "output port name (intra-pipeline only)")
	cmd.Flags().StringVar(&input, "input", "", "SQL table alias for this input")

	return cmd
}

func newNodeDisconnectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disconnect [pipeline-dir] <edge-id>",
		Short: "Remove an edge between nodes",
		Long:  "Remove an edge between nodes.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, rest, _, err := resolvePipelineDir(cmd, args, 1)
			if err != nil {
				return err
			}
			edgeID := rest[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if _, err := svc.DeleteEdge(dir, edgeID); err != nil {
				return fmt.Errorf("disconnect edge: %w", err)
			}
			fmt.Printf("Disconnected edge: %s\n", edgeID)
			return nil
		},
	}
}

func newNodePreviewCmd() *cobra.Command {
	var jsonOut bool
	var rows, offset int

	cmd := &cobra.Command{
		Use:   "preview [pipeline-dir] <node-id>",
		Short: "Preview data flowing through a node",
		Long:  "Preview data flowing through a node.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, rest, ws, err := resolvePipelineDir(cmd, args, 1)
			if err != nil {
				return err
			}
			nodeID := rest[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			g, err := svc.GetPipeline(dir)
			if err != nil {
				return fmt.Errorf("get pipeline: %w", err)
			}
			n := findNode(&g, nodeID)
			if n == nil {
				return fmt.Errorf("node %q not found in %s", nodeID, displayDir(ws, dir))
			}
			switch n.Type {
			case "destination":
				var upstream *struct{ id, typ string }
				for _, e := range g.Edges {
					if e.ToNode == nodeID {
						u := findNode(&g, e.FromNode)
						if u != nil {
							upstream = &struct{ id, typ string }{u.ID, u.Type}
						}
						break
					}
				}
				if upstream == nil {
					fmt.Fprintln(os.Stderr, "no upstream node connected to this destination")
					return nil
				}
				if upstream.typ == "transform" {
					return previewTransform(svc, dir, upstream.id, rows, jsonOut)
				}
				return previewSource(svc, dir, upstream.id, offset, jsonOut)
			case "transform":
				return previewTransform(svc, dir, nodeID, rows, jsonOut)
			default:
				return previewSource(svc, dir, nodeID, offset, jsonOut)
			}
		},
	}

	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().IntVar(&rows, "rows", 15, "number of rows to fetch (transform nodes)")
	cmd.Flags().IntVar(&offset, "offset", 0, "row offset (source nodes)")

	return cmd
}

func previewSource(svc *service.Service, dir, nodeID string, offset int, jsonOut bool) error {
	result, err := svc.PreviewSource(context.Background(), dir, nodeID, offset, 10)
	if err != nil {
		return fmt.Errorf("preview source: %w", err)
	}
	if jsonOut {
		return printJSON(os.Stdout, result)
	}
	for _, item := range result.Items {
		b, _ := json.Marshal(item)
		fmt.Printf("%s\n", b)
	}
	return nil
}

func previewTransform(svc *service.Service, dir, nodeID string, rowCount int, jsonOut bool) error {
	result, err := svc.PreviewTransform(context.Background(), dir, nodeID, rowCount)
	if err != nil {
		return fmt.Errorf("preview transform: %w", err)
	}
	if jsonOut {
		return printJSON(os.Stdout, result)
	}
	for _, pair := range result.Pairs {
		for _, row := range pair.Output {
			b, _ := json.Marshal(row)
			fmt.Printf("%s\n", b)
		}
	}
	return nil
}
