package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/servingsql"
	wspkg "github.com/vesahyp/clavesa/internal/workspace"
)

// setFlags accumulates multiple --set key=value flags.
type setFlags map[string]interface{}

func (f setFlags) String() string { return fmt.Sprint(map[string]interface{}(f)) }
func (f setFlags) Type() string   { return "key=value" }

func (f setFlags) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("--set requires key=value format, got %q", s)
	}
	// Treat bare HCL expressions as references so they are written without
	// quotes: function calls (file, toset, …), variable refs, module
	// outputs, and inline object/list literals.
	if strings.HasPrefix(v, "file(") {
		v = quoteFileArg(v)
		f[k] = service.Ref(v)
	} else if strings.HasPrefix(v, "var.") ||
		strings.HasPrefix(v, "module.") ||
		strings.HasPrefix(v, "{") ||
		strings.HasPrefix(v, "[") {
		f[k] = service.Ref(v)
	} else {
		f[k] = v
	}
	return nil
}

// resolveWorkspace returns an absolute workspace path and applies the
// workspace's persisted AWS profile to this process's environment.
// Resolution order:
//  1. --workspace flag (explicit override)
//  2. $CLAVESA_WORKSPACE env var (per-shell selection)
//  3. cwd-walk searching for clavesa.json
//  4. State file written by `workspace init` / `workspace use`
//  5. cwd as last resort
//
// The cwd-walk ranks above the state file deliberately: standing physically
// inside a workspace means you mean *that* workspace. The state file is the
// fallback for when you are not inside any workspace — it stays usable across
// terminals, and the env var lets users pin a different workspace per shell.
//
// Most users will only ever have one workspace; threading --workspace through
// every command is pure noise.
//
// resolveWorkspace is the chokepoint every command uses to establish its
// workspace context, so the AWS-profile selection — which is part of that
// context — is applied here too (see applyWorkspaceAWSProfile).
func resolveWorkspace(cmd *cobra.Command) (string, error) {
	root, err := resolveWorkspaceRoot(cmd)
	if err != nil {
		return "", err
	}
	applyWorkspaceAWSProfile(root)
	return root, nil
}

// resolveWorkspaceRoot resolves the workspace path with no side effects.
func resolveWorkspaceRoot(cmd *cobra.Command) (string, error) {
	if w, _ := cmd.Flags().GetString("workspace"); w != "" {
		return filepath.Abs(w)
	}
	if w := os.Getenv("CLAVESA_WORKSPACE"); w != "" {
		return filepath.Abs(w)
	}
	if w := walkUpForManifest(); w != "" {
		return w, nil
	}
	if w := readWorkspaceStateFile(); w != "" {
		if _, err := os.Stat(filepath.Join(w, "clavesa.json")); err == nil {
			return w, nil
		}
	}
	return os.Getwd()
}

// applyWorkspaceAWSProfile makes the workspace's persisted AWS profile
// (`.clavesa/aws-profile.json`, set by `workspace use --profile` or
// the UI switcher) this process's AWS_PROFILE — so every CLI command
// that builds an AWS client or forwards credentials into the runner
// operates as the workspace declares, the same rule `clavesa ui`
// applies. The file is authoritative: when set it overrides an exported
// AWS_PROFILE, matching the UI's behaviour. No-op when no profile is
// persisted.
func applyWorkspaceAWSProfile(root string) {
	if p := wspkg.LoadAWSProfile(root); p != "" {
		os.Setenv("AWS_PROFILE", p)
	}
}

// printTargetContext writes a short summary — resolved workspace,
// warehouse, and effective AWS profile — to stderr before an operating
// or destructive command (run / deploy / destroy) acts, so which world
// the command targets is never a mystery. stderr, not stdout, keeps
// `--json` output clean. An empty warehouse is omitted (deploy /
// destroy have no local/cloud axis). workspaceRoot may be empty; if
// non-empty, the workspace line is printed with the manifest name when
// readable and the absolute path.
func printTargetContext(action, workspaceRoot string, warehouse wspkg.Warehouse) {
	if workspaceRoot != "" {
		name := workspaceRoot
		if m, err := wspkg.Load(workspaceRoot); err == nil && m != nil && m.Name != "" {
			name = m.Name
		} else {
			name = filepath.Base(workspaceRoot)
		}
		fmt.Fprintf(os.Stderr, "→ workspace: %s (%s)\n", name, workspaceRoot)
	}
	profile := os.Getenv("AWS_PROFILE")
	if profile == "" {
		profile = "default credential chain"
	}
	if warehouse != "" {
		fmt.Fprintf(os.Stderr, "%s · warehouse: %s · aws profile: %s\n", action, warehouse, profile)
	} else {
		fmt.Fprintf(os.Stderr, "%s · aws profile: %s\n", action, profile)
	}
}

// workspaceStateFile is the path of the small file `workspace init`/`use`
// writes to remember the current workspace across shells. XDG-style: respect
// $XDG_CONFIG_HOME, otherwise fall back to ~/.config/clavesa/.
func workspaceStateFile() string {
	cfg := os.Getenv("XDG_CONFIG_HOME")
	if cfg == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		cfg = filepath.Join(home, ".config")
	}
	return filepath.Join(cfg, "clavesa", "current-workspace")
}

// readWorkspaceStateFile returns the path stored by `workspace init`/`use`,
// or "" if absent or unreadable.
func readWorkspaceStateFile() string {
	p := workspaceStateFile()
	if p == "" {
		return ""
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// writeWorkspaceStateFile records the absolute workspace path for future
// invocations. Best-effort: a state-file write failure does not abort
// `workspace init` (the workspace was created either way).
func writeWorkspaceStateFile(absPath string) error {
	p := workspaceStateFile()
	if p == "" {
		return fmt.Errorf("cannot determine config dir for workspace state file")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(absPath+"\n"), 0o644)
}

// walkUpForManifest looks for clavesa.json in cwd and each ancestor.
// Returns the directory containing the manifest, or "" if none found.
func walkUpForManifest() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "clavesa.json")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// cliCloseables holds Close functions registered by newService so the
// CLI can shut down warm-worker containers spawned during a one-shot
// command. Each `clavesa <cmd>` invocation that touches the SQL parser
// would otherwise leave a ~1GB container behind.
var cliCloseables []func()

// registerCloseable appends a teardown to run after the current CLI
// command finishes. Safe to call from any newService /
// newDashboardService path.
func registerCloseable(fn func()) {
	cliCloseables = append(cliCloseables, fn)
}

// runCloseables stops every registered teardown. Called by Execute /
// Run after the root command returns (success or failure). Idempotent
// — resets the slice afterward so a future Execute call starts clean
// (relevant for unit tests that invoke Run() repeatedly).
func runCloseables() {
	for _, fn := range cliCloseables {
		fn()
	}
	cliCloseables = nil
}

// newService builds a service.Service after resolving the workspace from the
// command's flags.
//
// The service gets a lazy SQL parser wired (Slice 3): the underlying
// persistent runner only spawns a docker container the first time
// Parse is called, so commands that never validate SQL stay
// docker-free. Commands that do hit the parser (node edit, node
// preview, dashboards apply, sql lint) pay one ~30s cold spawn per
// CLI invocation; the container is torn down by runCloseables() at
// command exit.
func newService(cmd *cobra.Command) (*service.Service, string, error) {
	workspace, err := resolveWorkspace(cmd)
	if err != nil {
		return nil, "", fmt.Errorf("resolve workspace: %w", err)
	}
	svc := service.New(workspace)
	warm := observability.NewPersistentQueryRunner(workspace)
	// Parser resolves the active warehouse (ADR-024) lazily on first
	// Parse; cloud + undeployed surfaces an actionable error there.
	parser := warm.SQLParserForWorkspace()
	registerCloseable(warm.Close)
	sidecar := observability.NewTranspileSidecar(workspace)
	registerCloseable(sidecar.Close)
	transpiler := servingsql.NewCachedTranspiler(filepath.Join(workspace, ".clavesa", "cache", "transpile"), sidecar.ToServing)
	svc = svc.WithSQLParser(parser).WithTranspiler(transpiler).WithMetastoreEnsurer(metastoreEnsurer())
	return svc, workspace, nil
}

// metastoreEnsurer returns the production shared-metastore ensurer wired
// into the Service via WithMetastoreEnsurer (CLI + UI). It brings up (or
// reuses) the per-workspace Derby metastore container and returns the
// (docker network, addr) the run containers attach to. Best-effort: on
// failure it logs to stderr and returns ("", ""), so the containers fall
// back to embedded Derby — the exact semantics the run.go call sites had
// before the seam was extracted.
func metastoreEnsurer() func(ctx context.Context, workspaceRoot, workspaceName string) (string, string) {
	return func(ctx context.Context, workspaceRoot, workspaceName string) (string, string) {
		addr, err := observability.EnsureMetastore(ctx, workspaceRoot, workspaceName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "clavesa: falling back to embedded metastore (shared metastore unavailable): %v\n", err)
			return "", ""
		}
		return observability.MetastoreNetwork(), addr
	}
}

// pipelineDirHelp documents the <pipeline-dir> positional shared by every
// pipeline-scoped command. Appended to each command's Long text.
const pipelineDirHelp = `Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.`

// resolvePipelineDir unifies the <pipeline-dir> positional that every
// pipeline-scoped command shares. fixedCount is the number of required
// non-dir positionals that FOLLOW the dir (0 for `pipeline show`, 1 for
// `node show <node-id>` / `backfill diff <run_id>`). Callers set
// Args: cobra.RangeArgs(fixedCount, fixedCount+1).
//
// It returns an absolute pipeline dir (service.resolveDir accepts absolute
// paths, and terraform commands need one anyway), the trailing fixed
// positionals, and the resolved workspace root (callers need it for
// displayDir and deploy/run flows).
func resolvePipelineDir(cmd *cobra.Command, args []string, fixedCount int) (absDir string, rest []string, ws string, err error) {
	ws, err = resolveWorkspace(cmd)
	if err != nil {
		return "", nil, "", err
	}
	switch len(args) {
	case fixedCount: // dir omitted — infer from cwd
		cwd, err := os.Getwd()
		if err != nil {
			return "", nil, "", err
		}
		abs, _ := filepath.Abs(cwd)
		if !service.IsPipelineDir(abs) {
			return "", nil, "", fmt.Errorf("no pipeline directory given and the current directory is not a pipeline — pass <pipeline-dir> or cd into one")
		}
		return abs, args, ws, nil
	case fixedCount + 1: // dir explicit
		dir := args[0]
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(ws, dir)
		}
		abs := filepath.Clean(dir)
		if !service.IsPipelineDir(abs) {
			return "", nil, "", fmt.Errorf("%s is not a pipeline directory (no .tf files, or it is the workspace root)", args[0])
		}
		return abs, args[1:], ws, nil
	default:
		// Unreachable when Args is RangeArgs(fixedCount, fixedCount+1).
		return "", nil, "", fmt.Errorf("expected at most one <pipeline-dir> argument")
	}
}

// displayDir renders an absolute pipeline dir relative to the workspace root
// for user-facing output, falling back to the absolute path when the dir is
// outside the workspace.
func displayDir(ws, absDir string) string {
	if rel, err := filepath.Rel(ws, absDir); err == nil && !strings.HasPrefix(rel, "..") {
		return rel
	}
	return absDir
}

// findNode returns a pointer to the node with the given ID, or nil.
func findNode(g *graph.PipelineGraph, nodeID string) *graph.Node {
	for i := range g.Nodes {
		if g.Nodes[i].ID == nodeID {
			return &g.Nodes[i]
		}
	}
	return nil
}

// internalKeys are config keys managed by the tool itself. Users should not
// need to see or edit these — they are set automatically by node add and
// workspace init.
var internalKeys = map[string]bool{
	"source":             true,
	"pipeline_name":      true,
	"name":               true,
	"output_bucket":      true,
	"pipeline_bucket":    true,
	"depends_on":         true,
	"runner_image":       true,
	"output_definitions": true,
	// ADR-017: parser surfaces source-registry references as a synthetic
	// config key (`{alias: "sources.<name>"}`); rendered separately by
	// `node show`'s "Inputs:" block. Hide from the generic config dump.
	"source_inputs": true,
}

// mergeOptionalFields returns a display map that includes all existing config
// keys plus any well-known optional fields for the given node type that are not
// already present. Internal keys managed by the tool are excluded.
func mergeOptionalFields(nodeType string, config map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(config))
	for k, v := range config {
		if internalKeys[k] {
			continue
		}
		if s, ok := v.(string); ok && isTerraformRef(s) {
			continue
		}
		result[k] = v
	}
	switch nodeType {
	case "source":
		if _, ok := result["json_path"]; !ok {
			result["json_path"] = ""
		}
	}
	return result
}

// filterInternalKeys returns a copy of config with internal keys removed,
// as well as any keys whose values are Terraform references (e.g.
// "data.terraform_remote_state..." or "aws_s3_bucket...") that were set
// automatically and are not meaningful to end users.
func filterInternalKeys(config map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(config))
	for k, v := range config {
		if internalKeys[k] {
			continue
		}
		if s, ok := v.(string); ok && isTerraformRef(s) {
			continue
		}
		result[k] = v
	}
	return result
}

func isTerraformRef(s string) bool {
	return strings.HasPrefix(s, "data.") ||
		strings.HasPrefix(s, "aws_") ||
		strings.HasPrefix(s, "module.") ||
		strings.HasPrefix(s, "var.")
}

// sortedKeys returns sorted keys of a map[string]interface{}.
func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// quoteFileArg ensures the argument inside a file() call is double-quoted so
// that hclwrite doesn't reformat path separators as division operators.
func quoteFileArg(expr string) string {
	inner := strings.TrimPrefix(expr, "file(")
	inner = strings.TrimSuffix(inner, ")")
	inner = strings.TrimSpace(inner)
	if strings.HasPrefix(inner, `"`) {
		return expr
	}
	return `file("` + inner + `")`
}
