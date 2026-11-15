package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/service"
	wspkg "github.com/vesahyp/clavesa/internal/workspace"
	"github.com/vesahyp/clavesa/internal/wsdeps"
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
//
// The workspace's recorded region (GH #107, wspkg.ResolveRegion) becomes
// AWS_REGION and AWS_DEFAULT_REGION the same way, so every AWS client and
// every Terraform run of the command uses the region the workspace lives
// in. It wins over an exported AWS_REGION (see internal/workspace/region.go
// for why); when the two differ the command says so on stderr.
func applyWorkspaceAWSProfile(root string) {
	if p := wspkg.LoadAWSProfile(root); p != "" {
		os.Setenv("AWS_PROFILE", p)
	}
	r := wspkg.ResolveRegion(context.Background(), root)
	if r == "" {
		return
	}
	if cur, src := explicitEnvRegion(); cur != "" && cur != r {
		fmt.Fprintf(os.Stderr, "clavesa: using the workspace region %s, not %s=%s (the workspace is deployed in %s; to change it, edit \"region\" in clavesa.json)\n", r, src, cur, r)
	}
	os.Setenv("AWS_REGION", r)
	os.Setenv("AWS_DEFAULT_REGION", r)
}

// explicitEnvRegion returns the region exported in the environment and the
// variable it came from: AWS_REGION first, then AWS_DEFAULT_REGION.
func explicitEnvRegion() (string, string) {
	if v := strings.TrimSpace(os.Getenv("AWS_REGION")); v != "" {
		return v, "AWS_REGION"
	}
	if v := strings.TrimSpace(os.Getenv("AWS_DEFAULT_REGION")); v != "" {
		return v, "AWS_DEFAULT_REGION"
	}
	return "", ""
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
	if r, _ := explicitEnvRegion(); r != "" {
		profile += " · region: " + r
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

// cliCloseables holds Close functions registered by newServiceDeps so the
// CLI can shut down warm-worker containers spawned during a one-shot
// command. Each `clavesa <cmd>` invocation that touches the SQL parser
// would otherwise leave a ~1GB container behind.
var cliCloseables []func()

// registerCloseable appends a teardown to run after the current CLI
// command finishes. Safe to call from any newServiceDeps path.
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

// keepMetastoreEnv is the environment variable that keeps the workspace's
// shared metastore container running after a one-shot command, the same
// as the `--keep-metastore` persistent flag. For cron and CI wrappers that
// run many commands back to back and want the warm metastore without
// editing every invocation (GH #99).
const keepMetastoreEnv = "CLAVESA_KEEP_METASTORE"

// metastoreReleaseTimeout bounds the whole post-command metastore release
// (a handful of short docker calls per container). Command exit must stay
// prompt even when the daemon is wedged; the release is best-effort.
const metastoreReleaseTimeout = 20 * time.Second

// keepMetastoreRequested decides whether the metastore this command
// started should be left running: the flag wins when set, otherwise the
// environment variable counts as set unless it is empty or one of the
// usual "off" spellings ("0", "false", "off", "no", any case). Pure so
// the precedence and the spellings are unit-testable.
func keepMetastoreRequested(flag bool, env string) bool {
	if flag {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "", "0", "false", "off", "no":
		return false
	}
	return true
}

// releaseMetastores is the post-command hook that takes down the shared
// metastore container when this process started it and nothing else is
// using it (GH #99; the decision rules are in
// observability.ReleaseCreatedMetastores). Runs after runCloseables from
// Execute and Run. Quiet on every path: this runs under cron, where any
// output becomes mail, and the release is best-effort by contract. With
// `--keep-metastore` or CLAVESA_KEEP_METASTORE set the container is left
// warm for the next command. `clavesa ui` needs no special case: its own
// shutdown already removed the container it owns, so the ownership check
// inside the release finds nothing to do.
func releaseMetastores(cmd *cobra.Command) {
	keep, _ := cmd.PersistentFlags().GetBool("keep-metastore")
	if keepMetastoreRequested(keep, os.Getenv(keepMetastoreEnv)) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), metastoreReleaseTimeout)
	defer cancel()
	_ = observability.ReleaseCreatedMetastores(ctx)
}

// serviceDeps is what newServiceDeps assembles: the fully-wired Service
// plus the individual pieces the commands with extra wiring need beyond
// the Service itself (`clavesa ui` hands the whole set to
// ui.BuildWorkspaceMux and layers the notebook runner on top; `clavesa
// query` and `clavesa notebook` eagerly warm the local worker). The
// recipe itself is wsdeps.New, shared with the multi-workspace UI.
type serviceDeps struct {
	svc       *service.Service
	workspace string
	warm      *observability.PersistentQueryRunner
	full      *wsdeps.Deps
}

// reapOrphansOnce gates the GH #59 orphan reaper to once per process —
// newServiceDeps is the funnel every command passes through, and some
// commands construct it more than once.
var reapOrphansOnce sync.Once

// newServiceDeps is the single CLI service constructor (2026-07-02 session
// I P2-1; GH #76). Every command-side service.Service is built by
// wsdeps.New with the FULL option set — resolver, SQL parser, transpiler,
// metastore ensurer — so no per-command recipe can silently drop a seam.
// Unused pieces cost nothing: the SQL parser's runner spawns a container
// only on first Parse, the transpile sidecar on first ToServing, the AWS
// clients open no connections until a cloud dispatch, and the metastore
// ensurer runs only when a local runner container launches.
//
// Teardown is uniform too: the warm runner and sidecar are registered as
// closeables and stopped by runCloseables() when the root command exits.
func newServiceDeps(cmd *cobra.Command) (*serviceDeps, error) {
	workspace, err := resolveWorkspace(cmd)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace: %w", err)
	}
	// Reap helper containers orphaned by DELETED workspaces (GH #59) — the
	// per-root sweeps in ui.go only see the current workspace, so
	// metastores and warm workers from removed tempdir workspaces leaked
	// forever. Once per process, best-effort: docker absent / daemon down is
	// the normal quiet case, and each docker call inside is short-timeout so
	// a wedged daemon can't hang command startup.
	reapOrphansOnce.Do(func() {
		_, _ = observability.ReapOrphanContainers(cmd.Context())
	})
	d := wsdeps.New(cmd.Context(), workspace, wsdeps.Options{})
	for _, fn := range d.Closers {
		registerCloseable(fn)
	}
	return &serviceDeps{
		svc:       d.Svc,
		workspace: workspace,
		warm:      d.Warm,
		full:      d,
	}, nil
}

// newService is the plain-command entry point over newServiceDeps for the
// ~60 call sites that need only the Service and the workspace root.
func newService(cmd *cobra.Command) (*service.Service, string, error) {
	deps, err := newServiceDeps(cmd)
	if err != nil {
		return nil, "", err
	}
	return deps.svc, deps.workspace, nil
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
