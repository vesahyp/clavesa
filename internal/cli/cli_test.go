package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/workspace"
)

func TestVersion(t *testing.T) {
	t.Parallel()
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"version"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("version: %v", err)
	}
	got := strings.TrimSpace(out.String())
	if got != service.ModuleVersion {
		t.Fatalf("version output = %q, want %q", got, service.ModuleVersion)
	}
}

// setupWorkspace creates a minimal pipeline fixture in a temp dir and returns
// the workspace root.
func setupWorkspace(t *testing.T) string {
	t.Helper()
	ws := t.TempDir()

	// create a simple pipeline directory
	pDir := filepath.Join(ws, "my-pipeline")
	if err := os.MkdirAll(pDir, 0o755); err != nil {
		t.Fatal(err)
	}

	mainTF := `terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

module "source1" {
  source = "../../modules/source/aws"
  bucket = "my-bucket"
  prefix = "raw/"
  format = "csv"
}
`
	if err := os.WriteFile(filepath.Join(pDir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}

	variablesTF := `variable "pipeline_name" {
  description = "Human-readable name for this pipeline"
  default     = "my-pipeline"
}
`
	if err := os.WriteFile(filepath.Join(pDir, "variables.tf"), []byte(variablesTF), 0o644); err != nil {
		t.Fatal(err)
	}

	return ws
}

func TestPipelineList(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"pipeline", "list", "--workspace", ws})
	if err != nil {
		t.Fatalf("pipeline list: %v", err)
	}
}

func TestPipelineListJSON(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"pipeline", "list", "--workspace", ws, "--json"})
	if err != nil {
		t.Fatalf("pipeline list --json: %v", err)
	}
}

func TestPipelineCreate(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	err := Run([]string{"pipeline", "create", "new-pipeline", "--workspace", ws})
	if err != nil {
		t.Fatalf("pipeline create: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ws, "new-pipeline", "main.tf")); err != nil {
		t.Fatalf("main.tf not created: %v", err)
	}
}

func TestPipelineShow(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"pipeline", "show", "my-pipeline", "--workspace", ws})
	if err != nil {
		t.Fatalf("pipeline show: %v", err)
	}
}

func TestPipelineDeleteRequiresForce(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"pipeline", "delete", "my-pipeline", "--workspace", ws})
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected --force error, got: %v", err)
	}
}

func TestPipelineDeleteWithForce(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"pipeline", "delete", "--workspace", ws, "--force", "my-pipeline"})
	if err != nil {
		t.Fatalf("pipeline delete --force: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ws, "my-pipeline")); !os.IsNotExist(err) {
		t.Fatalf("pipeline directory still exists after delete")
	}
}

func TestNodeList(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"node", "list", "my-pipeline", "--workspace", ws})
	if err != nil {
		t.Fatalf("node list: %v", err)
	}
}

func TestNodeShow(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"node", "show", "my-pipeline", "source1", "--workspace", ws})
	if err != nil {
		t.Fatalf("node show: %v", err)
	}
}

func TestNodeShowMissing(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"node", "show", "my-pipeline", "doesnotexist", "--workspace", ws})
	if err == nil {
		t.Fatal("expected error for missing node, got nil")
	}
}

func TestNodeAdd(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"node", "add", "--type", "transform", "--workspace", ws, "my-pipeline"})
	if err != nil {
		t.Fatalf("node add: %v", err)
	}
}

func TestNodeAddRequiresType(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"node", "add", "my-pipeline", "--workspace", ws})
	if err == nil || !strings.Contains(err.Error(), "--type") {
		t.Fatalf("expected --type error, got: %v", err)
	}
}

// ADR-017 slice 4: --from and --type source are gone. The replacement
// flow is `clavesa source register --from <url>` + `clavesa source
// attach <pipeline> <source> --to <transform>`, exercised at the
// service layer in source_test.go. These two cover the CLI's new error
// surface so users moving from `node add --from` get a clear redirect
// instead of a silent acceptance.
func TestNodeAddTypeSourceIsBlocked(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"node", "add", "my-pipeline", "--type", "source", "--workspace", ws})
	if err == nil || !strings.Contains(err.Error(), "source register") {
		t.Fatalf("expected `source register` redirect, got: %v", err)
	}
}

func TestNodeEdit(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"node", "edit", "--set", "bucket=other-bucket", "--workspace", ws, "my-pipeline", "source1"})
	if err != nil {
		t.Fatalf("node edit: %v", err)
	}
}

func TestNodeEditNoSetShowsConfig(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"node", "edit", "my-pipeline", "source1", "--workspace", ws})
	if err != nil {
		t.Fatalf("node edit with no --set should print config, got error: %v", err)
	}
}

// TestNodeEditOutputFlags verifies --output-mode and --output-merge-keys edit
// output_definitions["default"] without losing other config or other output keys.
func TestNodeEditOutputFlags(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	svc := service.New(ws)
	if _, err := svc.AddNode("my-pipeline", "transform", "dim_customers"); err != nil {
		t.Fatalf("add transform: %v", err)
	}

	// Set both flags at once. Merge keys alone is the canonical recipe;
	// also flip mode explicitly to verify each flag travels independently.
	if err := Run([]string{
		"node", "edit", "my-pipeline", "dim_customers",
		"--workspace", ws,
		"--output-mode", "merge",
		"--output-merge-keys", "customer_id,as_of_date",
	}); err != nil {
		t.Fatalf("node edit --output-mode --output-merge-keys: %v", err)
	}

	g, err := svc.GetPipeline("my-pipeline")
	if err != nil {
		t.Fatalf("get pipeline: %v", err)
	}
	var n *graph.Node
	for i := range g.Nodes {
		if g.Nodes[i].ID == "dim_customers" {
			n = &g.Nodes[i]
			break
		}
	}
	if n == nil {
		t.Fatalf("dim_customers not found")
	}
	defs, ok := n.Config["output_definitions"].(map[string]interface{})
	if !ok {
		t.Fatalf("output_definitions missing or wrong type: %T", n.Config["output_definitions"])
	}
	def, ok := defs["default"].(map[string]interface{})
	if !ok {
		t.Fatalf("default missing: %v", defs)
	}
	if got, _ := def["mode"].(string); got != "merge" {
		t.Errorf("mode: got %q want merge", got)
	}
	mk, _ := def["merge_keys"].([]interface{})
	if len(mk) != 2 || mk[0] != "customer_id" || mk[1] != "as_of_date" {
		t.Errorf("merge_keys: got %v want [customer_id as_of_date]", mk)
	}

	// Clear merge_keys with an empty value; mode should stay.
	if err := Run([]string{
		"node", "edit", "my-pipeline", "dim_customers",
		"--workspace", ws,
		"--output-merge-keys", "",
	}); err != nil {
		t.Fatalf("node edit --output-merge-keys=\"\": %v", err)
	}
	g, _ = svc.GetPipeline("my-pipeline")
	for i := range g.Nodes {
		if g.Nodes[i].ID == "dim_customers" {
			defs, _ := g.Nodes[i].Config["output_definitions"].(map[string]interface{})
			def, _ := defs["default"].(map[string]interface{})
			if _, present := def["merge_keys"]; present {
				t.Errorf("merge_keys should be cleared, still present: %v", def)
			}
			if got, _ := def["mode"].(string); got != "merge" {
				t.Errorf("mode dropped after clearing merge_keys: %v", def)
			}
		}
	}
}

// TestNodeEditMergeKeysImpliesMergeMode covers the recipe's claim that
// `--output-merge-keys <col>` on its own flips mode → "merge" — both the
// flag's own help text and merge-dim-table.md make that contract.
// Without the implicit flip, the emitted HCL carries merge_keys but no
// mode, the local-run path defaults mode to "replace", and the runner
// runs createOrReplace instead of MERGE INTO.
func TestNodeEditMergeKeysImpliesMergeMode(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	svc := service.New(ws)
	if _, err := svc.AddNode("my-pipeline", "transform", "dim_customers"); err != nil {
		t.Fatalf("add transform: %v", err)
	}

	if err := Run([]string{
		"node", "edit", "my-pipeline", "dim_customers",
		"--workspace", ws,
		"--output-merge-keys", "customer_id",
	}); err != nil {
		t.Fatalf("node edit --output-merge-keys (no --output-mode): %v", err)
	}

	g, _ := svc.GetPipeline("my-pipeline")
	for _, n := range g.Nodes {
		if n.ID != "dim_customers" {
			continue
		}
		defs, _ := n.Config["output_definitions"].(map[string]interface{})
		def, _ := defs["default"].(map[string]interface{})
		if got, _ := def["mode"].(string); got != "merge" {
			t.Errorf("mode after --output-merge-keys only: got %q, want %q", got, "merge")
		}
	}

	// Explicit --output-mode append + --output-merge-keys: user override
	// wins; don't silently swap to merge.
	if err := Run([]string{
		"node", "edit", "my-pipeline", "dim_customers",
		"--workspace", ws,
		"--output-mode", "append",
		"--output-merge-keys", "event_id",
	}); err != nil {
		t.Fatalf("node edit --output-mode append --output-merge-keys: %v", err)
	}
	g, _ = svc.GetPipeline("my-pipeline")
	for _, n := range g.Nodes {
		if n.ID != "dim_customers" {
			continue
		}
		defs, _ := n.Config["output_definitions"].(map[string]interface{})
		def, _ := defs["default"].(map[string]interface{})
		if got, _ := def["mode"].(string); got != "append" {
			t.Errorf("explicit mode=append override lost: got %q", got)
		}
	}
}

func TestNodeRemove(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	err := Run([]string{"node", "remove", "my-pipeline", "source1", "--workspace", ws})
	if err != nil {
		t.Fatalf("node remove: %v", err)
	}
}

// TestNodeDisableEnable locks the `node disable` / `node enable` CLI
// subcommands: disable writes `enabled = false` onto the module block,
// enable flips it back to `enabled = true`. The seeded transform module is
// the target. Mirrors the Run([]string{...}) harness the other node tests use.
func TestNodeDisableEnable(t *testing.T) {
	t.Parallel()
	ws := setupWorkspace(t)
	svc := service.New(ws)
	if _, err := svc.AddNode("my-pipeline", "transform", "t1"); err != nil {
		t.Fatalf("add transform: %v", err)
	}
	mainTF := filepath.Join(ws, "my-pipeline", "main.tf")

	// disable
	if err := Run([]string{"node", "disable", "my-pipeline", "t1", "--workspace", ws}); err != nil {
		t.Fatalf("node disable: %v", err)
	}
	// Collapse whitespace before matching: hclwrite column-aligns the `=`
	// within a block (`enabled        = false`), so a single-space substring
	// won't match the padded form. The alignment is deterministic now (#17),
	// so this only normalizes the padding, not run-to-run flakiness.
	flat := func() string {
		b, _ := os.ReadFile(mainTF)
		return strings.Join(strings.Fields(string(b)), " ")
	}
	if f := flat(); !strings.Contains(f, "enabled = false") {
		t.Errorf("main.tf should contain `enabled = false` after disable:\n%s", f)
	}

	// enable
	if err := Run([]string{"node", "enable", "my-pipeline", "t1", "--workspace", ws}); err != nil {
		t.Fatalf("node enable: %v", err)
	}
	if f := flat(); !strings.Contains(f, "enabled = true") {
		t.Errorf("main.tf should contain `enabled = true` after re-enable:\n%s", f)
	}
	if f := flat(); strings.Contains(f, "enabled = false") {
		t.Errorf("`enabled = false` should be gone after re-enable:\n%s", f)
	}
}

func TestNodeConnectDisconnect(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	pDir := filepath.Join(ws, "p")
	if err := os.MkdirAll(pDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

module "source1" {
  source = "../../modules/source/aws"
  bucket = "my-bucket"
  prefix = "raw/"
  format = "csv"
}

module "transform1" {
  source = "../../modules/transform/aws"
  sql    = "SELECT * FROM source1"
}
`
	if err := os.WriteFile(filepath.Join(pDir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}

	// connect
	err := Run([]string{"node", "connect", "--from", "source1", "--to", "transform1", "--workspace", ws, "p"})
	if err != nil {
		t.Fatalf("node connect: %v", err)
	}

	// disconnect
	err = Run([]string{"node", "disconnect", "--workspace", ws, "p", "source1->transform1"})
	if err != nil {
		t.Fatalf("node disconnect: %v", err)
	}
}

// withCwd chdir's into dir for the duration of fn and restores the
// previous cwd afterward. Tests using it must NOT be t.Parallel() — cwd
// is process-global.
func withCwd(t *testing.T, dir string, fn func()) {
	t.Helper()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(prev); err != nil {
			t.Fatal(err)
		}
	}()
	fn()
}

// TestPipelineCommandInfersDirFromCwd: standing inside the pipeline dir,
// a command with no <pipeline-dir> argument uses the current directory.
func TestPipelineCommandInfersDirFromCwd(t *testing.T) {
	ws := setupWorkspace(t)
	withCwd(t, filepath.Join(ws, "my-pipeline"), func() {
		if err := Run([]string{"pipeline", "show", "--workspace", ws}); err != nil {
			t.Fatalf("pipeline show (inferred dir): %v", err)
		}
		if err := Run([]string{"node", "list", "--workspace", ws}); err != nil {
			t.Fatalf("node list (inferred dir): %v", err)
		}
		// node show keeps the node-id positional; the dir is inferred.
		if err := Run([]string{"node", "show", "source1", "--workspace", ws}); err != nil {
			t.Fatalf("node show source1 (inferred dir): %v", err)
		}
	})
}

// TestPipelineCommandNonPipelineCwdErrors: a bare command from a directory
// that is not a pipeline reports a clear error, not a bare usage line.
func TestPipelineCommandNonPipelineCwdErrors(t *testing.T) {
	ws := setupWorkspace(t) // ws root has no .tf files
	withCwd(t, ws, func() {
		err := Run([]string{"pipeline", "show", "--workspace", ws})
		if err == nil || !strings.Contains(err.Error(), "not a pipeline") {
			t.Fatalf("expected 'not a pipeline' error, got: %v", err)
		}
	})
}

// TestWorkspacePrecedenceCwdBeatsStateFile: when the current directory is
// inside a workspace, that workspace wins over one pinned in the state
// file by `workspace use`.
func TestWorkspacePrecedenceCwdBeatsStateFile(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir()) // isolate the state file

	mkWorkspace := func(name string) string {
		root := t.TempDir()
		// EvalSymlinks so the path matches os.Getwd() after Chdir — on
		// macOS /var is a symlink to /private/var.
		root, err := filepath.EvalSymlinks(root)
		if err != nil {
			t.Fatal(err)
		}
		manifest := `{"name":"` + name + `","cloud":"aws","version":1,"catalog":"clavesa_` + name + `"}` + "\n"
		if err := os.WriteFile(filepath.Join(root, "clavesa.json"), []byte(manifest), 0o644); err != nil {
			t.Fatal(err)
		}
		return root
	}
	wsA := mkWorkspace("a")
	wsB := mkWorkspace("b")

	// Pin workspace A via the state file.
	if err := writeWorkspaceStateFile(wsA); err != nil {
		t.Fatal(err)
	}

	cmd := newRootCmd()
	withCwd(t, wsB, func() {
		got, err := resolveWorkspaceRoot(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if got != wsB {
			t.Errorf("resolveWorkspaceRoot from inside workspace B = %q, want %q (state file pins %q)", got, wsB, wsA)
		}
	})

	// From outside any workspace, the pinned workspace A still applies.
	withCwd(t, t.TempDir(), func() {
		got, err := resolveWorkspaceRoot(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if got != wsA {
			t.Errorf("resolveWorkspaceRoot outside any workspace = %q, want pinned %q", got, wsA)
		}
	})
}

func TestUnknownCommand(t *testing.T) {
	t.Parallel()
	err := Run([]string{"bogus"})
	if err == nil {
		t.Fatal("expected error for unknown command")
	}
}

func TestMissingSubcommand(t *testing.T) {
	t.Parallel()
	err := Run([]string{"pipeline"})
	if err == nil {
		t.Fatal("expected error for missing pipeline subcommand")
	}
}

func TestPipelineUpgrade(t *testing.T) {
	t.Parallel()
	// Pipeline laid out as <ws>/pipeline-dir/main.tf so the upgrade's
	// embedded-source rewrite has a workspace root to compute relative
	// paths against.
	ws := t.TempDir()
	dir := filepath.Join(ws, "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `# clavesa pipeline
module "source1" {
  source = "github.com/vesahyp/clavesa//modules/source/aws?ref=v0.1.0"
  bucket = "my-bucket"
}
module "transform1" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.1.0"
  sql    = "SELECT * FROM source1"
}
`
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := Run([]string{"pipeline", "upgrade", "--version", service.ModuleVersion, dir}); err != nil {
		t.Fatalf("upgrade: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	// Old github URLs are gone — pipeline upgrade rewrites every
	// clavesa module source to the embedded-modules form.
	if strings.Contains(string(got), "github.com/vesahyp/clavesa") {
		t.Errorf("old github source still present after upgrade:\n%s", got)
	}
	if strings.Contains(string(got), "?ref=v0.1.0") {
		t.Error("old ref still present after upgrade")
	}
	// New form embeds the current module version in a local-path source.
	want := ".clavesa/modules/" + service.ModuleVersion + "/source/aws"
	if !strings.Contains(string(got), want) {
		t.Errorf("expected embedded source %q after upgrade, got:\n%s", want, got)
	}
}

func TestPipelineUpgradeAlreadyCurrent(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	dir := filepath.Join(ws, "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Pipeline already on the embedded form at the current ModuleVersion —
	// upgrade is a no-op (no source rewrites, no error).
	mainTF := `module "source1" {
  source = "../.clavesa/modules/` + service.ModuleVersion + `/source/aws"
}
`
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := Run([]string{"pipeline", "upgrade", "--version", service.ModuleVersion, dir}); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
}

// TestPipelineUpgradeNoClavesaSources — pipeline has no clavesa
// module sources at all (just a non-clavesa local-path module). Upgrade
// is a no-op rather than an error: the rewriter doesn't touch unfamiliar
// sources, and there's no longer a hard "github URL required" precondition.
func TestPipelineUpgradeNoClavesaSources(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	dir := filepath.Join(ws, "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `module "unrelated" {
  source = "../../some-other-module"
}
`
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := Run([]string{"pipeline", "upgrade", "--version", service.ModuleVersion, dir}); err != nil {
		t.Fatalf("upgrade on pipeline without clavesa sources: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "main.tf"))
	if !strings.Contains(string(got), "../../some-other-module") {
		t.Errorf("unrelated module source was rewritten unexpectedly:\n%s", got)
	}
}

func TestWorkspaceUseSetsEnvMode(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	manifest := `{"name":"use-ws","cloud":"aws","version":1,"catalog":"clavesa_use_ws"}` + "\n"
	if err := os.WriteFile(filepath.Join(ws, "clavesa.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}

	// Default — no environment.json yet — resolves to local.
	if got := workspace.LoadEnvironmentMode(ws); got != workspace.ModeLocal {
		t.Fatalf("default env mode = %q, want local", got)
	}

	if err := Run([]string{"workspace", "use", "--env", "cloud", "--workspace", ws}); err != nil {
		t.Fatalf("workspace use --env cloud: %v", err)
	}
	if got := workspace.LoadEnvironmentMode(ws); got != workspace.ModeCloud {
		t.Errorf("env mode = %q, want cloud", got)
	}

	if err := Run([]string{"workspace", "use", "--env", "local", "--workspace", ws}); err != nil {
		t.Fatalf("workspace use --env local: %v", err)
	}
	if got := workspace.LoadEnvironmentMode(ws); got != workspace.ModeLocal {
		t.Errorf("env mode = %q, want local", got)
	}

	if err := Run([]string{"workspace", "use", "--env", "banana", "--workspace", ws}); err == nil {
		t.Error("workspace use --env banana: expected an error for an unknown mode")
	}
}
