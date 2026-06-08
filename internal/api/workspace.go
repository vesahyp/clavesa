package api

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/pathutil"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// WorkspaceHandler serves workspace-level and pipeline-lifecycle endpoints.
type WorkspaceHandler struct {
	root string // absolute path to workspace root
	svc  *service.Service
	// restart, when set, re-execs the server process. Wired by
	// `clavesa ui`; nil in tests and CLI one-shots. PUT
	// /workspace/aws-profile calls it so a profile change takes effect
	// without the user restarting the server by hand — the AWS SDK
	// clients are built once at startup and can't be hot-swapped.
	restart func()
}

// NewWorkspaceHandler returns a handler rooted at root.
func NewWorkspaceHandler(root string) *WorkspaceHandler {
	return &WorkspaceHandler{root: root}
}

// WithService attaches the canonical service.Service so the pipeline-
// lifecycle handlers route through the wired Service instead of
// instantiating a fresh service.New(wh.root) per request (A P1-1).
func (wh *WorkspaceHandler) WithService(s *service.Service) *WorkspaceHandler {
	wh.svc = s
	return wh
}

func (wh *WorkspaceHandler) service() *service.Service {
	if wh.svc != nil {
		return wh.svc
	}
	return service.New(wh.root)
}

// resolveDir folds path resolution + workspace-containment for ?dir=
// endpoints. Writes 400 and returns ("", false) on escape attempts; the
// caller should return early. Mirrors Handler.resolveDir (C9, A P1-2).
func (wh *WorkspaceHandler) resolveDir(w http.ResponseWriter, dir string) (string, bool) {
	abs := pathutil.ResolveDir(wh.root, dir)
	if wh.root != "" && !pathutil.IsWithin(wh.root, abs) {
		httputil.WriteError(w, http.StatusBadRequest, "dir must be within the workspace root")
		return "", false
	}
	return abs, true
}

// WithRestart wires the server self-restart hook. Used by PUT
// /workspace/aws-profile.
func (wh *WorkspaceHandler) WithRestart(restart func()) *WorkspaceHandler {
	wh.restart = restart
	return wh
}

// RegisterRoutes registers workspace-level routes on mux.
func (wh *WorkspaceHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /workspace", wh.GetWorkspace)
	mux.HandleFunc("POST /workspace/init", wh.InitWorkspace)
	mux.HandleFunc("GET /workspace/environment", wh.GetEnvironment)
	mux.HandleFunc("PUT /workspace/environment", wh.SetEnvironment)
	mux.HandleFunc("GET /workspace/aws-profile", wh.GetAWSProfile)
	mux.HandleFunc("PUT /workspace/aws-profile", wh.SetAWSProfile)
	mux.HandleFunc("GET /pipelines", wh.ListPipelines)
	mux.HandleFunc("POST /pipelines", wh.CreatePipeline)
	mux.HandleFunc("DELETE /pipelines", wh.DeletePipeline)
	mux.HandleFunc("GET /pipeline/module-version", wh.GetPipelineModuleVersion)
	mux.HandleFunc("POST /pipeline/upgrade", wh.UpgradePipeline)
	mux.HandleFunc("POST /workspace/upgrade", wh.UpgradeWorkspace)
	mux.HandleFunc("GET /pipeline/vars", wh.GetVars)
	mux.HandleFunc("PUT /pipeline/vars", wh.PutVars)
}

// ---------------------------------------------------------------------------
// GET /workspace
// ---------------------------------------------------------------------------

type workspaceInfo struct {
	Root string `json:"root"`
	// Exists is false when the server's root directory has no
	// clavesa.json — the first-launch state the UI gates on to show
	// the "create workspace" screen.
	Exists  bool   `json:"exists"`
	Name    string `json:"name,omitempty"`
	Catalog string `json:"catalog,omitempty"`
}

// GetWorkspace returns the server's workspace root and whether an
// clavesa.json manifest exists there. The UI uses `exists` to decide
// between the first-launch create-workspace screen and the normal app.
func (wh *WorkspaceHandler) GetWorkspace(w http.ResponseWriter, _ *http.Request) {
	info := workspaceInfo{Root: wh.root}
	if m, _ := workspace.Load(wh.root); m != nil {
		info.Exists = true
		info.Name = m.Name
		info.Catalog = m.Catalog
	}
	httputil.WriteJSON(w, http.StatusOK, info)
}

// ---------------------------------------------------------------------------
// GET / PUT /workspace/environment
// ---------------------------------------------------------------------------

// environmentResponse carries the workspace environment mode (TODO
// bucket 16) — "local" or "cloud". Same shape for the GET response and
// the PUT request/response body so the UI toggle reads and writes one
// type.
type environmentResponse struct {
	Mode string `json:"mode"`
}

// GetEnvironment returns the workspace environment mode. Absent file
// resolves to "local" — see workspace.LoadEnvironmentMode.
func (wh *WorkspaceHandler) GetEnvironment(w http.ResponseWriter, _ *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, environmentResponse{
		Mode: string(workspace.LoadEnvironmentMode(wh.root)),
	})
}

// SetEnvironment persists the workspace environment mode. The CLI twin
// is `clavesa workspace use --env` (ADR-015 parity); both write
// .clavesa/environment.json via workspace.WriteEnvironmentMode.
func (wh *WorkspaceHandler) SetEnvironment(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[environmentResponse](w, r)
	if !ok {
		return
	}
	mode, ok := workspace.ParseMode(req.Mode)
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest, `mode must be "local" or "cloud"`)
		return
	}
	if err := workspace.WriteEnvironmentMode(wh.root, mode); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "write environment mode: "+err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, environmentResponse{Mode: string(mode)})
}

// ---------------------------------------------------------------------------
// GET / PUT /workspace/aws-profile
// ---------------------------------------------------------------------------

// awsProfileResponse carries the workspace AWS profile selection. The
// GET form fills `profiles` with the host's available profiles; the PUT
// request body uses only `profile` (empty clears the override).
type awsProfileResponse struct {
	// Profile is the persisted per-workspace profile, "" when no
	// override is set (ambient / default credential chain).
	Profile string `json:"profile"`
	// Profiles is the list of profiles configured in ~/.aws — the
	// choices the UI dropdown offers. Omitted from the PUT response.
	Profiles []string `json:"profiles,omitempty"`
	// Restarting is true in the PUT response when the change triggered
	// a server self-restart — the UI then polls for the server to come
	// back and reloads.
	Restarting bool `json:"restarting,omitempty"`
}

// GetAWSProfile returns the persisted workspace AWS profile and the
// profiles available on this host.
func (wh *WorkspaceHandler) GetAWSProfile(w http.ResponseWriter, _ *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, awsProfileResponse{
		Profile:  workspace.LoadAWSProfile(wh.root),
		Profiles: workspace.ListAWSProfiles(),
	})
}

// SetAWSProfile persists the workspace AWS profile and re-execs the
// server so the change takes effect (the AWS SDK clients are built once
// at startup). The CLI twin is `clavesa workspace use --profile`
// (ADR-015 parity) — the CLI doesn't restart anything, since the next
// `clavesa ui` start picks the file up.
func (wh *WorkspaceHandler) SetAWSProfile(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[awsProfileResponse](w, r)
	if !ok {
		return
	}
	profile := strings.TrimSpace(req.Profile)
	// A non-empty profile must exist in ~/.aws — catch a typo here
	// rather than letting the re-exec'd server silently fall into
	// local-only mode.
	if profile != "" {
		avail := workspace.ListAWSProfiles()
		found := false
		for _, p := range avail {
			if p == profile {
				found = true
				break
			}
		}
		if !found {
			httputil.WriteError(w, http.StatusBadRequest,
				fmt.Sprintf("AWS profile %q not found in ~/.aws (available: %s)",
					profile, strings.Join(avail, ", ")))
			return
		}
	}
	if err := workspace.WriteAWSProfile(wh.root, profile); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "write AWS profile: "+err.Error())
		return
	}
	restarting := wh.restart != nil
	httputil.WriteJSON(w, http.StatusOK, awsProfileResponse{Profile: profile, Restarting: restarting})
	// Flush the response, then re-exec — the client needs the 200 in
	// hand before the process image is replaced.
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	if restarting {
		go wh.restart()
	}
}

// ---------------------------------------------------------------------------
// POST /workspace/init
// ---------------------------------------------------------------------------

type initWorkspaceRequest struct {
	Name    string `json:"name"`
	Catalog string `json:"catalog,omitempty"`
}

// InitWorkspace creates a workspace in the server's existing root
// directory — clavesa.json, the _workspace/ Terraform, the extracted
// runner source, and the local preview Docker image. Mirrors
// `clavesa workspace init`; because the root path is unchanged (only
// its contents gain a manifest) the running server's handlers pick the
// new workspace up with no restart.
//
// The Docker image build is synchronous and can take minutes on a cold
// machine — the UI shows a progress state. Cloud defaults to "aws"
// (the only supported value); catalog is optional and defaults to
// clavesa_<sanitize(name)>.
func (wh *WorkspaceHandler) InitWorkspace(w http.ResponseWriter, r *http.Request) {
	if m, _ := workspace.Load(wh.root); m != nil {
		httputil.WriteError(w, http.StatusConflict,
			fmt.Sprintf("workspace %q already exists at %s", m.Name, wh.root))
		return
	}
	req, ok := httputil.DecodeJSON[initWorkspaceRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"name": req.Name}) {
		return
	}
	if err := workspace.Init(wh.root, req.Name, "aws", req.Catalog, service.ModuleVersion); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "workspace init: "+err.Error())
		return
	}
	m, err := workspace.Load(wh.root)
	if err != nil || m == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "workspace created but manifest unreadable")
		return
	}
	httputil.WriteJSON(w, http.StatusCreated, workspaceInfo{
		Root:    wh.root,
		Exists:  true,
		Name:    m.Name,
		Catalog: m.Catalog,
	})
}

// ---------------------------------------------------------------------------
// GET /pipelines
// ---------------------------------------------------------------------------

// ListPipelines returns every pipeline in the workspace. Delegates to
// service.ListPipelines — the same call the CLI's `pipeline list` uses
// (ADR-015). The handler used to carry its own copy of the directory
// scan + cloud/compute derivation; the service version is now the only
// one, so any future enrichment reaches both surfaces.
func (wh *WorkspaceHandler) ListPipelines(w http.ResponseWriter, _ *http.Request) {
	pipelines, err := wh.service().ListPipelines()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "scan pipelines: "+err.Error())
		return
	}
	if pipelines == nil {
		pipelines = []service.PipelineInfo{}
	}
	httputil.WriteJSON(w, http.StatusOK, pipelines)
}

// ---------------------------------------------------------------------------
// POST /pipelines
// ---------------------------------------------------------------------------

type createPipelineRequest struct {
	Name   string `json:"name"`             // relative subdirectory path, e.g. "pipelines/cf-analytics"
	Schema string `json:"schema,omitempty"` // ADR-016 schema identifier; empty defaults to sanitize(name)
}

type createPipelineResponse struct {
	Dir string `json:"dir"` // relative path suitable for ?dir=
}

// CreatePipeline creates a new pipeline directory with boilerplate
// Terraform. Delegates to service.CreatePipeline so the CLI and HTTP
// surfaces emit byte-identical .tf — the pre-ADR-016 implementation
// re-implemented creation here and had drifted (no trigger vars, no
// .gitignore, no orchestration sync, no dashboard seed). Single body
// per ADR-015 CLI/UI parity.
func (wh *WorkspaceHandler) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[createPipelineRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"name": req.Name}) {
		return
	}

	rel, err := wh.service().CreatePipeline(req.Name, req.Schema)
	if err != nil {
		// Bad-request errors: a path-escaping name, or an ADR-016 schema
		// conflict (the requested schema is owned by another pipeline).
		// Both are caller-fixable input, not server faults — surface as 400.
		if strings.Contains(err.Error(), "invalid pipeline name") ||
			strings.Contains(err.Error(), "already owned by pipeline") {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	httputil.WriteJSON(w, http.StatusCreated, createPipelineResponse{Dir: rel})
}

// ---------------------------------------------------------------------------
// DELETE /pipelines?dir=<dir>
// ---------------------------------------------------------------------------

// DeletePipeline permanently removes a pipeline directory. Mirrors the CLI
// `clavesa pipeline delete <dir> --force`; the UI's confirm dialog is
// the equivalent of `--force`, so this handler doesn't require an
// additional flag. Cloud-side teardown is a separate flow
// (`clavesa pipeline destroy`); deleting a deployed pipeline's
// directory leaves AWS resources behind — same as the CLI today.
func (wh *WorkspaceHandler) DeletePipeline(w http.ResponseWriter, r *http.Request) {
	dir, ok := httputil.RequireQuery(w, r, "dir")
	if !ok {
		return
	}
	if _, ok := wh.resolveDir(w, dir); !ok {
		return
	}
	if err := wh.service().DeletePipeline(dir); err != nil {
		if strings.Contains(err.Error(), "path escapes") {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// GET /pipeline/module-version?dir=<dir>
// ---------------------------------------------------------------------------

// GetPipelineModuleVersion reports the current module ref in a pipeline's
// .tf files plus the latest available tag from the remote repo. Powers
// the "Module: vX.Y.Z (latest: vY.Z.A) [Upgrade]" chip on the per-pipeline
// dashboard. The remote call (`git ls-remote`) is paid on every hit; the
// UI caches via TanStack Query staleTime so the chip is cheap to render.
func (wh *WorkspaceHandler) GetPipelineModuleVersion(w http.ResponseWriter, r *http.Request) {
	dir, ok := httputil.RequireQuery(w, r, "dir")
	if !ok {
		return
	}
	if _, ok := wh.resolveDir(w, dir); !ok {
		return
	}
	info, err := wh.service().PipelineModuleVersion(dir)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, info)
}

// ---------------------------------------------------------------------------
// POST /pipeline/upgrade?dir=<dir>&version=<v0.Y.Z>
// ---------------------------------------------------------------------------

type upgradePipelineResponse struct {
	CurrentRef string `json:"current_ref"`
	TargetRef  string `json:"target_ref"`
	Updated    int    `json:"updated"`
	// Migrated counts transforms whose legacy compute = "local"
	// attribute was stripped (one-shot migration).
	Migrated int `json:"migrated"`
}

// UpgradePipeline rewrites every GitHub module `?ref=` in a pipeline's
// .tf files to the requested version (or the latest remote tag when
// version is empty). Mirrors `clavesa pipeline upgrade` — both
// surfaces delegate to service.UpgradePipeline so the resulting .tf
// is byte-identical regardless of caller.
func (wh *WorkspaceHandler) UpgradePipeline(w http.ResponseWriter, r *http.Request) {
	dir, ok := httputil.RequireQuery(w, r, "dir")
	if !ok {
		return
	}
	if _, ok := wh.resolveDir(w, dir); !ok {
		return
	}
	current, target, updated, migrated, err := wh.service().UpgradePipeline(dir, r.URL.Query().Get("version"))
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, upgradePipelineResponse{
		CurrentRef: current,
		TargetRef:  target,
		Updated:    updated,
		Migrated:   migrated,
	})
}

// ---------------------------------------------------------------------------
// POST /workspace/upgrade?version=<optional>&shell_only=<bool>
// ---------------------------------------------------------------------------

type pipelineUpgradeRow struct {
	Name       string `json:"name"`
	Dir        string `json:"dir"`
	CurrentRef string `json:"current_ref"`
	TargetRef  string `json:"target_ref"`
	Updated    int    `json:"updated"`
	Migrated   int    `json:"migrated"`
	Err        string `json:"err,omitempty"`
}

type upgradeWorkspaceResponse struct {
	PrevVersion        string `json:"prev_version"`
	TargetVersion      string `json:"target_version"`
	WorkspaceRewritten int    `json:"workspace_rewritten"`
	RunnerBuilt        bool   `json:"runner_built"`
	// Warning carries a non-fatal note (e.g. the local runner image
	// build failed); the upgrade itself still succeeded.
	Warning   string               `json:"warning,omitempty"`
	Pipelines []pipelineUpgradeRow `json:"pipelines"`
}

// UpgradeWorkspace upgrades the workspace shell and (unless shell_only is
// true) every pipeline in it to the requested version, then refreshes the
// local runner image. Mirrors `clavesa workspace upgrade` — both surfaces
// delegate to service.UpgradeWorkspace so the resulting .tf is
// byte-identical. Per-pipeline failures are continue-on-error and surface
// in each row's `err`; the request still returns 200.
func (wh *WorkspaceHandler) UpgradeWorkspace(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	shellOnly := q.Get("shell_only") == "true" || q.Get("shell_only") == "1"
	res, err := wh.service().UpgradeWorkspace(q.Get("version"), !shellOnly)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := upgradeWorkspaceResponse{
		PrevVersion:        res.PrevVersion,
		TargetVersion:      res.TargetVersion,
		WorkspaceRewritten: res.WorkspaceRewritten,
		Pipelines:          []pipelineUpgradeRow{},
	}
	for _, p := range res.Pipelines {
		resp.Pipelines = append(resp.Pipelines, pipelineUpgradeRow{
			Name:       p.Name,
			Dir:        p.Dir,
			CurrentRef: p.CurrentRef,
			TargetRef:  p.TargetRef,
			Updated:    p.Updated,
			Migrated:   p.Migrated,
			Err:        p.Err,
		})
	}
	// Image build mirrors the CLI: best-effort, never fails the request.
	// Unconditional build; docker's layer cache makes a no-change rebuild
	// a fast cache hit.
	if _, imgErr := workspace.EnsureLocalRunnerImage(wh.root); imgErr != nil {
		resp.Warning = "build local runner image: " + imgErr.Error()
	} else {
		resp.RunnerBuilt = true
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// GET /pipeline/vars?dir=<dir>
// ---------------------------------------------------------------------------

// GetVars reads variable values from terraform.tfvars (or .auto.tfvars) in dir
// and returns them as a flat key→value map.
func (wh *WorkspaceHandler) GetVars(w http.ResponseWriter, r *http.Request) {
	dir, ok := httputil.RequireQuery(w, r, "dir")
	if !ok {
		return
	}

	abs := pathutil.ResolveDir(wh.root, dir)
	vars, err := readTFVars(abs)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "read tfvars: "+err.Error())
		return
	}

	// Also read variable declarations from variables.tf so the UI knows which
	// vars exist even when tfvars is absent or incomplete.
	decls, err := readVariableDecls(abs)
	if err == nil {
		for k, def := range decls {
			if _, exists := vars[k]; !exists {
				vars[k] = def // fill in defaults
			}
		}
	}

	httputil.WriteJSON(w, http.StatusOK, vars)
}

// ---------------------------------------------------------------------------
// PUT /pipeline/vars
// ---------------------------------------------------------------------------

type putVarsRequest struct {
	Dir  string            `json:"dir"`
	Vars map[string]string `json:"vars"`
}

// PutVars writes key/value pairs to terraform.tfvars in the pipeline directory.
func (wh *WorkspaceHandler) PutVars(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[putVarsRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir}) {
		return
	}

	abs := pathutil.ResolveDir(wh.root, req.Dir)

	// Read existing vars, merge, write.
	existing, _ := readTFVars(abs) // ignore error — file may not exist yet
	if existing == nil {
		existing = make(map[string]string)
	}
	for k, v := range req.Vars {
		existing[k] = v
	}

	if err := writeTFVars(filepath.Join(abs, "terraform.tfvars"), existing); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "write tfvars: "+err.Error())
		return
	}

	httputil.WriteJSON(w, http.StatusOK, existing)
}

// ---------------------------------------------------------------------------
// tfvars helpers
// ---------------------------------------------------------------------------

// readTFVars parses terraform.tfvars (simple key = "value" format) in dir.
// Returns an empty map if the file does not exist.
func readTFVars(dir string) (map[string]string, error) {
	result := make(map[string]string)

	for _, name := range []string{"terraform.tfvars", "terraform.auto.tfvars"} {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			key := strings.TrimSpace(k)
			val := strings.TrimSpace(v)
			// Strip surrounding quotes.
			if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
				val = val[1 : len(val)-1]
			}
			result[key] = val
		}
	}

	return result, nil
}

// writeTFVars writes vars to path as a simple key = "value" tfvars file.
func writeTFVars(path string, vars map[string]string) error {
	var sb strings.Builder
	for k, v := range vars {
		sb.WriteString(fmt.Sprintf("%s = %q\n", k, v))
	}
	return os.WriteFile(path, []byte(sb.String()), 0o644)
}

// readVariableDecls reads variable blocks from variables.tf and returns
// a map of variable name → default value (empty string if no default).
func readVariableDecls(dir string) (map[string]string, error) {
	path := filepath.Join(dir, "variables.tf")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	content := string(data)

	// Simple line-by-line scan — we don't need full HCL parsing here.
	var currentVar string
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "variable ") {
			// variable "name" {
			parts := strings.Fields(trimmed)
			if len(parts) >= 2 {
				currentVar = strings.Trim(parts[1], `"`)
				result[currentVar] = ""
			}
			continue
		}

		if currentVar != "" && strings.HasPrefix(trimmed, "default") {
			// default = "value" or default = value
			_, val, ok := strings.Cut(trimmed, "=")
			if ok {
				v := strings.TrimSpace(val)
				if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
					v = v[1 : len(v)-1]
				}
				result[currentVar] = v
			}
		}

		if trimmed == "}" {
			currentVar = ""
		}
	}

	return result, nil
}
