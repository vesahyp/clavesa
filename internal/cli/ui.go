package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/ui"
	wspkg "github.com/vesahyp/clavesa/internal/workspace"
	"github.com/vesahyp/clavesa/internal/wsdeps"
)

// embeddedUI holds the embedded frontend filesystem, set from main.go.
var embeddedUI fs.FS

// SetEmbeddedUI injects the embedded UI filesystem before Execute is called.
func SetEmbeddedUI(f fs.FS) {
	embeddedUI = f
}

func newUICmd() *cobra.Command {
	var (
		noBrowser     bool
		workspacesDir string
		readOnly      bool
	)

	cmd := &cobra.Command{
		Use:   "ui",
		Short: "Start the visual editor in your browser",
		Long: `Start the UI server and open it in your browser.

The UI reads and writes .tf files in the workspace directory. It renders
pipelines as interactive DAGs and lets you edit nodes, connect edges,
and preview data visually.

With --workspaces <dir>, one server shows every workspace directly under
<dir> (each directory with a clavesa.json). / lists them, /w/<name>/ is
that workspace's UI and /w/<name>/api/ its API. Each workspace's AWS
clients use its own saved AWS profile; reads that go through the default
credential chain use AWS_PROFILE, so set it when the workspaces share
an account.

--read-only answers 405 to every request except GET and HEAD and hides
the controls that change state. It also refuses the two read POSTs
(the SQL query editor and dashboard widget queries).

CLAVESA_ATHENA_OUTPUT=s3://<bucket>/<prefix>/ sends the results of every
Athena query the UI runs to that prefix in place of
s3://<pipeline bucket>/athena-results/. Use it when your role may read
only its own prefix, so you never read other people's past results. The
role needs s3:PutObject, GetObject, ListBucket and GetBucketLocation on
that prefix. With --workspaces it applies to every workspace. A hosted
tree copy keeps its workgroup's location, and its queries fail while the
setting is set.

Examples:
  clavesa ui
  clavesa ui --workspace /path/to/project
  clavesa ui --no-browser
  clavesa ui --workspaces ~/clavesa-workspaces --read-only`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Check CLAVESA_ATHENA_OUTPUT at startup, so a bad value fails
			// before the first query.
			if _, err := observability.AthenaOutputLocation(""); err != nil {
				return err
			}
			addr := ":8080"
			if a := os.Getenv("CLAVESA_ADDR"); a != "" {
				addr = a
			}
			if embeddedUI == nil {
				return fmt.Errorf("embedded UI not available")
			}
			static, err := fs.Sub(embeddedUI, "dist")
			if err != nil {
				return fmt.Errorf("embed: %w", err)
			}
			if workspacesDir != "" {
				if w, _ := cmd.Flags().GetString("workspace"); w != "" {
					return fmt.Errorf("--workspace and --workspaces cannot be used together")
				}
				return runMultiUI(cmd, addr, workspacesDir, static, readOnly, noBrowser)
			}
			return runLocalUI(cmd, addr, static, readOnly, noBrowser)
		},
	}

	cmd.Flags().BoolVar(&noBrowser, "no-browser", false, "do not open the browser automatically")
	cmd.Flags().StringVar(&workspacesDir, "workspaces", "", "serve every workspace directly under this directory (multi-workspace mode)")
	cmd.Flags().BoolVar(&readOnly, "read-only", false, "answer 405 to every method except GET and HEAD, and hide the controls that change state")

	return cmd
}

// runLocalUI is `clavesa ui` for one workspace: the API at /api/, the SPA
// at /. The Spark warmup, Docker sweeps, notebook pool and the self-restart
// on an AWS profile change live here only, not in BuildWorkspaceMux.
func runLocalUI(cmd *cobra.Command, addr string, static fs.FS, readOnly, noBrowser bool) error {
	workspace, err := resolveWorkspace(cmd)
	if err != nil {
		return err
	}
	workspace, err = filepath.Abs(workspace)
	if err != nil {
		return fmt.Errorf("resolve workspace: %w", err)
	}

	// resolveWorkspace (above) has already applied the workspace's
	// persisted AWS profile to AWS_PROFILE, so the AWS clients built below
	// and the AWS_PROFILE forwarded into the runner (service/run.go) agree
	// on one profile.

	// Sweep leftovers from a prior SIGKILL'd session before the shared
	// constructor's lazy pieces can spawn fresh ones: warm Spark workers;
	// the stale metastore container that would otherwise keep holding the
	// Derby DB lock and block a fresh EnsureMetastore; and orphaned
	// per-workspace metastore networks left by pre-shared-network clavesa
	// versions (GH #42).
	observability.SweepWarmWorkers(workspace)
	observability.SweepMetastores(workspace)
	observability.SweepLegacyMetastoreNetworks()

	// Shared CLI service constructor — the same fully-wired stack every
	// command gets. The ui command's extras layer on top below: the STS
	// identity probe, the notebook REPL pool, the eager Spark warmup, and
	// the self-restart hook.
	deps, err := newServiceDeps(cmd)
	if err != nil {
		return err
	}
	clients := deps.full.Clients
	if clients.Err != nil {
		fmt.Fprintf(os.Stderr, "clavesa: AWS config unavailable (local-only mode): %v\n", clients.Err)
	}
	awsIdentity := ui.ProbeAWSIdentity(context.Background(), clients, "")

	// Eager Spark warmup: the Catalog landing page itself doesn't fire a
	// Spark query, so without this the runtime indicator stays "idle"
	// until the user clicks into a table — at which point Spark starts a
	// ~30s cold boot exactly when they wanted data. Gated on the workspace
	// being initialized; otherwise the runner image doesn't exist.
	//
	// Ensure the workspace runner image before warming (a pruned
	// `<image>:latest` tag would make every Spark-backed surface 500).
	// Runs in the background so a cold build doesn't block `ui` startup.
	if m, _ := wspkg.Load(workspace); m != nil {
		wsName := m.Name
		warmQuery := deps.warm
		go func() {
			if _, err := wspkg.EnsureLocalRunnerImage(workspace); err != nil {
				fmt.Fprintf(os.Stderr,
					"clavesa: local Spark is unavailable — could not prepare the runner image: %v\n"+
						"  Table preview, /query, and column profiles need it. Run `make build-runner` (dev) or re-run `clavesa workspace init`.\n",
					err)
				return
			}
			// Warm the worker for the workspace's *active* warehouse
			// (ADR-024). A cloud warehouse on an undeployed shell logs the
			// actionable error and skips.
			warehouseURI, whErr := wspkg.WarehouseURI(workspace)
			if whErr != nil {
				fmt.Fprintf(os.Stderr, "clavesa: skipping Spark warmup: %v\n", whErr)
				return
			}
			// Bring up the shared local Derby metastore before the first
			// query so the warm worker connects to it as a client rather
			// than racing to create it. Cloud warehouses use Glue — skip.
			if wspkg.LoadWarehouse(workspace) == wspkg.WarehouseLocal {
				if _, err := observability.EnsureMetastore(context.Background(), workspace, wsName); err != nil {
					fmt.Fprintf(os.Stderr,
						"clavesa: shared local metastore unavailable (queries fall back to embedded Derby): %v\n", err)
				}
			}
			warmQuery.Warmup(context.Background(), warehouseURI)
		}()
	}

	// Per-notebook REPL pool — shares the warm container's Spark Connect
	// plugin via per-notebook session_ids. The one Service seam beyond the
	// shared constructor's set — an explicit ui-only addition.
	deps.full.Svc = deps.svc.WithNotebookRunner(observability.NewNotebookSessionRunner(deps.warm))

	// Self-restart hook for PUT /workspace/aws-profile: the AWS SDK clients
	// are built once above and can't be hot-swapped, so a profile change
	// re-execs the process. syscall.Exec keeps the PID, fds, and terminal —
	// the browser just reloads once the server is back.
	restartFn := func() {
		exe, err := os.Executable()
		if err != nil {
			fmt.Fprintf(os.Stderr, "clavesa: cannot locate own binary to restart: %v\n", err)
			return
		}
		// Brief pause so the HTTP response flushes before the process
		// image is replaced.
		time.Sleep(250 * time.Millisecond)
		if err := syscall.Exec(exe, os.Args, os.Environ()); err != nil {
			fmt.Fprintf(os.Stderr, "clavesa: self-restart failed: %v\n", err)
		}
	}

	// API mux — all backend routes mount under /api/* so the SPA at / is
	// free to use any path without colliding with the backend.
	apiHandler := ui.BuildWorkspaceMux(ui.WorkspaceDeps{
		Deps:        deps.full,
		AWSIdentity: awsIdentity,
		Restart:     restartFn,
		ReadOnly:    readOnly,
	})
	mux := http.NewServeMux()
	mux.Handle("/api/", http.StripPrefix("/api", apiHandler))
	zone, _ := wspkg.TimeZone(workspace)
	cfg := ui.RuntimeConfig{Mode: "local", Basename: "", APIBase: "/api", ReadOnly: readOnly, TimeZone: zone}
	mux.Handle("/", ui.SPAHandler(static, func(*http.Request) ui.RuntimeConfig { return cfg }))
	var handler http.Handler = mux
	if readOnly {
		handler = ui.ReadOnlyGuard(mux)
	}

	suffix := ""
	if readOnly {
		suffix = ", read-only"
	}
	fmt.Printf("clavesa listening on %s (workspace: %s%s)\n", addr, workspace, suffix)

	// Graceful shutdown: runCloseables stops what newServiceDeps
	// registered (warm query workers, transpile sidecar); the shared Derby
	// metastore is stopped explicitly so it does not keep holding the
	// workspace's Derby lock after `clavesa ui` exits (#21).
	return serveUI(addr, handler, noBrowser, func() {
		runCloseables(cmd)
		observability.StopMetastore(workspace)
	})
}

// runMultiUI is `clavesa ui --workspaces <dir>` (ADR-032): one server over
// every workspace under dir. Each workspace's service stack and mux are
// built on its first request and cached. No Spark warmup, no Docker
// sweeps, no notebook pool and no self-restart: those belong to the
// one-workspace local command.
func runMultiUI(cmd *cobra.Command, addr, dir string, static fs.FS, readOnly, noBrowser bool) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve --workspaces: %w", err)
	}
	entries, err := ui.DiscoverWorkspaces(dir)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return fmt.Errorf("no workspaces under %s (a workspace is a directory with a clavesa.json)", dir)
	}

	var (
		mu    sync.Mutex
		built []*wsdeps.Deps
	)
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	factory := func(e ui.WorkspaceEntry) (http.Handler, error) {
		profile := wspkg.LoadAWSProfile(e.Path)
		d := wsdeps.New(ctx, e.Path, wsdeps.Options{AWSProfile: profile})
		mu.Lock()
		built = append(built, d)
		mu.Unlock()
		return ui.BuildWorkspaceMux(ui.WorkspaceDeps{
			Deps:        d,
			AWSIdentity: ui.ProbeAWSIdentity(ctx, d.Clients, profile),
			ReadOnly:    readOnly,
		}), nil
	}
	var handler http.Handler = ui.NewMultiRouter(entries, factory, static, readOnly)
	if readOnly {
		handler = ui.ReadOnlyGuard(handler)
	}

	suffix := ""
	if readOnly {
		suffix = ", read-only"
	}
	fmt.Printf("clavesa listening on %s (%d workspaces under %s%s)\n", addr, len(entries), dir, suffix)
	for _, e := range entries {
		fmt.Printf("  /w/%s/  %s (%s)\n", e.Slug, e.Name, e.Kind)
	}

	return serveUI(addr, handler, noBrowser, func() {
		mu.Lock()
		defer mu.Unlock()
		for _, d := range built {
			d.Close()
			observability.StopMetastore(d.Workspace)
		}
	})
}

// serveUI runs the HTTP server until it fails or SIGINT/SIGTERM arrives,
// then calls shutdown.
func serveUI(addr string, handler http.Handler, noBrowser bool, shutdown func()) error {
	if !noBrowser {
		go openBrowser("http://localhost" + addr)
	}
	srv := &http.Server{Addr: addr, Handler: handler}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	select {
	case err := <-serveErr:
		shutdown()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-sigCh:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		shutdown()
		return nil
	}
}

func openBrowser(u string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", u)
	default:
		cmd = exec.Command("xdg-open", u)
	}
	_ = cmd.Start()
}
