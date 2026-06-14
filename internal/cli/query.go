package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/servingsql"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// newQueryCmd implements `clavesa query` — workspace-level ad-hoc SQL,
// the CLI peer of the UI's /query page per ADR-015. Both surfaces ride
// the same service seam (service.Service.Query), so dispatch follows the
// workspace warehouse on both (ADR-024) and a cloud warehouse gets the
// same SparkSQL→Trino transpile (ADR-023). SQL-only by design (PySpark
// scratchpad lives in `clavesa notebook` per TODO bucket 10's v1 scope).
func newQueryCmd() *cobra.Command {
	var asJSON bool
	var warehouseOverride string
	cmd := &cobra.Command{
		Use:   "query [SQL]",
		Short: "Run an ad-hoc SQL query against the workspace catalog",
		Long: `Run a free-form SQL query against the workspace catalog, dispatched
by the workspace warehouse (the same routing the UI's /query page uses):

  - warehouse = local  →  the warm Spark container against the local
                          Hive metastore catalog. Full SparkSQL dialect.
  - warehouse = cloud  →  Athena over the deployed Glue catalog. Athena
                          speaks Trino — your SparkSQL is transpiled
                          automatically, so you author one dialect either
                          way (ADR-023).

The warehouse defaults to "local" and is set with ` + "`clavesa workspace use --warehouse`" + `.
Pass --warehouse local|cloud to override it for this query only.

Tables address as <workspace>__<schema>.<table> on both warehouses
(ADR-016/ADR-018; the v1.x ` + "`clavesa.`" + ` Iceberg-catalog prefix is gone).

Reads SQL from the first positional arg, or from STDIN when none given.

Examples:
  clavesa query "SHOW DATABASES"
  clavesa query "SELECT * FROM clavesa_demo__demo.trips LIMIT 5"
  clavesa query "SELECT count(*) FROM clavesa_demo__demo.trips" --warehouse cloud
  echo "SELECT count(*) FROM clavesa_demo__demo.trips" | clavesa query --json`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sql := ""
			if len(args) == 1 {
				sql = args[0]
			} else {
				bytes, err := io.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("read stdin: %w", err)
				}
				sql = string(bytes)
			}
			sql = strings.TrimSpace(sql)
			if sql == "" {
				return fmt.Errorf("no SQL provided (pass as argument or pipe via stdin)")
			}

			ws, err := resolveWorkspace(cmd)
			if err != nil {
				return fmt.Errorf("resolve workspace: %w", err)
			}

			wh := workspace.LoadWarehouse(ws)
			// --warehouse wins over the workspace's persisted warehouse
			// for this invocation only (same precedence as pipeline run).
			if warehouseOverride != "" {
				w, ok := workspace.ParseWarehouse(warehouseOverride)
				if !ok {
					return fmt.Errorf(`--warehouse must be "local" or "cloud", got %q`, warehouseOverride)
				}
				wh = w
			}

			// Wire the same resolver + transpiler stack `clavesa ui` hands
			// the /query route, so the two surfaces cannot diverge
			// (ADR-015). The cloud provider needs only Athena for ad-hoc
			// SQL; it stays nil when credentials don't load, and the
			// resolver surfaces a clear error if a cloud dispatch is then
			// attempted. The transpile sidecar and warm worker both spawn
			// lazily, so the warehouse that isn't used costs nothing.
			var cloud observability.Provider
			if awsCfg, cfgErr := awsconfig.LoadDefaultConfig(cmd.Context()); cfgErr == nil {
				bucket := os.Getenv("ATHENA_OUTPUT_BUCKET")
				if bucket == "" {
					bucket = workspace.PipelineBucket(ws)
				}
				cloud = observability.NewCloudProvider(athena.NewFromConfig(awsCfg), bucket, nil, nil)
			}
			warm := observability.NewPersistentQueryRunner(ws)
			defer warm.Close()
			local := observability.NewLocalProvider(ws).WithQueryRunner(warm)
			resolver := observability.NewResolver(ws, cloud, local)
			sidecar := observability.NewTranspileSidecar(ws)
			defer sidecar.Close()
			transpiler := servingsql.NewCachedTranspiler(filepath.Join(ws, ".clavesa", "cache", "transpile"), sidecar.ToServing)
			svc := service.New(ws).WithResolver(resolver).WithTranspiler(transpiler)

			if wh == workspace.WarehouseLocal {
				// Eagerly trigger spawn so the first query doesn't see the
				// 503-shaped "warm worker not ready" error from the catalog
				// path; the warmup is synchronous since the user is waiting.
				warm.Warmup(cmd.Context(), workspace.LocalWarehouseDir(ws))
			}

			res, err := svc.Query(cmd.Context(), sql, service.QueryOptions{Warehouse: wh})
			if err != nil {
				return err
			}

			if asJSON {
				// res carries `served` (engine + warehouse + transpiled,
				// ADR-024) via the QueryResult json tags — same wire shape
				// the UI's /data/query response exposes.
				return json.NewEncoder(os.Stdout).Encode(res)
			}
			// One engine-metadata header on stderr so the table on stdout
			// stays pipeable. Stamped by the code that executed the query
			// (ADR-024), so it cannot disagree with where the rows came from.
			if res.Served != nil {
				engine := res.Served.Engine
				if res.Served.Transpiled {
					engine += " (transpiled)"
				}
				fmt.Fprintf(os.Stderr, "engine: %s · %s warehouse\n", engine, res.Served.Warehouse)
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			names := make([]string, len(res.Columns))
			for i, c := range res.Columns {
				names[i] = c.Name
			}
			fmt.Fprintln(tw, strings.Join(names, "\t"))
			for _, row := range res.Rows {
				fmt.Fprintln(tw, strings.Join(row, "\t"))
			}
			return tw.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit columns + rows JSON")
	cmd.Flags().StringVar(&warehouseOverride, "warehouse", "", "override the workspace warehouse for this query: local | cloud")
	return cmd
}
