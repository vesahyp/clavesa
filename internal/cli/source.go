package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/sources"
)

// newSourceCmd implements ADR-017's `clavesa source` noun: workspace-
// level registry of where raw data lives. Slice 1 supports `kind=http`
// only.
func newSourceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "source",
		Short: "Manage workspace input sources (registry)",
		Long: `Manage the workspace-level input source registry (ADR-017).

A source is a named entry recording where raw data lives. Pipelines
reference sources by name in transform inputs:

    inputs = { raw = "sources.<name>" }

Slice 1: only kind=http (no auth).

Examples:
  clavesa source register trips --from https://example.com/trips.parquet
  clavesa source list
  clavesa source show trips
  clavesa source attach my-pipeline trips --to t1 --as raw
  clavesa source delete trips`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(
		newSourceRegisterCmd(),
		newSourceEditCmd(),
		newSourceListCmd(),
		newSourceShowCmd(),
		newSourcePreviewCmd(),
		newSourceDeleteCmd(),
		newSourceAttachCmd(),
		newSourceDetachCmd(),
	)
	return cmd
}

func newSourceRegisterCmd() *cobra.Command {
	var from, kind, bucket, prefix, format, table string
	var stream, region, startingPosition, endpoint string
	var attach, attachTo, attachAs, creds string
	var partitions []string
	var startFrom string
	var manageNotifications bool
	var noTrigger bool
	var readOptions map[string]string

	cmd := &cobra.Command{
		Use:   "register <name>",
		Short: "Register a new source in the workspace registry",
		Long: `Register a source under the workspace's registry.

Two shorthands cover the common cases — pass --from with the URL form:

  clavesa source register trips --from https://example.com/trips.parquet
  clavesa source register logs  --from s3://my-bucket/events/2024/

For s3, you can also pass kind/bucket/prefix explicitly:

  clavesa source register logs --kind s3 --bucket my-bucket --prefix events/ --format json

A table a Databricks step wrote (Unity Catalog) takes a uc:// URL, or
--table. It needs a Databricks connection (clavesa workspace connect
databricks) and reads in the cloud only:

  clavesa source register orders --from uc://main.sales.orders

An Amazon Kinesis data stream takes a kinesis:// URL, or --stream. A
transform that reads it must have it as its only input and append to one
output:

  clavesa source register events --from kinesis://my-stream
  clavesa source register events --kind kinesis --stream my-stream --region eu-west-1 --starting-position LATEST

--format is inferred from the trailing filename when omitted. Pass
--attach <pipeline-dir> --to <transform> [--as <alias>] to attach the
new source to a pipeline in one step.

Incremental ingest: a deployed s3 source reads only the newly-arrived
files each run by draining the bucket's notification queue (the SQS queue
fed by S3 Object Created events); no per-run bucket listing. Declare
Hive-style partition keys with --partitions year,month,day so the output
table recovers its partition columns. --start-from controls the first run
before the queue takes over: "all" (default; read pre-existing files),
"now" (skip them), or a literal "/"-joined cursor like "2024-01-01".
Local runs fall back to listing (no queue), with identical results.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			// Resolve shape: --from URL is the shorthand; explicit
			// flags win when both are passed (CLI rule of least
			// surprise).
			spec := service.SourceSpec{
				Name:                      name,
				Kind:                      kind,
				Bucket:                    bucket,
				Prefix:                    prefix,
				Format:                    format,
				Credentials:               creds,
				Partitions:                partitions,
				StartFrom:                 startFrom,
				ManageBucketNotifications: manageNotifications,
				NoTrigger:                 noTrigger,
				ReadOptions:               readOptions,
				Stream:                    stream,
				Region:                    region,
				StartingPosition:          startingPosition,
				Endpoint:                  endpoint,
			}
			if from != "" {
				if strings.HasPrefix(from, sources.UCScheme) {
					// AddSource derives the table from the URL.
					spec.Kind = sources.KindUnityCatalog
					spec.URL = from
				} else if strings.HasPrefix(from, sources.KinesisScheme) {
					// AddSource derives the stream from the URL.
					spec.Kind = sources.KindKinesis
					spec.URL = from
				} else if strings.HasPrefix(from, "s3://") {
					if spec.Kind == "" {
						spec.Kind = "s3"
					}
					// AddSource derives bucket/prefix/format from
					// the URL field — leave the bare URL there for
					// it to consume.
					spec.URL = from
				} else {
					if spec.Kind == "" {
						spec.Kind = "http"
					}
					spec.URL = from
				}
			}
			if table != "" {
				if spec.Kind == "" {
					spec.Kind = sources.KindUnityCatalog
				}
				spec.Table = table
			}
			if stream != "" && spec.Kind == "" {
				spec.Kind = sources.KindKinesis
			}
			if spec.Kind == "" {
				return fmt.Errorf("specify --from <url> or --kind explicitly")
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			stored, err := svc.AddSource(spec)
			if err != nil {
				return err
			}
			fmt.Printf("Registered source %s (%s, %s)\n", stored.Name, stored.Kind, stored.Format)
			switch stored.Kind {
			case sources.KindKinesis:
				fmt.Printf("  stream: %s\n", stored.Stream)
			case "http":
				fmt.Printf("  url:    %s\n", stored.URL)
			case "s3":
				fmt.Printf("  bucket: %s\n", stored.Bucket)
				fmt.Printf("  prefix: %s\n", stored.Prefix)
			case sources.KindUnityCatalog:
				fmt.Printf("  table:  %s\n", stored.Table)
			}

			if attach != "" {
				if attachTo == "" {
					return fmt.Errorf("--attach requires --to <transform-node>")
				}
				if err := svc.AttachSource(attach, name, attachTo, attachAs); err != nil {
					return fmt.Errorf("attach: %w", err)
				}
				alias := attachAs
				if alias == "" {
					alias = name
				}
				fmt.Printf("Attached to %s/%s as %s\n", attach, attachTo, alias)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "URL shorthand: https://… for kind=http, s3://… for kind=s3, uc://catalog.schema.table for kind=unity-catalog, kinesis://<stream> for kind=kinesis")
	cmd.Flags().StringVar(&kind, "kind", "", "source kind (http, s3, unity-catalog, kinesis); inferred from --from when omitted")
	cmd.Flags().StringVar(&stream, "stream", "", "Kinesis data stream name (kind=kinesis)")
	cmd.Flags().StringVar(&region, "region", "", "Kinesis stream region (kind=kinesis); default is the deploy region")
	cmd.Flags().StringVar(&startingPosition, "starting-position", "", "kind=kinesis: TRIM_HORIZON (default, oldest record) or LATEST")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "kind=kinesis: endpoint URL override (LocalStack)")
	cmd.Flags().StringVar(&table, "table", "", "Unity Catalog table <catalog>.<schema>.<table> (kind=unity-catalog; needs clavesa workspace connect databricks)")
	cmd.Flags().StringVar(&bucket, "bucket", "", "S3 bucket name (kind=s3)")
	cmd.Flags().StringVar(&prefix, "prefix", "", "S3 key prefix (kind=s3); auto-suffixed with /")
	cmd.Flags().StringVar(&format, "format", "", "data format (parquet, csv, json, tsv); inferred from filename if omitted")
	cmd.Flags().StringVar(&attach, "attach", "", "also attach to a pipeline in this workspace (pipeline dir)")
	cmd.Flags().StringVar(&attachTo, "to", "", "transform node id (when --attach is set)")
	cmd.Flags().StringVar(&attachAs, "as", "", "input alias (default: source name) when --attach is set")
	cmd.Flags().StringVar(&creds, "credentials", "", "name of a registered credential (slice 2: header auth)")
	cmd.Flags().StringSliceVar(&partitions, "partitions", nil, "kind=s3: comma-separated Hive partition keys (e.g. year,month,day) for partition-column recovery")
	cmd.Flags().StringVar(&startFrom, "start-from", "", `kind=s3 with --partitions: first-run seed before queue-drain takes over ("all" | "now" | "<cursor>")`)
	cmd.Flags().BoolVar(&manageNotifications, "manage-notifications", false, "kind=s3: have terraform manage the bucket's EventBridge notification config (authoritative — replaces existing notification config). Default off; only enable when clavesa owns the source bucket")
	cmd.Flags().BoolVar(&noTrigger, "no-trigger", false, "kind=s3: new files do not start runs; the pipeline runs on its schedule and drains the queue then")
	cmd.Flags().StringToStringVar(&readOptions, "read-option", nil, "repeatable Spark read option for delimited text (format=tsv/csv): delimiter, comment, header, columns (e.g. --read-option delimiter=$'\\t' --read-option header=false)")
	return cmd
}

// nonEmptyStrings drops blank entries. `--partitions ""` arrives from
// cobra as [""], so filtering it lets an explicit empty value clear the
// partition list rather than fail validation on an empty partition key.
func nonEmptyStrings(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		if strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out
}

func newSourceEditCmd() *cobra.Command {
	var from, kind, bucket, prefix, format, creds, table string
	var stream, region, startingPosition, endpoint string
	var partitions []string
	var startFrom string
	var manageNotifications bool
	var noTrigger bool
	var readOptions map[string]string

	cmd := &cobra.Command{
		Use:   "edit <name>",
		Short: "Edit a registered source",
		Long: `Update fields of an already-registered source.

Only the flags you pass change; every other field keeps its current
value. The source name is fixed — pipelines reference sources by name,
so a rename is a delete + re-register, not an edit.

  clavesa source edit trips --from https://example.com/trips-v2.parquet
  clavesa source edit logs  --prefix events/2024/ --start-from now
  clavesa source edit trips --credentials ""    # clear the credential

Editing a kind=s3 source does not re-sync pipelines already attached to
it — re-run 'source attach' to propagate. kind=http edits take effect on
the next run automatically.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			spec, err := svc.GetSource(name)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("source %q not registered", name)
				}
				return err
			}
			changed := cmd.Flags().Changed
			touched := false
			if changed("from") {
				// Re-derive the location from scratch; an explicit
				// --kind/--bucket/--prefix/--format below still wins.
				spec.URL, spec.Bucket, spec.Prefix, spec.Format, spec.Table = "", "", "", "", ""
				spec.Stream, spec.Region, spec.StartingPosition, spec.Endpoint = "", "", "", ""
				if strings.HasPrefix(from, sources.KinesisScheme) {
					spec.Kind, spec.URL = sources.KindKinesis, from
					spec.Partitions = nil
					spec.StartFrom = ""
					spec.ManageBucketNotifications = false
					spec.NoTrigger = false
				} else if strings.HasPrefix(from, sources.UCScheme) {
					spec.Kind, spec.URL = sources.KindUnityCatalog, from
					spec.Partitions = nil
					spec.StartFrom = ""
					spec.ManageBucketNotifications = false
					spec.NoTrigger = false
				} else if strings.HasPrefix(from, "s3://") {
					spec.Kind, spec.URL = "s3", from
				} else {
					spec.Kind, spec.URL = "http", from
					// partitions / start_from / managed notifications
					// are kind=s3 only — drop any left over from a
					// previous s3 spec so the switch to http validates.
					spec.Partitions = nil
					spec.StartFrom = ""
					spec.ManageBucketNotifications = false
					spec.NoTrigger = false
				}
				touched = true
			}
			if changed("kind") {
				spec.Kind = kind
				touched = true
			}
			if changed("table") {
				spec.Table = table
				touched = true
			}
			if changed("stream") {
				spec.Stream = stream
				touched = true
			}
			if changed("region") {
				spec.Region = region
				touched = true
			}
			if changed("starting-position") {
				spec.StartingPosition = startingPosition
				touched = true
			}
			if changed("endpoint") {
				spec.Endpoint = endpoint
				touched = true
			}
			if changed("bucket") {
				spec.Bucket = bucket
				touched = true
			}
			if changed("prefix") {
				spec.Prefix = prefix
				touched = true
			}
			if changed("format") {
				spec.Format = format
				touched = true
			}
			if changed("credentials") {
				spec.Credentials = creds
				touched = true
			}
			if changed("partitions") {
				spec.Partitions = nonEmptyStrings(partitions)
				touched = true
			}
			if changed("start-from") {
				spec.StartFrom = startFrom
				touched = true
			}
			if changed("manage-notifications") {
				spec.ManageBucketNotifications = manageNotifications
				touched = true
			}
			if changed("no-trigger") {
				spec.NoTrigger = noTrigger
				touched = true
			}
			if changed("read-option") {
				spec.ReadOptions = readOptions
				touched = true
			}
			if !touched {
				return fmt.Errorf("nothing to change — pass a flag (see `clavesa source edit --help`)")
			}
			stored, err := svc.UpdateSource(name, spec)
			if err != nil {
				return err
			}
			fmt.Printf("Updated source %s (%s, %s)\n", stored.Name, stored.Kind, stored.Format)
			switch stored.Kind {
			case "http":
				fmt.Printf("  url:    %s\n", stored.URL)
			case "s3":
				fmt.Printf("  bucket: %s\n", stored.Bucket)
				fmt.Printf("  prefix: %s\n", stored.Prefix)
			case sources.KindUnityCatalog:
				fmt.Printf("  table:  %s\n", stored.Table)
			case sources.KindKinesis:
				fmt.Printf("  stream: %s\n", stored.Stream)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "URL shorthand: https://… for kind=http, s3://… for kind=s3, uc://catalog.schema.table for kind=unity-catalog, kinesis://<stream> for kind=kinesis")
	cmd.Flags().StringVar(&kind, "kind", "", "source kind (http, s3, unity-catalog, kinesis)")
	cmd.Flags().StringVar(&table, "table", "", "Unity Catalog table <catalog>.<schema>.<table> (kind=unity-catalog)")
	cmd.Flags().StringVar(&stream, "stream", "", "Kinesis data stream name (kind=kinesis)")
	cmd.Flags().StringVar(&region, "region", "", `Kinesis stream region (kind=kinesis); pass "" for the deploy region`)
	cmd.Flags().StringVar(&startingPosition, "starting-position", "", `kind=kinesis: TRIM_HORIZON or LATEST; pass "" for the default`)
	cmd.Flags().StringVar(&endpoint, "endpoint", "", `kind=kinesis: endpoint URL override (LocalStack); pass "" to clear`)
	cmd.Flags().StringVar(&bucket, "bucket", "", "S3 bucket name (kind=s3)")
	cmd.Flags().StringVar(&prefix, "prefix", "", "S3 key prefix (kind=s3); auto-suffixed with /")
	cmd.Flags().StringVar(&format, "format", "", "data format (parquet, csv, json, tsv)")
	cmd.Flags().StringVar(&creds, "credentials", "", `name of a registered credential; pass "" to clear`)
	cmd.Flags().StringSliceVar(&partitions, "partitions", nil, `kind=s3: comma-separated Hive partition keys; pass "" to clear`)
	cmd.Flags().StringVar(&startFrom, "start-from", "", `kind=s3 with --partitions: watermark seed ("all" | "now" | "<cursor>")`)
	cmd.Flags().BoolVar(&manageNotifications, "manage-notifications", false, "kind=s3: have terraform manage the bucket's EventBridge notification config")
	cmd.Flags().BoolVar(&noTrigger, "no-trigger", false, "kind=s3: new files do not start runs; the pipeline runs on its schedule and drains the queue then")
	cmd.Flags().StringToStringVar(&readOptions, "read-option", nil, "repeatable Spark read option for delimited text (format=tsv/csv): delimiter, comment, header, columns")
	return cmd
}

func newSourceListCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List registered sources",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			list, err := svc.ListSources()
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, sourcesJSONView(list))
			}
			if len(list) == 0 {
				fmt.Println("No sources registered.")
				fmt.Println("Try: clavesa source register <name> --from <url>")
				return nil
			}
			rows := make([][]string, len(list))
			for i, s := range list {
				location := s.URL
				if s.Kind == "s3" {
					location = "s3://" + s.Bucket + "/" + s.Prefix
				}
				if s.Kind == sources.KindUnityCatalog {
					location = sources.UCScheme + s.Table
				}
				if s.Kind == sources.KindKinesis {
					location = sources.KinesisScheme + s.Stream
				}
				rows[i] = []string{s.Name, s.Kind, s.Format, s.Credentials, location}
			}
			printTable(os.Stdout, []string{"NAME", "KIND", "FORMAT", "CREDENTIAL", "LOCATION"}, rows)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func newSourceShowCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "show <name>",
		Short: "Show a source's spec",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			spec, err := svc.GetSource(args[0])
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("source %q not registered", args[0])
				}
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, sourceJSONOne(spec))
			}
			fmt.Printf("Name:    %s\n", spec.Name)
			fmt.Printf("Kind:    %s\n", spec.Kind)
			fmt.Printf("Format:  %s\n", spec.Format)
			switch spec.Kind {
			case "http":
				fmt.Printf("URL:     %s\n", spec.URL)
			case "s3":
				fmt.Printf("Bucket:  %s\n", spec.Bucket)
				fmt.Printf("Prefix:  %s\n", spec.Prefix)
			case sources.KindUnityCatalog:
				fmt.Printf("Table:   %s\n", spec.Table)
			case sources.KindKinesis:
				fmt.Printf("Stream:  %s\n", spec.Stream)
				if spec.Region != "" {
					fmt.Printf("Region:  %s\n", spec.Region)
				}
				pos := spec.StartingPosition
				if pos == "" {
					pos = sources.KinesisTrimHorizon
				}
				fmt.Printf("Start:   %s\n", pos)
				if spec.Endpoint != "" {
					fmt.Printf("Endpoint: %s\n", spec.Endpoint)
				}
			}
			if spec.Credentials != "" {
				fmt.Printf("Cred:    %s\n", spec.Credentials)
			}
			if len(spec.Partitions) > 0 {
				fmt.Printf("Parts:   %s\n", strings.Join(spec.Partitions, ", "))
				if spec.StartFrom != "" {
					fmt.Printf("Start:   %s\n", spec.StartFrom)
				}
			}
			if spec.NoTrigger {
				fmt.Println("Trigger: off (new files do not start runs; the schedule runs the pipeline)")
			}
			if spec.ManageBucketNotifications {
				fmt.Println("Notif:   managed (terraform owns the bucket's EventBridge notification config)")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func newSourcePreviewCmd() *cobra.Command {
	var jsonOut bool
	var offset, limit int
	cmd := &cobra.Command{
		Use:   "preview <name>",
		Short: "Preview a registered source's data",
		Long: `Sample a registered source's data without attaching it to a pipeline.

Fetches the source through the same host-side path preview uses for
transform inputs — http and s3 sources both work. Sources that
reference a credential aren't previewable yet (works in pipeline run).

  clavesa source preview trips
  clavesa source preview trips --limit 50 --json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			result, err := svc.PreviewRegistrySource(context.Background(), args[0], offset, limit)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("source %q not registered", args[0])
				}
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, result)
			}
			for _, item := range result.Items {
				b, _ := json.Marshal(item)
				fmt.Printf("%s\n", b)
			}
			if len(result.Items) == 0 {
				fmt.Println("(no rows)")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().IntVar(&offset, "offset", 0, "row offset")
	cmd.Flags().IntVar(&limit, "limit", 15, "max rows to show")
	return cmd
}

func newSourceDeleteCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a registered source",
		Long: `Delete a registered source.

Refuses if any pipeline in the workspace references the source. Use
--force to delete anyway (intended for scripted teardown — manually
clean up the dangling references afterwards).`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			err = svc.DeleteSource(args[0], force)
			if err != nil {
				return err
			}
			fmt.Printf("Deleted source: %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "delete even if pipelines reference this source")
	return cmd
}

// sourceJSON is the on-the-wire CLI shape for `source list/show --json`.
// Storage's service.SourceSpec drops Name (it's the filename); we add it
// back so users can pipe the output to jq without losing the identifier.
type sourceJSON struct {
	Name                      string   `json:"name"`
	Kind                      string   `json:"kind"`
	URL                       string   `json:"url,omitempty"`
	Bucket                    string   `json:"bucket,omitempty"`
	Prefix                    string   `json:"prefix,omitempty"`
	Format                    string   `json:"format,omitempty"`
	Credentials               string   `json:"credentials,omitempty"`
	Partitions                []string `json:"partitions,omitempty"`
	StartFrom                 string   `json:"start_from,omitempty"`
	ManageBucketNotifications bool     `json:"manage_bucket_notifications,omitempty"`
	NoTrigger                 bool     `json:"no_trigger,omitempty"`
	Table                     string   `json:"table,omitempty"`
	Stream                    string   `json:"stream,omitempty"`
	Region                    string   `json:"region,omitempty"`
	StartingPosition          string   `json:"starting_position,omitempty"`
	Endpoint                  string   `json:"endpoint,omitempty"`
}

func sourceJSONOne(s service.SourceSpec) sourceJSON {
	return sourceJSON{
		Name: s.Name, Kind: s.Kind, URL: s.URL,
		Bucket: s.Bucket, Prefix: s.Prefix,
		Format: s.Format, Credentials: s.Credentials,
		Partitions: s.Partitions, StartFrom: s.StartFrom,
		ManageBucketNotifications: s.ManageBucketNotifications,
		NoTrigger:                 s.NoTrigger,
		Table:                     s.Table,
		Stream:                    s.Stream, Region: s.Region,
		StartingPosition: s.StartingPosition, Endpoint: s.Endpoint,
	}
}

func sourcesJSONView(list []service.SourceSpec) []sourceJSON {
	out := make([]sourceJSON, len(list))
	for i, s := range list {
		out[i] = sourceJSONOne(s)
	}
	return out
}

func newSourceAttachCmd() *cobra.Command {
	var to, as string
	cmd := &cobra.Command{
		Use:   "attach [pipeline-dir] <source>",
		Short: "Attach a registered source to a transform input",
		Long: `Attach a registered source to a transform's inputs map.

Writes inputs = { <alias> = "sources.<source>" } into the transform
block; the orchestration emitter resolves the reference at sync time.

Examples:
  clavesa source attach my-pipeline trips --to t1 --as raw
  clavesa source attach my-pipeline trips --to t1   # alias defaults to source name

` + pipelineDirHelp,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if to == "" {
				return fmt.Errorf("--to <transform-node-id> is required")
			}
			dir, rest, ws, err := resolvePipelineDir(cmd, args, 1)
			if err != nil {
				return err
			}
			name := rest[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.AttachSource(dir, name, to, as); err != nil {
				return err
			}
			alias := as
			if alias == "" {
				alias = name
			}
			fmt.Printf("Attached %s to %s/%s as %s\n", name, displayDir(ws, dir), to, alias)
			return nil
		},
	}
	cmd.Flags().StringVar(&to, "to", "", "transform node id to attach to")
	cmd.Flags().StringVar(&as, "as", "", "input alias (default: source name)")
	return cmd
}

func newSourceDetachCmd() *cobra.Command {
	var to, as string
	cmd := &cobra.Command{
		Use:   "detach [pipeline-dir]",
		Short: "Detach a named input from a transform",
		Long: `Remove an aliased input from a transform's inputs map.

Covers all three attachment kinds — registry sources, external Glue
tables (` + "`<schema>.<table>`" + ` refs), and transform→transform edges — so
one command works regardless of how the input was originally attached.
For transform→transform edges, ` + "`clavesa node disconnect`" + ` is the
direct equivalent.

Examples:
  clavesa source detach my-pipeline --to t1 --as raw
  clavesa source detach              --to t1 --as raw   # cwd is the pipeline dir

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if to == "" {
				return fmt.Errorf("--to <transform-node-id> is required")
			}
			if as == "" {
				return fmt.Errorf("--as <alias> is required (the input key to remove)")
			}
			dir, _, ws, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.DetachInput(dir, to, as); err != nil {
				return err
			}
			fmt.Printf("Detached %s from %s/%s\n", as, displayDir(ws, dir), to)
			return nil
		},
	}
	cmd.Flags().StringVar(&to, "to", "", "transform node id to detach from")
	cmd.Flags().StringVar(&as, "as", "", "input alias to remove")
	return cmd
}
