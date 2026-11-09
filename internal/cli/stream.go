package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/service"
)

// newStreamCmd is the `clavesa stream` group (ADR-028, proposed): stream
// nodes that read Kinesis or Kafka with Spark Structured Streaming and write
// one Delta table. Slice 1 is local only.
func newStreamCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stream",
		Short: "Run stream nodes (Kinesis, Kafka) into Delta tables",
		RunE:  requireSubcommand(),
	}
	cmd.AddCommand(newStreamRunCmd())
	return cmd
}

func newStreamRunCmd() *cobra.Command {
	var (
		req       service.StreamRunRequest
		recordKey string
		awsEnv    []string
		jsonOut   bool
	)
	cmd := &cobra.Command{
		Use:   "run <name>",
		Short: "Run one stream node against the local warehouse",
		Long: `Run a Spark Structured Streaming query that reads a Kinesis stream or a
Kafka topic and writes the Delta table <catalog>__<schema>.<name> in the local
warehouse. Nothing is staged: records go from the stream into the table.

Offsets live in the Spark checkpoint under the warehouse
(_streams/<table>/gen-<n>). Each micro-batch is written with Delta's
idempotent-write options, so a batch that Spark replays after a crash is not
written twice. failOnDataLoss is on: a shard or partition that expired before
it was read fails the run.

Without --drain the query runs until it is stopped. With --drain it stops once
every source reports no new data, and prints a summary.

--record-key drops producer duplicates (dropDuplicatesWithinWatermark over the
arrival time). --recover is for a lost or reset checkpoint: it starts a new
checkpoint generation at the earliest retained position and MERGEs on the
record key (or on _source_partition + _source_offset) until drained.

Examples:
  clavesa stream run clicks --kind kinesis --stream clicks \
    --endpoint http://localstack:4566 --network my-net --drain
  clavesa stream run orders --kind kafka --bootstrap-servers kafka:9092 \
    --topic orders --network my-net`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			req.Name = args[0]
			if recordKey != "" {
				for _, k := range strings.Split(recordKey, ",") {
					if k = strings.TrimSpace(k); k != "" {
						req.RecordKey = append(req.RecordKey, k)
					}
				}
			}
			for _, name := range awsEnv {
				if v, ok := os.LookupEnv(name); ok {
					req.AWSEnv = append(req.AWSEnv, name+"="+v)
				}
			}
			req.Stderr = os.Stderr
			res, err := svc.StreamRun(cmd.Context(), req)
			if res != nil {
				if jsonOut {
					if perr := printJSON(os.Stdout, res); perr != nil {
						return perr
					}
				} else {
					fmt.Printf("%s: %s, generation %d (%s), %d batches, %d rows in %.1fs\n",
						res.Table, res.Status, res.Generation, res.Mode, res.Batches, res.RowsWritten, res.Seconds)
				}
			}
			return err
		},
	}
	f := cmd.Flags()
	f.StringVar(&req.Kind, "kind", "kinesis", "source kind: kinesis or kafka")
	f.StringVar(&req.Schema, "schema", "streams", "schema the table lands in")
	f.StringVar(&req.StreamName, "stream", "", "Kinesis stream name")
	f.StringVar(&req.EndpointURL, "endpoint", "", "Kinesis endpoint URL (required by the connector)")
	f.StringVar(&req.Region, "region", "", "Kinesis region (default: from the endpoint)")
	f.StringVar(&req.StartingPosition, "starting-position", "TRIM_HORIZON", "Kinesis start for a new checkpoint: TRIM_HORIZON or LATEST")
	f.StringVar(&req.BootstrapServers, "bootstrap-servers", "", "Kafka bootstrap servers")
	f.StringVar(&req.Topic, "topic", "", "Kafka topic")
	f.StringVar(&req.StartingOffsets, "starting-offsets", "earliest", "Kafka start for a new checkpoint: earliest or latest")
	f.StringVar(&req.SQL, "sql", "", "optional SQL over the view `batch`; must keep the source columns")
	f.StringVar(&recordKey, "record-key", "", "comma-separated columns that identify one event; drops producer duplicates")
	f.StringVar(&req.Watermark, "watermark", "10 minutes", "how long a record key is remembered for duplicate drops")
	f.IntVar(&req.TriggerSeconds, "trigger-seconds", 120, "micro-batch interval")
	f.IntVar(&req.OptimizeEverySeconds, "optimize-every-seconds", 3600, "OPTIMIZE the previous hour partition at this interval (0 = off); not used with --drain")
	f.BoolVar(&req.Drain, "drain", false, "stop once every source reports no new data")
	f.BoolVar(&req.Recover, "recover", false, "new checkpoint generation from the earliest position, MERGE until drained (needs --drain)")
	f.StringVar(&req.Network, "network", "", "docker network the endpoint is on")
	f.StringVar(&req.ContainerName, "container-name", "", "container name (default clavesa-stream-<workspace>-<name>)")
	f.StringSliceVar(&awsEnv, "forward-env", []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION"}, "env vars forwarded to the runner when set")
	f.BoolVar(&jsonOut, "json", false, "print the summary as JSON")
	return cmd
}
