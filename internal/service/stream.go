package service

// stream.go runs one stream node (ADR-028, proposed) in the local runner
// container: a long-lived Spark Structured Streaming query that reads a
// Kinesis stream or a Kafka topic and writes one Delta table in the local
// warehouse. Slice 1 takes the node config from the caller (CLI flags); the
// Terraform module (modules/stream/aws) and the cloud ECS service are slice 2.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// StreamRunRequest is the config of one local stream run.
type StreamRunRequest struct {
	// Name is the stream node name. It is also the output table name.
	Name string
	// Kind is "kinesis" or "kafka".
	Kind string
	// Schema is the schema (ADR-016) the table lands in. Default "streams".
	Schema string

	// Kinesis.
	StreamName       string
	EndpointURL      string
	Region           string
	StartingPosition string

	// Kafka.
	BootstrapServers string
	Topic            string
	StartingOffsets  string

	// SQL is an optional statement over the view `batch`; it must keep the
	// source columns (raw, _source_partition, _source_offset, arrival_ts,
	// dt, hour).
	SQL string
	// RecordKey turns on dropDuplicatesWithinWatermark over these columns.
	RecordKey []string
	Watermark string

	TriggerSeconds       int
	OptimizeEverySeconds int

	// Drain stops the query once every source reports no new data.
	Drain bool
	// Recover starts a new checkpoint generation at the earliest retained
	// position and MERGEs until drained. Requires Drain.
	Recover bool

	// Network is an extra docker network the container must reach (the
	// Kinesis or Kafka endpoint). The shared metastore is attached to it.
	Network string
	// ContainerName overrides the default container name.
	ContainerName string
	// AWSEnv are KEY=VALUE pairs forwarded to the container (credentials
	// for the Kinesis endpoint).
	AWSEnv []string

	// Stderr, when set, receives the runner's stderr live (batch events).
	Stderr io.Writer
}

// StreamRunResult is the runner's summary line for a drain run.
type StreamRunResult struct {
	Status      string  `json:"status"`
	Error       string  `json:"error,omitempty"`
	Table       string  `json:"table"`
	Generation  int     `json:"generation"`
	Mode        string  `json:"mode"`
	Batches     int     `json:"batches"`
	RowsWritten int64   `json:"rows_written"`
	Seconds     float64 `json:"seconds"`
}

// StreamTableID returns the metastore identifier of a stream node's table:
// the same flat <catalog>__<schema>.<table> form transform outputs use.
func StreamTableID(catalog, schema, name string) string {
	if schema == "" {
		schema = "streams"
	}
	if catalog == "" {
		catalog = "clavesa"
	}
	return identutil.EncodeGlueDatabase(catalog, schema) + "." + identutil.Sanitize(name)
}

// streamConfig is the JSON the runner reads from CLAVESA_STREAM_CONFIG.
func streamConfig(req StreamRunRequest, table string) (map[string]any, error) {
	cfg := map[string]any{
		"name":    identutil.Sanitize(req.Name),
		"kind":    req.Kind,
		"table":   table,
		"drain":   req.Drain,
		"recover": req.Recover,
	}
	switch req.Kind {
	case "kinesis":
		if req.StreamName == "" || req.EndpointURL == "" {
			return nil, errors.New("kinesis needs --stream and --endpoint")
		}
		k := map[string]any{"stream_name": req.StreamName, "endpoint_url": req.EndpointURL}
		if req.Region != "" {
			k["region"] = req.Region
		}
		if req.StartingPosition != "" {
			k["starting_position"] = req.StartingPosition
		}
		cfg["kinesis"] = k
	case "kafka":
		if req.BootstrapServers == "" || req.Topic == "" {
			return nil, errors.New("kafka needs --bootstrap-servers and --topic")
		}
		k := map[string]any{"bootstrap_servers": req.BootstrapServers, "topic": req.Topic}
		if req.StartingOffsets != "" {
			k["starting_offsets"] = req.StartingOffsets
		}
		cfg["kafka"] = k
	default:
		return nil, fmt.Errorf("--kind must be kinesis or kafka, got %q", req.Kind)
	}
	if req.Recover && !req.Drain {
		return nil, errors.New("--recover runs only with --drain")
	}
	if req.SQL != "" {
		cfg["sql"] = req.SQL
	}
	if len(req.RecordKey) > 0 {
		cfg["record_key"] = req.RecordKey
	}
	if req.Watermark != "" {
		cfg["watermark"] = req.Watermark
	}
	if req.TriggerSeconds > 0 {
		cfg["trigger_seconds"] = req.TriggerSeconds
	}
	if req.OptimizeEverySeconds >= 0 {
		cfg["optimize_every_seconds"] = req.OptimizeEverySeconds
	}
	return cfg, nil
}

// StreamContainerName is the default container name of a stream node, so a
// running stream can be found with docker ps and stopped by name.
func StreamContainerName(workspaceName, node string) string {
	n := "clavesa-stream-" + identutil.Sanitize(node)
	if workspaceName != "" {
		n = "clavesa-stream-" + identutil.Sanitize(workspaceName) + "-" + identutil.Sanitize(node)
	}
	return strings.ReplaceAll(n, "_", "-")
}

// StreamRun runs one stream node against the local warehouse. With Drain it
// returns the runner's summary; without it, it blocks until the container
// stops (Ctrl-C, docker stop) and returns the exit status as an error.
func (s *Service) StreamRun(ctx context.Context, req StreamRunRequest) (*StreamRunResult, error) {
	if req.Name == "" {
		return nil, errors.New("stream name is required")
	}
	image := workspace.LocalRunnerImageTag(s.workspace)
	catalog, systemCatalog := "", ""
	if m, _ := workspace.Load(s.workspace); m != nil {
		ensured, err := workspace.EnsureLocalRunnerImage(s.workspace)
		if err != nil {
			return nil, fmt.Errorf("ensure runner image: %w", err)
		}
		image = ensured
		catalog = m.CatalogIdentifier()
		systemCatalog = m.SystemCatalogIdentifier()
	}
	if req.Schema == "" {
		req.Schema = "streams"
	}
	table := StreamTableID(catalog, req.Schema, req.Name)
	cfg, err := streamConfig(req, table)
	if err != nil {
		return nil, err
	}
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	warehouse := workspace.LocalWarehouseDir(s.workspace)
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		return nil, fmt.Errorf("create warehouse: %w", err)
	}

	network, addr := s.metastoreEnsure(ctx, s.workspace, s.workspaceName())
	if req.Network != "" {
		if addr != "" {
			// Attach the shared metastore to the endpoint's network so one
			// network reaches both. Idempotent: "already exists" is fine.
			host := strings.SplitN(addr, ":", 2)[0]
			out, cerr := exec.CommandContext(ctx, "docker", "network", "connect", req.Network, host).CombinedOutput()
			if cerr != nil && !strings.Contains(string(out), "already exists") {
				return nil, fmt.Errorf("attach metastore to network %s: %v: %s", req.Network, cerr, strings.TrimSpace(string(out)))
			}
		}
		network = req.Network
	}

	name := req.ContainerName
	if name == "" {
		name = StreamContainerName(s.workspaceName(), req.Name)
	}
	env := []string{
		"CLAVESA_STREAM_CONFIG=" + string(cfgJSON),
		"CLAVESA_CATALOG=" + catalog,
		"CLAVESA_SCHEMA=" + identutil.Sanitize(req.Schema),
		"CLAVESA_SYSTEM_CATALOG=" + systemCatalog,
	}
	env = append(env, req.AWSEnv...)
	args := dockerRunArgs(dockerRunSpec{
		Image:            image,
		Stream:           true,
		Name:             name,
		Warehouse:        warehouse,
		MetastoreNetwork: network,
		MetastoreAddr:    addr,
		Env:              env,
		Heap:             true,
	})
	if addr == "" && network != "" {
		// No shared metastore: still join the endpoint network.
		args = append(args[:3], append([]string{"--network", network}, args[3:]...)...)
	}

	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout strings.Builder
	cmd.Stdout = &stdout
	if req.Stderr != nil {
		cmd.Stderr = req.Stderr
	}
	runErr := cmd.Run()

	var res StreamRunResult
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	last := lines[len(lines)-1]
	if jerr := json.Unmarshal([]byte(last), &res); jerr == nil && res.Status != "" {
		if res.Status != "ok" {
			return &res, fmt.Errorf("stream %s: %s", req.Name, res.Error)
		}
		return &res, runErr
	}
	if runErr != nil {
		return nil, fmt.Errorf("stream %s: runner container exited: %w", req.Name, runErr)
	}
	return nil, fmt.Errorf("stream %s: no summary from the runner", req.Name)
}
