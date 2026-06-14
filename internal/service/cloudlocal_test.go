package service

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/vesahyp/clavesa/internal/workspace"
)

func TestValidateCompute(t *testing.T) {
	cases := []struct {
		name      string
		warehouse workspace.Warehouse
		compute   string
		wantErr   bool
	}{
		{"empty + local warehouse → ok", workspace.WarehouseLocal, "", false},
		{"empty + cloud warehouse → ok", workspace.WarehouseCloud, "", false},
		{"local + local warehouse → ok (no-op)", workspace.WarehouseLocal, "local", false},
		{"local + cloud warehouse → ok", workspace.WarehouseCloud, "local", false},
		{"cloud + cloud warehouse → ok", workspace.WarehouseCloud, "cloud", false},
		{"cloud + local warehouse → error", workspace.WarehouseLocal, "cloud", true},
		{"bogus + local warehouse → error", workspace.WarehouseLocal, "fargate", true},
		{"bogus + cloud warehouse → error", workspace.WarehouseCloud, "fargate", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCompute(tc.warehouse, tc.compute)
			if tc.wantErr && err == nil {
				t.Fatalf("validateCompute(%q, %q) = nil, want error", tc.warehouse, tc.compute)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validateCompute(%q, %q) = %v, want nil", tc.warehouse, tc.compute, err)
			}
		})
	}
}

// fakeLambdaEnv implements lambdaEnvGetter, returning a canned
// GetFunctionConfiguration so lambdaRunnerEnv is exercised without AWS.
type fakeLambdaEnv struct {
	vars  map[string]string
	err   error
	noEnv bool
}

func (f fakeLambdaEnv) GetFunctionConfiguration(ctx context.Context, in *lambda.GetFunctionConfigurationInput, _ ...func(*lambda.Options)) (*lambda.GetFunctionConfigurationOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := &lambda.GetFunctionConfigurationOutput{}
	if !f.noEnv {
		out.Environment = &lambdatypes.EnvironmentResponse{Variables: f.vars}
	}
	return out, nil
}

func TestLambdaRunnerEnv(t *testing.T) {
	t.Run("returns the full variable map", func(t *testing.T) {
		want := map[string]string{
			"CLAVESA_CATALOG":   "clavesa_demo",
			"CLAVESA_SCHEMA":    "trips",
			"CLAVESA_WAREHOUSE": "s3://bucket/_warehouse/",
		}
		got, err := lambdaRunnerEnv(context.Background(), fakeLambdaEnv{vars: want}, "fn")
		if err != nil {
			t.Fatalf("lambdaRunnerEnv: %v", err)
		}
		if got["CLAVESA_CATALOG"] != "clavesa_demo" || got["CLAVESA_SCHEMA"] != "trips" || got["CLAVESA_WAREHOUSE"] != "s3://bucket/_warehouse/" {
			t.Fatalf("lambdaRunnerEnv = %v, want %v", got, want)
		}
	})
	t.Run("propagates GetFunctionConfiguration error", func(t *testing.T) {
		_, err := lambdaRunnerEnv(context.Background(), fakeLambdaEnv{err: fmt.Errorf("boom")}, "fn")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("errors when the Lambda has no environment", func(t *testing.T) {
		_, err := lambdaRunnerEnv(context.Background(), fakeLambdaEnv{noEnv: true}, "fn")
		if err == nil {
			t.Fatal("expected error for missing environment, got nil")
		}
	})
}

func TestCloudLocalDockerArgs(t *testing.T) {
	env := map[string]string{
		"CLAVESA_WAREHOUSE":        "s3://bucket/_warehouse/",
		"CLAVESA_SYSTEM_WAREHOUSE": "s3://bucket/_workspace/_warehouse/",
		"CLAVESA_CATALOG":          "clavesa_demo",
		"CLAVESA_SCHEMA":           "trips",
		"CLAVESA_SYSTEM_CATALOG":   "clavesa_demo",
		"CLAVESA_PIPELINE":         "demo",
		"CLAVESA_WATERMARKS":       "s3://bucket/demo/_watermarks/",
		// A local-only key the dispatcher must NOT forward.
		"CLAVESA_METASTORE_ADDR": "derby:1527",
	}
	perNode := map[string]string{
		"CLAVESA_NODE":          "trips",
		"CLAVESA_LANGUAGE":      "sql",
		"CLAVESA_LOGIC_S3_PATH": "s3://bucket/demo/logic/trips.sql",
	}
	awsArgs := []string{"-e", "AWS_ACCESS_KEY_ID=AKIA", "-v", "/home/u/.aws:/root/.aws:ro"}

	t.Setenv("CLAVESA_JVM_HEAP_MB", "8192")

	args := cloudLocalDockerArgs("clavesa/ws/transform-runner:latest", env, perNode, awsArgs)
	joined := strings.Join(args, " ")

	// CLAVESA_RUN=1 sentinel present.
	if !contains(args, "CLAVESA_RUN=1") {
		t.Errorf("missing CLAVESA_RUN=1; args=%v", args)
	}
	// s3 warehouse env mirrored verbatim.
	for _, want := range []string{
		"CLAVESA_WAREHOUSE=s3://bucket/_warehouse/",
		"CLAVESA_SYSTEM_WAREHOUSE=s3://bucket/_workspace/_warehouse/",
		"CLAVESA_CATALOG=clavesa_demo",
		"CLAVESA_SCHEMA=trips",
		"CLAVESA_SYSTEM_CATALOG=clavesa_demo",
		"CLAVESA_PIPELINE=demo",
		"CLAVESA_WATERMARKS=s3://bucket/demo/_watermarks/",
	} {
		if !contains(args, want) {
			t.Errorf("missing warehouse env %q; args=%v", want, args)
		}
	}
	// Per-node env present.
	for _, want := range []string{
		"CLAVESA_NODE=trips",
		"CLAVESA_LANGUAGE=sql",
		"CLAVESA_LOGIC_S3_PATH=s3://bucket/demo/logic/trips.sql",
	} {
		if !contains(args, want) {
			t.Errorf("missing per-node env %q; args=%v", want, args)
		}
	}
	// JVM heap forwarded because it's set in the host env.
	if !contains(args, "CLAVESA_JVM_HEAP_MB=8192") {
		t.Errorf("expected CLAVESA_JVM_HEAP_MB forwarded; args=%v", args)
	}
	// Module version override present.
	if !contains(args, "CLAVESA_MODULE_VERSION="+ModuleVersion) {
		t.Errorf("expected CLAVESA_MODULE_VERSION=%s; args=%v", ModuleVersion, args)
	}
	// AWS args appended.
	if !contains(args, "AWS_ACCESS_KEY_ID=AKIA") {
		t.Errorf("expected AWS args appended; args=%v", args)
	}
	// NO local-only machinery: no -v mounts beyond the AWS one, no metastore.
	if strings.Contains(joined, "CLAVESA_METASTORE_ADDR") {
		t.Errorf("must not forward local metastore env; args=%v", args)
	}
	// The only -v allowed is the one inside awsArgs (the ~/.aws mount); the
	// dispatcher itself adds none. Count -v occurrences = exactly the awsArgs one.
	if got := countFlag(args, "-v"); got != 1 {
		t.Errorf("expected exactly 1 -v (from awsArgs), got %d; args=%v", got, args)
	}
	// Image is the last arg.
	if args[len(args)-1] != "clavesa/ws/transform-runner:latest" {
		t.Errorf("expected image as last arg, got %q", args[len(args)-1])
	}
}

func TestCloudLocalDockerArgsHeapAbsentWhenUnset(t *testing.T) {
	// With the heap env genuinely unset, no override arg is emitted. Use
	// os.Unsetenv (t.Cleanup restores) since t.Setenv only ever *sets*.
	prev, had := os.LookupEnv("CLAVESA_JVM_HEAP_MB")
	_ = os.Unsetenv("CLAVESA_JVM_HEAP_MB")
	t.Cleanup(func() {
		if had {
			_ = os.Setenv("CLAVESA_JVM_HEAP_MB", prev)
		}
	})
	args := cloudLocalDockerArgs("img", map[string]string{}, map[string]string{}, nil)
	for _, a := range args {
		if strings.HasPrefix(a, "CLAVESA_JVM_HEAP_MB=") {
			t.Errorf("did not expect a heap override arg when env unset; got %q", a)
		}
	}
}

// stage dispatch routing + the skipped-window regression pin. A stubbed
// dispatcher stands in for the docker run; we assert Compute=="local"
// routes through it and that a non-ok runner status still fails stage.
func TestStageLocalDispatchRouting(t *testing.T) {
	t.Run("Compute=local routes through cloudLocalDispatch", func(t *testing.T) {
		called := false
		s := New(t.TempDir())
		s.cloudLocalDispatch = func(ctx context.Context, env, perNodeEnv map[string]string, event any, onEvent func(map[string]any)) (*cloudLocalResult, error) {
			called = true
			// Per-node env must carry the stage node identity.
			if perNodeEnv["CLAVESA_NODE"] != "trips" {
				t.Errorf("expected CLAVESA_NODE=trips, got %q", perNodeEnv["CLAVESA_NODE"])
			}
			return &cloudLocalResult{RawResponse: []byte(`{"status":"ok"}`)}, nil
		}
		run := &BackfillRun{Node: "trips", Compute: "local"}
		payload, err := s.stageLocalDispatch(context.Background(), run, map[string]string{}, "trips", "sql", "s3://x/logic.sql", map[string]any{})
		if err != nil {
			t.Fatalf("stageLocalDispatch: %v", err)
		}
		if !called {
			t.Fatal("cloudLocalDispatch was not called")
		}
		// The post-dispatch non-ok check (shared with the Lambda path) must
		// treat this ok envelope as success.
		if status := runnerResponseStatus(payload); status != "" && status != "ok" {
			t.Fatalf("expected ok status, got %q", status)
		}
	})

	t.Run("non-ok runner status still fails stage (c8f55f2 regression)", func(t *testing.T) {
		s := New(t.TempDir())
		s.cloudLocalDispatch = func(ctx context.Context, env, perNodeEnv map[string]string, event any, onEvent func(map[string]any)) (*cloudLocalResult, error) {
			// Runner ran but skipped the window (no new partitions) — the
			// shape that once reported success without writing compute.
			return &cloudLocalResult{RawResponse: []byte(`{"status":"skipped","reason":"no partitions in window"}`)}, nil
		}
		run := &BackfillRun{Node: "trips", Compute: "local"}
		payload, err := s.stageLocalDispatch(context.Background(), run, map[string]string{}, "trips", "sql", "s3://x/logic.sql", map[string]any{})
		if err != nil {
			t.Fatalf("dispatch itself should not error (the status check does): %v", err)
		}
		status := runnerResponseStatus(payload)
		if status == "" || status == "ok" {
			t.Fatalf("expected a non-ok status to fail stage, got %q", status)
		}
		if msg := runnerResponseMessage(payload); !strings.Contains(msg, "no partitions") {
			t.Errorf("expected the skip reason surfaced, got %q", msg)
		}
	})

	t.Run("dispatch error stamps failed status on the run", func(t *testing.T) {
		s := New(t.TempDir())
		s.cloudLocalDispatch = func(ctx context.Context, env, perNodeEnv map[string]string, event any, onEvent func(map[string]any)) (*cloudLocalResult, error) {
			return nil, fmt.Errorf("docker absent")
		}
		run := &BackfillRun{Node: "trips", Compute: "local"}
		_, err := s.stageLocalDispatch(context.Background(), run, map[string]string{}, "trips", "sql", "x", map[string]any{})
		if err == nil {
			t.Fatal("expected dispatch error")
		}
		if run.Status != "failed" {
			t.Errorf("expected run.Status=failed, got %q", run.Status)
		}
	})
}

// operationLocalDispatch is the shared promote/discard local routing. A
// stubbed dispatcher stands in for the docker run; we assert the `_operation`
// payload routes through it with NO per-node env, that the raw runner response
// flows back so the caller's status check still fires (the c8f55f2 regression
// class on the operation path), and that a dispatch error propagates.
func TestOperationLocalDispatchRouting(t *testing.T) {
	t.Run("routes through cloudLocalDispatch with no per-node env", func(t *testing.T) {
		called := false
		s := New(t.TempDir())
		s.cloudLocalDispatch = func(ctx context.Context, env, perNodeEnv map[string]string, event any, onEvent func(map[string]any)) (*cloudLocalResult, error) {
			called = true
			// Operations route to _run_operation before any node logic — the
			// dispatcher must carry no per-node env.
			if perNodeEnv != nil {
				t.Errorf("expected nil perNodeEnv for an operation, got %v", perNodeEnv)
			}
			// The Lambda env (resolved via the fake getter) must reach the
			// dispatcher so the container targets the cloud warehouse.
			if env["CLAVESA_WAREHOUSE"] != "s3://bucket/_warehouse/" {
				t.Errorf("expected warehouse env forwarded, got %q", env["CLAVESA_WAREHOUSE"])
			}
			return &cloudLocalResult{RawResponse: []byte(`{"status":"ok","columns_added":["x"]}`)}, nil
		}
		fake := fakeLambdaEnv{vars: map[string]string{"CLAVESA_WAREHOUSE": "s3://bucket/_warehouse/"}}
		payload := map[string]any{"_operation": "backfill_promote", "staging": "db.t__backfill__abc"}
		raw, err := s.operationLocalDispatch(context.Background(), fake, "fn", payload)
		if err != nil {
			t.Fatalf("operationLocalDispatch: %v", err)
		}
		if !called {
			t.Fatal("cloudLocalDispatch was not called")
		}
		if status := runnerResponseStatus(raw); status != "ok" {
			t.Fatalf("expected ok status, got %q", status)
		}
	})

	t.Run("non-ok runner status surfaces in the raw response", func(t *testing.T) {
		s := New(t.TempDir())
		s.cloudLocalDispatch = func(ctx context.Context, env, perNodeEnv map[string]string, event any, onEvent func(map[string]any)) (*cloudLocalResult, error) {
			// MERGE/DROP failed inside the runner.
			return &cloudLocalResult{RawResponse: []byte(`{"status":"failed","reason":"target locked"}`)}, nil
		}
		raw, err := s.operationLocalDispatch(context.Background(), fakeLambdaEnv{vars: map[string]string{}}, "fn", map[string]any{})
		if err != nil {
			t.Fatalf("dispatch itself should not error (the status check does): %v", err)
		}
		// The caller (BackfillPromote/Discard) runs this exact check.
		status := runnerResponseStatus(raw)
		if status == "" || status == "ok" {
			t.Fatalf("expected a non-ok status, got %q", status)
		}
		if msg := runnerResponseMessage(raw); !strings.Contains(msg, "target locked") {
			t.Errorf("expected the failure reason surfaced, got %q", msg)
		}
	})

	t.Run("dispatch error propagates", func(t *testing.T) {
		s := New(t.TempDir())
		s.cloudLocalDispatch = func(ctx context.Context, env, perNodeEnv map[string]string, event any, onEvent func(map[string]any)) (*cloudLocalResult, error) {
			return nil, fmt.Errorf("docker absent")
		}
		_, err := s.operationLocalDispatch(context.Background(), fakeLambdaEnv{vars: map[string]string{}}, "fn", map[string]any{})
		if err == nil {
			t.Fatal("expected dispatch error to propagate")
		}
	})

	t.Run("lambda env resolution error propagates", func(t *testing.T) {
		s := New(t.TempDir())
		_, err := s.operationLocalDispatch(context.Background(), fakeLambdaEnv{err: fmt.Errorf("no such function")}, "fn", map[string]any{})
		if err == nil {
			t.Fatal("expected env resolution error to propagate")
		}
	})
}

// Glue compute-tag round-trip: a run's Compute survives set→params→read,
// and an absent tag (old tables) reconstructs as "lambda".
func TestBackfillComputeTagRoundTrip(t *testing.T) {
	t.Run("local compute survives the round-trip", func(t *testing.T) {
		run := &BackfillRun{
			RunID:          "abc123",
			Node:           "trips",
			OutputKey:      "default",
			From:           []string{"2026", "04", "26"},
			To:             []string{"2026", "04", "27"},
			CanonicalTable: "clavesa_demo__trips.trips",
			Compute:        "local",
		}
		params := map[string]string{}
		setBackfillGlueParams(params, run)
		if params[glueTagBackfillCompute] != "local" {
			t.Fatalf("expected compute tag=local, got %q", params[glueTagBackfillCompute])
		}
		got := backfillRunFromGlueParams(params)
		if got.Compute != "local" {
			t.Errorf("round-trip compute = %q, want local", got.Compute)
		}
		if got.RunID != "abc123" || got.Node != "trips" || got.CanonicalTable != "clavesa_demo__trips.trips" {
			t.Errorf("round-trip lost identity: %+v", got)
		}
		if joinCursor(got.From) != "2026/04/26" || joinCursor(got.To) != "2026/04/27" {
			t.Errorf("round-trip lost window: from=%v to=%v", got.From, got.To)
		}
	})

	t.Run("lambda compute survives the round-trip", func(t *testing.T) {
		params := map[string]string{}
		setBackfillGlueParams(params, &BackfillRun{RunID: "x", Compute: "lambda"})
		if got := backfillRunFromGlueParams(params).Compute; got != "lambda" {
			t.Errorf("compute = %q, want lambda", got)
		}
	})

	t.Run("absent compute tag reconstructs as lambda", func(t *testing.T) {
		// An older staging table: tags set without ever writing the compute
		// key (run.Compute == "" → setBackfillGlueParams skips it).
		params := map[string]string{}
		setBackfillGlueParams(params, &BackfillRun{RunID: "old"})
		if _, present := params[glueTagBackfillCompute]; present {
			t.Fatal("expected the compute tag to be absent for an empty-compute run")
		}
		if got := backfillRunFromGlueParams(params).Compute; got != "lambda" {
			t.Errorf("absent tag → compute = %q, want lambda", got)
		}
	})
}

func contains(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func countFlag(args []string, flag string) int {
	n := 0
	for _, a := range args {
		if a == flag {
			n++
		}
	}
	return n
}
