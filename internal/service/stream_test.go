package service

import (
	"slices"
	"testing"
)

func TestStreamTableID(t *testing.T) {
	if got := StreamTableID("my-ws", "", "click-events"); got != "my_ws__streams.click_events" {
		t.Fatalf("got %q", got)
	}
	if got := StreamTableID("", "raw", "c"); got != "clavesa__raw.c" {
		t.Fatalf("got %q", got)
	}
}

func TestStreamConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		req  StreamRunRequest
		ok   bool
	}{
		{"kinesis ok", StreamRunRequest{Kind: "kinesis", StreamName: "s", EndpointURL: "http://x"}, true},
		{"kinesis needs endpoint", StreamRunRequest{Kind: "kinesis", StreamName: "s"}, false},
		{"kafka ok", StreamRunRequest{Kind: "kafka", BootstrapServers: "k:9092", Topic: "t"}, true},
		{"kafka needs topic", StreamRunRequest{Kind: "kafka", BootstrapServers: "k:9092"}, false},
		{"unknown kind", StreamRunRequest{Kind: "sqs"}, false},
		{"recover needs drain", StreamRunRequest{Kind: "kafka", BootstrapServers: "k", Topic: "t", Recover: true}, false},
	}
	for _, c := range cases {
		_, err := streamConfig(c.req, "t")
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v", c.name, err)
		}
	}
}

func TestStreamConfigCarriesOptions(t *testing.T) {
	cfg, err := streamConfig(StreamRunRequest{
		Name: "a-b", Kind: "kafka", BootstrapServers: "k", Topic: "t",
		RecordKey: []string{"id"}, TriggerSeconds: 5, Drain: true, OptimizeEverySeconds: 0,
	}, "c__s.a_b")
	if err != nil {
		t.Fatal(err)
	}
	if cfg["name"] != "a_b" || cfg["table"] != "c__s.a_b" || cfg["trigger_seconds"] != 5 || cfg["drain"] != true {
		t.Fatalf("cfg = %v", cfg)
	}
	if k, _ := cfg["record_key"].([]string); !slices.Equal(k, []string{"id"}) {
		t.Fatalf("record_key = %v", cfg["record_key"])
	}
}

func TestDockerRunArgsStreamMode(t *testing.T) {
	args := dockerRunArgs(dockerRunSpec{Image: "img", Stream: true, Name: "clavesa-stream-x", Warehouse: "/wh"})
	if !slices.Contains(args, "CLAVESA_STREAM=1") || slices.Contains(args, "CLAVESA_RUN=1") {
		t.Fatalf("mode env wrong: %v", args)
	}
	i := slices.Index(args, "--name")
	if i < 0 || args[i+1] != "clavesa-stream-x" {
		t.Fatalf("--name missing: %v", args)
	}
	if args[len(args)-1] != "img" {
		t.Fatalf("image not last: %v", args)
	}
}

func TestStreamContainerName(t *testing.T) {
	if got := StreamContainerName("my_ws", "k_1"); got != "clavesa-stream-my-ws-k-1" {
		t.Fatalf("got %q", got)
	}
}
