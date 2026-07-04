package service

import (
	"reflect"
	"testing"
)

// TestDockerRunArgs pins the argv the shared builder produces for the
// shapes the four run.go call sites use, so a builder change that drops or
// reorders a load-bearing flag fails here instead of at `docker run`. The
// image refs are deliberately non-existent so dockerImageDigest degrades
// to "" and the argv stays deterministic with or without a docker daemon.
func TestDockerRunArgs(t *testing.T) {
	// Deterministic heap: an explicit CLAVESA_JVM_HEAP_MB always wins over
	// the docker-info probe.
	t.Setenv("CLAVESA_JVM_HEAP_MB", "2048")

	cases := []struct {
		name string
		spec dockerRunSpec
		want []string
	}{
		{
			// runTransform / runPipelineBundle shape: metastore + site env
			// + AWS extra args + heap + rw and ro mounts.
			name: "transform shape",
			spec: dockerRunSpec{
				Image:            "clavesa-test/absent-runner:none",
				Warehouse:        "/ws/.clavesa/warehouse",
				MetastoreNetwork: "clavesa-metastore-net",
				MetastoreAddr:    "metastore:1527",
				Env: []string{
					"CLAVESA_PIPELINE=demo",
					"CLAVESA_NODE=trips",
					"CLAVESA_CATALOG=clavesa_demo",
					"CLAVESA_SCHEMA=demo",
					"CLAVESA_SYSTEM_CATALOG=clavesa_demo",
					"MY_TOKEN=s3cret",
				},
				ExtraArgs: []string{"-e", "AWS_REGION=eu-north-1", "-v", "/home/u/.aws:/root/.aws:ro"},
				Heap:      true,
				Mounts: []dockerMount{
					{Host: "/tmp/workdir"},
					{Host: "/pipe/.clavesa/watermarks"},
					{Host: "/ws/.clavesa/credentials", RO: true},
					{Host: "/data/inputs", RO: true},
				},
			},
			want: []string{
				"run", "--rm", "-i",
				"-e", "CLAVESA_RUN=1",
				"--network", "clavesa-metastore-net",
				"-e", "CLAVESA_METASTORE_ADDR=metastore:1527",
				"-e", "CLAVESA_WAREHOUSE=/ws/.clavesa/warehouse",
				"-e", "CLAVESA_PIPELINE=demo",
				"-e", "CLAVESA_NODE=trips",
				"-e", "CLAVESA_CATALOG=clavesa_demo",
				"-e", "CLAVESA_SCHEMA=demo",
				"-e", "CLAVESA_SYSTEM_CATALOG=clavesa_demo",
				"-e", "MY_TOKEN=s3cret",
				"-e", "AWS_REGION=eu-north-1",
				"-v", "/home/u/.aws:/root/.aws:ro",
				"-e", "CLAVESA_MODULE_VERSION=" + ModuleVersion,
				"-e", "CLAVESA_JVM_HEAP_MB=2048",
				"-v", "/ws/.clavesa/warehouse:/ws/.clavesa/warehouse",
				"-v", "/tmp/workdir:/tmp/workdir",
				"-v", "/pipe/.clavesa/watermarks:/pipe/.clavesa/watermarks",
				"-v", "/ws/.clavesa/credentials:/ws/.clavesa/credentials:ro",
				"-v", "/data/inputs:/data/inputs:ro",
				"clavesa-test/absent-runner:none",
			},
		},
		{
			// recordLocalRun shape: record mode, no metastore (Ensure
			// failed → embedded-Derby fallback), no heap, warehouse only.
			name: "record-run shape",
			spec: dockerRunSpec{
				Image:     "clavesa-test/absent-runner:none",
				RecordRun: true,
				Warehouse: "/ws/.clavesa/warehouse",
				Env: []string{
					"CLAVESA_PIPELINE=demo",
					"CLAVESA_CATALOG=",
					"CLAVESA_SCHEMA=demo",
					"CLAVESA_SYSTEM_CATALOG=",
				},
			},
			want: []string{
				"run", "--rm", "-i",
				"-e", "CLAVESA_RECORD_RUN=1",
				"-e", "CLAVESA_WAREHOUSE=/ws/.clavesa/warehouse",
				"-e", "CLAVESA_PIPELINE=demo",
				"-e", "CLAVESA_CATALOG=",
				"-e", "CLAVESA_SCHEMA=demo",
				"-e", "CLAVESA_SYSTEM_CATALOG=",
				"-e", "CLAVESA_MODULE_VERSION=" + ModuleVersion,
				"-v", "/ws/.clavesa/warehouse:/ws/.clavesa/warehouse",
				"clavesa-test/absent-runner:none",
			},
		},
		{
			// runOperation shape: bare control-plane invocation — no
			// pipeline env at all, heap for OPTIMIZE/VACUUM.
			name: "operation shape",
			spec: dockerRunSpec{
				Image:     "clavesa-test/absent-runner:none",
				Warehouse: "/ws/.clavesa/warehouse",
				Heap:      true,
			},
			want: []string{
				"run", "--rm", "-i",
				"-e", "CLAVESA_RUN=1",
				"-e", "CLAVESA_WAREHOUSE=/ws/.clavesa/warehouse",
				"-e", "CLAVESA_MODULE_VERSION=" + ModuleVersion,
				"-e", "CLAVESA_JVM_HEAP_MB=2048",
				"-v", "/ws/.clavesa/warehouse:/ws/.clavesa/warehouse",
				"clavesa-test/absent-runner:none",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dockerRunArgs(tc.spec)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("dockerRunArgs mismatch\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}
