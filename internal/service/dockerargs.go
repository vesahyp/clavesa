package service

// dockerargs.go holds the single docker-run argv builder every local
// runner-container invocation in this package shares (2026-07-02 session C
// P2-1: the env block, metastore wiring, module-version/digest triage
// stamps, heap sizing, and mounts had drifted across four hand-rolled
// copies). runTransform, runPipelineBundle, recordLocalRun, and
// runOperation populate a dockerRunSpec; only cloudlocal.go keeps its own
// builder, deliberately, for the different cloud-warehouse contract (see
// cloudLocalDockerArgs' header comment).

// dockerMount is one -v mount in a dockerRunSpec. Container defaults to
// Host (the same-path-inside-and-outside convention local runs rely on so
// table locations remain stable); set it only for foreign targets like the
// ~/.aws → /root/.aws forward.
type dockerMount struct {
	Host      string
	Container string
	RO        bool
}

// dockerRunSpec describes one local runner-container invocation.
//
// The builder owns everything every site shares: the run-mode env, the
// shared-Derby-metastore wiring, the warehouse env + mount, the
// CLAVESA_MODULE_VERSION / CLAVESA_RUNNER_IMAGE_DIGEST triage stamps, and
// the Spark heap sizing. Site-specific env rides in Env (ordered
// "KEY=VALUE" strings) and site-specific mounts in Mounts; pre-formed
// docker args (runner.AWSEnvDockerArgs) ride in ExtraArgs.
type dockerRunSpec struct {
	// Image is the runner image ref, emitted last. Also the digest-lookup
	// key for the CLAVESA_RUNNER_IMAGE_DIGEST stamp (blank-degrades when
	// the image isn't inspectable).
	Image string
	// RecordRun selects CLAVESA_RECORD_RUN=1 (the runs-row writer) instead
	// of CLAVESA_RUN=1.
	RecordRun bool
	// Warehouse is the local Hadoop-catalog warehouse dir; exported as
	// CLAVESA_WAREHOUSE and mounted read-write at the same path.
	Warehouse string
	// MetastoreNetwork/MetastoreAddr point the container's Spark at the
	// shared Derby Network Server (appendMetastoreArgs semantics: both
	// empty → no-op, container falls back to embedded Derby).
	MetastoreNetwork string
	MetastoreAddr    string
	// Env is the site-specific "KEY=VALUE" env block, kept in the caller's
	// order (CLAVESA_PIPELINE, CLAVESA_CATALOG trio, credential
	// passthrough, …).
	Env []string
	// ExtraArgs are pre-formed docker args appended verbatim after Env —
	// the runner.AWSEnvDockerArgs -e/-v vector.
	ExtraArgs []string
	// Heap adds localHeapArgs sizing (GH #58). The runs-row writer leaves
	// it off — a single-row append doesn't need a sized JVM.
	Heap bool
	// Mounts are the site-specific -v mounts beyond the warehouse
	// (workdir, watermarks, credentials dir, input roots).
	Mounts []dockerMount
}

// dockerRunArgs assembles the `docker run` argv for a local runner
// invocation. Flag order within the argv is not load-bearing for docker;
// the canonical order here is mode, metastore, warehouse, site env, extra
// args, triage stamps, heap, warehouse mount, site mounts, image.
func dockerRunArgs(spec dockerRunSpec) []string {
	args := []string{"run", "--rm", "-i"}
	if spec.RecordRun {
		args = append(args, "-e", "CLAVESA_RECORD_RUN=1")
	} else {
		args = append(args, "-e", "CLAVESA_RUN=1")
	}
	args = appendMetastoreArgs(args, spec.MetastoreNetwork, spec.MetastoreAddr)
	args = append(args, "-e", "CLAVESA_WAREHOUSE="+spec.Warehouse)
	for _, kv := range spec.Env {
		args = append(args, "-e", kv)
	}
	args = append(args, spec.ExtraArgs...)
	// Override the version baked into the image: the cache-retag path in
	// workspace.EnsureLocalRunnerImage can rebrand an image built at a
	// different version, so node_runs.module_version must reflect the CLI
	// that orchestrated the run. The digest comes from `docker image
	// inspect` and changes on every runner rebuild; failures are non-fatal
	// (blank degrades to the older leave-the-column-empty behavior).
	args = append(args, "-e", "CLAVESA_MODULE_VERSION="+ModuleVersion)
	if digest := dockerImageDigest(spec.Image); digest != "" {
		args = append(args, "-e", "CLAVESA_RUNNER_IMAGE_DIGEST="+digest)
	}
	if spec.Heap {
		// Size the Spark JVM heap from the Docker VM (GH #58) — an
		// uncapped local container otherwise leaves spark-class on its
		// 1 GB fallback. Reserve for the co-resident metastore container.
		args = append(args, localHeapArgs(reserveSharedVMMB)...)
	}
	args = append(args, "-v", spec.Warehouse+":"+spec.Warehouse)
	for _, m := range spec.Mounts {
		target := m.Container
		if target == "" {
			target = m.Host
		}
		mnt := m.Host + ":" + target
		if m.RO {
			mnt += ":ro"
		}
		args = append(args, "-v", mnt)
	}
	args = append(args, spec.Image)
	return args
}
