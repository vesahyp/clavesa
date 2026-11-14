.PHONY: setup dev build build-bin build-ui build-runner push-runner sync-runner sync-modules test test-go test-cli test-runner test-runner-py smoke-cloud smoke-cloud-setup verify-readme verify-cookbook release-gates release-check release-public sync-public validate-examples docs-cli shadow-compare test-stream smoke-kinesis

RUNNER_IMAGE   ?= clavesa/transform-runner
RUNNER_VERSION ?= $(shell grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' internal/version/version.go | head -1)

# What a fresh clone needs before the other targets (and, in clavesa-dev,
# the nightly site/analytics job) work. Safe to rerun: each step runs only when its
# marker is missing. bin/clavesa is git-ignored and the nightly job puts it
# first on PATH, so a clone without it runs the Homebrew clavesa instead.
setup: ## After a fresh clone: mise tools, Go modules, npm packages, bin/clavesa, site infra init
	mise install
	go mod download
	@test -d ui/node_modules || $(MAKE) ui/node_modules
	@test -x bin/clavesa || $(MAKE) build
	@$(MAKE) -s .venv-tools
	@# site/ is private: the public clavesa snapshot has no site/.
	@if [ -d site ]; then \
	  test -d site/web/node_modules || (cd site/web && npm ci --no-fund --no-audit); \
	  $(MAKE) -s -C site infra/.terraform; \
	fi

# The Python tools verify-cookbook needs (moto's local S3, boto3, pyarrow),
# in a git-ignored venv of this repo rather than the machine's Python: a
# fresh Mac has none of them, and Homebrew's Python refuses global pip
# installs.
.venv-tools:
	python3 -m venv .venv-tools
	.venv-tools/bin/pip install --quiet --disable-pip-version-check 'moto[server]' boto3 pyarrow

dev: ## Start backend (:8080) and frontend (:5173)
	@./scripts/dev.sh

sync-runner: ## Copy runner/ → internal/runner/files/ so the embedded copy used by `clavesa workspace init` stays in sync
	cp runner/Dockerfile runner/runner.py runner/run_lock.py runner/stream_query.py runner/spark_conf.py runner/unity_catalog.py runner/bundle_task.py runner/notebook_supervisor.py runner/notebook_repl.py runner/requirements.txt runner/extra-requirements.txt runner/entrypoint.sh runner/spark-class runner/download_jars.sh internal/runner/files/
	mkdir -p internal/runner/files/patches && cp runner/patches/*.patch internal/runner/files/patches/

sync-modules: ## Copy modules/ → internal/modules/files/ (tracked .tf/.py/.md/.hcl files only) so `terraform init` resolves embedded modules locally
	@rm -rf internal/modules/files
	@mkdir -p internal/modules/files
	@git ls-files modules/ | while IFS= read -r f; do \
	  dst="internal/modules/files/$${f#modules/}"; \
	  mkdir -p "$$(dirname "$$dst")"; \
	  cp "$$f" "$$dst"; \
	done
	@echo "synced $$(find internal/modules/files -type f | wc -l | tr -d ' ') files"

build-runner: sync-runner ## Build the transform runner container image locally
	docker build --build-arg CLAVESA_MODULE_VERSION=$(RUNNER_VERSION) \
		-t $(RUNNER_IMAGE):$(RUNNER_VERSION) -t $(RUNNER_IMAGE):latest runner/

push-runner: ## Push runner image to your ECR (requires ECR_REPO=<account>.dkr.ecr.<region>.amazonaws.com/<name>)
	@test -n "$(ECR_REPO)" || (echo "Error: set ECR_REPO=<account>.dkr.ecr.<region>.amazonaws.com/<name>" && exit 1)
	aws ecr get-login-password --region $(shell echo $(ECR_REPO) | cut -d. -f4) \
	  | docker login --username AWS --password-stdin $(shell echo $(ECR_REPO) | cut -d/ -f1)
	docker tag $(RUNNER_IMAGE):$(RUNNER_VERSION) $(ECR_REPO):$(RUNNER_VERSION)
	docker tag $(RUNNER_IMAGE):$(RUNNER_VERSION) $(ECR_REPO):latest
	docker push $(ECR_REPO):$(RUNNER_VERSION)
	docker push $(ECR_REPO):latest

build-bin: sync-modules sync-runner ## Build binary only → bin/clavesa (embeds modules + runner files)
	go build -o bin/clavesa ./cmd/clavesa

ui/node_modules: ui/package.json ui/package-lock.json
	cd ui && npm install
	@touch ui/node_modules

build-ui: ui/node_modules ## Build the React frontend → internal/ui/dist/
	cd ui && npm run build
	@touch internal/ui/dist/.gitkeep

build: build-ui sync-modules sync-runner ## Build everything → bin/clavesa (UI + modules + runner files embedded)
	go build -o bin/clavesa ./cmd/clavesa

# SKIP_BUILD is INTERNAL to release-gates, which runs `make build` once and
# then launches the gates with SKIP_BUILD=1 so three concurrent sub-makes
# don't race each other rewriting bin/clavesa. NEVER set it manually — the
# build dep is what keeps every gate off a stale binary.
test: test-go test-runner-py test-cli test-runner ## Run all tests (incl. docker-gated runner suite — GH #27)
	@./scripts/green-stamp.sh write test

test-go: ## Go unit tests (fast, no binary)
	go test -v ./...

# -count=1 on the integration suites: they exercise bin/clavesa and the
# runner image — external artifacts Go's test cache can't see — so a
# cached PASS can predate the binary it claims to verify.
test-cli: $(if $(SKIP_BUILD),,build-bin) ## Build binary + CLI pipeline cycle (Go-driven)
	go test -v -count=1 -timeout 30m -tags integration ./tests/cli/...

test-runner: ## Docker-gated runner integration tests (preview + handler)
	@docker info >/dev/null 2>&1 || { \
	  echo "✗ docker is required for test-runner (GH #27 — it is part of the release gate);"; \
	  echo "  start Docker or run the individual suites (test-go / test-runner-py / test-cli)."; \
	  exit 1; \
	}
	go test -v -count=1 -timeout 30m -tags integration ./tests/runner/...

test-runner-py: ## Pure-Python runner unit tests (stdlib only, no docker, no Spark)
	@for f in tests/runner/test_*.py; do \
	  echo "→ $$f"; \
	  python3 $$f || exit $$?; \
	done

docs-cli: ## Regenerate the CLI reference (docs/reference/cli/) from the Cobra tree; test-go fails if it drifts
	go run ./cmd/docsgen docs/reference/cli

validate-examples: ## terraform validate every modules/*/aws/examples/* (catches example drift before tagging)
	@set -e; \
	 status=0; \
	 for ex in modules/*/aws/examples/*/; do \
	   echo "→ $$ex"; \
	   ( cd "$$ex" && \
	     rm -rf .terraform .terraform.lock.hcl && \
	     terraform init -backend=false -input=false >/dev/null && \
	     terraform validate ) || status=$$?; \
	   rm -rf "$$ex/.terraform" "$$ex/.terraform.lock.hcl"; \
	 done; \
	 exit $$status

release-check: validate-examples ## Pre-tag guard: confirm CHANGELOG entry, cloud-smoke stamp, tree-exact test/verify-readme stamps, examples validate, tag not yet created
	@VERSION=$$(grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' internal/version/version.go | head -1); \
	 echo "→ ModuleVersion: $$VERSION"; \
	 grep -qE "^## \[$$VERSION\]" CHANGELOG.md || { \
	   echo "✗ CHANGELOG.md is missing a section for $$VERSION."; \
	   echo "  Add: ## [$$VERSION] — $$(date +%Y-%m-%d)"; \
	   exit 1; \
	 }; \
	 echo "✓ CHANGELOG.md has an entry for $$VERSION"; \
	 if [ "$$CLAVESA_SKIP_SMOKE" = "1" ]; then \
	   echo "##############################################################"; \
	   echo "## WARNING: CLAVESA_SKIP_SMOKE=1 — cloud smoke gate BYPASSED."; \
	   echo "## $$VERSION ships without cloud verification. Document why"; \
	   echo "## in the release commit message."; \
	   echo "##############################################################"; \
	 else \
	   if [ ! -f .cloud-smoke-green.json ]; then \
	     echo "✗ .cloud-smoke-green.json is missing — the cloud smoke gate has not passed."; \
	     echo "  run 'make smoke-cloud' (or CLAVESA_SKIP_SMOKE=1 to bypass — document why in the release commit)"; \
	     exit 1; \
	   fi; \
	   if command -v jq >/dev/null 2>&1; then \
	     STAMP=$$(jq -r .version .cloud-smoke-green.json); \
	   else \
	     STAMP=$$(grep -oE '"version"[[:space:]]*:[[:space:]]*"v[0-9]+\.[0-9]+\.[0-9]+"' .cloud-smoke-green.json | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+'); \
	   fi; \
	   if [ "$$STAMP" != "$$VERSION" ]; then \
	     echo "✗ cloud smoke stamp is for $$STAMP, but releasing $$VERSION."; \
	     echo "  run 'make smoke-cloud' (or CLAVESA_SKIP_SMOKE=1 to bypass — document why in the release commit)"; \
	     exit 1; \
	   fi; \
	   echo "✓ cloud smoke stamp matches $$VERSION ($$(grep -oE '"timestamp"[^,}]*' .cloud-smoke-green.json))"; \
	 fi; \
	 if [ "$$CLAVESA_SKIP_LOCAL_GATES" = "1" ]; then \
	   echo "##############################################################"; \
	   echo "## WARNING: CLAVESA_SKIP_LOCAL_GATES=1 — local gates BYPASSED."; \
	   echo "## $$VERSION ships without tree-exact test / verify-readme"; \
	   echo "## verification. Document why in the release commit message."; \
	   echo "##############################################################"; \
	 else \
	   for g in test verify-readme verify-cookbook; do \
	     ./scripts/green-stamp.sh check $$g || { \
	       echo "  run 'make release-gates' (or CLAVESA_SKIP_LOCAL_GATES=1 to bypass — document why in the release commit)"; \
	       exit 1; \
	     }; \
	   done; \
	 fi; \
	 if git tag --list | grep -qx "$$VERSION"; then \
	   echo "✗ git tag $$VERSION already exists locally — bump ModuleVersion or delete the stale tag."; \
	   exit 1; \
	 fi; \
	 echo "✓ tag $$VERSION not yet created (next step: git tag -a $$VERSION ...)"; \
	 echo "release-check passed for $$VERSION."

release-public: release-check ## Snapshot the dev tree into the public clavesa repo, commit + tag, then prompt before push
	@./scripts/release-public.sh

sync-public: ## Publish docs/source updates to the public repo on main, NO version bump or tag (cookbook / README / web-tracker). No build, no gates. Version-bearing changes use release-public.
	@./scripts/sync-public.sh

test-stream: $(if $(SKIP_BUILD),,build) ## Stream mode (ADR-028 slice 1) on this machine: LocalStack Kinesis + Kafka containers, exact row counts after kill -9 cycles, a shard split, producer duplicates and a checkpoint reset. Docker-gated, ~25 min, no AWS
	@./scripts/test-stream.sh

smoke-cloud: $(if $(SKIP_BUILD),,build) ## Per-release cloud gate: drive bin/clavesa against the deployed smoke workspace (SMOKE_WS / SMOKE_PIPELINE env-overridable)
	@./scripts/cloud-smoke.sh run

smoke-databricks: $(if $(SKIP_BUILD),,build) ## Unity Catalog gate (ADR-027): run a published pipeline on a connected, deployed workspace and assert the table, lineage and downstream Databricks run in Databricks (UC_SMOKE_WS / UC_SMOKE_PIPELINE)
	bash scripts/uc-smoke.sh

e2e-bundle: $(if $(SKIP_BUILD),,build) ## ADR-031 end-to-end: Databricks' default-sql example deployed as a Databricks job and as a clavesa job, tables compared, and a mixed run (E2E_WS= E2E_WAREHOUSE= E2E_SOURCE_BUCKET= E2E_PROFILE= [E2E_DEPLOY_WORKSPACE=1] [E2E_TEARDOWN=1]); cents of DBUs and AWS
	bash scripts/bundle-e2e.sh

smoke-kinesis: $(if $(SKIP_BUILD),,build) ## Kinesis gate (ADR-028): a 1-shard stream, a pipeline that reads it on Fargate then Lambda, counts and samples checked in Databricks, a run stopped mid-drain; deletes the stream on exit (KIN_SMOKE_* env, see the script). Under 1 USD
	bash scripts/kinesis-smoke.sh

smoke-databricks-shadow: $(if $(SKIP_BUILD),,build) ## Shadow-mode gate: preflight, a shadow run and a passing comparison on the test workspace (UC_SHADOW_* env, see the script); no DBUs
	bash scripts/uc-shadow-smoke.sh

# Shadow mode's daily check (docs/cookbook/replace-a-databricks-step.md).
# Runs against a deployed, connected workspace; exit status 1 on FAIL, so
# cron (or jeeves' cronrun) can alert on it. DAYS=n limits both tables to
# the last n days of PARTITION; WHERE=<predicate> instead for any filter.
SHADOW_CLAVESA ?= $(CURDIR)/bin/clavesa
shadow-compare: ## Shadow mode: compare the Databricks step's table with clavesa's per partition (WS= PIPELINE= EXISTING= SHADOW= PARTITION= SUMS=a,b [DAYS=7 | WHERE=...] [JSON=1]); exit 1 on fail
	@test -n "$(WS)" -a -n "$(PIPELINE)" -a -n "$(EXISTING)" -a -n "$(SHADOW)" -a -n "$(PARTITION)" || \
	  { echo "usage: make shadow-compare WS=<workspace dir> PIPELINE=<pipeline> EXISTING=<c.s.t> SHADOW=<c.s.t> PARTITION=<column> SUMS=<a,b> [DAYS=7 | WHERE=<predicate>] [JSON=1]"; exit 2; }
	cd "$(WS)" && "$(SHADOW_CLAVESA)" pipeline databricks-compare "$(PIPELINE)" \
	  --existing "$(EXISTING)" --shadow "$(SHADOW)" --partition "$(PARTITION)" \
	  $(if $(SUMS),--sum "$(SUMS)") \
	  $(if $(WHERE),--where "$(WHERE)",$(if $(DAYS),--where "$(PARTITION) >= date_sub(current_date(), $(DAYS))")) \
	  $(if $(JSON),--json)

screenshots-unity-catalog: ## Screenshot + assert the Unity Catalog page and a run's Databricks card (needs a running `clavesa ui` on a connected workspace; UC_RUN=<run id>, see the script)
	bash scripts/uc-screenshots.sh

verify-readme: $(if $(SKIP_BUILD),,build) ## Walk the README quick-start literally (UI via playwright-cli) and assert the mandatory pages (CLAVESA_VERIFY_ADDR to override the :8080 UI port)
	@./scripts/verify-readme.sh
	@./scripts/green-stamp.sh write verify-readme

verify-cookbook: $(if $(SKIP_BUILD),,build) .venv-tools ## Walk the cookbook recipes literally against a fresh tempdir workspace (local S3 via moto) and assert outcomes — release-gates / nightly, NOT per-commit (GH #86)
	@PATH="$(CURDIR)/.venv-tools/bin:$$PATH" ./scripts/verify-cookbook.sh
	@./scripts/green-stamp.sh write verify-cookbook

release-gates: build ## Run the four release gates (memory-gated serial/concurrent) + stamps; logs in .gates/, heartbeat in .gates/progress.log (GH #84)
	@./scripts/release-gates.sh
smoke-cloud-setup: build ## One-time: create + deploy the persistent cloud smoke workspace (SMOKE_WS / SMOKE_PROFILE env-overridable)
	@./scripts/cloud-smoke.sh setup

help: ## Show available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-16s %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
