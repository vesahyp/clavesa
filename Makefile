.PHONY: dev build build-bin build-ui build-runner push-runner sync-runner sync-modules test test-go test-cli test-runner test-runner-py release-check release-public validate-examples

RUNNER_IMAGE   ?= clavesa/transform-runner
RUNNER_VERSION ?= $(shell grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' internal/version/version.go | head -1)

dev: ## Start backend (:8080) and frontend (:5173)
	@./scripts/dev.sh

sync-runner: ## Copy runner/ → internal/runner/files/ so the embedded copy used by `clavesa workspace init` stays in sync
	cp runner/Dockerfile runner/runner.py runner/spark_conf.py runner/notebook_supervisor.py runner/notebook_repl.py runner/requirements.txt runner/entrypoint.sh runner/spark-class runner/download_jars.sh internal/runner/files/

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

build-bin: sync-modules ## Build binary only → bin/clavesa (embeds modules)
	go build -o bin/clavesa ./cmd/clavesa

ui/node_modules: ui/package.json ui/package-lock.json
	cd ui && npm install
	@touch ui/node_modules

build-ui: ui/node_modules ## Build the React frontend → internal/ui/dist/
	cd ui && npm run build
	@touch internal/ui/dist/.gitkeep

build: build-ui sync-modules ## Build everything → bin/clavesa (UI + modules embedded)
	go build -o bin/clavesa ./cmd/clavesa

test: test-go test-runner-py test-cli ## Run all tests

test-go: ## Go unit tests (fast, no binary)
	go test -v ./...

test-cli: build-bin ## Build binary + CLI pipeline cycle (Go-driven)
	go test -v -tags integration ./tests/cli/...

test-runner: ## Docker-gated runner integration tests (preview + handler)
	go test -v -tags integration ./tests/runner/...

test-runner-py: ## Pure-Python runner unit tests (stdlib only, no docker, no Spark)
	@for f in tests/runner/test_*.py; do \
	  echo "→ $$f"; \
	  python3 $$f || exit $$?; \
	done

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

release-check: validate-examples ## Pre-tag guard: confirm CHANGELOG entry, examples validate, tag not yet created
	@VERSION=$$(grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' internal/version/version.go | head -1); \
	 echo "→ ModuleVersion: $$VERSION"; \
	 grep -qE "^## \[$$VERSION\]" CHANGELOG.md || { \
	   echo "✗ CHANGELOG.md is missing a section for $$VERSION."; \
	   echo "  Add: ## [$$VERSION] — $$(date +%Y-%m-%d)"; \
	   exit 1; \
	 }; \
	 echo "✓ CHANGELOG.md has an entry for $$VERSION"; \
	 if git tag --list | grep -qx "$$VERSION"; then \
	   echo "✗ git tag $$VERSION already exists locally — bump ModuleVersion or delete the stale tag."; \
	   exit 1; \
	 fi; \
	 echo "✓ tag $$VERSION not yet created (next step: git tag -a $$VERSION ...)"; \
	 echo "release-check passed for $$VERSION."

release-public: release-check ## Snapshot the dev tree into the public clavesa repo, commit + tag, then prompt before push
	@./scripts/release-public.sh

help: ## Show available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-16s %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
