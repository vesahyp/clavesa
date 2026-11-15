// Package wsdeps builds the fully-wired service stack for one workspace:
// the AWS clients, the observability resolver, the warm Spark query runner,
// the transpile sidecar and the service.Service on top. Both the CLI
// (every command, through cli.newServiceDeps) and the UI servers (the
// single-workspace `clavesa ui` and the multi-workspace router, ADR-032)
// build their workspaces here, so no surface carries its own recipe.
//
// Everything is lazy: no container spawns and no network calls happen
// until a caller uses a piece. Building Deps for a workspace that is never
// queried costs a few file reads.
package wsdeps

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/servingsql"
	wspkg "github.com/vesahyp/clavesa/internal/workspace"
)

// AWSClients bundles the AWS SDK clients (and the derived Athena output
// bucket) wired into the cloud observability provider and handed to the
// UI's HTTP handlers. All clients are nil when the AWS config can't load
// (local-only mode); Err carries the load failure for surfaces that want
// to explain it.
type AWSClients struct {
	Config aws.Config
	Err    error // non-nil ⇔ config load failed ⇔ every client below is nil
	S3     *s3.Client
	Athena *athena.Client
	Glue   *glue.Client
	SFN    *sfn.Client
	CWL    *cloudwatchlogs.Client
	// AthenaOutputBucket: explicit $ATHENA_OUTPUT_BUCKET wins; otherwise
	// auto-derived from the workspace's Terraform state
	// (`pipeline_bucket` output, the same bucket the Athena workgroup uses
	// for `athena-results/`). Empty in local-only or pre-deploy mode.
	AthenaOutputBucket string
	// AthenaWorkGroup is the hosted reader's workgroup when the workspace
	// is a hosted tree copy with deployed.json (ADR-032); empty otherwise.
	AthenaWorkGroup string
}

// LoadAWSClients resolves the AWS config once and constructs the client
// set. Cheap: the config load parses local config files (and, once per
// workspace that records no region, its Terraform state to derive one),
// and client construction opens no connections. The region is the
// workspace's own (wspkg.LoadAWSConfig, GH #107). profile, when set, selects that
// shared-config profile for these clients only (the multi-workspace UI
// reads each workspace with its own `.clavesa/aws-profile.json`); empty
// uses the default chain, AWS_PROFILE included.
func LoadAWSClients(ctx context.Context, workspaceRoot, profile string) AWSClients {
	bucket := os.Getenv("ATHENA_OUTPUT_BUCKET")
	if bucket == "" {
		bucket = wspkg.PipelineBucket(workspaceRoot)
	}
	c := AWSClients{AthenaOutputBucket: bucket, AthenaWorkGroup: wspkg.HostedAthenaWorkGroup(workspaceRoot)}
	// The workspace's own profile and recorded region (GH #107): the
	// multi-workspace UI reads each workspace in its own region.
	var extra []func(*awsconfig.LoadOptions) error
	if profile != "" {
		extra = append(extra, awsconfig.WithSharedConfigProfile(profile))
	}
	cfg, err := wspkg.LoadAWSConfig(ctx, workspaceRoot, extra...)
	if err != nil {
		c.Err = err
		return c
	}
	c.Config = cfg
	c.S3 = s3.NewFromConfig(cfg)
	c.Athena = athena.NewFromConfig(cfg)
	c.Glue = glue.NewFromConfig(cfg)
	c.SFN = sfn.NewFromConfig(cfg)
	c.CWL = cloudwatchlogs.NewFromConfig(cfg)
	return c
}

// CloudProvider builds the full-client cloud observability provider, or
// nil when AWS config didn't load (the resolver then routes everything to
// the local provider and surfaces a clear error on an attempted cloud
// dispatch). Glue+S3 ride along for the ADR-018 Delta `_delta_log/` reads.
func (c AWSClients) CloudProvider() observability.Provider {
	if c.Err != nil {
		return nil
	}
	return observability.NewCloudProvider(c.Athena, c.AthenaOutputBucket, c.SFN, c.CWL).
		WithGlue(c.Glue).
		WithS3(c.S3).
		WithAthenaWorkGroup(c.AthenaWorkGroup)
}

// Options tunes New. The zero value is the CLI's behaviour.
type Options struct {
	// AWSProfile selects a shared-config profile for this workspace's AWS
	// clients. Empty uses the default chain.
	AWSProfile string
}

// Deps is the fully-wired Service plus the individual pieces the surfaces
// with extra wiring need beyond the Service itself.
type Deps struct {
	Svc       *service.Service
	Workspace string
	Clients   AWSClients
	Warm      *observability.PersistentQueryRunner
	Resolver  *observability.Resolver
	// Closers stop what New started lazily (the warm query worker and the
	// transpile sidecar). The CLI registers them as closeables; the UI
	// servers run them on shutdown. Each is idempotent.
	Closers []func()
}

// New builds the Deps for the workspace at root with the full option set:
// resolver, SQL parser, transpiler and metastore ensurer, so no caller can
// silently drop a seam (GH #76).
func New(ctx context.Context, root string, opts Options) *Deps {
	clients := LoadAWSClients(ctx, root, opts.AWSProfile)
	cloud := clients.CloudProvider()
	warm := observability.NewPersistentQueryRunner(root)
	local := observability.NewLocalProvider(root).WithQueryRunner(warm)
	resolver := observability.NewResolver(root, cloud, local)
	sidecar := observability.NewTranspileSidecar(root)
	transpiler := servingsql.NewCachedTranspiler(filepath.Join(root, ".clavesa", "cache", "transpile"), sidecar.ToServing)
	svc := service.New(root).
		WithResolver(resolver).
		// Parser resolves the active warehouse (ADR-024) lazily on first
		// Parse; cloud + undeployed surfaces an actionable error there.
		WithSQLParser(warm.SQLParserForWorkspace()).
		WithTranspiler(transpiler).
		WithMetastoreEnsurer(MetastoreEnsurer())
	if clients.Err == nil {
		svc.WithAWSRegion(clients.Config.Region)
	}
	return &Deps{
		Svc:       svc,
		Workspace: root,
		Clients:   clients,
		Warm:      warm,
		Resolver:  resolver,
		Closers:   []func(){warm.Close, sidecar.Close},
	}
}

// Close runs every closer. Idempotent.
func (d *Deps) Close() {
	for _, fn := range d.Closers {
		fn()
	}
}

// MetastoreEnsurer returns the production shared-metastore ensurer wired
// into the Service via WithMetastoreEnsurer. It brings up (or reuses) the
// per-workspace Derby metastore container and returns the (docker network,
// addr) the run containers attach to. Best-effort: on failure it logs to
// stderr and returns ("", ""), so the containers fall back to embedded
// Derby.
func MetastoreEnsurer() func(ctx context.Context, workspaceRoot, workspaceName string) (string, string) {
	return func(ctx context.Context, workspaceRoot, workspaceName string) (string, string) {
		addr, err := observability.EnsureMetastore(ctx, workspaceRoot, workspaceName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "clavesa: falling back to embedded metastore (shared metastore unavailable): %v\n", err)
			return "", ""
		}
		return observability.MetastoreNetwork(), addr
	}
}
