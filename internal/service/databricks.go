package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/vesahyp/clavesa/internal/credentials"
	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/orchestration/tfgen"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// DatabricksConnectInput is what `clavesa workspace connect databricks`
// and PUT /workspace/databricks both decode into (ADR-015: one struct, two
// wrappers). PublishTables and PushLineage are pointers so "not given"
// keeps the default (on) and a given false turns the API off.
type DatabricksConnectInput struct {
	Host             string `json:"host"`
	Catalog          string `json:"catalog"`
	Credential       string `json:"credential"`
	ExternalLocation string `json:"external_location,omitempty"`
	// CreateCatalog is the --create-catalog opt-in (ADR-027 section 1).
	CreateCatalog bool  `json:"create_catalog,omitempty"`
	PublishTables *bool `json:"publish_tables,omitempty"`
	PushLineage   *bool `json:"push_lineage,omitempty"`
	// Readers get SELECT on every table clavesa registers.
	Readers []string `json:"readers,omitempty"`
}

// DatabricksStatus is the shape both surfaces render: the recorded
// connection (nil when none) and, when checks ran, their results.
type DatabricksStatus struct {
	Connected  bool                  `json:"connected"`
	Connection *workspace.Databricks `json:"connection,omitempty"`
	ClientID   string                `json:"client_id,omitempty"`
	Checks     []databricks.Check    `json:"checks,omitempty"`
	Saved      bool                  `json:"saved"`
	// Released lists the Terraform addresses a disconnect took out of
	// clavesa's state: Databricks and AWS objects that stay, unmanaged.
	Released []string `json:"released,omitempty"`
}

// ErrDatabricksChecksFailed is returned by ConnectDatabricks when at least
// one admin prerequisite is missing. The status still carries every check
// so the caller can print each fix; nothing was written.
var ErrDatabricksChecksFailed = errors.New("databricks prerequisites missing")

// ErrCredentialUsedByConnection is returned by DeleteCredential when the
// workspace's Databricks connection names the credential.
type ErrCredentialUsedByConnection struct{ Name string }

func (e *ErrCredentialUsedByConnection) Error() string {
	return fmt.Sprintf("credential %q is used by the workspace's Databricks connection; run `clavesa workspace disconnect databricks` first", e.Name)
}

// newDatabricksAPI builds the client the checks talk to. Tests replace it
// to point at an httptest server.
var newDatabricksAPI = func(host, clientID, secret string) databricks.API {
	return databricks.NewClient(host, clientID, secret)
}

// GetDatabricks returns the recorded connection without reaching
// Databricks.
func (s *Service) GetDatabricks() (DatabricksStatus, error) {
	m, err := s.loadManifest()
	if err != nil {
		return DatabricksStatus{}, err
	}
	st := DatabricksStatus{Connected: m.Databricks != nil, Connection: m.Databricks, Saved: m.Databricks != nil}
	if m.Databricks != nil {
		if cred, err := s.GetCredential(m.Databricks.Credential); err == nil {
			st.ClientID = cred.ClientID
		}
	}
	return st, nil
}

// ConnectDatabricks validates the input, runs every admin prerequisite
// check against Databricks, and records the connection in clavesa.json
// only when no check failed. It never changes anything in Databricks.
func (s *Service) ConnectDatabricks(ctx context.Context, in DatabricksConnectInput) (DatabricksStatus, error) {
	m, err := s.loadManifest()
	if err != nil {
		return DatabricksStatus{}, err
	}
	host, err := workspace.NormalizeDatabricksHost(in.Host)
	if err != nil {
		return DatabricksStatus{}, err
	}
	conn := &workspace.Databricks{
		Host:             host,
		Catalog:          strings.TrimSpace(in.Catalog),
		Credential:       strings.TrimSpace(in.Credential),
		ExternalLocation: strings.TrimSpace(in.ExternalLocation),
		CreateCatalog:    in.CreateCatalog,
		PublishTables:    in.PublishTables == nil || *in.PublishTables,
		PushLineage:      in.PushLineage == nil || *in.PushLineage,
		Readers:          cleanPrincipals(in.Readers),
	}
	if err := conn.Validate(); err != nil {
		return DatabricksStatus{}, err
	}
	cred, err := s.databricksCredential(conn.Credential)
	if err != nil {
		return DatabricksStatus{}, err
	}
	checks, err := s.runDatabricksChecks(ctx, conn, cred)
	if err != nil {
		return DatabricksStatus{}, err
	}
	st := DatabricksStatus{Connection: conn, ClientID: cred.ClientID, Checks: checks}
	if !databricks.AllPass(checks) {
		st.Connected = m.Databricks != nil
		return st, ErrDatabricksChecksFailed
	}
	m.Databricks = conn
	if err := workspace.SaveManifest(s.workspace, m); err != nil {
		return st, fmt.Errorf("write clavesa.json: %w", err)
	}
	if err := s.syncUnityCatalogTF(""); err != nil {
		return st, fmt.Errorf("write %s: %w", workspace.UnityCatalogTFFile, err)
	}
	st.Connected, st.Saved = true, true
	return st, nil
}

// syncUnityCatalogTF makes the workspace's unity_catalog.tf match
// clavesa.json (ADR-027): written while a connection exists, removed
// otherwise. version "" means the version main.tf already pins.
func (s *Service) syncUnityCatalogTF(version string) error {
	m, err := s.loadManifest()
	if err != nil {
		return err
	}
	if m.Databricks == nil {
		return workspace.SyncUnityCatalogTF(s.workspace, m, "", "", "")
	}
	if version == "" {
		if version, err = workspace.CurrentModuleVersion(s.workspace); err != nil {
			return err
		}
	}
	cred, err := s.databricksCredential(m.Databricks.Credential)
	if err != nil {
		return err
	}
	return workspace.SyncUnityCatalogTF(s.workspace, m, version, cred.ClientID, cred.Secret)
}

// CheckDatabricks re-runs the checks against the recorded connection.
// Writes nothing.
func (s *Service) CheckDatabricks(ctx context.Context) (DatabricksStatus, error) {
	m, err := s.loadManifest()
	if err != nil {
		return DatabricksStatus{}, err
	}
	if m.Databricks == nil {
		return DatabricksStatus{}, fmt.Errorf("this workspace has no Databricks connection (run `clavesa workspace connect databricks`)")
	}
	cred, err := s.databricksCredential(m.Databricks.Credential)
	if err != nil {
		return DatabricksStatus{}, err
	}
	checks, err := s.runDatabricksChecks(ctx, m.Databricks, cred)
	if err != nil {
		return DatabricksStatus{}, err
	}
	return DatabricksStatus{Connected: true, Saved: true, Connection: m.Databricks, ClientID: cred.ClientID, Checks: checks}, nil
}

// DisconnectDatabricks removes the connection from clavesa.json and
// unity_catalog.tf from the workspace, and re-emits every pipeline's
// orchestration.tf without the Unity Catalog parts.
//
// With --create-catalog the workspace and pipeline stacks own Databricks
// objects (catalog, schemas, storage credential, external location, the
// IAM role behind them). Deploying without unity_catalog.tf would try to
// drop a catalog that still holds tables, which Databricks refuses, and
// the module's own provider block is gone by then, so Terraform cannot
// even plan it. So disconnect first takes those objects out of the
// Terraform state (`terraform state rm`): they stay in Databricks and AWS,
// the team's to keep or delete, and clavesa no longer manages them. The
// returned list names every address it released. The Secrets Manager
// secret for the runner is clavesa's own and is deleted by the next
// deploy as usual, so it is not released.
func (s *Service) DisconnectDatabricks(ctx context.Context) ([]string, error) {
	m, err := s.loadManifest()
	if err != nil {
		return nil, err
	}
	if m.Databricks == nil {
		return nil, nil
	}
	released := []string{}
	if m.Databricks.CreateCatalog {
		stacks := []struct{ dir, module string }{{s.workspace, "module.unity_catalog"}}
		pipelines, err := s.ListPipelines()
		if err != nil {
			return nil, err
		}
		for _, p := range pipelines {
			stacks = append(stacks, struct{ dir, module string }{s.resolveDir(p.Dir), "module.unity_catalog_schema"})
		}
		for _, st := range stacks {
			addrs, err := s.releaseFromState(ctx, st.dir, st.module)
			if err != nil {
				return released, err
			}
			released = append(released, addrs...)
		}
	}
	m.Databricks = nil
	if err := workspace.SaveManifest(s.workspace, m); err != nil {
		return released, err
	}
	if err := s.syncUnityCatalogTF(""); err != nil {
		return released, err
	}
	pipelines, err := s.ListPipelines()
	if err != nil {
		return released, err
	}
	for _, p := range pipelines {
		if strings.HasPrefix(p.Name, "_") {
			continue
		}
		if err := s.SyncOrchestration(p.Dir, ""); err != nil {
			return released, fmt.Errorf("re-emit %s/orchestration.tf: %w", p.Name, err)
		}
	}
	return released, nil
}

// releaseFromState removes every resource under module (except the
// runner's Secrets Manager secret) from the Terraform state in dir and
// returns the released addresses. A stack that was never deployed has
// nothing to release.
func (s *Service) releaseFromState(ctx context.Context, dir, module string) ([]string, error) {
	if _, err := os.Stat(filepath.Join(dir, ".terraform")); err != nil {
		if _, serr := os.Stat(filepath.Join(dir, "terraform.tfstate")); serr != nil {
			return nil, nil // never initialised, never deployed
		}
	}
	out, code, err := terraformStateCmd(ctx, s, dir, "state", "list")
	if err != nil || code != 0 {
		return nil, fmt.Errorf("terraform state list in %s failed (%v): %s\nRun `clavesa deploy` once so the stack is initialised, then disconnect again", dir, err, strings.TrimSpace(out))
	}
	var addrs []string
	for _, line := range strings.Split(out, "\n") {
		a := strings.TrimSpace(line)
		if !strings.HasPrefix(a, module+".") {
			continue
		}
		if strings.Contains(a, "aws_secretsmanager_secret.") {
			continue
		}
		addrs = append(addrs, a)
	}
	if len(addrs) == 0 {
		return nil, nil
	}
	args := append([]string{"state", "rm"}, addrs...)
	if out, code, err := terraformStateCmd(ctx, s, dir, args...); err != nil || code != 0 {
		return nil, fmt.Errorf("terraform state rm in %s failed (%v): %s", dir, err, strings.TrimSpace(out))
	}
	return addrs, nil
}

// terraformStateCmd runs one terraform command in dir with the
// workspace's Terraform environment and returns its combined output.
// A variable so tests can replace it.
var terraformStateCmd = func(ctx context.Context, s *Service, dir string, args ...string) (string, int, error) {
	var buf strings.Builder
	code, err := s.runTerraformWithEnv(ctx, dir, &buf, &buf, args...)
	return buf.String(), code, err
}

// TerraformEnv is the environment every `terraform` call in this
// workspace runs with: the process environment, plus
// DATABRICKS_CLIENT_SECRET when the workspace has a Databricks
// connection (ADR-027). The unity-catalog modules configure the provider
// with host and client ID only; the secret reaches the provider through
// this variable and never enters a file, a variable or the state.
func (s *Service) TerraformEnv(ctx context.Context) ([]string, error) {
	env := os.Environ()
	m, err := workspace.Load(s.workspace)
	if err != nil || m == nil || m.Databricks == nil {
		return env, err
	}
	cred, err := s.databricksCredential(m.Databricks.Credential)
	if err != nil {
		return nil, fmt.Errorf("databricks connection: %w", err)
	}
	secret, err := s.resolveSecret(ctx, cred.Secret)
	if err != nil {
		return nil, fmt.Errorf("databricks connection: credential %q: %w", cred.Name, err)
	}
	return append(env, "DATABRICKS_CLIENT_SECRET="+secret), nil
}

// PutDatabricksRunnerSecret copies the OAuth secret into the Secrets
// Manager secret the workspace stack created for the runner, when the
// credential's reference is local only (env: or file:). secretARN is the
// workspace stack's `databricks_runner_secret_arn` output. A no-op
// without a connection or with an arn: reference (the runner reads that
// one directly). Runs after the workspace apply, from the CLI, so the
// value never passes through Terraform.
//
// secretARN is called only when a copy is needed: Terraform does not
// store a null output, so for an arn: credential the output does not
// exist at all.
func (s *Service) PutDatabricksRunnerSecret(ctx context.Context, secretARN func() (string, error)) (bool, error) {
	m, err := s.loadManifest()
	if err != nil || m.Databricks == nil {
		return false, err
	}
	cred, err := s.databricksCredential(m.Databricks.Credential)
	if err != nil {
		return false, err
	}
	if !workspace.UnityCatalogNeedsRunnerSecret(cred.Secret) {
		return false, nil
	}
	arn, err := secretARN()
	if err != nil {
		return false, err
	}
	return s.putRunnerSecret(ctx, cred, arn)
}

func (s *Service) putRunnerSecret(ctx context.Context, cred CredentialSpec, secretARN string) (bool, error) {
	if !strings.HasPrefix(secretARN, "arn:aws:secretsmanager:") {
		return false, fmt.Errorf("workspace stack output databricks_runner_secret_arn is %q, want a Secrets Manager ARN", secretARN)
	}
	value, err := s.resolveSecret(ctx, cred.Secret)
	if err != nil {
		return false, fmt.Errorf("credential %q: %w", cred.Name, err)
	}
	region := strings.Split(secretARN, ":")[3]
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return false, fmt.Errorf("load AWS config: %w", err)
	}
	if _, err := secretsmanager.NewFromConfig(cfg).PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     &secretARN,
		SecretString: &value,
	}); err != nil {
		return false, fmt.Errorf("put %s: %w", secretARN, err)
	}
	return true, nil
}

// unityCatalogConfig is the pipeline emitter's view of the connection,
// nil when there is none. An unreadable credential is an error, not a
// silent nil: emitting without the schema would make the next deploy
// try to drop it.
func (s *Service) unityCatalogConfig(m *workspace.Manifest, pipelineDir string) (*tfgen.UnityCatalogConfig, error) {
	if m == nil || m.Databricks == nil {
		return nil, nil
	}
	cred, err := s.databricksCredential(m.Databricks.Credential)
	if err != nil {
		return nil, fmt.Errorf("databricks connection: %w", err)
	}
	secretExpr := "data.terraform_remote_state.workspace.outputs.databricks_runner_secret_arn"
	if !workspace.UnityCatalogNeedsRunnerSecret(cred.Secret) {
		secretExpr = fmt.Sprintf("%q", cred.Secret)
	}
	return &tfgen.UnityCatalogConfig{
		Host:          m.Databricks.Host,
		ClientID:      cred.ClientID,
		Catalog:       m.Databricks.Catalog,
		ModuleSource:  s.ModuleSource(pipelineDir, "unity-catalog/aws/schema"),
		CreateSchema:  m.Databricks.CreateCatalog,
		SecretARNExpr: secretExpr,
		PublishTables: m.Databricks.PublishTables,
		PushLineage:   m.Databricks.PushLineage,
		Readers:       m.Databricks.Readers,
		JobsExpr:      "var.databricks_jobs",
	}, nil
}

// cleanPrincipals trims, drops empties and duplicates, keeping order.
func cleanPrincipals(in []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, p := range in {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return out
}

func (s *Service) loadManifest() (*workspace.Manifest, error) {
	m, err := workspace.Load(s.workspace)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("%s is not a clavesa workspace (no clavesa.json)", s.workspace)
	}
	return m, nil
}

func (s *Service) databricksCredential(name string) (CredentialSpec, error) {
	cred, err := s.GetCredential(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CredentialSpec{}, fmt.Errorf("%w: %q (register it with `clavesa credential register %s --kind databricks-oauth --client-id <id> --secret <ref>`)",
				ErrCredentialNotFound, name, name)
		}
		return CredentialSpec{}, err
	}
	if cred.Kind != credentials.KindDatabricksOAuth {
		return CredentialSpec{}, fmt.Errorf("credential %q is kind %s; the Databricks connection needs kind %s", name, cred.Kind, credentials.KindDatabricksOAuth)
	}
	return cred, nil
}

func (s *Service) runDatabricksChecks(ctx context.Context, conn *workspace.Databricks, cred CredentialSpec) ([]databricks.Check, error) {
	secret, err := s.resolveSecret(ctx, cred.Secret)
	if err != nil {
		return nil, fmt.Errorf("credential %q: %w", cred.Name, err)
	}
	api := newDatabricksAPI(conn.Host, cred.ClientID, secret)
	return databricks.RunChecks(ctx, api, databricks.Target{
		Host:             conn.Host,
		ClientID:         cred.ClientID,
		Catalog:          conn.Catalog,
		CreateCatalog:    conn.CreateCatalog,
		ExternalLocation: conn.ExternalLocation,
	}), nil
}

// resolveSecret reads the secret material a credential reference points
// at, on this machine. Used only for the connect checks; the runner
// resolves secrets itself at run time.
func (s *Service) resolveSecret(ctx context.Context, ref string) (string, error) {
	switch {
	case strings.HasPrefix(ref, "env:"):
		name := strings.TrimPrefix(ref, "env:")
		v, ok := os.LookupEnv(name)
		if !ok || v == "" {
			return "", fmt.Errorf("environment variable %s is not set", name)
		}
		return v, nil
	case strings.HasPrefix(ref, "file:"):
		rel := strings.TrimPrefix(ref, "file:")
		p := rel
		if !filepath.IsAbs(p) {
			p = filepath.Join(s.workspace, rel)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return "", fmt.Errorf("read secret file: %w", err)
		}
		return strings.TrimSpace(string(data)), nil
	case strings.HasPrefix(ref, "arn:aws:secretsmanager:"):
		parts := strings.Split(ref, ":")
		if len(parts) < 4 {
			return "", fmt.Errorf("malformed secret ARN %q", ref)
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(parts[3]))
		if err != nil {
			return "", fmt.Errorf("load AWS config: %w", err)
		}
		out, err := secretsmanager.NewFromConfig(cfg).GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &ref})
		if err != nil {
			return "", fmt.Errorf("read %s: %w", ref, err)
		}
		if out.SecretString == nil {
			return "", fmt.Errorf("%s has no string value", ref)
		}
		return strings.TrimSpace(*out.SecretString), nil
	default:
		return "", fmt.Errorf("unknown secret backend in %q", ref)
	}
}
