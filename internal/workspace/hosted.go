package workspace

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Hosted is the manifest's `hosted` block (ADR-032 section 2): the hosted
// read-only UI this workspace joins. When set, the workspace stack gets
// the reader role, the Athena workgroup and (with EventBusARN) the
// forwarding rules, and every deploy uploads a read-only copy of the
// workspace tree to s3://<pipeline_bucket>/_clavesa/tree/.
type Hosted struct {
	// URL is the hosted UI, https and no path: https://ui.clavesa.dev
	URL string `json:"url"`
	// Kind is HostedKindCloud (read through the reader role) or
	// HostedKindLocal (summary published from the Mac). It is recorded
	// at enable time so it never follows a developer's own warehouse
	// setting. Empty in blocks written before the field existed; IsLocal
	// then infers it from the other fields.
	Kind string `json:"kind,omitempty"`
	// ReaderPrincipals are the IAM principal ARNs the reader role trusts,
	// normally the hosted Lambda's role. The ExternalId is the workspace
	// name.
	ReaderPrincipals []string `json:"reader_principals"`
	// EventBusARN is the hosted clavesa-ui event bus. Empty: no
	// forwarding rule.
	EventBusARN string `json:"event_bus_arn,omitempty"`
	// PublisherRoleARN, DataBucket and Region are set for a
	// local-warehouse workspace, which has no reader principals: the role
	// it assumes to write summaries/<name>.json to the data bucket at the
	// end of every `clavesa pipeline run` (ADR-032 section 2).
	PublisherRoleARN string `json:"publisher_role_arn,omitempty"`
	DataBucket       string `json:"data_bucket,omitempty"`
	Region           string `json:"region,omitempty"`
}

// The values of Hosted.Kind.
const (
	HostedKindCloud = "cloud"
	HostedKindLocal = "local"
)

// IsLocal reports whether the block is a local-warehouse workspace's.
// Such a workspace has no stack, so no hosted.tf and nothing to deploy.
// The recorded Kind decides; a block without one (written before the
// field) is local when it has a publisher role and no reader principals.
func (h *Hosted) IsLocal() bool {
	if h == nil {
		return false
	}
	if h.Kind != "" {
		return h.Kind == HostedKindLocal
	}
	return len(h.ReaderPrincipals) == 0 && h.PublisherRoleARN != ""
}

var (
	principalARNRE = regexp.MustCompile(`^arn:aws[a-z-]*:(iam|sts)::[0-9]{12}:[A-Za-z0-9+=,.@_/:-]+$`)
	eventBusARNRE  = regexp.MustCompile(`^arn:aws[a-z-]*:events:[a-z0-9-]+:[0-9]{12}:event-bus/[A-Za-z0-9._/-]+$`)
)

// NormalizeHostedURL trims a trailing slash and checks the URL is https
// (or http on localhost, for a local test server) with a host and no
// path, query or fragment.
func NormalizeHostedURL(raw string) (string, error) {
	h := strings.TrimRight(strings.TrimSpace(raw), "/")
	if h == "" {
		return "", fmt.Errorf("hosted.url is required")
	}
	u, err := url.Parse(h)
	if err != nil {
		return "", fmt.Errorf("hosted.url: %w", err)
	}
	// http only for a hosted UI on this machine (a local test server).
	loopback := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if !(u.Scheme == "https" || (u.Scheme == "http" && loopback)) || u.Host == "" {
		return "", fmt.Errorf("hosted.url must be an https URL such as https://ui.clavesa.dev, got %q", raw)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("hosted.url must have no path or query, got %q", raw)
	}
	return u.Scheme + "://" + u.Host, nil
}

// Validate checks the block is well-formed. It reaches no network.
func (h *Hosted) Validate() error {
	if _, err := NormalizeHostedURL(h.URL); err != nil {
		return err
	}
	if h.Kind != "" && h.Kind != HostedKindCloud && h.Kind != HostedKindLocal {
		return fmt.Errorf("hosted.kind must be %q or %q, got %q", HostedKindCloud, HostedKindLocal, h.Kind)
	}
	if h.IsLocal() && h.PublisherRoleARN == "" {
		return fmt.Errorf("hosted.publisher_role_arn is required for a local-warehouse workspace")
	}
	if h.PublisherRoleARN != "" && !principalARNRE.MatchString(h.PublisherRoleARN) {
		return fmt.Errorf("hosted.publisher_role_arn: %q is not an IAM role ARN", h.PublisherRoleARN)
	}
	if h.IsLocal() {
		if len(h.ReaderPrincipals) > 0 || h.EventBusARN != "" {
			return fmt.Errorf("a local-warehouse workspace has no reader role or event rule: hosted.reader_principals and hosted.event_bus_arn must be empty")
		}
		if h.DataBucket == "" {
			return fmt.Errorf("hosted.data_bucket is required with hosted.publisher_role_arn")
		}
		return nil
	}
	if len(h.ReaderPrincipals) == 0 {
		return fmt.Errorf("hosted.reader_principals needs at least one IAM principal ARN (the hosted UI's Lambda role), or a local-warehouse workspace's hosted.publisher_role_arn")
	}
	for _, p := range h.ReaderPrincipals {
		if !principalARNRE.MatchString(p) {
			return fmt.Errorf("hosted.reader_principals: %q is not an IAM principal ARN (arn:aws:iam::<account>:role/<name>)", p)
		}
	}
	if h.EventBusARN != "" && !eventBusARNRE.MatchString(h.EventBusARN) {
		return fmt.Errorf("hosted.event_bus_arn: %q is not an event bus ARN (arn:aws:events:<region>:<account>:event-bus/<name>)", h.EventBusARN)
	}
	return nil
}

// HostedTFFile is the clavesa-owned file in the workspace root that calls
// the hosted-reader module (ADR-032). It exists only while clavesa.json
// has a `hosted` block.
const HostedTFFile = "hosted.tf"

const hostedTFHeader = `# clavesa-managed hosted UI reader, generated from clavesa.json's "hosted"
# field (ADR-032). Do not edit by hand: clavesa overwrites this file on
# "workspace hosted enable", on every deploy and upgrade, and removes it on
# "workspace hosted disable".

`

// hostedName returns prefix+workspace when it fits max characters, else
// the first max-9 characters, "-" and 8 hex characters of the SHA-256 of
// the full name, so the name stays unique and inside the AWS limit.
func hostedName(prefix, workspaceName string, max int) string {
	full := prefix + workspaceName
	if len(full) <= max {
		return full
	}
	sum := sha256.Sum256([]byte(full))
	return strings.TrimRight(full[:max-9], "-") + "-" + hex.EncodeToString(sum[:])[:8]
}

// HostedReaderRoleName is the reader role: clavesa-ui-reader-<workspace>.
// Capped at 60 characters so the forwarding role (<name>-fwd) fits IAM's 64.
func HostedReaderRoleName(workspaceName string) string {
	return hostedName("clavesa-ui-reader-", workspaceName, 60)
}

// HostedWorkGroupName is the reader's Athena workgroup:
// clavesa-ui-<workspace>. Capped at 55 so the rule names
// (<name>-progress) fit EventBridge's 64.
func HostedWorkGroupName(workspaceName string) string {
	return hostedName("clavesa-ui-", workspaceName, 55)
}

// pipelineAWSNamePrefix is the prefix every workspace-scoped pipeline base
// name starts with (see PipelineAWSName): "clavesa-<workspace>-", or the
// first 31 characters of it when the base is cut. ok is false for legacy
// naming, where names carry no workspace.
func pipelineAWSNamePrefix(m *Manifest) (string, bool) {
	if m == nil || m.AWSNames != AWSNamesWorkspace {
		return "", false
	}
	p := "clavesa-" + m.Name + "-"
	if len(p) > 31 {
		p = strings.TrimRight(p[:31], "-")
	}
	return p, true
}

// HostedResourcePatterns returns the state machine and log group name
// patterns of the workspace's pipelines. Workspace-scoped naming gives one
// prefix pattern that also covers pipelines added later; legacy naming
// lists each pipeline (so a new pipeline needs a workspace deploy, which
// `clavesa deploy` does first).
func HostedResourcePatterns(m *Manifest, pipelines []string) (stateMachines, logGroups []string) {
	if p, ok := pipelineAWSNamePrefix(m); ok {
		return []string{p + "*"}, []string{"/aws/lambda/" + p + "*", "/clavesa/" + p + "*"}
	}
	names := append([]string(nil), pipelines...)
	sort.Strings(names)
	for _, n := range names {
		base := PipelineAWSName(m, n)
		stateMachines = append(stateMachines, base)
		logGroups = append(logGroups, "/aws/lambda/"+base+"-*", "/clavesa/"+n+"/sfn")
	}
	return stateMachines, logGroups
}

// HostedGlueDatabasePatterns returns the Glue database patterns of the
// workspace's catalog and system catalog (`<catalog>__<schema>`).
func HostedGlueDatabasePatterns(m *Manifest) []string {
	safe := func(s string) string { return strings.ReplaceAll(s, "-", "_") }
	return []string{safe(m.CatalogIdentifier()) + "__*", safe(m.SystemCatalogIdentifier()) + "__*"}
}

// hostedStateObjectPatterns are the pipeline-bucket keys the reader is
// explicitly denied: any *.tfstate object, and the whole state prefix when
// the S3 backend (ADR-025) shares the pipeline bucket.
func hostedStateObjectPatterns(m *Manifest) []string {
	pats := []string{"*.tfstate", "*.tfstate.*"}
	if m.Backend != nil && m.Backend.Bucket == m.Name+"-clavesa" {
		pats = append(pats, m.Backend.KeyPrefixOrDefault()+"*")
	}
	return pats
}

// hostedLFDatabases is the module's lf_databases expression: the system
// database through the workspace module's output (same stack, so the
// grant waits for the database), then the deployed pipelines' databases
// sorted, without duplicates.
func hostedLFDatabases(m *Manifest, pipelineDBs []string) string {
	sys := strings.ReplaceAll(m.SystemCatalogIdentifier(), "-", "_") + "__pipelines"
	seen := map[string]bool{sys: true, "": true}
	var dbs []string
	for _, d := range pipelineDBs {
		if !seen[d] {
			seen[d] = true
			dbs = append(dbs, d)
		}
	}
	sort.Strings(dbs)
	return fmt.Sprintf("concat([module.workspace.system_pipelines_db], [%s])", quotedHCLList(dbs))
}

// RenderHostedTF renders hosted.tf. pipelines are the workspace's pipeline
// names, used only for legacy (non workspace-scoped) AWS names.
// pipelineDBs are the Glue databases of the deployed pipelines, which get
// Lake Formation grants for the reader (with the system database).
func RenderHostedTF(m *Manifest, moduleVersion string, pipelines, pipelineDBs []string) string {
	h := m.Hosted
	sms, logs := HostedResourcePatterns(m, pipelines)
	return hostedTFHeader + fmt.Sprintf(`module "hosted_reader" {
  source                 = "./.clavesa/modules/%s/hosted-reader/aws"
  workspace_name         = var.workspace_name
  role_name              = %q
  workgroup_name         = %q
  pipeline_bucket        = module.workspace.pipeline_bucket
  glue_database_patterns = [%s]
  lf_databases           = %s
  state_machine_patterns = [%s]
  log_group_patterns     = [%s]
  state_object_patterns  = [%s]
  reader_principals      = [%s]
  event_bus_arn          = %q
}

output "hosted_reader_role_arn" {
  description = "Role the hosted UI assumes to read this workspace (ADR-032); ExternalId = the workspace name."
  value       = module.hosted_reader.reader_role_arn
}

output "hosted_workgroup" {
  description = "Athena workgroup the hosted UI queries through (ADR-032)."
  value       = module.hosted_reader.workgroup_name
}
`, moduleVersion, HostedReaderRoleName(m.Name), HostedWorkGroupName(m.Name),
		quotedHCLList(HostedGlueDatabasePatterns(m)), hostedLFDatabases(m, pipelineDBs), quotedHCLList(sms), quotedHCLList(logs),
		quotedHCLList(hostedStateObjectPatterns(m)), quotedHCLList(h.ReaderPrincipals), h.EventBusARN)
}

// SyncHostedTF makes hosted.tf match the manifest: written when m.Hosted
// is set, removed when it is not or when it is a local-warehouse
// workspace's block (no stack, no reader role).
func SyncHostedTF(root string, m *Manifest, moduleVersion string, pipelines, pipelineDBs []string) error {
	path := filepath.Join(root, HostedTFFile)
	if m.Hosted == nil || m.Hosted.IsLocal() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if moduleVersion == "" {
		return fmt.Errorf("no module version: main.tf has no recognised workspace module source")
	}
	return os.WriteFile(path, []byte(RenderHostedTF(m, moduleVersion, pipelines, pipelineDBs)), 0o644)
}
