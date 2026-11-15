package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vesahyp/clavesa/internal/modules"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// HostedInput is what `clavesa workspace hosted enable` and PUT
// /workspace/hosted both decode into (ADR-015, ADR-032).
type HostedInput struct {
	URL              string   `json:"url"`
	ReaderPrincipals []string `json:"reader_principals,omitempty"`
	EventBusARN      string   `json:"event_bus_arn,omitempty"`
}

// HostedStatus is the shape both surfaces render: the recorded block (nil
// when the workspace is not hosted) and the names the deploy creates.
type HostedStatus struct {
	Enabled        bool              `json:"enabled"`
	Hosted         *workspace.Hosted `json:"hosted,omitempty"`
	ReaderRoleName string            `json:"reader_role_name,omitempty"`
	WorkGroup      string            `json:"workgroup,omitempty"`
	// TreeURI is where `clavesa deploy` uploads the read-only tree copy;
	// empty until the workspace is deployed.
	TreeURI string `json:"tree_uri,omitempty"`
	// Kind is "cloud" (read through the reader role) or "local" (the
	// summary is published from this machine at run end); empty when
	// not hosted. SummaryURI is where a local workspace's summary goes.
	Kind       string `json:"kind,omitempty"`
	SummaryURI string `json:"summary_uri,omitempty"`
}

// HostedTreePrefix is the key prefix of the tree copy in the pipeline
// bucket (ADR-032).
const HostedTreePrefix = "_clavesa/tree/"

// HostedWellKnownPath is where a hosted UI publishes its join settings
// (ADR-032): GET <url>/.well-known/clavesa.json, public, no sign-in.
const HostedWellKnownPath = "/.well-known/clavesa.json"

// HostedWellKnown is the document at HostedWellKnownPath. The hosted
// Lambda serves it (internal/hosted); `workspace hosted enable` and
// `workspace summary --publish` read it.
type HostedWellKnown struct {
	// ReaderPrincipal is the hosted Lambda's role: the principal every
	// workspace's reader role trusts.
	ReaderPrincipal string `json:"reader_principal"`
	// EventBusARN is the clavesa-ui bus the workspace forwards run
	// events to.
	EventBusARN string `json:"event_bus_arn"`
	// PublisherRoleARN is the role a local-warehouse workspace assumes
	// to write its summary; DataBucket and Region locate that object.
	PublisherRoleARN string `json:"publisher_role_arn"`
	DataBucket       string `json:"data_bucket"`
	Region           string `json:"region"`
}

// hostedWellKnown fetches <url>/.well-known/clavesa.json. Tests swap it.
var hostedWellKnown = fetchHostedWellKnown

// hostedHTTPClient is the client of the well-known fetch.
var hostedHTTPClient = &http.Client{Timeout: 15 * time.Second}

func fetchHostedWellKnown(ctx context.Context, baseURL string) (HostedWellKnown, error) {
	u := strings.TrimRight(baseURL, "/") + HostedWellKnownPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return HostedWellKnown{}, err
	}
	req.Header.Set("Accept", "application/json")
	res, err := hostedHTTPClient.Do(req)
	if err != nil {
		return HostedWellKnown{}, fmt.Errorf("fetch %s: %w", u, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if err != nil {
		return HostedWellKnown{}, fmt.Errorf("fetch %s: %w", u, err)
	}
	if res.StatusCode != http.StatusOK {
		return HostedWellKnown{}, fmt.Errorf("fetch %s: HTTP %d", u, res.StatusCode)
	}
	var wk HostedWellKnown
	if err := json.Unmarshal(body, &wk); err != nil {
		return HostedWellKnown{}, fmt.Errorf("fetch %s: not the hosted UI's settings: %w", u, err)
	}
	if wk.ReaderPrincipal == "" {
		return HostedWellKnown{}, fmt.Errorf("fetch %s: no reader_principal in the document", u)
	}
	return wk, nil
}

// GetHosted returns the recorded hosted block. It reaches no network
// except the state read for the tree URI.
func (s *Service) GetHosted() (HostedStatus, error) {
	m, err := s.loadManifest()
	if err != nil {
		return HostedStatus{}, err
	}
	st := HostedStatus{Enabled: m.Hosted != nil, Hosted: m.Hosted}
	if m.Hosted.IsLocal() {
		// A local-warehouse workspace has no reader role, workgroup or
		// tree copy: its Mac publishes the summary at run end.
		st.Kind = "local"
		st.SummaryURI = "s3://" + m.Hosted.DataBucket + "/" + HostedSummaryKey(m.Name)
		return st, nil
	}
	if m.Hosted != nil {
		st.Kind = "cloud"
		st.ReaderRoleName = workspace.HostedReaderRoleName(m.Name)
		st.WorkGroup = workspace.HostedWorkGroupName(m.Name)
		if b := workspace.PipelineBucket(s.workspace); b != "" {
			st.TreeURI = "s3://" + b + "/" + HostedTreePrefix
		}
	}
	return st, nil
}

// EnableHosted validates the input, writes the `hosted` block into
// clavesa.json and hosted.tf into the workspace. Nothing in AWS changes
// until the next deploy.
func (s *Service) EnableHosted(ctx context.Context, in HostedInput) (HostedStatus, error) {
	m, err := s.loadManifest()
	if err != nil {
		return HostedStatus{}, err
	}
	u, err := workspace.NormalizeHostedURL(in.URL)
	if err != nil {
		return HostedStatus{}, err
	}
	// The hosted UI's own principal and bus come from its well-known
	// document; ReaderPrincipals given here are kept as extra principals
	// (a CLI user that runs the smoke gate's reader leg, say).
	wk, err := hostedWellKnown(ctx, u)
	if err != nil {
		return HostedStatus{}, err
	}
	if workspace.LoadWarehouse(s.workspace) == workspace.WarehouseLocal {
		return s.enableHostedLocal(m, u, in, wk)
	}
	in.ReaderPrincipals = cleanPrincipals(append([]string{wk.ReaderPrincipal}, in.ReaderPrincipals...))
	if strings.TrimSpace(in.EventBusARN) == "" {
		in.EventBusARN = wk.EventBusARN
	}
	h := &workspace.Hosted{URL: u, ReaderPrincipals: in.ReaderPrincipals, EventBusARN: strings.TrimSpace(in.EventBusARN)}
	if err := h.Validate(); err != nil {
		return HostedStatus{}, err
	}
	m.Hosted = h
	if err := workspace.SaveManifest(s.workspace, m); err != nil {
		return HostedStatus{}, fmt.Errorf("write clavesa.json: %w", err)
	}
	if err := s.SyncHostedTF(""); err != nil {
		return HostedStatus{}, fmt.Errorf("write %s: %w", workspace.HostedTFFile, err)
	}
	return s.GetHosted()
}

// enableHostedLocal records the block of a local-warehouse workspace
// (ADR-032 section 2, "pushed from the Mac"): the URL and, from the
// well-known document, the publisher role, the data bucket and its
// region. No reader principals, no event bus and no hosted.tf: the
// workspace has no stack, so nothing is deployed. The publisher role can
// write summaries/<name>.json only after the name is registered as a
// local workspace in the hosted UI's hosted/infra/workspaces.tf.
func (s *Service) enableHostedLocal(m *workspace.Manifest, u string, in HostedInput, wk HostedWellKnown) (HostedStatus, error) {
	if len(cleanPrincipals(in.ReaderPrincipals)) > 0 || strings.TrimSpace(in.EventBusARN) != "" {
		return HostedStatus{}, fmt.Errorf("this is a local-warehouse workspace: it has no reader role or event rule, so reader principals and an event bus do not apply")
	}
	if wk.PublisherRoleARN == "" || wk.DataBucket == "" {
		return HostedStatus{}, fmt.Errorf("%s%s names no publisher role or data bucket, which a local-warehouse workspace needs", u, HostedWellKnownPath)
	}
	h := &workspace.Hosted{URL: u, PublisherRoleARN: wk.PublisherRoleARN, DataBucket: wk.DataBucket, Region: wk.Region}
	if err := h.Validate(); err != nil {
		return HostedStatus{}, err
	}
	m.Hosted = h
	if err := workspace.SaveManifest(s.workspace, m); err != nil {
		return HostedStatus{}, fmt.Errorf("write clavesa.json: %w", err)
	}
	if err := s.SyncHostedTF(""); err != nil {
		return HostedStatus{}, fmt.Errorf("remove %s: %w", workspace.HostedTFFile, err)
	}
	return s.GetHosted()
}

// DisableHosted removes the `hosted` block and hosted.tf. The next deploy
// deletes the reader role, the workgroup and the rules; the tree copy in
// the bucket stays until removed by hand.
func (s *Service) DisableHosted() (HostedStatus, error) {
	m, err := s.loadManifest()
	if err != nil {
		return HostedStatus{}, err
	}
	m.Hosted = nil
	if err := workspace.SaveManifest(s.workspace, m); err != nil {
		return HostedStatus{}, fmt.Errorf("write clavesa.json: %w", err)
	}
	if err := s.SyncHostedTF(""); err != nil {
		return HostedStatus{}, fmt.Errorf("remove %s: %w", workspace.HostedTFFile, err)
	}
	return HostedStatus{}, nil
}

// SyncHostedTF makes hosted.tf match clavesa.json: written while the
// workspace is hosted, removed otherwise. Deploy calls it before the
// workspace plan so the legacy per-pipeline name list stays current.
//
// version "" means the binary's ModuleVersion, not the version main.tf
// pins: the hosted-reader module is self-contained and new in v2.26.0, so
// a workspace pinned to an older module version joins without a full
// `workspace upgrade` (which would rewrite its pipelines). The embedded
// module tree is extracted to .clavesa/modules/<version>/ when it is not
// there yet. `workspace upgrade` passes its target, whose tree it has
// just extracted.
func (s *Service) SyncHostedTF(version string) error {
	m, err := s.loadManifest()
	if err != nil {
		return err
	}
	if m.Hosted == nil || m.Hosted.IsLocal() {
		// A local-warehouse workspace has no stack: nothing to deploy.
		return workspace.SyncHostedTF(s.workspace, m, "", nil)
	}
	if version == "" {
		version = ModuleVersion
	}
	if err := modules.Extract(s.workspace, version); err != nil {
		return fmt.Errorf("extract modules %s: %w", version, err)
	}
	pipes, err := s.ListPipelines()
	if err != nil {
		return fmt.Errorf("list pipelines: %w", err)
	}
	names := make([]string, 0, len(pipes))
	for _, p := range pipes {
		names = append(names, p.Name)
	}
	return workspace.SyncHostedTF(s.workspace, m, version, names)
}

// ---------------------------------------------------------------------------
// Tree copy on deploy (ADR-032)
// ---------------------------------------------------------------------------

// HostedTreeFile is one object of the tree copy: Key is relative to
// HostedTreePrefix; the content comes from Path on disk or from Body.
type HostedTreeFile struct {
	Key  string
	Path string
	Body []byte
}

// hostedTreeDenied reports file names that never leave the Mac, whatever
// rule selected them: Terraform state and plans, env files, credential
// material. A second line of defence behind the allow-list below.
func hostedTreeDenied(name string) bool {
	n := strings.ToLower(name)
	return strings.Contains(n, ".tfstate") || n == "tfplan" || strings.HasPrefix(n, ".env") ||
		strings.HasSuffix(n, ".secret") || strings.Contains(n, "credential")
}

// HostedTreeFiles selects what the tree copy holds. Allow-list, not a
// deny-list: clavesa.json; a generated .clavesa/environment.json saying
// cloud (what was deployed); and per pipeline only its .tf files (except
// backend.tf) and terraform.tfvars / *.auto.tfvars, which carry the
// trigger_schedule. These are what the hosted pages read
// (ui.BuildWorkspaceMux's GET handlers for Workspaces, Workspace, Pipeline
// and Run). Never: Terraform state or plans, .terraform/, .env files,
// .clavesa/credentials, the AWS profile, sources, dashboards, sidecars.
// pipelineDirs are workspace-relative.
func HostedTreeFiles(root string, pipelineDirs []string) ([]HostedTreeFile, error) {
	env, err := json.MarshalIndent(map[string]string{"warehouse": "cloud", "mode": "cloud"}, "", "  ")
	if err != nil {
		return nil, err
	}
	files := []HostedTreeFile{
		{Key: "clavesa.json", Path: filepath.Join(root, "clavesa.json")},
		{Key: ".clavesa/environment.json", Body: append(env, '\n')},
	}
	dirs := append([]string(nil), pipelineDirs...)
	sort.Strings(dirs)
	for _, rel := range dirs {
		rel = filepath.Clean(rel)
		if rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(root, rel))
		if err != nil {
			return nil, fmt.Errorf("read pipeline dir %s: %w", rel, err)
		}
		for _, e := range entries {
			name := e.Name()
			if !e.Type().IsRegular() || hostedTreeDenied(name) {
				continue
			}
			keep := (strings.HasSuffix(name, ".tf") && name != "backend.tf") ||
				name == "terraform.tfvars" || strings.HasSuffix(name, ".auto.tfvars")
			if !keep {
				continue
			}
			files = append(files, HostedTreeFile{
				Key:  filepath.ToSlash(filepath.Join(rel, name)),
				Path: filepath.Join(root, rel, name),
			})
		}
	}
	return files, nil
}

// HostedTreeStore is the S3 seam of the tree upload. Tests swap it via
// newHostedTreeStore.
type HostedTreeStore interface {
	Put(ctx context.Context, bucket, key string, body []byte) error
	List(ctx context.Context, bucket, prefix string) ([]string, error)
	Delete(ctx context.Context, bucket string, keys []string) error
	// Region is the AWS region the store's config names, the fallback
	// for deployed.json when no state machine ARN gives one.
	Region() string
}

var newHostedTreeStore = func(ctx context.Context, root string) (HostedTreeStore, error) {
	cfg, err := workspace.LoadAWSConfig(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return s3TreeStore{c: s3.NewFromConfig(cfg), region: cfg.Region}, nil
}

type s3TreeStore struct {
	c      *s3.Client
	region string
}

func (t s3TreeStore) Region() string { return t.region }

func (t s3TreeStore) Put(ctx context.Context, bucket, key string, body []byte) error {
	_, err := t.c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body)})
	return err
}

func (t s3TreeStore) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(t.c, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys, nil
}

func (t s3TreeStore) Delete(ctx context.Context, bucket string, keys []string) error {
	for len(keys) > 0 {
		n := min(len(keys), 1000)
		objs := make([]s3types.ObjectIdentifier, n)
		for i, k := range keys[:n] {
			objs[i] = s3types.ObjectIdentifier{Key: aws.String(k)}
		}
		if _, err := t.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &s3types.Delete{Objects: objs, Quiet: aws.Bool(true)}}); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}

// HostedTreeResult reports one tree upload.
type HostedTreeResult struct {
	URI      string   `json:"uri"`
	Uploaded []string `json:"uploaded"`
	Removed  []string `json:"removed,omitempty"`
}

// arnField returns the i-th colon-separated field of an ARN, or "".
func arnField(arn string, i int) string {
	parts := strings.Split(arn, ":")
	if len(parts) <= i {
		return ""
	}
	return parts[i]
}

// BuildDeployed reads the workspace and pipeline states (through
// ReadStackState, so local or S3 backend alike) and returns deployed.json:
// the outputs listed in workspace.DeployedWorkspaceOutputs and each
// pipeline's state machine ARN. Nothing else from the state is copied.
func (s *Service) BuildDeployed(ctx context.Context, pipes []PipelineInfo, fallbackRegion string, now time.Time) (*workspace.Deployed, error) {
	wsState, err := workspace.ReadStackState(ctx, s.workspace, s.workspace)
	if err != nil {
		return nil, fmt.Errorf("read workspace state: %w", err)
	}
	if wsState == nil {
		return nil, fmt.Errorf("the workspace is not deployed (no workspace state)")
	}
	d := &workspace.Deployed{
		Version:     workspace.DeployedVersion,
		GeneratedAt: now.UTC().Format(time.RFC3339),
		Workspace:   workspace.DeployedStack{Outputs: map[string]string{}},
		Pipelines:   map[string]workspace.DeployedPipeline{},
	}
	for _, k := range workspace.DeployedWorkspaceOutputs {
		if v := workspace.StackOutput(wsState, k); v != "" {
			d.Workspace.Outputs[k] = v
		}
	}
	d.Account = arnField(d.Workspace.Outputs["hosted_reader_role_arn"], 4)
	for _, p := range pipes {
		data, err := workspace.ReadStackState(ctx, s.workspace, filepath.Join(s.workspace, p.Dir))
		if err != nil {
			return nil, fmt.Errorf("read state of pipeline %s: %w", p.Name, err)
		}
		arn := workspace.StateMachineARNFromState(data)
		if arn == "" {
			continue // not deployed yet
		}
		d.Pipelines[filepath.Base(p.Dir)] = workspace.DeployedPipeline{StateMachineARN: arn}
		if d.Region == "" {
			d.Region = arnField(arn, 3)
		}
		if d.Account == "" {
			d.Account = arnField(arn, 4)
		}
	}
	if d.Region == "" {
		d.Region = fallbackRegion
	}
	return d, nil
}

// UploadHostedTree uploads the read-only tree copy and deployed.json to
// s3://<pipeline_bucket>/_clavesa/tree/ and removes objects there that the
// tree no longer has (a deleted pipeline). nil result and no error when
// the workspace is not hosted. Deploy calls it after a successful apply.
func (s *Service) UploadHostedTree(ctx context.Context) (*HostedTreeResult, error) {
	m, err := s.loadManifest()
	if err != nil {
		return nil, err
	}
	if m.Hosted == nil || m.Hosted.IsLocal() {
		return nil, nil
	}
	// The apply just changed the outputs; read them fresh.
	workspace.ForgetCachedState()
	bucket := workspace.PipelineBucket(s.workspace)
	if bucket == "" {
		return nil, fmt.Errorf("hosted tree copy: the workspace is not deployed (no pipeline_bucket output)")
	}
	pipes, err := s.ListPipelines()
	if err != nil {
		return nil, fmt.Errorf("hosted tree copy: list pipelines: %w", err)
	}
	dirs := make([]string, len(pipes))
	for i, p := range pipes {
		dirs[i] = p.Dir
	}
	files, err := HostedTreeFiles(s.workspace, dirs)
	if err != nil {
		return nil, fmt.Errorf("hosted tree copy: %w", err)
	}
	store, err := newHostedTreeStore(ctx, s.workspace)
	if err != nil {
		return nil, fmt.Errorf("hosted tree copy: %w", err)
	}
	d, err := s.BuildDeployed(ctx, pipes, store.Region(), time.Now())
	if err != nil {
		return nil, fmt.Errorf("hosted tree copy: %w", err)
	}
	dj, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return nil, err
	}
	files = append(files, HostedTreeFile{Key: workspace.DeployedFile, Body: append(dj, '\n')})

	res := &HostedTreeResult{URI: "s3://" + bucket + "/" + HostedTreePrefix}
	keep := map[string]bool{}
	for _, f := range files {
		body := f.Body
		if f.Path != "" {
			if body, err = os.ReadFile(f.Path); err != nil {
				return nil, fmt.Errorf("hosted tree copy: %w", err)
			}
		}
		key := HostedTreePrefix + f.Key
		if err := store.Put(ctx, bucket, key, body); err != nil {
			return nil, fmt.Errorf("hosted tree copy: put s3://%s/%s: %w", bucket, key, err)
		}
		keep[key] = true
		res.Uploaded = append(res.Uploaded, f.Key)
	}
	existing, err := store.List(ctx, bucket, HostedTreePrefix)
	if err != nil {
		return nil, fmt.Errorf("hosted tree copy: list s3://%s/%s: %w", bucket, HostedTreePrefix, err)
	}
	var stale []string
	for _, k := range existing {
		if !keep[k] {
			stale = append(stale, k)
		}
	}
	if len(stale) > 0 {
		if err := store.Delete(ctx, bucket, stale); err != nil {
			return nil, fmt.Errorf("hosted tree copy: remove stale objects: %w", err)
		}
		for _, k := range stale {
			res.Removed = append(res.Removed, strings.TrimPrefix(k, HostedTreePrefix))
		}
	}
	return res, nil
}
