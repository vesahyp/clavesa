package hosted

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/ui"
	"github.com/vesahyp/clavesa/internal/workspace"
	"github.com/vesahyp/clavesa/internal/wsdeps"
)

// ---------------------------------------------------------------------------
// The data bucket
// ---------------------------------------------------------------------------

// S3SummaryStore is the SummaryStore in the data bucket, read and written
// with the Lambda's own role.
type S3SummaryStore struct {
	Client *s3.Client
	Bucket string
}

func (s S3SummaryStore) Get(ctx context.Context, name string) ([]byte, error) {
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket), Key: aws.String(service.HostedSummaryKey(name)),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil
		}
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// GetStats reads stats/ui.json (StatsReader); (nil, nil) when it is not
// there yet.
func (s S3SummaryStore) GetStats(ctx context.Context) ([]byte, error) {
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket), Key: aws.String(StatsKey),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil
		}
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s S3SummaryStore) Put(ctx context.Context, name string, body []byte) error {
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(s.Bucket),
		Key:          aws.String(service.HostedSummaryKey(name)),
		Body:         bytes.NewReader(body),
		ContentType:  aws.String("application/json"),
		CacheControl: aws.String("no-cache"),
	})
	return err
}

// ---------------------------------------------------------------------------
// Cloud workspaces through their reader roles
// ---------------------------------------------------------------------------

// treeCheckInterval is how often a warm instance asks whether the tree
// copy changed (one HeadObject on deployed.json).
const treeCheckInterval = 30 * time.Second

// Workspaces opens cloud workspaces for the Lambda. Per workspace it:
//
//  1. assumes the reader role (ExternalId = the workspace name, 900 s)
//     through a credential cache that renews before expiry;
//  2. syncs s3://<bucket>/_clavesa/tree/ into <Root>/<name>/, again when
//     deployed.json's ETag changes;
//  3. registers the assumed config for that directory with
//     workspace.SetAWSConfigOverride, in the region deployed.json names,
//     so every AWS client of the read paths (wsdeps' clients and the
//     cloud observability provider, the pipelinestatus handler, the
//     service's lazy clients) reads as the reader role;
//  4. builds the deps (wsdeps.New) and the read-only mux, cached for the
//     life of the warm instance.
type Workspaces struct {
	Base aws.Config
	Root string
	// Store reads the stored summaries a local-warehouse workspace is
	// served from (OpenLocal). Nil: local workspaces cannot be opened.
	Store SummaryStore

	mu    sync.Mutex
	cache map[string]*openWorkspace
}

type openWorkspace struct {
	ws      *Workspace
	deps    *wsdeps.Deps
	cfg     aws.Config
	s3      *s3.Client
	etag    string
	checked time.Time
	// sum is the hash of the stored summary a local workspace was built
	// from.
	sum [32]byte
}

// roleARN is the role this view of the workspace assumes.
func (e Entry) roleARN() string {
	if e.Admin {
		return e.AdminRoleARN
	}
	return e.ReaderRoleARN
}

// viewName names the view in the cache and the tree root: the workspace
// name, with "--admin" for an admin's view ("--" cannot start or end a
// workspace name segment the registry accepts).
func (e Entry) viewName() string {
	if e.Admin {
		return e.Name + "--admin"
	}
	return e.Name
}

// NewWorkspaces returns the opener; base is the Lambda's own config (the
// STS client that assumes each reader role uses it).
func NewWorkspaces(base aws.Config, root string) *Workspaces {
	return &Workspaces{Base: base, Root: root, cache: map[string]*openWorkspace{}}
}

func (w *Workspaces) readerConfig(e Entry) aws.Config {
	stsCfg := w.Base.Copy()
	stsCfg.Region = e.Region
	prov := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(stsCfg), e.roleARN(), func(o *stscreds.AssumeRoleOptions) {
		o.ExternalID = aws.String(e.Name)
		o.RoleSessionName = "clavesa-hosted-" + e.Name
		if len(o.RoleSessionName) > 64 {
			o.RoleSessionName = o.RoleSessionName[:64]
		}
		o.Duration = 15 * time.Minute
	})
	cfg := w.Base.Copy()
	cfg.Region = e.Region
	cfg.Credentials = aws.NewCredentialsCache(prov, func(o *aws.CredentialsCacheOptions) {
		o.ExpiryWindow = 2 * time.Minute
	})
	return cfg
}

// Open returns the workspace, syncing its tree copy when it changed.
func (w *Workspaces) Open(ctx context.Context, e Entry) (*Workspace, error) {
	if e.Kind == KindLocal {
		return w.openLocal(ctx, e)
	}
	if e.Kind != KindCloud {
		return nil, fmt.Errorf("workspace %s is not a cloud workspace", e.Name)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	// An admin's view (column tags) is its own: another role, another
	// tree root, another cache entry.
	key := e.viewName()
	ow := w.cache[key]
	if ow == nil {
		cfg := w.readerConfig(e)
		ow = &openWorkspace{cfg: cfg, s3: s3.NewFromConfig(cfg)}
		w.cache[key] = ow
	}
	if ow.ws != nil && time.Since(ow.checked) < treeCheckInterval {
		return ow.ws, nil
	}
	head, err := ow.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(e.Bucket), Key: aws.String(service.HostedTreePrefix + workspace.DeployedFile),
	})
	if err != nil {
		if ow.ws != nil {
			// Keep serving the copy we have; the next request checks again.
			return ow.ws, nil
		}
		return nil, fmt.Errorf("read s3://%s/%s%s as %s: %w (is the workspace deployed with its hosted block?)",
			e.Bucket, service.HostedTreePrefix, workspace.DeployedFile, e.roleARN(), err)
	}
	ow.checked = time.Now()
	etag := aws.ToString(head.ETag)
	if ow.ws != nil && etag == ow.etag {
		return ow.ws, nil
	}
	root := filepath.Join(w.Root, key)
	if err := syncTree(ctx, ow.s3, e, root); err != nil {
		return nil, err
	}
	cfg := ow.cfg.Copy()
	if d, err := workspace.LoadDeployed(root); err == nil && d != nil && d.Region != "" {
		cfg.Region = d.Region
	}
	workspace.SetAWSConfigOverride(root, cfg)
	if ow.deps != nil {
		ow.deps.Close()
	}
	deps := wsdeps.New(ctx, root, wsdeps.Options{Admin: e.Admin})
	ow.deps = deps
	ow.etag = etag
	ow.ws = &Workspace{
		Entry: e,
		Root:  root,
		Summarize: func(ctx context.Context) (*service.WorkspaceSummary, error) {
			return deps.Svc.WorkspaceSummary(ctx, service.WorkspaceSummaryQuery{})
		},
		Handler: ui.BuildWorkspaceMux(ui.WorkspaceDeps{
			Deps:     deps,
			ReadOnly: true,
		}),
	}
	return ow.ws, nil
}

// openLocal returns the summary-backed workspace of a local-warehouse
// entry, rebuilt when the stored summary changed (checked at most every
// treeCheckInterval).
func (w *Workspaces) openLocal(ctx context.Context, e Entry) (*Workspace, error) {
	if w.Store == nil {
		return nil, fmt.Errorf("workspace %s: no summary store", e.Name)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	ow := w.cache[e.Name]
	if ow != nil && ow.ws != nil && time.Since(ow.checked) < treeCheckInterval {
		return ow.ws, nil
	}
	body, err := w.Store.Get(ctx, e.Name)
	if err != nil {
		if ow != nil && ow.ws != nil {
			return ow.ws, nil // keep serving what we have
		}
		return nil, fmt.Errorf("read the stored summary: %w", err)
	}
	if body == nil {
		return nil, ErrNoSummary
	}
	h := sha256.Sum256(body)
	if ow != nil && ow.ws != nil && ow.sum == h {
		ow.checked = time.Now()
		return ow.ws, nil
	}
	ws, deps, err := OpenLocal(ctx, filepath.Join(w.Root, e.Name), e, body)
	if err != nil {
		return nil, err
	}
	if ow != nil && ow.deps != nil {
		ow.deps.Close()
	}
	w.cache[e.Name] = &openWorkspace{ws: ws, deps: deps, sum: h, checked: time.Now()}
	return ws, nil
}

// syncTree replaces root with the tree copy under _clavesa/tree/ of
// e.Bucket.
func syncTree(ctx context.Context, c *s3.Client, e Entry, root string) error {
	bucket := e.Bucket
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(service.HostedTreePrefix),
	})
	n := 0
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list s3://%s/%s: %w", bucket, service.HostedTreePrefix, err)
		}
		for _, o := range page.Contents {
			key := aws.ToString(o.Key)
			rel := strings.TrimPrefix(key, service.HostedTreePrefix)
			if rel == "" || strings.HasSuffix(rel, "/") {
				continue
			}
			dst := filepath.Join(root, filepath.FromSlash(rel))
			if !strings.HasPrefix(dst, filepath.Clean(root)+string(filepath.Separator)) {
				continue // a key that climbs out of the tree
			}
			out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			if err != nil {
				return fmt.Errorf("get s3://%s/%s: %w", bucket, key, err)
			}
			body, err := io.ReadAll(out.Body)
			out.Body.Close()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(dst, body, 0o644); err != nil {
				return err
			}
			n++
		}
	}
	if n == 0 {
		return fmt.Errorf("s3://%s/%s is empty", bucket, service.HostedTreePrefix)
	}
	return nil
}

