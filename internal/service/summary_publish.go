package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/vesahyp/clavesa/internal/observability"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// HostedSummaryKey is the data-bucket key of a workspace's stored summary
// (ADR-032 section 2): summaries/<name>.json.
func HostedSummaryKey(name string) string { return "summaries/" + name + ".json" }

// PublishResult reports one `workspace summary --publish`.
type PublishResult struct {
	URI         string `json:"uri"`
	GeneratedAt string `json:"generated_at"`
}

// SummaryPutter writes the summary object. Tests swap newSummaryPutter.
type SummaryPutter interface {
	PutSummary(ctx context.Context, bucket, key string, body []byte) error
}

// newSummaryPutter assumes the hosted UI's publisher role with the
// workspace's own AWS identity (the Mac's profile; no new key) and
// returns an S3 writer in the data bucket's region.
var newSummaryPutter = func(ctx context.Context, root, sessionName string, wk HostedWellKnown) (SummaryPutter, error) {
	cfg, err := workspace.LoadAWSConfig(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	prov := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), wk.PublisherRoleARN, func(o *stscreds.AssumeRoleOptions) {
		o.RoleSessionName = sessionName
		o.Duration = 15 * time.Minute
	})
	pub := cfg.Copy()
	pub.Credentials = aws.NewCredentialsCache(prov)
	if wk.Region != "" {
		pub.Region = wk.Region
	}
	return s3SummaryPutter{c: s3.NewFromConfig(pub)}, nil
}

type s3SummaryPutter struct{ c *s3.Client }

func (p s3SummaryPutter) PutSummary(ctx context.Context, bucket, key string, body []byte) error {
	_, err := p.c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(body),
		ContentType:  aws.String("application/json"),
		CacheControl: aws.String("no-cache"),
	})
	return err
}

// PublishSummary writes sum to the hosted UI's data bucket at
// summaries/<workspace name>.json through the publisher role (ADR-032
// section 2, "Local-warehouse workspaces: pushed from the Mac"). The
// bucket, region and role come from the hosted UI's well-known document,
// read from the workspace's hosted block URL. The publisher role may
// write only the names registered as local-warehouse workspaces in
// hosted/infra/workspaces.tf; any other name is refused by S3.
func (s *Service) PublishSummary(ctx context.Context, sum *WorkspaceSummary) (*PublishResult, error) {
	m, err := s.loadManifest()
	if err != nil {
		return nil, err
	}
	if m.Hosted == nil {
		return nil, fmt.Errorf("the workspace is not hosted: run clavesa workspace hosted enable --url <url> first")
	}
	// A local workspace's block records the publisher role and the data
	// bucket at enable time; any other block reads them from the
	// well-known document.
	var wk HostedWellKnown
	if m.Hosted.IsLocal() {
		wk = HostedWellKnown{PublisherRoleARN: m.Hosted.PublisherRoleARN, DataBucket: m.Hosted.DataBucket, Region: m.Hosted.Region}
	} else if wk, err = hostedWellKnown(ctx, m.Hosted.URL); err != nil {
		return nil, err
	}
	if wk.PublisherRoleARN == "" || wk.DataBucket == "" {
		return nil, fmt.Errorf("%s%s names no publisher role or data bucket", m.Hosted.URL, HostedWellKnownPath)
	}
	name := sum.Name
	if name == "" {
		name = m.Name
	}
	// A local-warehouse workspace has no live read path from the hosted
	// UI: its published summary carries the drill-down the Pipeline and
	// Run pages draw from (ADR-032 section 2).
	if sum.Kind == "local" && sum.Drill == nil {
		withDrill := *sum
		withDrill.Drill = s.SummaryDrill(ctx, sum)
		sum = &withDrill
	}
	body, err := marshalWithinCap(sum, PublishMaxBytes)
	if err != nil {
		return nil, err
	}
	// No path on the Mac leaves it (ADR-032; slice 5 review, finding 4):
	// a snapshot whose bytes still name the workspace root is dropped.
	if abs, _ := filepath.Abs(s.workspace); abs != "" && sum.Drill != nil && sum.Drill.Snapshot != nil && bytes.Contains(body, []byte(abs)) {
		cp, d := *sum, *sum.Drill
		d.Snapshot, cp.Drill = nil, &d
		if body, err = marshalWithinCap(&cp, PublishMaxBytes); err != nil {
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "Hosted: snapshot not published: it named a path on this machine\n")
	}
	// The upload has its own time, so a slow summary or snapshot never
	// loses the whole publish (slice 5 review, finding 3).
	putCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), PutTimeout)
	defer cancel()
	put, err := newSummaryPutter(putCtx, s.workspace, "clavesa-publish-"+sessionSafe(name), wk)
	if err != nil {
		return nil, fmt.Errorf("publish summary: %w", err)
	}
	key := HostedSummaryKey(name)
	if err := put.PutSummary(putCtx, wk.DataBucket, key, body); err != nil {
		return nil, fmt.Errorf("publish summary to s3://%s/%s: %w", wk.DataBucket, key, err)
	}
	return &PublishResult{URI: "s3://" + wk.DataBucket + "/" + key, GeneratedAt: sum.GeneratedAt}, nil
}

// sessionSafe keeps the characters an STS role session name allows, cut
// to fit the 64-character limit with its prefix.
func sessionSafe(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s) && len(out) < 40; i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '@' || c == '=' || c == ',' {
			out = append(out, c)
		}
	}
	return string(out)
}

// RunEndPublishTimeout bounds the summary and the upload at the end of a
// run, so a slow network never holds up a nightly job for long.
const RunEndPublishTimeout = 30 * time.Second

// PublishAfterRun computes the workspace summary and publishes it when
// the workspace is a local-warehouse workspace with a hosted block
// (ADR-032 section 2, "pushed from the Mac"). It returns (nil, nil) when
// there is nothing to publish. `clavesa pipeline run` calls it after a
// local run, failed or not; its error never changes the run's result.
func (s *Service) PublishAfterRun(ctx context.Context) (*PublishResult, error) {
	m, err := s.loadManifest()
	if err != nil || m == nil || !m.Hosted.IsLocal() {
		return nil, nil
	}
	if workspace.LoadWarehouse(s.workspace) != workspace.WarehouseLocal {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, RunEndPublishTimeout)
	defer cancel()
	sum, err := s.WorkspaceSummary(ctx, WorkspaceSummaryQuery{})
	if err != nil {
		return nil, err
	}
	return s.PublishSummary(ctx, sum)
}

// PublishMaxBytes caps a published summary (ADR-036). The runs part is
// about 100 to 160 KB; the snapshot fills the rest.
const PublishMaxBytes = 2 << 20

// marshalWithinCap marshals sum, and when it is over max drops the
// snapshot's widget results, then its table snapshots, one at a time, each
// with a note, until it fits. The runs part is never dropped.
func marshalWithinCap(sum *WorkspaceSummary, max int) ([]byte, error) {
	body, err := json.Marshal(sum)
	if err != nil || len(body) <= max || sum.Drill == nil || sum.Drill.Snapshot == nil {
		return body, err
	}
	cp := *sum
	d := *sum.Drill
	snap := *sum.Drill.Snapshot
	d.Snapshot, cp.Drill = &snap, &d
	widgets := make(map[string]*observability.QueryResult, len(snap.Widgets))
	for k, v := range snap.Widgets {
		widgets[k] = v
	}
	snaps := make(map[string]*observability.SnapshotsResult, len(snap.Snapshots))
	for k, v := range snap.Snapshots {
		snaps[k] = v
	}
	snap.Widgets, snap.Snapshots = widgets, snaps
	for len(body) > max {
		switch {
		case len(widgets) > 0:
			for k := range widgets {
				delete(widgets, k)
				break
			}
			snap.Notes = append(snap.Notes, "a dashboard widget result was left out: the published summary is capped at 2 MB")
		case len(snaps) > 0:
			for k := range snaps {
				delete(snaps, k)
				break
			}
			snap.Notes = append(snap.Notes, "a table's snapshots were left out: the published summary is capped at 2 MB")
		default:
			return body, nil
		}
		if body, err = json.Marshal(&cp); err != nil {
			return nil, err
		}
	}
	return body, nil
}

// PutTimeout bounds the role assume and the upload of a publish, apart
// from the time the summary and its snapshot took.
const PutTimeout = 15 * time.Second
