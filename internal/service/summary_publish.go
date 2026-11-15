package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	wk, err := hostedWellKnown(ctx, m.Hosted.URL)
	if err != nil {
		return nil, err
	}
	if wk.PublisherRoleARN == "" || wk.DataBucket == "" {
		return nil, fmt.Errorf("%s%s names no publisher role or data bucket", m.Hosted.URL, HostedWellKnownPath)
	}
	name := sum.Name
	if name == "" {
		name = m.Name
	}
	body, err := json.Marshal(sum)
	if err != nil {
		return nil, err
	}
	put, err := newSummaryPutter(ctx, s.workspace, "clavesa-publish-"+sessionSafe(name), wk)
	if err != nil {
		return nil, fmt.Errorf("publish summary: %w", err)
	}
	key := HostedSummaryKey(name)
	if err := put.PutSummary(ctx, wk.DataBucket, key, body); err != nil {
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
