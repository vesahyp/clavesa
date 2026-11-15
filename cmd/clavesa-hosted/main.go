// Command clavesa-hosted is the hosted read-only UI's Lambda (ADR-032,
// slice 6): the HTTP API behind https://ui.clavesa.dev and the summary
// refresh on EventBridge events, in one binary. Build it with
// `make hosted-build-lambda` (linux/arm64, provided.al2023, -tags nodist
// so the SPA is not embedded: CloudFront serves it from S3).
//
// Environment (set by hosted/infra/api.tf):
//
//	REGISTRY            JSON array of workspaces (hosted/infra/workspaces.tf)
//	DATA_BUCKET         the private bucket of summaries/<name>.json
//	READER_PRINCIPAL    this function's role ARN (the well-known document)
//	EVENT_BUS_ARN       the clavesa-ui bus
//	PUBLISHER_ROLE_ARN  the role local workspaces publish through
//	WORK_ROOT           where tree copies are synced (default /tmp/ws)
package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vesahyp/clavesa/internal/hosted"
	"github.com/vesahyp/clavesa/internal/service"
)

func main() {
	log.SetFlags(0)
	ctx := context.Background()
	reg, err := hosted.ParseRegistry([]byte(os.Getenv("REGISTRY")))
	if err != nil {
		log.Fatalf("clavesa-hosted: %v", err)
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("clavesa-hosted: load AWS config: %v", err)
	}
	root := os.Getenv("WORK_ROOT")
	if root == "" {
		root = "/tmp/ws"
	}
	wk := service.HostedWellKnown{
		ReaderPrincipal:  os.Getenv("READER_PRINCIPAL"),
		EventBusARN:      os.Getenv("EVENT_BUS_ARN"),
		PublisherRoleARN: os.Getenv("PUBLISHER_ROLE_ARN"),
		DataBucket:       os.Getenv("DATA_BUCKET"),
		Region:           cfg.Region,
	}
	store := hosted.S3SummaryStore{Client: s3.NewFromConfig(cfg), Bucket: wk.DataBucket}
	srv := hosted.NewServer(reg, wk, store, hosted.NewWorkspaces(cfg, root))
	lambda.StartWithOptions(srv.Invoke, lambda.WithContext(ctx))
}
