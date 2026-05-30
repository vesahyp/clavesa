package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// emptyVersionedBucket pages ListObjectVersions on `bucketName` and
// deletes every object version + delete marker in 1000-key batches via
// DeleteObjects. Required before `terraform destroy` on the workspace
// bucket — versioning is on (see `modules/workspace/aws/main.tf`), so
// `aws_s3_bucket.workspace_bucket` won't delete while versioned objects
// or delete markers remain.
//
// Idempotent on a missing bucket (catches NoSuchBucket) — no-op return.
func emptyVersionedBucket(ctx context.Context, bucketName string, out io.Writer) error {
	awsCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(awsCtx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	sc := s3.NewFromConfig(cfg)

	var keyMarker, versionMarker *string
	deleted := 0
	for {
		resp, err := sc.ListObjectVersions(awsCtx, &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucketName),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionMarker,
		})
		if err != nil {
			var nsb *s3types.NoSuchBucket
			if errors.As(err, &nsb) {
				fmt.Fprintf(out, "→ S3 bucket %s does not exist; nothing to empty.\n", bucketName)
				return nil
			}
			// `NoSuchBucket` sometimes comes through as a generic API
			// error in older endpoints — fall back to string match.
			if strings.Contains(err.Error(), "NoSuchBucket") {
				fmt.Fprintf(out, "→ S3 bucket %s does not exist; nothing to empty.\n", bucketName)
				return nil
			}
			return fmt.Errorf("list object versions in %s: %w", bucketName, err)
		}

		var ids []s3types.ObjectIdentifier
		for _, v := range resp.Versions {
			ids = append(ids, s3types.ObjectIdentifier{
				Key:       v.Key,
				VersionId: v.VersionId,
			})
		}
		for _, m := range resp.DeleteMarkers {
			ids = append(ids, s3types.ObjectIdentifier{
				Key:       m.Key,
				VersionId: m.VersionId,
			})
		}

		for len(ids) > 0 {
			batch := ids
			if len(batch) > 1000 {
				batch = ids[:1000]
				ids = ids[1000:]
			} else {
				ids = nil
			}
			quiet := true
			_, err := sc.DeleteObjects(awsCtx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucketName),
				Delete: &s3types.Delete{
					Objects: batch,
					Quiet:   &quiet,
				},
			})
			if err != nil {
				return fmt.Errorf("delete object versions in %s: %w", bucketName, err)
			}
			deleted += len(batch)
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		keyMarker = resp.NextKeyMarker
		versionMarker = resp.NextVersionIdMarker
	}

	if deleted == 0 {
		fmt.Fprintf(out, "→ S3 bucket %s is already empty.\n", bucketName)
		return nil
	}
	fmt.Fprintf(out, "[clavesa] emptied %d object versions from s3://%s\n", deleted, bucketName)
	return nil
}

// drainAthenaWorkgroup deletes `workgroupName` with RecursiveDeleteOption
// so query result history doesn't block the delete. Required before
// `terraform destroy` because the workspace module sets
// `force_destroy = var.force_destroy` (default false in user-facing
// emits) and even with `true` the AWS provider doesn't pass
// RecursiveDeleteOption when stored queries / named queries linger.
//
// terraform destroy will then silently drop the gone resource from
// state. Idempotent: a missing workgroup returns nil.
func drainAthenaWorkgroup(ctx context.Context, workgroupName string, out io.Writer) error {
	awsCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(awsCtx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	ac := athena.NewFromConfig(cfg)

	// Pre-check existence so the success log doesn't claim a drain that
	// didn't happen (DeleteWorkGroup is idempotent — returns nil on a
	// missing workgroup, no typed NotFound error).
	_, getErr := ac.GetWorkGroup(awsCtx, &athena.GetWorkGroupInput{
		WorkGroup: aws.String(workgroupName),
	})
	if getErr != nil {
		var ire *athenatypes.InvalidRequestException
		if errors.As(getErr, &ire) && strings.Contains(strings.ToLower(getErr.Error()), "not found") {
			fmt.Fprintf(out, "→ Athena workgroup %s does not exist; nothing to drain.\n", workgroupName)
			return nil
		}
		if strings.Contains(getErr.Error(), "WorkGroup is not found") || strings.Contains(getErr.Error(), "does not exist") {
			fmt.Fprintf(out, "→ Athena workgroup %s does not exist; nothing to drain.\n", workgroupName)
			return nil
		}
		return fmt.Errorf("get athena workgroup %s: %w", workgroupName, getErr)
	}

	recursive := true
	_, err = ac.DeleteWorkGroup(awsCtx, &athena.DeleteWorkGroupInput{
		WorkGroup:             aws.String(workgroupName),
		RecursiveDeleteOption: &recursive,
	})
	if err != nil {
		// DeleteWorkGroup on a missing workgroup returns
		// InvalidRequestException with "WorkGroup is not found" — there
		// is no typed NotFound in the Athena SDK for this op.
		var ire *athenatypes.InvalidRequestException
		if errors.As(err, &ire) && strings.Contains(strings.ToLower(err.Error()), "not found") {
			fmt.Fprintf(out, "→ Athena workgroup %s does not exist; nothing to drain.\n", workgroupName)
			return nil
		}
		if strings.Contains(err.Error(), "WorkGroup is not found") || strings.Contains(err.Error(), "does not exist") {
			fmt.Fprintf(out, "→ Athena workgroup %s does not exist; nothing to drain.\n", workgroupName)
			return nil
		}
		return fmt.Errorf("delete athena workgroup %s: %w", workgroupName, err)
	}
	fmt.Fprintf(out, "[clavesa] drained Athena workgroup %s\n", workgroupName)
	return nil
}

// autoYesReader returns a reader that yields "yes\n" once and then EOF.
// Used by `--yes` paths to feed the existing interactive sweep
// confirmation without prompting the operator.
func autoYesReader() io.Reader {
	return strings.NewReader("yes\n")
}

