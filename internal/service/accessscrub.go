package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// progressScrubbedKey records that a workspace's _progress/ markers were
// scrubbed of error text once. Markers written later carry none: the
// runner and the run marker writer leave it out in a workspace with
// column tags.
const progressScrubbedKey = "_clavesa/access/progress-scrubbed.json"

type progressScrubS3 interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// scrubProgressErrors removes the error text ("error" and "error_msg")
// from every _progress/ marker of the bucket, once: markers written
// before column tags could quote a row, and the hosted reader may read
// _progress/. It returns how many markers it rewrote, and 0 when the
// bucket was scrubbed before.
func scrubProgressErrors(ctx context.Context, c progressScrubS3, bucket string) (int, error) {
	if _, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(progressScrubbedKey)}); err == nil {
		return 0, nil
	} else {
		var nk *s3types.NoSuchKey
		if !errors.As(err, &nk) {
			return 0, fmt.Errorf("read %s: %w", progressScrubbedKey, err)
		}
	}
	n := 0
	var token *string
	for {
		page, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("_progress/"), ContinuationToken: token})
		if err != nil {
			return n, fmt.Errorf("list _progress/: %w", err)
		}
		for _, o := range page.Contents {
			key := aws.ToString(o.Key)
			if !strings.HasSuffix(key, ".json") {
				continue
			}
			changed, err := scrubProgressObject(ctx, c, bucket, key)
			if err != nil {
				return n, err
			}
			if changed {
				n++
			}
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
	}
	stamp, _ := json.Marshal(map[string]int{"rewritten": n})
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(progressScrubbedKey), Body: bytes.NewReader(stamp), ContentType: aws.String("application/json")}); err != nil {
		return n, fmt.Errorf("write %s: %w", progressScrubbedKey, err)
	}
	return n, nil
}

func scrubProgressObject(ctx context.Context, c progressScrubS3, bucket, key string) (bool, error) {
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return false, fmt.Errorf("get %s: %w", key, err)
	}
	body, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil {
		return false, err
	}
	var m map[string]any
	if json.Unmarshal(body, &m) != nil {
		return false, nil // not a marker
	}
	// Only a finished marker: a running node rewrites its own marker, and
	// a scrub between its GET and PUT must not put back an older status.
	if st, _ := m["status"].(string); !strings.EqualFold(st, "failed") {
		return false, nil
	}
	changed := false
	for _, f := range []string{"error", "error_msg"} {
		if s, ok := m[f].(string); ok && s != "" {
			m[f] = ""
			changed = true
		}
	}
	if !changed {
		return false, nil
	}
	nb, err := json.Marshal(m)
	if err != nil {
		return false, err
	}
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(nb), ContentType: aws.String("application/json")}); err != nil {
		return false, fmt.Errorf("put %s: %w", key, err)
	}
	return true, nil
}
