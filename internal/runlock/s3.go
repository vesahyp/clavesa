package runlock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3API is the slice of the S3 client the lock needs. *s3.Client satisfies
// it; tests fake it with in-memory conditional semantics.
type S3API interface {
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// s3Backend stores the lease at `s3://<bucket>/<key>` where key is
// `<pipeline>/_locks/run.json` under the warehouse prefix — inside the
// per-pipeline IAM grant (`${bucket}/${pipeline}/*/*`, tfgen.go) by design.
// CAS rides S3's conditional writes: If-None-Match: * creates only when the
// object is absent; If-Match: <etag> replaces only when nobody moved it.
// Release writes the `state: "released"` tombstone via If-Match rather than
// DELETE — S3 DELETE carries no If-Match, so a conditional delete can't be
// made race-free.
type s3Backend struct {
	client S3API
	bucket string
	key    string
}

func (b *s3Backend) where() string { return "s3://" + b.bucket + "/" + b.key }

func (b *s3Backend) get(ctx context.Context) (*leaseDoc, string, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, "", errNotExist
		}
		return nil, "", err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}
	var doc leaseDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, "", fmt.Errorf("parse lease %s: %w", b.where(), err)
	}
	return &doc, aws.ToString(out.ETag), nil
}

func (b *s3Backend) create(ctx context.Context, doc *leaseDoc) (string, error) {
	return b.put(ctx, doc, func(in *s3.PutObjectInput) {
		in.IfNoneMatch = aws.String("*")
	})
}

func (b *s3Backend) replace(ctx context.Context, token string, doc *leaseDoc) (string, error) {
	return b.put(ctx, doc, func(in *s3.PutObjectInput) {
		in.IfMatch = aws.String(token)
	})
}

func (b *s3Backend) put(ctx context.Context, doc *leaseDoc, cond func(*s3.PutObjectInput)) (string, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	in := &s3.PutObjectInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(b.key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}
	cond(in)
	out, err := b.client.PutObject(ctx, in)
	if err != nil {
		if isConditionalFailure(err) {
			return "", errRaced
		}
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

// isNotFound matches the GET-on-absent-key failure (NoSuchKey / 404).
func isNotFound(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	var re *smithyhttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound
}

// isConditionalFailure matches a conditional PUT losing its race:
// 412 PreconditionFailed (If-Match / If-None-Match miss) and
// 409 ConditionalRequestConflict (concurrent conditional writers).
func isConditionalFailure(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		code := re.HTTPStatusCode()
		return code == http.StatusPreconditionFailed || code == http.StatusConflict
	}
	return false
}
