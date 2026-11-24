package service

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeScrubS3 struct {
	objs map[string]string
	puts int
}

func (f *fakeScrubS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := &s3.ListObjectsV2Output{}
	for _, k := range keys {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
	}
	return out, nil
}

func (f *fakeScrubS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	b, ok := f.objs[aws.ToString(in.Key)]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(b))}, nil
}

func (f *fakeScrubS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(in.Body)
	f.objs[aws.ToString(in.Key)] = buf.String()
	f.puts++
	return &s3.PutObjectOutput{}, nil
}

func TestScrubProgressErrors(t *testing.T) {
	f := &fakeScrubS3{objs: map[string]string{
		"_progress/r1/trips.json": `{"status":"failed","error":"bad row 203.0.113.7","updated_ms":1}`,
		"_progress/r1/_run.json":  `{"status":"FAILED","error_class":"ValueError","error_msg":"bad row 203.0.113.7"}`,
		"_progress/r2/trips.json": `{"status":"succeeded","error":""}`,
		"taxi/_warehouse/x.json":  `{"error":"not a marker"}`,
	}}
	n, err := scrubProgressErrors(context.Background(), f, "b")
	if err != nil || n != 2 {
		t.Fatalf("scrubbed %d, %v; want 2", n, err)
	}
	for k, v := range f.objs {
		if strings.HasPrefix(k, "_progress/") && strings.Contains(v, "203.0.113.7") {
			t.Errorf("%s keeps the text: %s", k, v)
		}
	}
	if !strings.Contains(f.objs["_progress/r1/_run.json"], `"error_class":"ValueError"`) {
		t.Errorf("error class lost: %s", f.objs["_progress/r1/_run.json"])
	}
	if f.objs["taxi/_warehouse/x.json"] != `{"error":"not a marker"}` {
		t.Error("an object outside _progress/ changed")
	}
	// Once only.
	f.puts = 0
	if n, err := scrubProgressErrors(context.Background(), f, "b"); err != nil || n != 0 || f.puts != 0 {
		t.Errorf("second scrub: %d, %v, %d put(s)", n, err, f.puts)
	}
}
