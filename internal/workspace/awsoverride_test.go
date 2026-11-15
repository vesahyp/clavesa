package workspace

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// TestAWSConfigOverride: a registered config wins for that root (the
// hosted Lambda's assumed reader role), and only for that root.
func TestAWSConfigOverride(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	root := t.TempDir()
	other := t.TempDir()
	cfg := aws.Config{Region: "eu-north-1", Credentials: credentials.NewStaticCredentialsProvider("AKIDREADER", "s", "t")}
	SetAWSConfigOverride(filepath.Join(root, "."), cfg)
	t.Cleanup(func() { ClearAWSConfigOverride(root) })

	got, err := LoadAWSConfig(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := got.Credentials.Retrieve(context.Background())
	if err != nil || creds.AccessKeyID != "AKIDREADER" || got.Region != "eu-north-1" {
		t.Errorf("override not used: %+v %v region %s", creds, err, got.Region)
	}
	if got, err := LoadAWSConfig(context.Background(), other); err == nil && got.Credentials != nil {
		if c, err := got.Credentials.Retrieve(context.Background()); err == nil && c.AccessKeyID == "AKIDREADER" {
			t.Error("the override leaked to another root")
		}
	}
	ClearAWSConfigOverride(root)
	if _, ok := AWSConfigOverride(root); ok {
		t.Error("clear left the override")
	}
}
