package runner

import (
	"context"
	"os"
	"path/filepath"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// AWSEnvDockerArgs returns the `docker run` -e/-v arguments that hand a
// host-spawned runner container working AWS credentials. Used by every
// container that must reach AWS — s3 source inputs on local runs, and any
// container pointed at a cloud (`s3://`) warehouse (ADR-024).
//
// Credentials are resolved HOST-SIDE via the Go SDK's default chain and
// injected as explicit AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
// AWS_SESSION_TOKEN env vars. The host SDK understands every profile
// shape — static keys in ~/.aws/credentials OR ~/.aws/config, SSO,
// credential_process — while the JVMs inside the container do not: the
// Java v1 SDK (the Glue Hive metastore client, S3A's provider chain)
// cannot read static keys that live only in ~/.aws/config and can never
// resolve SSO profiles. Explicit env creds are step one of every SDK's
// chain, so injecting them works for all consumers at once. The resolved
// region rides along the same way.
//
// The host's AWS_* env and a read-only ~/.aws mount are still forwarded
// as a fallback for whatever the host-side resolution didn't cover, and
// CLAVESA_S3_ENDPOINT so moto/MinIO test infra keeps working without an
// image rebuild.
//
// Caveat: injected credentials are a point-in-time snapshot — a session
// credential (SSO) expires and is NOT refreshed inside a long-lived warm
// worker; the next worker respawn re-resolves. Static keys don't expire.
func AWSEnvDockerArgs(ctx context.Context) []string {
	var args []string
	for _, name := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE",
		"CLAVESA_S3_ENDPOINT",
	} {
		if v, ok := os.LookupEnv(name); ok {
			args = append(args, "-e", name+"="+v)
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		awsDir := filepath.Join(home, ".aws")
		if st, err := os.Stat(awsDir); err == nil && st.IsDir() {
			args = append(args, "-v", awsDir+":/root/.aws:ro")
			// Lets the Java v1 SDK read ~/.aws/config at all (no-op for
			// boto3); kept as belt-and-braces under the explicit
			// injection below.
			args = append(args, "-e", "AWS_SDK_LOAD_CONFIG=1")
		}
	}
	// Host-side resolution. Best-effort: when the host has no AWS context
	// at all this resolves nothing and the container simply gets the
	// passthrough above (matching the old behavior).
	if cfg, err := awsconfig.LoadDefaultConfig(ctx); err == nil {
		if creds, err := cfg.Credentials.Retrieve(ctx); err == nil && creds.AccessKeyID != "" {
			args = append(args,
				"-e", "AWS_ACCESS_KEY_ID="+creds.AccessKeyID,
				"-e", "AWS_SECRET_ACCESS_KEY="+creds.SecretAccessKey,
			)
			if creds.SessionToken != "" {
				args = append(args, "-e", "AWS_SESSION_TOKEN="+creds.SessionToken)
			}
		}
		if cfg.Region != "" {
			args = append(args,
				"-e", "AWS_REGION="+cfg.Region,
				"-e", "AWS_DEFAULT_REGION="+cfg.Region,
			)
		}
	}
	return args
}
