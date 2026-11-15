package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// The AWS region of a cloud workspace (GH #107).
//
// A workspace's AWS resources live in exactly one region: the one its
// workspace stack was applied in. That region is recorded in clavesa.json
// (`region`) and every AWS call and Terraform run of the workspace uses it.
// Without it they used the profile's default region, and a workspace
// deployed anywhere else read and deployed against the wrong region.
//
// Precedence, highest first:
//
//  1. The recorded region (clavesa.json `region`; deployed.json `region`
//     in a hosted tree copy).
//  2. AWS_REGION / AWS_DEFAULT_REGION in the environment.
//  3. The AWS profile's region.
//
// The recorded region wins over AWS_REGION on purpose. The workspace's
// resources exist in that region only, so any other region is always the
// wrong one; an AWS_REGION exported for some other project would otherwise
// silently send reads, and worse a deploy, to the wrong region. The CLI
// says so on stderr when the two differ. To move the region, edit
// `region` in clavesa.json.
//
// Before a workspace is deployed nothing is recorded (unless `workspace
// init` saw an explicit AWS_REGION) and 2 and 3 apply as before. `workspace
// deploy` records the region from the state it just applied. A workspace
// deployed before this field existed gets its region derived once from
// its workspace stack state (ResolveRegion) and persisted.

var regionRe = regexp.MustCompile(`^[a-z]{2}(?:-[a-z]+)+-\d+$`)

// ValidRegion reports whether r has the shape of an AWS region name.
func ValidRegion(r string) bool { return regionRe.MatchString(r) }

// RecordedRegion returns the workspace's recorded region without touching
// the network: deployed.json's region in a hosted tree copy, else
// clavesa.json's `region`. "" when none is recorded.
func RecordedRegion(root string) string {
	if d, err := LoadDeployed(root); err == nil && d != nil {
		return d.Region
	}
	m, err := Load(root)
	if err != nil || m == nil {
		return ""
	}
	return m.Region
}

// RecordRegion writes region to clavesa.json. A no-op when it is already
// recorded; an error for a directory with no clavesa.json or a value that
// is not a region name.
func RecordRegion(root, region string) error {
	region = strings.TrimSpace(region)
	if !ValidRegion(region) {
		return fmt.Errorf("%q is not an AWS region name", region)
	}
	m, err := Load(root)
	if err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("no clavesa.json in %s", root)
	}
	if m.Region == region {
		return nil
	}
	m.Region = region
	return SaveManifest(root, m)
}

// deriveMu serialises derive-and-persist, so concurrent UI requests do not
// write clavesa.json at the same time.
var deriveMu sync.Mutex

// ResolveRegion returns the workspace's region: the recorded one, or for a
// workspace with none recorded, the region derived from its workspace
// stack state, which is then persisted to clavesa.json (once; best effort).
// "" when the workspace is not deployed and records no region: callers
// then fall back to AWS_REGION and the profile.
func ResolveRegion(ctx context.Context, root string) string {
	if r := RecordedRegion(root); r != "" {
		return r
	}
	if d, err := LoadDeployed(root); err == nil && d != nil {
		return ""
	}
	deriveMu.Lock()
	defer deriveMu.Unlock()
	m, err := Load(root)
	if err != nil || m == nil {
		return ""
	}
	if m.Region != "" {
		return m.Region
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	data, err := ReadStackState(ctx, root, root)
	if err != nil || data == nil {
		return ""
	}
	r := RegionFromState(data)
	if r == "" {
		return ""
	}
	m.Region = r
	if err := SaveManifest(root, m); err != nil {
		fmt.Fprintf(os.Stderr, "workspace: could not record region %s in clavesa.json: %v\n", r, err)
	}
	return r
}

// arnRegionRe finds the region of a regional ARN anywhere in state JSON.
var arnRegionRe = regexp.MustCompile(`"arn:aws[a-z-]*:[a-z0-9-]+:([a-z]{2}(?:-[a-z]+)+-\d+):\d{12}:`)

// ecrHostRe finds the region in an ECR registry host.
var ecrHostRe = regexp.MustCompile(`\.dkr\.ecr\.([a-z]{2}(?:-[a-z]+)+-\d+)\.amazonaws\.com`)

// RegionFromState derives the region a workspace stack was applied in from
// its Terraform state JSON. It reads the stack's own resources, never the
// backend: the state bucket may be in another region. In order: the
// pipeline bucket's `region` attribute, the `region` attribute of any
// other AWS resource, the region of a regional ARN, the runner image's
// ECR host. "" when the state says nothing about a region.
func RegionFromState(data []byte) string {
	var st struct {
		Outputs map[string]struct {
			Value any `json:"value"`
		} `json:"outputs"`
		Resources []struct {
			Mode      string `json:"mode"`
			Type      string `json:"type"`
			Instances []struct {
				Attributes map[string]any `json:"attributes"`
			} `json:"instances"`
		} `json:"resources"`
	}
	if json.Unmarshal(data, &st) != nil {
		return ""
	}
	attrRegion := func(attrs map[string]any) string {
		if r, ok := attrs["region"].(string); ok && ValidRegion(r) {
			return r
		}
		return ""
	}
	bucket := ""
	if o, ok := st.Outputs["pipeline_bucket"]; ok {
		bucket, _ = o.Value.(string)
	}
	// 1. The pipeline bucket's location.
	for _, r := range st.Resources {
		if r.Mode != "managed" || r.Type != "aws_s3_bucket" {
			continue
		}
		for _, in := range r.Instances {
			if b, _ := in.Attributes["bucket"].(string); bucket != "" && b != bucket {
				continue
			}
			if reg := attrRegion(in.Attributes); reg != "" {
				return reg
			}
		}
	}
	// 2. Any managed AWS resource that records its region.
	for _, r := range st.Resources {
		if r.Mode != "managed" || !strings.HasPrefix(r.Type, "aws_") {
			continue
		}
		for _, in := range r.Instances {
			if reg := attrRegion(in.Attributes); reg != "" {
				return reg
			}
		}
	}
	// 3. A regional ARN.
	if m := arnRegionRe.FindSubmatch(data); m != nil {
		return string(m[1])
	}
	// 4. The runner image's registry.
	if o, ok := st.Outputs["runner_image"]; ok {
		if s, _ := o.Value.(string); s != "" {
			if m := ecrHostRe.FindStringSubmatch(s); m != nil {
				return m[1]
			}
		}
	}
	return ""
}

// AWSConfigOptions returns the AWS config load options for the workspace
// at root: its `.clavesa/aws-profile.json` profile and its region
// (ResolveRegion), each only when set. See the precedence above.
func AWSConfigOptions(ctx context.Context, root string) []func(*awsconfig.LoadOptions) error {
	var opts []func(*awsconfig.LoadOptions) error
	if p := LoadAWSProfile(root); p != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(p))
	}
	if r := ResolveRegion(ctx, root); r != "" {
		opts = append(opts, awsconfig.WithRegion(r))
	}
	return opts
}

// LoadAWSConfig loads the AWS config every AWS call of the workspace at
// root uses: the default chain with the workspace's profile and region on
// top (AWSConfigOptions). extra options apply last. A config registered
// with SetAWSConfigOverride for root wins over all of it (the hosted
// Lambda's assumed reader role, ADR-032).
func LoadAWSConfig(ctx context.Context, root string, extra ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
	if cfg, ok := AWSConfigOverride(root); ok {
		return cfg.Copy(), nil
	}
	opts := append(AWSConfigOptions(ctx, root), extra...)
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}

// SetRegionEnv returns env with AWS_REGION and AWS_DEFAULT_REGION set to
// region, replacing any earlier values. env is unchanged when region is "".
// Every Terraform run of a workspace gets its env through this, so the
// AWS provider applies in the workspace's region.
func SetRegionEnv(env []string, region string) []string {
	if region == "" {
		return env
	}
	out := make([]string, 0, len(env)+2)
	for _, kv := range env {
		if strings.HasPrefix(kv, "AWS_REGION=") || strings.HasPrefix(kv, "AWS_DEFAULT_REGION=") {
			continue
		}
		out = append(out, kv)
	}
	return append(out, "AWS_REGION="+region, "AWS_DEFAULT_REGION="+region)
}
