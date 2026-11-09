package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/vesahyp/clavesa/internal/databricks"
)

// PreflightInput is `clavesa databricks preflight` and POST
// /databricks/preflight (ADR-015). Everything it does is a read: it is
// meant to run against a work workspace before anything is set up.
type PreflightInput struct {
	databricks.PreflightTarget
	// Auth, one of: Profile (a Databricks CLI profile, a person's login),
	// or ClientID + Secret (a secret reference: env:VAR, file:, arn:).
	Profile  string `json:"profile,omitempty"`
	ClientID string `json:"client_id,omitempty"`
	Secret   string `json:"secret,omitempty"`
	// SkipAWS leaves the AWS checks out (for a laptop with no AWS login
	// to the target account).
	SkipAWS bool `json:"skip_aws,omitempty"`
	// Compute is where the step will run: "fargate" (default, the size of
	// a tens-of-GB day) or "lambda". Fargate adds its deploy actions, a
	// network to run in and the vCPU quota to the AWS checks.
	Compute    string   `json:"compute,omitempty"`
	FargateCPU int      `json:"fargate_cpu,omitempty"`
	Subnets    []string `json:"subnets,omitempty"`
}

// Extra actions a compute = "fargate" deploy calls.
var fargateDeployActions = []string{
	"ecs:CreateCluster", "ecs:RegisterTaskDefinition", "ec2:CreateSecurityGroup",
	"ec2:AuthorizeSecurityGroupEgress", "ec2:DescribeSubnets", "ec2:DescribeVpcs",
}

// PreflightResult is every check, Databricks first, then AWS.
type PreflightResult struct {
	Checks []databricks.Check `json:"checks"`
	Ready  bool               `json:"ready"`
}

// Actions `clavesa deploy` and `pipeline deploy` call on the deploying
// identity. Simulated, not called.
var deployActions = []string{
	"s3:CreateBucket", "ecr:CreateRepository", "iam:CreateRole", "iam:PutRolePolicy", "iam:PassRole",
	"lambda:CreateFunction", "states:CreateStateMachine", "glue:CreateDatabase", "glue:CreateTable",
	"logs:CreateLogGroup", "events:PutRule", "scheduler:CreateSchedule", "secretsmanager:CreateSecret",
	"lakeformation:GrantPermissions", "athena:CreateWorkGroup",
}

// newPreflightGetter builds the Databricks side; tests replace it.
var newPreflightGetter = func(ctx context.Context, s *Service, in PreflightInput) (databricks.Getter, error) {
	switch {
	case in.Profile != "":
		out, err := exec.CommandContext(ctx, "databricks", "auth", "token", "--profile", in.Profile, "--output", "json").Output()
		if err != nil {
			return nil, fmt.Errorf("databricks auth token --profile %s: %w (log in with `databricks auth login --host %s --profile %s`)", in.Profile, err, in.Host, in.Profile)
		}
		var tok struct {
			AccessToken string `json:"access_token"`
		}
		if err := json.Unmarshal(out, &tok); err != nil || tok.AccessToken == "" {
			return nil, fmt.Errorf("databricks auth token --profile %s: no access_token", in.Profile)
		}
		return databricks.NewTokenClient(in.Host, tok.AccessToken), nil
	case in.ClientID != "" && in.Secret != "":
		secret, err := s.resolveSecret(ctx, in.Secret)
		if err != nil {
			return nil, err
		}
		return databricks.NewClient(in.Host, in.ClientID, secret), nil
	}
	return nil, fmt.Errorf("give --profile <databricks CLI profile> (a person's login) or --client-id with --secret env:VAR (the service principal)")
}

// Preflight checks every right and setting clavesa needs to replace one
// Databricks step, and returns the exact grant for each one missing.
func (s *Service) Preflight(ctx context.Context, in PreflightInput) (PreflightResult, error) {
	host, err := normalizeHost(in.Host)
	if err != nil {
		return PreflightResult{}, err
	}
	in.Host = host
	if in.Principal == "" && in.ClientID != "" {
		in.Principal = in.ClientID
	}
	g, err := newPreflightGetter(ctx, s, in)
	if err != nil {
		return PreflightResult{}, err
	}
	if in.Compute == "" {
		in.Compute = "fargate"
	}
	if in.Compute != "fargate" && in.Compute != "lambda" {
		return PreflightResult{}, fmt.Errorf("--compute must be fargate or lambda (emr-serverless is not built)")
	}
	if in.FargateCPU == 0 {
		in.FargateCPU = 16
	}
	checks := databricks.Preflight(ctx, g, in.PreflightTarget)
	if !in.SkipAWS {
		checks = append(checks, awsPreflight(ctx, g, in)...)
	}
	ready := true
	for _, c := range checks {
		if c.Status == databricks.StatusFail {
			ready = false
		}
	}
	return PreflightResult{Checks: checks, Ready: ready}, nil
}

func normalizeHost(h string) (string, error) {
	h = strings.TrimRight(strings.TrimSpace(h), "/")
	if !strings.HasPrefix(h, "https://") || strings.Count(h, "/") > 2 {
		return "", fmt.Errorf("--host must be the workspace URL, https and no path (got %q)", h)
	}
	return h, nil
}

// awsPreflight checks the AWS side with the caller's AWS login: who
// deploys, whether it may create what clavesa deploys, Lake Formation
// admin, and the buckets of the tables (reachable, encryption, policy).
func awsPreflight(ctx context.Context, g databricks.Getter, in PreflightInput) []databricks.Check {
	t := in.PreflightTarget
	row := func(title, status, detail, fix string) databricks.Check {
		return databricks.Check{ID: databricks.PreAWS, Title: title, Status: status, Detail: detail, Fix: fix}
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return []databricks.Check{row("AWS login", databricks.StatusFail, err.Error(), "Log in to the AWS account clavesa deploys into (aws sso login / aws login), or pass --skip-aws.")}
	}
	who, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return []databricks.Check{row("AWS login", databricks.StatusFail, err.Error(), "Log in to the AWS account clavesa deploys into, or pass --skip-aws.")}
	}
	arn := aws.ToString(who.Arn)
	principalARN := iamPrincipalARN(arn)
	out := []databricks.Check{row("AWS login", databricks.StatusPass,
		fmt.Sprintf("account %s, region %s, as %s", aws.ToString(who.Account), cfg.Region, arn), "")}

	// Deploy rights, simulated.
	actions := deployActions
	if in.Compute == "fargate" {
		actions = append(append([]string{}, deployActions...), fargateDeployActions...)
	}
	sim, err := iam.NewFromConfig(cfg).SimulatePrincipalPolicy(ctx, &iam.SimulatePrincipalPolicyInput{
		PolicySourceArn: aws.String(principalARN),
		ActionNames:     actions,
	})
	if err != nil {
		out = append(out, row("Rights to deploy clavesa", databricks.StatusUnknown,
			"could not simulate ("+shortErr(err)+")", "An AWS admin confirms the deploying identity may create: "+strings.Join(actions, ", ")+"."))
	} else {
		var denied []string
		for _, r := range sim.EvaluationResults {
			if string(r.EvalDecision) != "allowed" {
				denied = append(denied, aws.ToString(r.EvalActionName))
			}
		}
		if len(denied) == 0 {
			out = append(out, row("Rights to deploy clavesa", databricks.StatusPass, "every action clavesa deploy calls is allowed for "+principalARN, ""))
		} else {
			out = append(out, row("Rights to deploy clavesa", databricks.StatusFail, "denied: "+strings.Join(denied, ", "),
				"An AWS admin allows these actions for "+principalARN+" (or deploys with a role that has them)."))
		}
	}

	if in.Compute == "fargate" {
		out = append(out, fargatePreflight(ctx, cfg, in)...)
	}

	// Lake Formation: every Glue table grant clavesa makes needs a data lake admin.
	lf, err := lakeformation.NewFromConfig(cfg).GetDataLakeSettings(ctx, &lakeformation.GetDataLakeSettingsInput{})
	if err != nil {
		out = append(out, row("Lake Formation data lake admin", databricks.StatusUnknown, shortErr(err), ""))
	} else {
		admin := false
		for _, a := range lf.DataLakeSettings.DataLakeAdmins {
			if id := aws.ToString(a.DataLakePrincipalIdentifier); id == principalARN || id == arn {
				admin = true
			}
		}
		if admin {
			out = append(out, row("Lake Formation data lake admin", databricks.StatusPass, principalARN+" in "+cfg.Region, ""))
		} else {
			out = append(out, row("Lake Formation data lake admin", databricks.StatusFail,
				principalARN+" is not a data lake admin in "+cfg.Region+"; pipeline deploy fails on its Lake Formation grants",
				"An AWS admin adds it: Lake Formation console ("+cfg.Region+") > Administrative roles and tasks > Data lake administrators > Add."))
		}
	}

	// The buckets the step's tables live in.
	folders := map[string]string{}
	var tbl struct {
		TableType       string `json:"table_type"`
		StorageLocation string `json:"storage_location"`
	}
	if t.Table != "" && g.GetJSON(ctx, "/api/2.1/unity-catalog/tables/"+t.Table, &tbl) == nil && tbl.StorageLocation != "" {
		if tbl.TableType == "EXTERNAL" {
			folders[s3Folder(tbl.StorageLocation)] = "write (clavesa takes the table over)"
		} else {
			folders[s3Folder(tbl.StorageLocation)] = "none (managed; read with vended credentials)"
		}
	}
	if t.ShadowLocation != "" {
		folders[s3Folder(t.ShadowLocation)] = "write (the shadow table)"
	}
	for _, in := range t.Inputs {
		var it struct {
			TableType       string `json:"table_type"`
			StorageLocation string `json:"storage_location"`
		}
		if g.GetJSON(ctx, "/api/2.1/unity-catalog/tables/"+in, &it) == nil && it.TableType == "EXTERNAL" && it.StorageLocation != "" {
			folders[s3Folder(it.StorageLocation)] = "read (input " + in + ")"
		}
	}
	s3c := s3.NewFromConfig(cfg)
	for folder, access := range folders {
		bucket := strings.SplitN(strings.TrimPrefix(folder, "s3://"), "/", 2)[0]
		title := "Bucket " + bucket + " (" + folder + ")"
		if _, err := s3c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucket}); err != nil {
			out = append(out, row(title, databricks.StatusFail, "not reachable from this AWS account: "+shortErr(err),
				"Clavesa runs in the bucket's account, or the bucket's owner adds a bucket policy that lets clavesa's runner role "+access+"."))
			continue
		}
		detail := "runner role access: " + access
		if key, err := bucketKMSKey(ctx, bucket); err == nil && key != "" {
			detail += "; SSE-KMS key " + key + " (pipeline deploy grants kms:Decrypt and kms:GenerateDataKey on it; the key policy must allow IAM grants)"
		}
		st := databricks.StatusPass
		fix := ""
		if pol, err := s3c.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: &bucket}); err == nil && strings.Contains(aws.ToString(pol.Policy), `"Deny"`) {
			st = databricks.StatusUnknown
			detail += "; the bucket policy has Deny statements"
			fix = "The bucket's owner checks that no Deny in its policy matches clavesa's runner role (role name clavesa-<workspace>-<pipeline>-runner)."
		}
		out = append(out, row(title, st, detail, fix))
	}
	return out
}

// fargatePreflight checks what compute = "fargate" needs in the account:
// a network for the task (the given subnets, or the default VPC's default
// subnets) and the Fargate On-Demand vCPU quota for one task.
func fargatePreflight(ctx context.Context, cfg aws.Config, in PreflightInput) []databricks.Check {
	var out []databricks.Check
	row := func(title, status, detail, fix string) {
		out = append(out, databricks.Check{ID: databricks.PreAWS, Title: title, Status: status, Detail: detail, Fix: fix})
	}
	e := ec2.NewFromConfig(cfg)
	if len(in.Subnets) > 0 {
		sn, err := e.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: in.Subnets})
		switch {
		case err != nil:
			row("Fargate subnets", databricks.StatusFail, shortErr(err), "Check the subnet IDs (--subnet) and that they are in "+cfg.Region+".")
		default:
			public := 0
			for _, s := range sn.Subnets {
				if aws.ToBool(s.MapPublicIpOnLaunch) {
					public++
				}
			}
			row("Fargate subnets", databricks.StatusPass,
				fmt.Sprintf("%d subnet(s), %d with public IPs on launch. Without a public IP the task needs a NAT gateway or VPC endpoints for S3, ECR, Glue, STS, Secrets Manager, CloudWatch Logs, and a route to Databricks; set fargate_subnets, and fargate_assign_public_ip = false for private subnets", len(sn.Subnets), public), "")
		}
	} else {
		vpcs, err := e.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []ec2types.Filter{{Name: aws.String("is-default"), Values: []string{"true"}}}})
		switch {
		case err != nil:
			row("Fargate network", databricks.StatusUnknown, shortErr(err), "")
		case len(vpcs.Vpcs) == 0:
			row("Fargate network", databricks.StatusFail, "no default VPC in "+cfg.Region+" and no --subnet given",
				"Pick subnets for the task (a route to S3, ECR, Glue and Databricks: public IP or NAT), pass them with --subnet, and set fargate_subnets in the pipeline's terraform.tfvars.")
		default:
			row("Fargate network", databricks.StatusPass, "the default VPC "+aws.ToString(vpcs.Vpcs[0].VpcId)+" (its default subnets, public IP)", "")
		}
	}
	q, err := servicequotas.NewFromConfig(cfg).GetServiceQuota(ctx, &servicequotas.GetServiceQuotaInput{
		ServiceCode: aws.String("fargate"), QuotaCode: aws.String("L-3032A538"),
	})
	switch {
	case err != nil:
		row("Fargate vCPU quota", databricks.StatusUnknown, shortErr(err), "")
	case q.Quota == nil || q.Quota.Value == nil:
		row("Fargate vCPU quota", databricks.StatusUnknown, "no value", "")
	case int(*q.Quota.Value) >= in.FargateCPU:
		row("Fargate vCPU quota", databricks.StatusPass, fmt.Sprintf("%.0f On-Demand vCPUs, one task takes %d", *q.Quota.Value, in.FargateCPU), "")
	default:
		row("Fargate vCPU quota", databricks.StatusFail, fmt.Sprintf("%.0f On-Demand vCPUs, one task takes %d", *q.Quota.Value, in.FargateCPU),
			"An AWS admin raises \"Fargate On-Demand vCPU resource count\" (L-3032A538) in Service Quotas, or set a smaller fargate_cpu.")
	}
	return out
}

// iamPrincipalARN turns an assumed-role session ARN into its role ARN,
// which is what IAM simulation and Lake Formation admin lists use.
func iamPrincipalARN(arn string) string {
	// arn:aws:sts::123:assumed-role/Name/session -> arn:aws:iam::123:role/Name
	if strings.Contains(arn, ":assumed-role/") {
		parts := strings.Split(arn, ":")
		if len(parts) == 6 {
			rest := strings.Split(strings.TrimPrefix(parts[5], "assumed-role/"), "/")
			return "arn:aws:iam::" + parts[4] + ":role/" + rest[0]
		}
	}
	return arn
}

func shortErr(err error) string {
	s := err.Error()
	if i := strings.LastIndex(s, "api error "); i >= 0 {
		s = s[i+len("api error "):]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}
