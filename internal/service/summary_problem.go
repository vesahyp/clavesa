package service

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Problem codes of a pipeline whose history the summary could not read.
const (
	ProblemNotDeployed   = "not_deployed"
	ProblemWrongRegion   = "wrong_region"
	ProblemAccessDenied  = "access_denied"
	ProblemNeverRun      = "never_run"
	ProblemNoCredentials = "no_credentials"
	ProblemUnreadable    = "unreadable"
)

// SummaryProblem says in one plain line what is wrong with a pipeline's
// history and what to do about it. The raw provider error stays in
// PipelineSummary.Error, for the drill-down; the cards show only this.
type SummaryProblem struct {
	Code   string `json:"code"`
	Cause  string `json:"cause"`
	Action string `json:"action,omitempty"`
}

// Line is the problem as one sentence: "<cause>. <action>".
func (p SummaryProblem) Line() string {
	if p.Action == "" {
		return p.Cause
	}
	return p.Cause + ". " + p.Action
}

// SummaryProblemGroup is one problem shared by one or more pipelines of a
// workspace, so the card says it once.
type SummaryProblemGroup struct {
	SummaryProblem
	Pipelines []string `json:"pipelines"`
}

// summaryEnv is what the classifier knows about where the workspace is
// deployed and where it is read from. Empty fields are unknown.
type summaryEnv struct {
	// DeployedRegion is the region in the workspace's Terraform state.
	DeployedRegion string
	// ReadRegion is the region of the AWS clients the summary reads with.
	ReadRegion string
}

var (
	// "is not authorized to perform: athena:StartQueryExecution on ..."
	deniedActionRe = regexp.MustCompile(`not authorized to perform:?\s+([a-z0-9-]+:[A-Za-z0-9*]+)`)
	// "StartQueryExecution: operation error Athena: StartQueryExecution, ..."
	sdkOperationRe = regexp.MustCompile(`operation error ([A-Za-z0-9 ]+): ([A-Za-z0-9]+),`)
)

// classifySummaryError turns a provider read error into a short cause and
// the action that fixes it. The match is on the error text, the only
// thing every provider and the AWS SDK have in common.
func classifySummaryError(err error, env summaryEnv) SummaryProblem {
	if err == nil {
		return SummaryProblem{}
	}
	s := err.Error()
	lower := strings.ToLower(s)
	has := func(markers ...string) bool {
		for _, m := range markers {
			if strings.Contains(lower, strings.ToLower(m)) {
				return true
			}
		}
		return false
	}
	switch {
	case has("ExpiredToken", "token has expired", "security token included in the request is expired",
		"failed to refresh cached credentials", "failed to retrieve credentials", "no valid credential sources",
		"NoCredentialProviders", "no AWS credentials", "InvalidClientTokenId", "UnrecognizedClientException",
		"SSO session", "the SSO session associated with this profile has expired"):
		return SummaryProblem{
			Code:   ProblemNoCredentials,
			Cause:  "AWS credentials missing or expired",
			Action: "Sign in again (aws login) or set AWS_PROFILE.",
		}
	case has("AccessDenied", "not authorized to perform", "UnauthorizedOperation"):
		action := ""
		if m := deniedActionRe.FindStringSubmatch(s); m != nil {
			action = m[1]
		} else if m := sdkOperationRe.FindStringSubmatch(s); m != nil {
			action = strings.ToLower(strings.ReplaceAll(m[1], " ", "")) + ":" + m[2]
		}
		cause := "no permission to read run history"
		if action != "" {
			cause += " (" + action + ")"
		}
		return SummaryProblem{
			Code:   ProblemAccessDenied,
			Cause:  cause,
			Action: "Grant it to the AWS identity in use.",
		}
	case has("S3 location provided to save your query results is invalid", "NoSuchBucket",
		"specified bucket does not exist"):
		// Athena says the same thing for a bucket that is gone and for one
		// in another region. The Terraform state tells the two apart. Since
		// GH #107 every read uses the workspace's recorded region, so this
		// is left for a clavesa.json whose `region` is wrong.
		if env.DeployedRegion != "" && env.ReadRegion != "" && env.DeployedRegion != env.ReadRegion {
			return SummaryProblem{
				Code:   ProblemWrongRegion,
				Cause:  fmt.Sprintf("workspace is deployed in %s, read from %s", env.DeployedRegion, env.ReadRegion),
				Action: fmt.Sprintf(`Set "region": %q in clavesa.json.`, env.DeployedRegion),
			}
		}
		return SummaryProblem{
			Code:   ProblemNotDeployed,
			Cause:  "pipeline bucket not found: the workspace is not deployed",
			Action: "Run `clavesa workspace deploy`.",
		}
	case has("EntityNotFoundException", "TABLE_NOT_FOUND", "Table not found"):
		return SummaryProblem{
			Code:   ProblemNeverRun,
			Cause:  "never run: no run history table yet",
			Action: "Run `clavesa pipeline run`.",
		}
	}
	return SummaryProblem{
		Code:   ProblemUnreadable,
		Cause:  "run history unreadable",
		Action: "See `clavesa workspace summary --json` for the error.",
	}
}

// notDeployedProblem is the problem of every pipeline of a cloud
// workspace that has no Terraform state (or no pipeline_bucket output).
func notDeployedProblem() SummaryProblem {
	return SummaryProblem{
		Code:   ProblemNotDeployed,
		Cause:  "not deployed: no Terraform state",
		Action: "Run `clavesa workspace deploy`.",
	}
}

// groupProblems folds the pipelines that share a problem into one group,
// in the order the problems first appear.
func groupProblems(pipes []PipelineSummary) []SummaryProblemGroup {
	out := []SummaryProblemGroup{}
	idx := map[SummaryProblem]int{}
	for _, p := range pipes {
		if p.Problem == nil {
			continue
		}
		i, ok := idx[*p.Problem]
		if !ok {
			i = len(out)
			idx[*p.Problem] = i
			out = append(out, SummaryProblemGroup{SummaryProblem: *p.Problem, Pipelines: []string{}})
		}
		out[i].Pipelines = append(out[i].Pipelines, p.Name)
	}
	for i := range out {
		sort.Strings(out[i].Pipelines)
	}
	return out
}

// Line is the group as one status line: the pipeline's name when it is
// one, the count when several share it.
func (g SummaryProblemGroup) Line() string {
	who := fmt.Sprintf("%d pipelines", len(g.Pipelines))
	if len(g.Pipelines) == 1 {
		who = g.Pipelines[0]
	}
	return who + ": " + g.SummaryProblem.Line()
}
