package tfgen

import (
	"fmt"
	"strings"
)

// PipelineSegment is one segment of a mixed bundle job (ADR-031 section
// 8): a run of tasks in one place. A clavesa segment carries its runner
// entries; a Databricks segment the task keys run-now starts.
type PipelineSegment struct {
	Place      string // clavesa | databricks
	Transforms []TransformConfig
	Tasks      []string
}

// DatabricksPollSeconds is the wait between two runs/get calls while a
// Databricks segment runs. Waiting in Step Functions costs nothing.
const DatabricksPollSeconds = 30

// emitSegmentStateMachine writes the state machine of a mixed job: one
// runner Task per clavesa segment, and per Databricks segment a run-now
// (the runner Lambda calls the Databricks API, ADR-031) followed by a Wait
// and runs/get loop until the run ends. The task states travel from
// segment to segment in $.states, so run_if and outcomes work across the
// boundary. The execution fails at the end when any task failed.
func emitSegmentStateMachine(b *strings.Builder, p Pipeline) {
	name := func(i int, s string) string { return fmt.Sprintf("Seg%d%s", i, s) }
	first := func(i int) string {
		if i >= len(p.Bundle.Segments) {
			return "Done"
		}
		if p.Bundle.Segments[i].Place == "databricks" {
			return name(i, "Start")
		}
		return name(i, "")
	}
	lambda := func(in string) {
		fmt.Fprintf(b, "%sType           = \"Task\"\n", in)
		fmt.Fprintf(b, "%sResource       = \"arn:aws:states:::lambda:invoke\"\n", in)
		fmt.Fprintf(b, "%sTimeoutSeconds = 900\n", in)
	}
	selector := func(in string, withRun bool) {
		fmt.Fprintf(b, "%sResultSelector = {\n", in)
		fmt.Fprintf(b, "%s  \"states.$\" = \"$.Payload.states\"\n", in)
		fmt.Fprintf(b, "%s  \"failed.$\" = \"$.Payload.failed\"\n", in)
		if withRun {
			fmt.Fprintf(b, "%s  \"run_id.$\" = \"$.Payload.run_id\"\n", in)
			fmt.Fprintf(b, "%s  \"done.$\"   = \"$.Payload.done\"\n", in)
		}
		fmt.Fprintf(b, "%s}\n", in)
	}
	context := func(in string) {
		fmt.Fprintf(b, "%s\"_sf_execution_arn.$\"        = \"$$.Execution.Id\"\n", in)
		fmt.Fprintf(b, "%s\"_sf_execution_started_at.$\" = \"$$.Execution.StartTime\"\n", in)
		fmt.Fprintf(b, "%s\"_execution_input.$\"         = \"$$.Execution.Input\"\n", in)
	}

	fmt.Fprintf(b, "# Step Functions state machine of a mixed bundle job (ADR-031 section 8):\n")
	fmt.Fprintf(b, "# clavesa segments run in the runner Lambda; Databricks segments run in the\n")
	fmt.Fprintf(b, "# job deployed by databricks bundle deploy (run-now with only), polled here.\n")
	fmt.Fprintf(b, "resource \"aws_sfn_state_machine\" \"pipeline\" {\n")
	fmt.Fprintf(b, "  name     = \"%s\"\n", p.awsBase())
	fmt.Fprintf(b, "  role_arn = aws_iam_role.sfn_exec.arn\n")
	fmt.Fprintf(b, "  type     = \"STANDARD\"\n\n")
	fmt.Fprintf(b, "  definition = jsonencode({\n")
	fmt.Fprintf(b, "    Comment = \"Clavesa bundle job: ${%s}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "    StartAt = \"Init\"\n")
	fmt.Fprintf(b, "    States = {\n")
	fmt.Fprintf(b, "      Init = {\n        Type       = \"Pass\"\n        Parameters = { states = {}, failed = false }\n        Next       = \"%s\"\n      }\n", first(0))
	for i, seg := range p.Bundle.Segments {
		next := first(i + 1)
		in := "        "
		if seg.Place != "databricks" {
			fmt.Fprintf(b, "      %s = {\n", name(i, ""))
			lambda(in)
			fmt.Fprintf(b, "%sParameters = {\n", in)
			fmt.Fprintf(b, "%s  FunctionName = aws_lambda_function.pipeline_runner.arn\n", in)
			fmt.Fprintf(b, "%s  Payload = {\n", in)
			fmt.Fprintf(b, "%s    _pipeline_run     = true\n", in)
			fmt.Fprintf(b, "%s    pipeline          = %s\n", in, p.PipelineNameExpr)
			fmt.Fprintf(b, "%s    \"_bundle_states.$\" = \"$.states\"\n", in)
			fmt.Fprintf(b, "%s    transforms = [\n", in)
			for _, t := range seg.Transforms {
				fmt.Fprintf(b, "%s      { node = %q, language = %q, logic_path = %s, inputs = %s, outputs = %s, parents = %s },\n",
					in, t.NodeID, t.Language, t.LogicS3URI, t.InputsExpr, t.OutputsExpr, renderParents(t.Parents))
			}
			fmt.Fprintf(b, "%s    ]\n", in)
			context(in + "    ")
			fmt.Fprintf(b, "%s  }\n", in)
			fmt.Fprintf(b, "%s}\n", in)
			selector(in, false)
			fmt.Fprintf(b, "%sNext = %q\n", in, next)
			fmt.Fprintf(b, "      }\n")
			continue
		}
		only := renderParents(seg.Tasks)
		for _, op := range []string{"Start", "Poll"} {
			fmt.Fprintf(b, "      %s = {\n", name(i, op))
			lambda(in)
			fmt.Fprintf(b, "%sParameters = {\n", in)
			fmt.Fprintf(b, "%s  FunctionName = aws_lambda_function.pipeline_runner.arn\n", in)
			fmt.Fprintf(b, "%s  Payload = {\n", in)
			if op == "Start" {
				fmt.Fprintf(b, "%s    _operation = \"dbx_run_now\"\n", in)
			} else {
				fmt.Fprintf(b, "%s    _operation = \"dbx_run_get\"\n", in)
				fmt.Fprintf(b, "%s    \"run_id.$\" = \"$.run_id\"\n", in)
			}
			fmt.Fprintf(b, "%s    job_id     = %d\n", in, p.Bundle.DatabricksJobID)
			fmt.Fprintf(b, "%s    only       = %s\n", in, only)
			fmt.Fprintf(b, "%s    \"states.$\" = \"$.states\"\n", in)
			fmt.Fprintf(b, "%s    \"failed.$\" = \"$.failed\"\n", in)
			fmt.Fprintf(b, "%s    logic_path = %s\n", in, p.Bundle.JobURI)
			context(in + "    ")
			fmt.Fprintf(b, "%s  }\n", in)
			fmt.Fprintf(b, "%s}\n", in)
			selector(in, true)
			fmt.Fprintf(b, "%sNext = %q\n", in, name(i, "Check"))
			fmt.Fprintf(b, "      }\n")
		}
		fmt.Fprintf(b, "      %s = {\n", name(i, "Check"))
		fmt.Fprintf(b, "        Type    = \"Choice\"\n")
		fmt.Fprintf(b, "        Choices = [{ Variable = \"$.done\", BooleanEquals = true, Next = %q }]\n", next)
		fmt.Fprintf(b, "        Default = %q\n", name(i, "Wait"))
		fmt.Fprintf(b, "      }\n")
		fmt.Fprintf(b, "      %s = {\n        Type    = \"Wait\"\n        Seconds = %d\n        Next    = %q\n      }\n", name(i, "Wait"), DatabricksPollSeconds, name(i, "Poll"))
	}
	fmt.Fprintf(b, "      Done = {\n")
	fmt.Fprintf(b, "        Type    = \"Choice\"\n")
	fmt.Fprintf(b, "        Choices = [{ Variable = \"$.failed\", BooleanEquals = true, Next = \"JobFailed\" }]\n")
	fmt.Fprintf(b, "        Default = \"JobSucceeded\"\n")
	fmt.Fprintf(b, "      }\n")
	fmt.Fprintf(b, "      JobFailed = {\n        Type  = \"Fail\"\n        Error = \"BundleTaskFailed\"\n        Cause = \"a task of the job failed; see the run page\"\n      }\n")
	fmt.Fprintf(b, "      JobSucceeded = {\n        Type = \"Succeed\"\n      }\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  })\n\n")
	fmt.Fprintf(b, "  logging_configuration {\n")
	fmt.Fprintf(b, "    log_destination        = \"${aws_cloudwatch_log_group.sfn_logs.arn}:*\"\n")
	fmt.Fprintf(b, "    include_execution_data = true\n")
	fmt.Fprintf(b, "    level                  = \"ERROR\"\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  tags       = local.clavesa_tags\n")
	fmt.Fprintf(b, "  depends_on = [aws_iam_role_policy.sfn_exec_policy]\n")
	fmt.Fprintf(b, "}\n\n")
}
