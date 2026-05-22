package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// sweepGlueDB lists every Glue table in `glueDB`, prints them, asks for
// `yes` confirmation, and deletes them via the Glue SDK. `label` is the
// human-facing noun used in the prompt ("pipeline" / "workspace") so
// the user sees which scope they're confirming. Missing DB → no-op.
//
// Runner / runs_writer-created Iceberg tables aren't in terraform state,
// so without this step `terraform destroy` refuses on the corresponding
// `aws_glue_catalog_database` with "database is not empty" and the user
// has to drop into the AWS console or run `aws glue delete-table` by
// hand.
func sweepGlueDB(ctx context.Context, glueDB, label string, out io.Writer, in io.Reader) error {
	awsCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(awsCtx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	gc := glue.NewFromConfig(cfg)

	var tableNames []string
	var nextToken *string
	for {
		resp, err := gc.GetTables(awsCtx, &glue.GetTablesInput{
			DatabaseName: aws.String(glueDB),
			NextToken:    nextToken,
		})
		if err != nil {
			var enf *types.EntityNotFoundException
			if errors.As(err, &enf) {
				fmt.Fprintf(out, "→ Glue DB %s does not exist; nothing to sweep.\n", glueDB)
				return nil
			}
			return fmt.Errorf("list tables in %s: %w", glueDB, err)
		}
		for _, t := range resp.TableList {
			tableNames = append(tableNames, aws.ToString(t.Name))
		}
		if resp.NextToken == nil || *resp.NextToken == "" {
			break
		}
		nextToken = resp.NextToken
	}

	if len(tableNames) == 0 {
		fmt.Fprintf(out, "→ Glue DB %s is empty; nothing to sweep.\n", glueDB)
		return nil
	}

	fmt.Fprintf(out, "\nFound %d runtime-created Glue table(s) in %s:\n", len(tableNames), glueDB)
	for _, n := range tableNames {
		fmt.Fprintf(out, "  - %s\n", n)
	}
	fmt.Fprintf(out, "\nThese are not in terraform state — `terraform destroy` would refuse with 'database is not empty'. Type 'yes' to delete all of them now and then run terraform destroy: ")

	r := bufio.NewReader(in)
	line, _ := r.ReadString('\n')
	if strings.TrimSpace(line) != "yes" {
		return fmt.Errorf("%s destroy cancelled", label)
	}

	for _, n := range tableNames {
		_, err := gc.DeleteTable(awsCtx, &glue.DeleteTableInput{
			DatabaseName: aws.String(glueDB),
			Name:         aws.String(n),
		})
		if err != nil {
			return fmt.Errorf("delete glue table %s.%s: %w", glueDB, n, err)
		}
		fmt.Fprintf(out, "  ✓ deleted %s.%s\n", glueDB, n)
	}
	return nil
}

// sweepPipelineGlueTables resolves the pipeline's Glue DB and delegates
// to sweepGlueDB. Schema falls back to sanitize(pipelineName) per
// ADR-016 unless the caller overrides — pass `glueDBOverride` for the
// rare case where the pipeline's var.schema was overridden from its
// default.
//
// System-DB row cleanup (deleting runs / node_runs / tables rows where
// pipeline = <this pipeline>) is NOT done here — those rows live inside
// shared Iceberg tables and need an Athena DELETE through the workspace
// workgroup. Filed as a follow-up.
func sweepPipelineGlueTables(ctx context.Context, workspaceRoot, pipelineName, glueDBOverride string, out io.Writer, in io.Reader) error {
	var glueDB string
	if glueDBOverride != "" {
		glueDB = glueDBOverride
	} else {
		m, err := workspace.Load(workspaceRoot)
		if err != nil {
			return fmt.Errorf("load manifest: %w", err)
		}
		if m == nil {
			return fmt.Errorf("not a clavesa workspace at %s (no clavesa.json)", workspaceRoot)
		}
		schema := identutil.Sanitize(pipelineName)
		glueDB = identutil.EncodeGlueDatabase(m.CatalogIdentifier(), schema)
	}
	return sweepGlueDB(ctx, glueDB, "pipeline", out, in)
}

// sweepWorkspaceSystemGlueTables resolves the workspace's system-catalog
// Glue DB (`<system_catalog>__pipelines`) and delegates to sweepGlueDB.
// Holds runs / node_runs / tables across every pipeline in the
// workspace; multi-writer by design (ADR-016 v0.20.0). Running this
// before `terraform destroy` lets the workspace tear down cleanly.
func sweepWorkspaceSystemGlueTables(ctx context.Context, workspaceRoot string, out io.Writer, in io.Reader) error {
	m, err := workspace.Load(workspaceRoot)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	if m == nil {
		return fmt.Errorf("not a clavesa workspace at %s (no clavesa.json)", workspaceRoot)
	}
	systemDB := identutil.EncodeGlueDatabase(m.SystemCatalogIdentifier(), "pipelines")
	return sweepGlueDB(ctx, systemDB, "workspace", out, in)
}
