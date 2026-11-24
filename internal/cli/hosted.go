package cli

import (
	"fmt"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/servingsql"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	tuiservice "github.com/vesahyp/clavesa/internal/service"
)

// newWorkspaceHostedCmd is `clavesa workspace hosted enable|disable|show`
// (ADR-032), the CLI twin of GET/PUT/DELETE /workspace/hosted.
func newWorkspaceHostedCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hosted",
		Short: "Join or leave the hosted read-only UI",
		Long: `Join or leave a hosted, read-only clavesa UI (ADR-032).

When the workspace is hosted, the workspace stack gets a read-only IAM
role (clavesa-ui-reader-<workspace>) that only the hosted UI's principal
may assume, with the workspace name as its ExternalId; an Athena workgroup
(clavesa-ui-<workspace>) with a 1 GB scan limit per query; and, with an
event bus, rules that forward this workspace's run events to it. Every
deploy also uploads a read-only copy of the workspace tree (clavesa.json,
the pipeline .tf files and deployed.json, never Terraform state,
.env files or credentials) to s3://<pipeline_bucket>/_clavesa/tree/.

A local-warehouse workspace has no stack and no reader role. Its hosted
block names the hosted UI's publisher role and data bucket instead, and
every clavesa pipeline run ends by publishing the workspace summary to
summaries/<workspace>.json through that role (turn it off for one run
with --no-publish or CLAVESA_NO_PUBLISH=1). The name must be registered
as a local workspace in the hosted UI's registry first.

  clavesa workspace hosted enable --url https://ui.clavesa.dev
  clavesa workspace hosted show
  clavesa workspace hosted disable`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(newWorkspaceHostedEnableCmd(), newWorkspaceHostedDisableCmd(), newWorkspaceHostedShowCmd())
	return cmd
}

func newWorkspaceHostedEnableCmd() *cobra.Command {
	var in tuiservice.HostedInput
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "enable",
		Short: "Write the hosted block into clavesa.json",
		Long: `Write the hosted block into clavesa.json and hosted.tf into the
workspace. Nothing in AWS changes until the next deploy: run
clavesa deploy to create the reader role and the workgroup and to upload
the tree copy.

--kind cloud or --kind local says how the hosted UI reads the workspace,
and is recorded in the block so it never changes with a developer's own
settings. Without --kind: cloud when clavesa.json names a remote
Terraform backend or the workspace stack is deployed (its pipeline_bucket
output is readable); otherwise the workspace's warehouse setting.

The hosted UI Lambda's role (the reader principal) and its event bus are
read from <url>/.well-known/clavesa.json. For a local-warehouse
workspace the command records the publisher role, the data bucket and its
region from that document instead; nothing is deployed, and
--reader-principal and --event-bus-arn do not apply. --reader-principal adds more
principals that may assume the reader role, kept beside the Lambda's
(repeat it for more than one; for example the IAM user that runs the
smoke gate's reader leg). --event-bus-arn overrides the bus.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			st, err := tuiservice.New(root).EnableHosted(cmd.Context(), in)
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, st)
			}
			printHostedStatus(os.Stdout, st)
			if st.Kind == "local" {
				fmt.Println("Next: register the workspace as kind \"local\" in the hosted UI's registry; the next pipeline run publishes its summary (or: clavesa workspace summary --publish).")
				return nil
			}
			fmt.Println("Next: clavesa deploy, to create the reader role and upload the tree copy.")
			return nil
		},
	}
	cmd.Flags().StringVar(&in.URL, "url", "", "the hosted UI, such as https://ui.clavesa.dev (required)")
	cmd.Flags().StringVar(&in.Kind, "kind", "", "cloud (read through the reader role) or local (summary published at run end); default: cloud when the workspace has a remote backend or is deployed, else its warehouse setting")
	cmd.Flags().StringArrayVar(&in.ReaderPrincipals, "reader-principal", nil, "extra IAM principal ARN allowed to assume the reader role, beside the hosted UI's own (repeatable)")
	cmd.Flags().StringVar(&in.EventBusARN, "event-bus-arn", "", "the hosted clavesa-ui event bus; empty: the one the hosted UI names")
	var snapshotOn, snapshotOff bool
	cmd.Flags().BoolVar(&snapshotOn, "snapshot", false, "local workspace: publish its catalog and dashboard snapshot at run end (ADR-036; off unless set)")
	cmd.Flags().BoolVar(&snapshotOff, "no-snapshot", false, "local workspace: stop publishing the snapshot")
	cmd.MarkFlagsMutuallyExclusive("snapshot", "no-snapshot")
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if snapshotOn || snapshotOff {
			v := snapshotOn
			in.Snapshot = &v
		}
		return nil
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	_ = cmd.MarkFlagRequired("url")
	return cmd
}

func newWorkspaceHostedDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Remove the hosted block from clavesa.json",
		Long: `Remove the hosted block from clavesa.json and hosted.tf from the
workspace. The next clavesa deploy deletes the reader role, the workgroup
and the forwarding rules. The tree copy under _clavesa/tree/ stays in the
bucket.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			if _, err := tuiservice.New(root).DisableHosted(); err != nil {
				return err
			}
			fmt.Println("Removed the hosted block. Next: clavesa deploy, to delete the reader role.")
			return nil
		},
	}
}

func newWorkspaceHostedShowCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "show",
		Short: "Show the hosted block",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			st, err := tuiservice.New(root).GetHosted()
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, st)
			}
			printHostedStatus(os.Stdout, st)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func printHostedStatus(w io.Writer, st tuiservice.HostedStatus) {
	if !st.Enabled || st.Hosted == nil {
		fmt.Fprintln(w, "Not hosted. Join with: clavesa workspace hosted enable --url <url>")
		return
	}
	h := st.Hosted
	fmt.Fprintf(w, "Hosted at:         %s\n", h.URL)
	if st.Kind == "local" {
		fmt.Fprintln(w, "Kind:              local warehouse (summary published at run end)")
		fmt.Fprintf(w, "Publisher role:    %s\n", h.PublisherRoleARN)
		fmt.Fprintf(w, "Summary:           %s\n", st.SummaryURI)
		return
	}
	fmt.Fprintf(w, "Reader principals: %s\n", strings.Join(h.ReaderPrincipals, ", "))
	bus := h.EventBusARN
	if bus == "" {
		bus = "none (no forwarding rule)"
	}
	fmt.Fprintf(w, "Event bus:         %s\n", bus)
	fmt.Fprintf(w, "Reader role:       %s (ExternalId = workspace name)\n", st.ReaderRoleName)
	fmt.Fprintf(w, "Athena workgroup:  %s\n", st.WorkGroup)
	if st.TreeURI != "" {
		fmt.Fprintf(w, "Tree copy:         %s\n", st.TreeURI)
	}
}

// uploadHostedTree runs after a successful apply: for a hosted workspace
// it uploads the tree copy and deployed.json (ADR-032). A no-op otherwise.
func uploadHostedTree(cmd *cobra.Command, root string, out io.Writer) error {
	// The tree copy ships the dashboards' Athena translations (ADR-034):
	// the same cached translator `clavesa ui` uses, so a widget already
	// translated is not translated again. The sidecar container starts
	// only when a translation is missing from the cache.
	sidecar := observability.NewTranspileSidecar(root)
	defer sidecar.Close()
	svc := tuiservice.New(root).WithTranspiler(
		servingsql.NewCachedTranspiler(filepath.Join(root, tuiservice.TranspileCacheDir), sidecar.ToServing))
	res, err := svc.UploadHostedTree(cmd.Context())
	if err != nil {
		return err
	}
	if res == nil {
		return nil
	}
	fmt.Fprintf(out, "Hosted: uploaded %d file(s) to %s", len(res.Uploaded), res.URI)
	if len(res.Removed) > 0 {
		fmt.Fprintf(out, ", removed %d stale", len(res.Removed))
	}
	fmt.Fprintln(out)
	return nil
}
