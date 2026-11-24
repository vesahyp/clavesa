package cli

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	tuiservice "github.com/vesahyp/clavesa/internal/service"
)

// newWorkspaceAccessCmd is `clavesa workspace access show|sync`
// (docs/ideas/column-tags.md), the CLI twin of GET /workspace/access.
func newWorkspaceAccessCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "access",
		Short: "Show and apply the column tags",
		Long: `Show and apply the column tags of a workspace with an "access" block in
clavesa.json (docs/ideas/column-tags.md).

A transform output tags its table with table_tag and its columns with
column_tags. Every value must be in clavesa.json "access" "tags". A table
or column with no tag has the value "unreviewed", which no viewer reads.

  clavesa workspace access show
  clavesa workspace access show --json
  clavesa workspace access sync`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(newWorkspaceAccessShowCmd(), newWorkspaceAccessSyncCmd())
	return cmd
}

func newWorkspaceAccessShowCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "show",
		Short: "List every table and its tags",
		Long: `List every table of the workspace with its table tag and its tagged
columns, as declared in the pipelines. It reads no AWS. It exits non-zero
when a declaration has a problem that the deploy would refuse.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			rep, err := tuiservice.New(root).AccessTags()
			if err != nil {
				return err
			}
			if jsonOut {
				if err := printJSON(os.Stdout, rep); err != nil {
					return err
				}
			} else {
				printAccessReport(os.Stdout, rep)
			}
			if len(rep.Problems) > 0 {
				return fmt.Errorf("%d problem(s) in the column tags", len(rep.Problems))
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "print JSON")
	return cmd
}

func newWorkspaceAccessSyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Apply the column tags to the tables in Lake Formation",
		Long: `Apply the declared column tags to every table that exists, in Lake
Formation. clavesa deploy does the same after each apply. Run it after a
pipeline's first run has made a new table: until then the table has the
value "unreviewed", which no viewer reads.

It runs as you, and you must be a Lake Formation data lake admin.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			res, err := tuiservice.New(root).AccessSync(cmd.Context())
			if err != nil {
				return err
			}
			if res == nil {
				fmt.Fprintln(cmd.OutOrStdout(), `No "access" block in clavesa.json: nothing to tag.`)
				return nil
			}
			printAccessSync(cmd.OutOrStdout(), cmd.ErrOrStderr(), res)
			return nil
		},
	}
	return cmd
}

func printAccessReport(w io.Writer, rep *tuiservice.AccessReport) {
	if !rep.Enabled {
		fmt.Fprintln(w, `No "access" block in clavesa.json: column tags are off.`)
	} else {
		fmt.Fprintf(w, "Tag key:    %s\n", rep.TagKey)
		fmt.Fprintf(w, "Tags:       %s (and %s, which no viewer reads)\n", strings.Join(rep.Tags, ", "), rep.Unreviewed)
		for _, a := range sortedAudiences(rep.Audiences) {
			fmt.Fprintf(w, "Audience:   %s reads %s\n", a, strings.Join(rep.Audiences[a], ", "))
		}
	}
	for _, t := range rep.Tables {
		fmt.Fprintf(w, "\n%s.%s  table: %s\n", t.Database, t.Table, t.TableTag)
		for _, c := range t.Columns {
			fmt.Fprintf(w, "  %s: %s\n", c.Name, c.Tag)
		}
	}
	for _, p := range rep.Problems {
		fmt.Fprintf(w, "\nproblem: %s\n", p)
	}
}

func sortedAudiences(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// printAccessSync prints what a tag sync changed. Deploy and `access sync`
// share it.
func printAccessSync(out, errw io.Writer, res *tuiservice.AccessSyncResult) {
	for _, c := range res.Changes {
		fmt.Fprintf(out, "Column tags: %s\n", c)
	}
	if res.ProgressScrubbed > 0 {
		fmt.Fprintf(out, "Column tags: removed the error text from %d _progress/ marker(s) written before column tags\n", res.ProgressScrubbed)
	}
	for _, w := range res.Warnings {
		fmt.Fprintf(errw, "warning: %s\n", w)
	}
	if len(res.NotCreated) > 0 {
		fmt.Fprintf(out, "Column tags: not created yet, hidden until their first run and the next `clavesa workspace access sync`: %s\n", strings.Join(res.NotCreated, ", "))
	}
	if len(res.Untagged) > 0 {
		fmt.Fprintf(out, "Column tags: no output declares these tables, so no viewer reads them: %s\n", strings.Join(res.Untagged, ", "))
	}
}
