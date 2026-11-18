package cli

import (
	"testing"

	"github.com/spf13/cobra"
)

// TestMetastoreKeptOnSubcommand: --keep-metastore is a root persistent
// flag; a subcommand must see it (review of 75033f2, finding 1).
func TestMetastoreKeptOnSubcommand(t *testing.T) {
	t.Setenv(keepMetastoreEnv, "")
	for _, c := range []struct {
		args []string
		want bool
	}{{[]string{"sub", "--keep-metastore"}, true}, {[]string{"sub"}, false}, {[]string{"--keep-metastore", "sub"}, true}} {
		root := newRootCmd()
		var got bool
		sub := &cobra.Command{Use: "sub", RunE: func(cmd *cobra.Command, _ []string) error { got = metastoreKept(cmd); return nil }}
		root.AddCommand(sub)
		root.SetArgs(c.args)
		if err := root.Execute(); err != nil {
			t.Fatalf("%v: %v", c.args, err)
		}
		if got != c.want {
			t.Errorf("%v: kept = %v, want %v", c.args, got, c.want)
		}
	}
}
