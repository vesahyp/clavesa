package cli

import (
	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/service"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the clavesa version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.Println(service.ModuleVersion)
			return nil
		},
	}
}
