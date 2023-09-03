package commands

import (
	"github.com/spf13/cobra"
)

func rootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "bcdbadmin",
		Short: "Config Orion via CLI.",
	}
	return rootCmd
}

func InitializeOrionCli() *cobra.Command {
	cmd := rootCmd()
	cmd.AddCommand(VersionCmd())
	cmd.AddCommand(configCmd())
	return cmd
}
