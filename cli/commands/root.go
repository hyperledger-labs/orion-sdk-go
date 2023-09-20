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
	cmd.AddCommand(versionCmd())
	cmd.AddCommand(configCmd())
	cmd.AddCommand(adminCmd())
	cmd.AddCommand(nodeCmd())
	cmd.AddCommand(casCmd())
	return cmd
}
