package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"runtime/debug"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Args:  cobra.NoArgs,
	Short: "Print the version of the CLI tool.",
	RunE: func(cmd *cobra.Command, args []string) error {
		bi, ok := debug.ReadBuildInfo()
		if !ok {
			return fmt.Errorf("failed to read build info")
		}

		cmd.Printf("SDK version: %+v\n", bi.Main.Version)

		for _, dep := range bi.Deps {
			if dep.Path == "github.com/hyperledger-labs/orion-server" {
				cmd.Printf("Orion server version: %+v\n", dep.Version)
				break
			}
		}

		return nil
	},
}
