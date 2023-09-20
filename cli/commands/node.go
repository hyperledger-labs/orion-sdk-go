package commands

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func nodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "manage cluster",
		Args:  nil,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "add",
		Short: "Add a cluster node",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "remove",
		Short: "Remove a cluster node",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "update",
		Short: "Update a cluster node",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	})

	return cmd
}
