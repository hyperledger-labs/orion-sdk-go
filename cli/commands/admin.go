package commands

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func AdminCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "manage administrators",
		Args:  nil,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "add",
		Short: "Add an admin",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "remove",
		Short: "Remove an admin",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "update",
		Short: "Update an admin",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	})

	return cmd
}
