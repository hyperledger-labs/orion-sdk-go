package commands

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func casCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "CAs",
		Short: "manage CA's",
		Args:  nil,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "add",
		Short: "Add CA",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "remove",
		Short: "Remove CA",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	})

	return cmd
}
