// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	orionconfig "github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	cmd := OrionCli()
	// On failure Cobra prints the usage message and error string, so we only need to exit with a non-0 status
	if cmd.Execute() != nil {
		os.Exit(1)
	}
}

type CliConnectionConfig struct {
	Connection config.ConnectionConfig `yaml:"connection"`
	Session    config.SessionConfig    `yaml:"session"`
}

func (c *CliConnectionConfig) Read(filePath string) error {
	if filePath == "" {
		return errors.New("file path is empty")
	}

	v := viper.New()
	v.SetConfigFile(filePath)

	if err := v.ReadInConfig(); err != nil {
		return err
	}

	if err := v.UnmarshalExact(c); err != nil {
		return err
	}

	clientLogger, err := logger.New(
		&logger.Config{
			Level:         "debug",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		},
	)
	if err != nil {
		return err
	}
	c.Connection.Logger = clientLogger

	return nil
}

func OrionCli() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "orion-cli",
		Short: "Config orion via CLI.",
	}
	cmd.AddCommand(versionCmd())
	cmd.AddCommand(configCmd())
	return cmd
}

func versionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Args:  cobra.NoArgs,
		Short: "Print the version of the cli tool.",
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

	return cmd
}

func abort(tx bcdb.TxContext) {
	_ = tx.Abort()
}

type cliConfigParams struct {
	cliConfigPath string
	cliConfig     CliConnectionConfig
	db            bcdb.BCDB
	session       bcdb.DBSession
}

func configCmd() *cobra.Command {
	params := &cliConfigParams{}
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Admin configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if err = params.cliConfig.Read(params.cliConfigPath); err != nil {
				return errors.Wrapf(err, "failed to read CLI configuration file")
			}

			params.db, err = bcdb.Create(&params.cliConfig.Connection)
			if err != nil {
				return errors.Wrapf(err, "failed to instanciate a databse connection")
			}

			params.session, err = params.db.Session(&params.cliConfig.Session)
			if err != nil {
				return errors.Wrapf(err, "failed to instanciate a databse session")
			}

			return nil
		},
	}

	cmd.PersistentFlags().StringVarP(&params.cliConfigPath, "cli-config-path", "c", "",
		"set the absolute path of CLI connection configuration file")
	if err := cmd.MarkPersistentFlagRequired("cli-config-path"); err != nil {
		panic(err)
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "get",
		Short: "Get server configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			tx, err := params.session.ConfigTx()
			if err != nil {
				return errors.Wrapf(err, "failed to instanciate a config TX")
			}
			defer abort(tx)

			conf, err := tx.GetClusterConfig()
			if err != nil {
				return errors.Wrapf(err, "failed to fetch cluster config")
			}

			cmd.SilenceUsage = true
			cmd.Printf(params.cliConfigPath)
			cmd.Printf("%v\n", conf)

			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "set",
		Short: "Set server configuration",
		RunE: func(cmd *cobra.Command, args []string) error {

			_, err := orionconfig.Read(params.cliConfigPath)
			if err != nil {
				return err
			}

			cmd.SilenceUsage = true
			cmd.Printf(params.cliConfigPath)

			return nil
		},
	})
	return cmd
}
