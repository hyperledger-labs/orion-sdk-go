// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"io/ioutil"
	"os"
	"path"
	"runtime/debug"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	orionconfig "github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

func main() {
	cmd := OrionCliInit()
	// On failure Cobra prints the usage message and error string, so we only need to exit with a non-0 status
	if cmd.Execute() != nil {
		os.Exit(1)
	}
}

func OrionCliInit() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "Orion-CLI",
		Short: "Config Orion via CLI.",
	}
	cmd.AddCommand(versionCmd())
	cmd.AddCommand(configCmd())
	cmd.AddCommand(adminCmd())
	cmd.AddCommand(nodeCmd())
	cmd.AddCommand(CAsCmd())
	return cmd
}

// versionCmd - check what command it is
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

type CliConnectionConfig struct {
	ConnectionConfig config.ConnectionConfig `yaml:"connection"`
	SessionConfig    config.SessionConfig    `yaml:"session"`
}

type cliConfigParams struct {
	cliConfigPath string
	cliConfig     CliConnectionConfig
	db            bcdb.BCDB
	session       bcdb.DBSession
}

// config command
func configCmd() *cobra.Command {
	params := &cliConfigParams{
		cliConfig: CliConnectionConfig{},
	}

	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Admin cluster configuration",
		Long: "The config command allows you to manage the cluster configuration. " +
			"You can use get to retrieve the current cluster configuration or set to update the cluster configuration",
	}

	configCmd.PersistentFlags().StringVarP(&params.cliConfigPath, "cli-config-path", "c", "",
		"set the absolute path of CLI connection configuration file")
	if err := configCmd.MarkPersistentFlagRequired("cli-config-path"); err != nil {
		panic(err)
	}

	var getServerConfigPath string
	getConfigCmd := &cobra.Command{
		Use:     "get",
		Short:   "Get cluster configuration",
		Example: "cli config get -c <path-to-connection-and-session-config> -p <path-to-cluster-config>",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := params.CreateDbAndOpenSession()
			if err != nil {
				return err
			}

			tx, err := params.session.ConfigTx()
			if err != nil {
				return errors.Wrapf(err, "failed to instanciate a config TX")
			}
			defer abort(tx)

			clusterConfig, err := tx.GetClusterConfig()
			if err != nil {
				return errors.Wrapf(err, "failed to fetch cluster config")
			}

			configCmd.SilenceUsage = true

			// TODO: parse the cluster config obj to certificate directory and yaml file using WriteClusterConfigToYaml
			configCmd.Printf(params.cliConfigPath)
			configCmd.Printf("%v\n", clusterConfig)

			return nil
		},
	}

	getConfigCmd.PersistentFlags().StringVarP(&getServerConfigPath, "server-config-path", "p", "",
		"set the absolute path to which the server configuration will be saved")
	if err := getConfigCmd.MarkPersistentFlagRequired("server-config-path"); err != nil {
		panic(err)
	}

	setConfigCmd := &cobra.Command{
		Use:   "set",
		Short: "Set cluster configuration",
		RunE: func(cmd *cobra.Command, args []string) error {

			_, err := orionconfig.Read(params.cliConfigPath)
			if err != nil {
				return err
			}

			err = params.CreateDbAndOpenSession()
			if err != nil {
				return err
			}

			tx, err := params.session.ConfigTx()
			if err != nil {
				return errors.Wrapf(err, "failed to instanciate a config TX")
			}
			defer abort(tx)
			// TODO: set the cluster configuration
			//err := tx.SetClusterConfig()
			//if err != nil {
			//	return errors.Wrapf(err, "failed to fetch cluster config")
			//}

			configCmd.SilenceUsage = true
			configCmd.Printf(params.cliConfigPath)

			return nil
		},
	}

	configCmd.AddCommand(getConfigCmd, setConfigCmd)
	return configCmd
}

// admin command
func adminCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "manage administrators",
		Args:  nil,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "add",
		Short: "Add an admin",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "remove",
		Short: "Remove an admin",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "update",
		Short: "Update an admin",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	})

	return cmd
}

// node command
func nodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "manage cluster",
		Args:  nil,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "add",
		Short: "Add a cluster node",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "remove",
		Short: "Remove a cluster node",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "update",
		Short: "Update a cluster node",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	})

	return cmd
}

// CAs command
func CAsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "CAs",
		Short: "manage CA's",
		Args:  nil,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "add",
		Short: "Add CA",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "remove",
		Short: "Remove CA",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	})

	return cmd
}

// Read unmarshal the yaml config file into a CliConnectionConfig object.
func (c *CliConnectionConfig) ReadAndConstructCliConnConfig(filePath string) error {
	if filePath == "" {
		return errors.New("path to the shared configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(filePath)

	if err := v.ReadInConfig(); err != nil {
		return errors.Wrapf(err, "error reading shared config file: %s", filePath)
	}

	if err := v.UnmarshalExact(c); err != nil {
		return errors.Wrapf(err, "unable to unmarshal shared config file: '%s' into struct", filePath)
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
	c.ConnectionConfig.Logger = clientLogger

	return nil
}

// CreateDbAndOpenSession read connection and session configurations to create a db instance and open a session with the server.
func (c *cliConfigParams) CreateDbAndOpenSession() error {
	var err error
	if err = c.cliConfig.ReadAndConstructCliConnConfig(c.cliConfigPath); err != nil {
		return errors.Wrapf(err, "failed to read CLI configuration file")
	}

	c.db, err = bcdb.Create(&c.cliConfig.ConnectionConfig)
	if err != nil {
		return errors.Wrapf(err, "failed to instanciate a databse connection")
	}

	c.session, err = c.db.Session(&c.cliConfig.SessionConfig)
	if err != nil {
		return errors.Wrapf(err, "failed to instanciate a databse session")
	}

	return nil
}

func abort(tx bcdb.TxContext) {
	_ = tx.Abort()
}

// WriteClusterConfigToYaml writes the shared clusterConfig object to a YAML file.
func WriteClusterConfigToYaml(clusterConfig *types.ClusterConfig, configYamlFilePath string) error {
	c, err := yaml.Marshal(clusterConfig)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path.Join(configYamlFilePath, "config", "config.yml"), c, 0644)
	if err != nil {
		return err
	}

	return nil
}
