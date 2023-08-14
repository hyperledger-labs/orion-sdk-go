package commands

import (
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	orionconfig "github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path"
)

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

var params cliConfigParams
var getServerConfigPath string

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.PersistentFlags().StringVarP(&params.cliConfigPath, "cli-config-path", "c", "", "set the absolute path of CLI connection configuration file")
	configCmd.MarkPersistentFlagRequired("cli-config-path")

	configCmd.AddCommand(getConfigCmd, setConfigCmd)

	getConfigCmd.PersistentFlags().StringVarP(&getServerConfigPath, "server-config-path", "p", "", "set the absolute path to which the server configuration will be saved")
	getConfigCmd.MarkPersistentFlagRequired("server-config-path")

	//TODO: add flags to setConfig command if needed
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Admin cluster configuration",
	Long: "The config command allows you to manage the cluster configuration. " +
		"You can use get to retrieve the current cluster configuration or set to update the cluster configuration",
}

var getConfigCmd = &cobra.Command{
	Use:     "get",
	Short:   "Get cluster configuration",
	Example: "cli config get -c <path-to-connection-and-session-config> -p <path-to-cluster-config>",
	RunE:    getConfig,
}

var setConfigCmd = &cobra.Command{
	Use:   "set",
	Short: "Set cluster configuration",
	RunE:  setConfig,
}

func getConfig(cmd *cobra.Command, args []string) error {
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
	//TODO: parse the cluster config obj to certificate directory and yaml file using WriteClusterConfigToYaml
	configCmd.Printf(params.cliConfigPath)
	configCmd.Printf("%v\n", clusterConfig)

	return nil
}

func setConfig(cmd *cobra.Command, args []string) error {

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

	//configCmd.SilenceUsage = true
	//configCmd.Printf(params.cliConfigPath)

	return nil
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
