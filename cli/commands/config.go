package commands

import (
	"encoding/pem"
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
	"os"
	"path"
	"strconv"
)

const (
	ConfigDirName = "configTestRes"
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
var getClusterConfigPath string

func configCmd() *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Admin cluster configuration",
		Long: "The config command allows you to manage the cluster configuration. " +
			"You can use get to retrieve the current cluster configuration or set to update the cluster configuration",
	}

	configCmd.PersistentFlags().StringVarP(&params.cliConfigPath, "cli-config-path", "c", "", "set the absolute path of CLI connection configuration file")
	configCmd.MarkPersistentFlagRequired("cli-config-path")

	configCmd.AddCommand(getConfigCmd(), setConfigCmd())

	return configCmd
}

func getConfigCmd() *cobra.Command {
	getConfigCmd := &cobra.Command{
		Use:     "get",
		Short:   "Get cluster configuration",
		Example: "cli config get -c <path-to-connection-and-session-config> -p <path-to-cluster-config>",
		RunE:    getConfig,
	}

	getConfigCmd.PersistentFlags().StringVarP(&getClusterConfigPath, "cluster-config-path", "p", "", "set the absolute path to which the server configuration will be saved")
	getConfigCmd.MarkPersistentFlagRequired("cluster-config-path")

	return getConfigCmd
}

func setConfigCmd() *cobra.Command {
	setConfigCmd := &cobra.Command{
		Use:   "set",
		Short: "Set cluster configuration",
		RunE:  setConfig,
	}
	return setConfigCmd
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

	//configCmd.SilenceUsage = true

	err = parseAndSaveCerts(clusterConfig, getClusterConfigPath)
	if err != nil {
		return errors.Wrapf(err, "failed to fetch certificates from cluster config")
	}

	err = WriteClusterConfigToYaml(clusterConfig, getClusterConfigPath)
	if err != nil {
		return errors.Wrapf(err, "failed to create cluster config yaml file")
	}

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

// ReadAndConstructCliConnConfig read unmarshal the yaml config file into a CliConnectionConfig object
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

// WriteClusterConfigToYaml builds the shared clusterConfig object and writes it to a YAML file.
func WriteClusterConfigToYaml(clusterConfig *types.ClusterConfig, configYamlFilePath string) error {
	sharedConfiguration := buildSharedClusterConfig(clusterConfig, configYamlFilePath)
	c, err := yaml.Marshal(sharedConfiguration)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path.Join(configYamlFilePath, ConfigDirName, "shared_cluster_config.yml"), c, 0644)
	if err != nil {
		return err
	}
	return nil
}

// parse certificates and save in a folder structure
func parseAndSaveCerts(clusterConfig *types.ClusterConfig, getClusterConfigPath string) error {
	for _, node := range clusterConfig.Nodes {
		nodeCert := node.Certificate
		nodePemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeCert})
		nodeCertDirPath := path.Join(getClusterConfigPath, ConfigDirName, "nodes")
		err := os.MkdirAll(nodeCertDirPath, 0755)
		if err != nil {
			return err
		}

		fileName := node.GetId() + ".pem"
		nodeCertFilePath := path.Join(nodeCertDirPath, fileName)

		err = os.WriteFile(nodeCertFilePath, nodePemCert, 0644)
		if err != nil {
			return err
		}
	}

	for _, adminNode := range clusterConfig.Admins {
		adminNodeCert := adminNode.Certificate
		adminNodePemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: adminNodeCert})
		adminNodeCertDirPath := path.Join(getClusterConfigPath, ConfigDirName, "admins")
		err := os.MkdirAll(adminNodeCertDirPath, 0755)
		if err != nil {
			return err
		}

		fileName := adminNode.GetId() + ".pem"
		nodeCertFilePath := path.Join(adminNodeCertDirPath, fileName)

		err = os.WriteFile(nodeCertFilePath, adminNodePemCert, 0644)
		if err != nil {
			return err
		}
	}

	for i, rootCACert := range clusterConfig.CertAuthConfig.Roots {
		rootCAPemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootCACert})
		rootCACertDirPath := path.Join(getClusterConfigPath, ConfigDirName, "rootCAs")
		err := os.MkdirAll(rootCACertDirPath, 0755)
		if err != nil {
			return err
		}

		fileName := "rootCA" + strconv.Itoa(i) + ".pem"
		rootCACertFilePath := path.Join(rootCACertDirPath, fileName)

		err = os.WriteFile(rootCACertFilePath, rootCAPemCert, 0644)
		if err != nil {
			return err
		}
	}

	for i, intermediateCACert := range clusterConfig.CertAuthConfig.Intermediates {
		intermediateCAPemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: intermediateCACert})
		intermediateCACertDirPath := path.Join(getClusterConfigPath, ConfigDirName, "intermediateCAs")
		err := os.MkdirAll(intermediateCACertDirPath, 0755)
		if err != nil {
			return err
		}

		fileName := "intermediateCA" + strconv.Itoa(i) + ".pem"
		intermediateCACertFilePath := path.Join(intermediateCACertDirPath, fileName)

		err = os.WriteFile(intermediateCACertFilePath, intermediateCAPemCert, 0644)
		if err != nil {
			return err
		}
	}

	return nil
}

// buildSharedClusterConfig builds the shared configuration from a clusterConfig
func buildSharedClusterConfig(clusterConfig *types.ClusterConfig, configYamlFilePath string) *orionconfig.SharedConfiguration {
	var nodesSharedConfiguration []*orionconfig.NodeConf
	for _, node := range clusterConfig.Nodes {
		nodeSharedConfiguration := &orionconfig.NodeConf{
			NodeID:          node.GetId(),
			Host:            node.GetAddress(),
			Port:            node.GetPort(),
			CertificatePath: path.Join(configYamlFilePath, ConfigDirName, "nodes", node.GetId()+".pem"),
		}
		nodesSharedConfiguration = append(nodesSharedConfiguration, nodeSharedConfiguration)
	}

	var membersSharedConfiguration []*orionconfig.PeerConf
	for _, member := range clusterConfig.ConsensusConfig.Members {
		memberSharedConfiguration := &orionconfig.PeerConf{
			NodeId:   member.GetNodeId(),
			RaftId:   member.GetRaftId(),
			PeerHost: member.GetPeerHost(),
			PeerPort: member.GetPeerPort(),
		}
		membersSharedConfiguration = append(membersSharedConfiguration, memberSharedConfiguration)
	}

	var observersSharedConfiguration []*orionconfig.PeerConf
	for _, observer := range clusterConfig.ConsensusConfig.Observers {
		observerSharedConfiguration := &orionconfig.PeerConf{
			NodeId:   observer.GetNodeId(),
			RaftId:   observer.GetRaftId(),
			PeerHost: observer.GetPeerHost(),
			PeerPort: observer.GetPeerPort(),
		}
		observersSharedConfiguration = append(observersSharedConfiguration, observerSharedConfiguration)
	}

	var rootCACertsPathSharedConfiguration []string
	for i, root := range clusterConfig.CertAuthConfig.Roots {
		rootCACertPathSharedConfiguration := "[]"
		if root != nil {
			rootCACertPathSharedConfiguration = path.Join(configYamlFilePath, ConfigDirName, "rootCAs", "rootCA"+strconv.Itoa(i)+".pem")
		}
		rootCACertsPathSharedConfiguration = append(rootCACertsPathSharedConfiguration, rootCACertPathSharedConfiguration)
	}

	var intermediateCACertsPathSharedConfiguration []string
	for i, intermediateCA := range clusterConfig.CertAuthConfig.Intermediates {
		intermediateCACertPathSharedConfiguration := "[]"
		if intermediateCA != nil {
			intermediateCACertPathSharedConfiguration = path.Join(configYamlFilePath, ConfigDirName, "intermediateCAs", "intermediateCA"+strconv.Itoa(i)+".pem")
		}
		intermediateCACertsPathSharedConfiguration = append(intermediateCACertsPathSharedConfiguration, intermediateCACertPathSharedConfiguration)
	}

	sharedConfiguration := &orionconfig.SharedConfiguration{
		Nodes: nodesSharedConfiguration,
		Consensus: &orionconfig.ConsensusConf{
			Algorithm: clusterConfig.ConsensusConfig.Algorithm,
			Members:   membersSharedConfiguration,
			Observers: observersSharedConfiguration,
			RaftConfig: &orionconfig.RaftConf{
				TickInterval:         clusterConfig.ConsensusConfig.RaftConfig.TickInterval,
				ElectionTicks:        clusterConfig.ConsensusConfig.RaftConfig.ElectionTicks,
				HeartbeatTicks:       clusterConfig.ConsensusConfig.RaftConfig.HeartbeatTicks,
				MaxInflightBlocks:    clusterConfig.ConsensusConfig.RaftConfig.MaxInflightBlocks,
				SnapshotIntervalSize: clusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize,
			},
		},
		CAConfig: orionconfig.CAConfiguration{
			RootCACertsPath:         rootCACertsPathSharedConfiguration,
			IntermediateCACertsPath: intermediateCACertsPathSharedConfiguration,
		},
		Admin: orionconfig.AdminConf{
			ID:              clusterConfig.Admins[0].Id,
			CertificatePath: path.Join(getClusterConfigPath, ConfigDirName, "admins", clusterConfig.Admins[0].Id+".pem"),
		},
		Ledger: orionconfig.LedgerConf{StateMerklePatriciaTrieDisabled: clusterConfig.LedgerConfig.StateMerklePatriciaTrieDisabled},
	}

	return sharedConfiguration
}
