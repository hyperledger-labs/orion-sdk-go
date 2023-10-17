package commands

import (
	"encoding/pem"
	"os"
	"path"
	"strconv"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	orionconfig "github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

func configCmd() *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Manage cluster configuration",
		Long: "The config command allows you to manage the cluster configuration. " +
			"You can use 'get' to retrieve the current cluster configuration or 'set' to update the cluster configuration",
	}

	configCmd.PersistentFlags().StringP("db-connection-config-path", "d", "", "set the absolute or relative path of CLI connection configuration file")
	if err := configCmd.MarkPersistentFlagRequired("db-connection-config-path"); err != nil {
		panic(err.Error())
	}

	configCmd.AddCommand(getConfigCmd(), setConfigCmd())

	return configCmd
}

func getConfigCmd() *cobra.Command {
	getConfigCmd := &cobra.Command{
		Use:     "get",
		Short:   "Get cluster configuration",
		Example: "cli config get -d <path-to-connection-and-session-config> -c <path-to-cluster-config>",
		RunE:    getConfig,
	}

	getConfigCmd.PersistentFlags().StringP("cluster-config-path", "c", "", "set the absolute or relative path to which the server configuration will be saved")
	if err := getConfigCmd.MarkPersistentFlagRequired("cluster-config-path"); err != nil {
		panic(err.Error())
	}

	return getConfigCmd
}

func setConfigCmd() *cobra.Command {
	setConfigCmd := &cobra.Command{
		Use:   "set",
		Short: "Set cluster configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	}
	return setConfigCmd
}

func getConfig(cmd *cobra.Command, args []string) error {
	cliConfigPath, err := cmd.Flags().GetString("db-connection-config-path")
	if err != nil {
		return errors.Wrapf(err, "failed to fetch the path of CLI connection configuration file")
	}

	getClusterConfigPath, err := cmd.Flags().GetString("cluster-config-path")
	if err != nil {
		return errors.Wrapf(err, "failed to fetch the path to which the server configuration will be saved")
	}

	params := cliConfigParams{
		cliConfigPath: cliConfigPath,
		cliConfig:     cliConnectionConfig{},
		db:            nil,
		session:       nil,
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

	clusterConfig, version, err := tx.GetClusterConfig()
	if err != nil {
		return errors.Wrapf(err, "failed to fetch cluster config")
	}

	err = parseAndSaveCerts(clusterConfig, getClusterConfigPath)
	if err != nil {
		return errors.Wrapf(err, "failed to fetch certificates from cluster config")
	}

	err = WriteClusterConfigToYaml(clusterConfig, version, getClusterConfigPath)
	if err != nil {
		return errors.Wrapf(err, "failed to create cluster config yaml file")
	}

	return nil
}

func abort(tx bcdb.TxContext) {
	_ = tx.Abort()
}

// WriteClusterConfigToYaml builds the shared clusterConfig object and writes it to a YAML file.
func WriteClusterConfigToYaml(clusterConfig *types.ClusterConfig, version *types.Version, configYamlFilePath string) error {
	sharedConfiguration := buildSharedClusterConfig(clusterConfig, configYamlFilePath)
	c, err := yaml.Marshal(sharedConfiguration)
	if err != nil {
		return err
	}

	v, err := yaml.Marshal(version)
	if err != nil {
		return err
	}

	err = os.WriteFile(path.Join(configYamlFilePath, "shared_cluster_config.yml"), c, 0644)
	err = os.WriteFile(path.Join(configYamlFilePath, "version.yml"), v, 0644)
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
		nodeCertDirPath := path.Join(getClusterConfigPath, "nodes")
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
		adminNodeCertDirPath := path.Join(getClusterConfigPath, "admins")
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
		rootCACertDirPath := path.Join(getClusterConfigPath, "rootCAs")
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
		intermediateCACertDirPath := path.Join(getClusterConfigPath, "intermediateCAs")
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
func buildSharedClusterConfig(clusterConfig *types.ClusterConfig, configYamlFilePath string) *SharedConfiguration {
	var nodesSharedConfiguration []*NodeConf
	for _, node := range clusterConfig.Nodes {
		nodeSharedConfiguration := &NodeConf{
			NodeID:          node.GetId(),
			Host:            node.GetAddress(),
			Port:            node.GetPort(),
			CertificatePath: path.Join(configYamlFilePath, "nodes", node.GetId()+".pem"),
		}
		nodesSharedConfiguration = append(nodesSharedConfiguration, nodeSharedConfiguration)
	}

	var membersSharedConfiguration []*PeerConf
	for _, member := range clusterConfig.ConsensusConfig.Members {
		memberSharedConfiguration := &PeerConf{
			NodeId:   member.GetNodeId(),
			RaftId:   member.GetRaftId(),
			PeerHost: member.GetPeerHost(),
			PeerPort: member.GetPeerPort(),
		}
		membersSharedConfiguration = append(membersSharedConfiguration, memberSharedConfiguration)
	}

	var observersSharedConfiguration []*PeerConf
	for _, observer := range clusterConfig.ConsensusConfig.Observers {
		observerSharedConfiguration := &PeerConf{
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
			rootCACertPathSharedConfiguration = path.Join(configYamlFilePath, "rootCAs", "rootCA"+strconv.Itoa(i)+".pem")
		}
		rootCACertsPathSharedConfiguration = append(rootCACertsPathSharedConfiguration, rootCACertPathSharedConfiguration)
	}

	var intermediateCACertsPathSharedConfiguration []string
	for i, intermediateCA := range clusterConfig.CertAuthConfig.Intermediates {
		intermediateCACertPathSharedConfiguration := "[]"
		if intermediateCA != nil {
			intermediateCACertPathSharedConfiguration = path.Join(configYamlFilePath, "intermediateCAs", "intermediateCA"+strconv.Itoa(i)+".pem")
		}
		intermediateCACertsPathSharedConfiguration = append(intermediateCACertsPathSharedConfiguration, intermediateCACertPathSharedConfiguration)
	}

	var adminsSharedConfiguration []*AdminConf
	for i, admin := range clusterConfig.Admins {
		adminSharedConfiguration := &AdminConf{
			ID:              admin.GetId(),
			CertificatePath: path.Join(configYamlFilePath, "admins", clusterConfig.Admins[i].GetId()+".pem"),
		}
		adminsSharedConfiguration = append(adminsSharedConfiguration, adminSharedConfiguration)
	}

	sharedConfiguration := &SharedConfiguration{
		Nodes: nodesSharedConfiguration,
		Consensus: &ConsensusConf{
			Algorithm: clusterConfig.ConsensusConfig.Algorithm,
			Members:   membersSharedConfiguration,
			Observers: observersSharedConfiguration,
			RaftConfig: &RaftConf{
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
		Admin:  adminsSharedConfiguration,
		Ledger: LedgerConf{StateMerklePatriciaTrieDisabled: clusterConfig.LedgerConfig.StateMerklePatriciaTrieDisabled},
	}

	return sharedConfiguration
}
