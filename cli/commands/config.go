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
	"github.com/spf13/viper"
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
		Use:     "set",
		Short:   "Set cluster configuration",
		Example: "cli config set -d <path-to-connection-and-session-config> -c <path-to-new-cluster-config>",
		RunE:    setConfig,
	}

	setConfigCmd.PersistentFlags().StringP("cluster-config-path", "c", "", "set the absolute or relative path of the new server configuration file")
	if err := setConfigCmd.MarkPersistentFlagRequired("cluster-config-path"); err != nil {
		panic(err.Error())
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

func setConfig(cmd *cobra.Command, args []string) error {
	cliConfigPath, err := cmd.Flags().GetString("db-connection-config-path")
	if err != nil {
		return errors.Wrapf(err, "failed to fetch the path of CLI connection configuration file")
	}

	newClusterConfigPath, err := cmd.Flags().GetString("cluster-config-path")
	if err != nil {
		return errors.Wrapf(err, "failed to fetch the new server configuration path")
	}

	lastVersion, err := readVersionYaml(path.Join(newClusterConfigPath, "version.yml"))
	if err != nil {
		return errors.Wrapf(err, "failed to read the version yaml file")
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

	_, version, err := tx.GetClusterConfig()
	if err != nil {
		return errors.Wrapf(err, "failed to fetch cluster config")
	}

	// Check that the version in the version.yaml file is the same version as recieved by GetClusterConfig TX
	if version != lastVersion {
		errors.New("Cluster configuration cannot be updated since the version is not up to date")
	}

	// Read the new shared configuration and build the new cluster config
	newSharedConfig, err := readSharedConfigYaml(path.Join(newClusterConfigPath, "new_cluster_config.yml"))
	if err != nil {
		return errors.Wrapf(err, "failed to read the new cluster configuration file")
	}

	newConfig, err := buildClusterConfig(newSharedConfig)
	if err != nil {
		return errors.Wrapf(err, "failed to build the cluster configuration from the shared configuration")
	}

	// Set cluster configuration to the new one
	err = tx.SetClusterConfig(newConfig)
	if err != nil {
		return errors.Wrapf(err, "failed to set the cluster config")
	}

	_, _, err = tx.Commit(true)
	if err != nil {
		return errors.Wrapf(err, "failed to commit tx")
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
	if err != nil {
		return err
	}

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

// readVersionYaml reads the version from the file and returns it.
func readVersionYaml(versionFile string) (*types.Version, error) {
	if versionFile == "" {
		return nil, errors.New("path to the shared configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(versionFile)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "error reading version file: %s", versionFile)
	}

	version := &types.Version{}
	if err := v.UnmarshalExact(version); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal version file: '%s' into struct", version)
	}
	return version, nil
}

// readSharedConfigYaml reads the shared config from the file and returns it.
func readSharedConfigYaml(sharedConfigFile string) (*SharedConfiguration, error) {
	if sharedConfigFile == "" {
		return nil, errors.New("path to the shared configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(sharedConfigFile)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "error reading shared config file: %s", sharedConfigFile)
	}

	sharedConf := &SharedConfiguration{}
	if err := v.UnmarshalExact(sharedConf); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal shared config file: '%s' into struct", sharedConfigFile)
	}
	return sharedConf, nil
}

// buildClusterConfig builds a cluster configuration from a shared configuration
func buildClusterConfig(sharedConfig *SharedConfiguration) (*types.ClusterConfig, error) {
	var nodesConfig []*types.NodeConfig
	for _, node := range sharedConfig.Nodes {
		cert, err := os.ReadFile(node.CertificatePath)
		pemBlock, _ := pem.Decode(cert)
		cert = pemBlock.Bytes
		if err != nil {
			return nil, errors.Wrapf(err, "unable read the certificate of node '%s'", node.NodeID)
		}
		nodeConfig := &types.NodeConfig{
			Id:          node.NodeID,
			Address:     node.Host,
			Port:        node.Port,
			Certificate: cert,
		}
		nodesConfig = append(nodesConfig, nodeConfig)
	}

	var adminsConfig []*types.Admin
	for _, admin := range sharedConfig.Admin {
		cert, err := os.ReadFile(admin.CertificatePath)
		pemBlock, _ := pem.Decode(cert)
		cert = pemBlock.Bytes
		if err != nil {
			return nil, errors.Wrapf(err, "unable read the certificate of admin '%s'", admin.ID)
		}
		adminConfig := &types.Admin{
			Id:          admin.ID,
			Certificate: cert,
		}
		adminsConfig = append(adminsConfig, adminConfig)
	}

	var rootCACertsConfig [][]byte
	for i, rootCACertPath := range sharedConfig.CAConfig.RootCACertsPath {
		rootCACertConfig, err := os.ReadFile(rootCACertPath)
		pemBlock, _ := pem.Decode(rootCACertConfig)
		rootCACertConfig = pemBlock.Bytes
		if err != nil {
			return nil, errors.Wrapf(err, "unable read the certificate of rootCA '%d'", i)
		}
		rootCACertsConfig = append(rootCACertsConfig, rootCACertConfig)
	}

	var intermediateCACertsConfig [][]byte
	for i, intermediateCACertPath := range sharedConfig.CAConfig.IntermediateCACertsPath {
		intermediateCACertConfig, err := os.ReadFile(intermediateCACertPath)
		pemBlock, _ := pem.Decode(intermediateCACertConfig)
		intermediateCACertConfig = pemBlock.Bytes
		if err != nil {
			return nil, errors.Wrapf(err, "unable read the certificate of intermediateCA '%d'", i)
		}
		intermediateCACertsConfig = append(intermediateCACertsConfig, intermediateCACertConfig)
	}

	var membersConfig []*types.PeerConfig
	for _, member := range sharedConfig.Consensus.Members {
		memberConfig := &types.PeerConfig{
			NodeId:   member.NodeId,
			RaftId:   member.RaftId,
			PeerHost: member.PeerHost,
			PeerPort: member.PeerPort,
		}
		membersConfig = append(membersConfig, memberConfig)
	}

	var observersConfig []*types.PeerConfig
	for _, observer := range sharedConfig.Consensus.Observers {
		observerConfig := &types.PeerConfig{
			NodeId:   observer.NodeId,
			RaftId:   observer.RaftId,
			PeerHost: observer.PeerHost,
			PeerPort: observer.PeerPort,
		}
		observersConfig = append(observersConfig, observerConfig)
	}

	clusterConfig := &types.ClusterConfig{
		Nodes:  nodesConfig,
		Admins: adminsConfig,
		CertAuthConfig: &types.CAConfig{
			Roots:         rootCACertsConfig,
			Intermediates: intermediateCACertsConfig,
		},
		ConsensusConfig: &types.ConsensusConfig{
			Algorithm: sharedConfig.Consensus.Algorithm,
			Members:   membersConfig,
			Observers: observersConfig,
			RaftConfig: &types.RaftConfig{
				TickInterval:         sharedConfig.Consensus.RaftConfig.TickInterval,
				ElectionTicks:        sharedConfig.Consensus.RaftConfig.ElectionTicks,
				HeartbeatTicks:       sharedConfig.Consensus.RaftConfig.HeartbeatTicks,
				MaxInflightBlocks:    sharedConfig.Consensus.RaftConfig.MaxInflightBlocks,
				SnapshotIntervalSize: sharedConfig.Consensus.RaftConfig.SnapshotIntervalSize,
			},
		},
		LedgerConfig: &types.LedgerConfig{StateMerklePatriciaTrieDisabled: sharedConfig.Ledger.StateMerklePatriciaTrieDisabled},
	}

	return clusterConfig, nil
}
