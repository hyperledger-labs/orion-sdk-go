package commands

import (
	"bytes"
	"encoding/pem"
	"fmt"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/test/setup"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestCheckCertsEncoderIsValid(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := os.MkdirTemp(os.TempDir(), "Cli-Check-Certs-Test")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. read connection configuration, create db and open session
	c, err := readConnConfig(path.Join(tempDir, "config.yml"))
	require.NoError(t, err)

	bcdb, err := bcdb.Create(&c.ConnectionConfig)
	require.NoError(t, err)

	session, err := bcdb.Session(&c.SessionConfig)
	require.NoError(t, err)

	// 3. get cluster configuration
	tx, err := session.ConfigTx()
	require.NoError(t, err)

	clusterConfig, version, err := tx.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig)
	require.NotNil(t, version)

	// 4. parse the certificates from the cluster configuration and save it in a folder structure
	parsedCertsDir, err := os.MkdirTemp(os.TempDir(), "certsTest")
	defer os.RemoveAll("certsTest")
	require.NoError(t, err)

	err = parseAndSaveCerts(clusterConfig, parsedCertsDir)
	require.NoError(t, err)

	// 5. compare the generated certs with the certs received from the tx
	err = compareFiles(path.Join(tempDir, "crypto", "admin", "admin.pem"), path.Join(parsedCertsDir, "admins", "admin.pem"))
	require.NoError(t, err)
	err = compareFiles(path.Join(tempDir, "crypto", "node", "node.pem"), path.Join(parsedCertsDir, "nodes", "server1.pem"))
	require.NoError(t, err)
	err = compareFiles(path.Join(tempDir, "crypto", "CA", "CA.pem"), path.Join(parsedCertsDir, "rootCAs", "rootCA0.pem"))
	require.NoError(t, err)
}

func TestCheckCertsDecoderIsValid(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := os.MkdirTemp(os.TempDir(), "Cli-Check-Certs-Test")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. read connection configuration, create db and open session
	c, err := readConnConfig(path.Join(tempDir, "config.yml"))
	require.NoError(t, err)

	bcdb, err := bcdb.Create(&c.ConnectionConfig)
	require.NoError(t, err)

	session, err := bcdb.Session(&c.SessionConfig)
	require.NoError(t, err)

	// 3. get cluster configuration
	tx, err := session.ConfigTx()
	require.NoError(t, err)

	clusterConfig, version, err := tx.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig)
	require.NotNil(t, version)

	// 4. Get current cluster configuration by the get command to output a shared_cluster_config.yaml file with the paths for the certs
	rootCmd := InitializeOrionCli()

	pwd, err := os.Getwd()
	require.NoError(t, err)
	testConnConfigFilePath := path.Join(tempDir, "config.yml")
	getConfigDirPath, err := os.MkdirTemp(os.TempDir(), "configTest")
	defer os.RemoveAll(getConfigDirPath)
	require.NoError(t, err)
	getConfigDirRelativePath := path.Join("..", "..", getConfigDirPath)

	rootCmd.SetArgs([]string{"config", "get", "-d", testConnConfigFilePath, "-c", path.Join(pwd, getConfigDirRelativePath)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// 5. read the shared configuration created by the get command to create a sharedConfig object
	sharedConfig, err := readSharedConfigYaml(path.Join(pwd, getConfigDirRelativePath, "shared_cluster_config.yml"))
	require.NoError(t, err)

	// 6. build the cluster config from the shared configuration and compare the certificate bytes with those received by the tx
	recievedClusterConfig, err := buildClusterConfig(sharedConfig)
	require.NoError(t, err)

	require.Equal(t, clusterConfig.Admins[0].Certificate, recievedClusterConfig.Admins[0].Certificate)
	require.Equal(t, clusterConfig.CertAuthConfig.Roots[0], recievedClusterConfig.CertAuthConfig.Roots[0])
	require.Equal(t, clusterConfig.CertAuthConfig.Intermediates, recievedClusterConfig.CertAuthConfig.Intermediates)
	require.Equal(t, clusterConfig.Nodes[0].Certificate, recievedClusterConfig.Nodes[0].Certificate)
}

func TestInvalidFlagsGetConfigCommand(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		expectedErrMsg string
	}{
		{
			name:           "No Flags",
			args:           []string{"config", "get"},
			expectedErrMsg: "required flag(s) \"cluster-config-path\", \"db-connection-config-path\" not set",
		},
		{
			name:           "Missing Cli DB Connection Config Flag",
			args:           []string{"config", "get", "-c", "/path/to/cluster-config.yaml"},
			expectedErrMsg: "required flag(s) \"db-connection-config-path\" not set",
		},
		{
			name:           "Missing Cluster Config Flag",
			args:           []string{"config", "get", "-d", "/path/to/cli-db-connection-config.yaml"},
			expectedErrMsg: "required flag(s) \"cluster-config-path\" not set",
		},
		{
			name:           "File path not found",
			args:           []string{"config", "get", "-d", "/path/to/cli-db-connection-config.yaml", "-c", "/path/to/cluster-config.yaml"},
			expectedErrMsg: "failed to read CLI connection configuration file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootCmd := InitializeOrionCli()
			rootCmd.SetArgs(tt.args)
			err := rootCmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.expectedErrMsg)
		})
	}
}

func TestGetConfigCommand(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := os.MkdirTemp(os.TempDir(), "Cli-Get-Config-Test")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Get cluster config from the server by the CLI GetConfig command
	rootCmd := InitializeOrionCli()
	pwd, err := os.Getwd()
	require.NoError(t, err)
	testDbConnectionConfigFilePath := path.Join(tempDir, "config.yml")
	createdDirName, err := os.MkdirTemp(os.TempDir(), "configTest")
	defer os.RemoveAll(createdDirName)
	relativePathForCreatedDirName := path.Join("..", "..", createdDirName)
	require.NoError(t, err)
	rootCmd.SetArgs([]string{"config", "get", "-d", testDbConnectionConfigFilePath, "-c", filepath.Join(pwd, relativePathForCreatedDirName)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// 3. Check the server response
	// check that certs are equal to the expected certs
	err = compareFiles(path.Join(tempDir, "crypto", "admin", "admin.pem"), path.Join(relativePathForCreatedDirName, "admins", "admin.pem"))
	require.NoError(t, err)
	err = compareFiles(path.Join(tempDir, "crypto", "node", "node.pem"), path.Join(relativePathForCreatedDirName, "nodes", "server1.pem"))
	require.NoError(t, err)
	err = compareFiles(path.Join(tempDir, "crypto", "CA", "CA.pem"), path.Join(relativePathForCreatedDirName, "rootCAs", "rootCA0.pem"))
	require.NoError(t, err)

	// extract server endpoint and compare to the endpoint received by Get Config command
	expectedConfigRes, err := readConnConfig(testDbConnectionConfigFilePath)
	if err != nil {
		errors.Wrapf(err, "failed to read expected shared configuration")
	}

	actualSharedConfigRes, err := readSharedConfigYaml(path.Join(relativePathForCreatedDirName, "shared_cluster_config.yml"))
	if err != nil {
		errors.Wrapf(err, "failed to read shared configuration")
	}

	require.Equal(t, expectedConfigRes.ConnectionConfig.ReplicaSet[0].ID, actualSharedConfigRes.Nodes[0].NodeID)
	url, err := url.Parse(expectedConfigRes.ConnectionConfig.ReplicaSet[0].Endpoint)
	if err != nil {
		errors.Wrapf(err, "failed to parse server endpoint")
	}
	require.Equal(t, url.Host, actualSharedConfigRes.Nodes[0].Host+":"+strconv.Itoa(int(actualSharedConfigRes.Nodes[0].Port)))

	// 4. check the version
	version, err := readVersionYaml(path.Join(relativePathForCreatedDirName, "version.yml"))
	if err != nil {
		errors.Wrapf(err, "failed to read version file")
	}
	require.Equal(t, version.GetBlockNum(), uint64(1))
	require.Equal(t, version.GetTxNum(), uint64(0))
}

func TestInvalidFlagsSetConfigCommand(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		expectedErrMsg string
	}{
		{
			name:           "No Flags",
			args:           []string{"config", "set"},
			expectedErrMsg: "required flag(s) \"cluster-config-path\", \"db-connection-config-path\" not set",
		},
		{
			name:           "Missing Cli Connection Config Flag",
			args:           []string{"config", "set", "-c", "/path/to/cluster-config.yaml"},
			expectedErrMsg: "required flag(s) \"db-connection-config-path\" not set",
		},
		{
			name:           "Missing Cluster Config Flag",
			args:           []string{"config", "set", "-d", "/path/to/cli-config.yaml"},
			expectedErrMsg: "required flag(s) \"cluster-config-path\" not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootCmd := InitializeOrionCli()
			rootCmd.SetArgs(tt.args)
			err := rootCmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.expectedErrMsg)
		})
	}
}

func TestSetConfigCommand_UpdateParam(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := os.MkdirTemp(os.TempDir(), "Cli-Set-Config-Test-Update-Param")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Get current cluster configuration
	rootCmd := InitializeOrionCli()

	pwd, err := os.Getwd()
	require.NoError(t, err)
	testConnConfigFilePath := path.Join(tempDir, "config.yml")
	getConfigDirPath, err := os.MkdirTemp(os.TempDir(), "configTest-1")
	defer os.RemoveAll(getConfigDirPath)
	require.NoError(t, err)
	getConfigDirRelativePath := path.Join("..", "..", getConfigDirPath)

	rootCmd.SetArgs([]string{"config", "get", "-d", testConnConfigFilePath, "-c", path.Join(pwd, getConfigDirRelativePath)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// 3. Prepare the new cluster configuration
	sharedConfigYaml, err := readSharedConfigYaml(path.Join(getConfigDirRelativePath, "shared_cluster_config.yml"))
	require.NoError(t, err)

	clusterConfig, err := buildClusterConfig(sharedConfigYaml)
	require.NoError(t, err)

	clusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize++
	lastSnapshotIntervalSize := clusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize

	err = buildSharedClusterConfigAndWriteConfigToYaml(clusterConfig, getConfigDirRelativePath, "new_cluster_config.yml")
	require.NoError(t, err)

	// 4. Set cluster configuration
	rootCmd.SetArgs([]string{"config", "set", "-d", testConnConfigFilePath, "-c", filepath.Join(pwd, getConfigDirRelativePath)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// 5. Restart the server to apply the new config
	require.NoError(t, testServer.Start())

	// 6. Get cluster configuration again and check the version and the parameter
	getConfigDirPath, err = os.MkdirTemp(os.TempDir(), "configTest-2")
	defer os.RemoveAll(getConfigDirPath)
	require.NoError(t, err)
	getConfigDirRelativePath = path.Join("..", "..", getConfigDirPath)

	rootCmd.SetArgs([]string{"config", "get", "-d", testConnConfigFilePath, "-c", path.Join(pwd, getConfigDirRelativePath)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// 7. check the version
	version, err := readVersionYaml(path.Join(getConfigDirRelativePath, "version.yml"))
	if err != nil {
		errors.Wrapf(err, "failed to read version file")
	}
	require.Equal(t, version.GetBlockNum(), uint64(2))
	require.Equal(t, version.GetTxNum(), uint64(0))

	// 8. check the parameter
	newSharedConfigYaml, err := readSharedConfigYaml(path.Join(getConfigDirRelativePath, "shared_cluster_config.yml"))
	require.NoError(t, err)
	newClusterConfig, err := buildClusterConfig(newSharedConfigYaml)
	require.NoError(t, err)

	require.Equal(t, newClusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize, lastSnapshotIntervalSize)
}

func TestInvalidFlagsGetLastConfigBlockCommand(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		expectedErrMsg string
	}{
		{
			name:           "No Flags",
			args:           []string{"config", "getLastConfigBlock"},
			expectedErrMsg: "required flag(s) \"db-connection-config-path\", \"last-config-block-path\" not set",
		},
		{
			name:           "Missing Cli DB Connection Config Flag",
			args:           []string{"config", "getLastConfigBlock", "-c", "/path/to/cluster-config.yaml"},
			expectedErrMsg: "required flag(s) \"db-connection-config-path\" not set",
		},
		{
			name:           "Missing Last Config Block Flag",
			args:           []string{"config", "getLastConfigBlock", "-d", "/path/to/cli-db-connection-config.yaml"},
			expectedErrMsg: "required flag(s) \"last-config-block-path\" not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootCmd := InitializeOrionCli()
			rootCmd.SetArgs(tt.args)
			err := rootCmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.expectedErrMsg)
		})
	}
}

func TestGetLastConfigBlockCommand(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := os.MkdirTemp(os.TempDir(), "Cli-Get-Last-Config-Block-Test")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Get last config block from the server by the CLI GetLastConfigBlock command
	rootCmd := InitializeOrionCli()

	pwd, err := os.Getwd()
	require.NoError(t, err)
	testDbConnectionConfigFilePath := path.Join(tempDir, "config.yml")
	pathForLastConfigBlockOutput, err := os.MkdirTemp(os.TempDir(), "TestOutput")
	defer os.RemoveAll(pathForLastConfigBlockOutput)
	require.NoError(t, err)
	relativePathForLastConfigBlockOutput := path.Join("..", "..", pathForLastConfigBlockOutput)

	rootCmd.SetArgs([]string{"config", "getLastConfigBlock", "-d", testDbConnectionConfigFilePath, "-c", filepath.Join(pwd, relativePathForLastConfigBlockOutput)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// 3. Check the server response
	blk, err := os.ReadFile(path.Join(relativePathForLastConfigBlockOutput, "last_config_block.yml"))
	if err != nil {
		errors.Wrapf(err, "failed to read last config block file")
	}
	require.NotNil(t, blk)
}

func TestInvalidFlagsGetClusterStatusCommand(t *testing.T) {
	rootCmd := InitializeOrionCli()
	rootCmd.SetArgs([]string{"config", "getClusterStatus"})
	err := rootCmd.Execute()
	require.Error(t, err)
	require.EqualError(t, err, "required flag(s) \"db-connection-config-path\" not set")
}

func TestGetClusterStatusCommand(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := os.MkdirTemp(os.TempDir(), "Cli-Get-Cluster-Status-Test")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Get cluster status by the CLI GetClusterStatus command
	rootCmd := InitializeOrionCli()
	output := new(bytes.Buffer)
	rootCmd.SetOut(output)

	testDbConnectionConfigFilePath := path.Join(tempDir, "config.yml")
	rootCmd.SetArgs([]string{"config", "getClusterStatus", "-d", testDbConnectionConfigFilePath})
	err = rootCmd.Execute()
	require.NoError(t, err)
	require.NotNil(t, output)

	idx := strings.Index(output.String(), "header:")
	extractedOutput := output.String()[idx : len(output.Bytes())-1]
	status := &types.GetClusterStatusResponse{}
	err = proto.UnmarshalText(extractedOutput, status)
	require.NoError(t, err)
	require.Equal(t, 1, len(status.GetNodes()))
	require.Equal(t, 1, len(status.GetActive()))
}

// Start a 3-node cluster.
// Add the 4th node using the CLI
func TestSetConfigCommand_ClusterAddNode(t *testing.T) {
	// create crypto material and start 3-node cluster
	dir, err := os.MkdirTemp("", "cluster-test")
	require.NoError(t, err)
	fmt.Printf("the path for cluster material is: %s", dir)
	nPort := uint32(6581)
	pPort := uint32(6681)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())

	require.Eventually(t, func() bool { return c.AgreedLeader(t, 0, 1, 2) >= 0 }, 30*time.Second, time.Second)

	// create connection configuration file
	connConfig := &sdkconfig.ConnectionConfig{
		RootCAs: []string{path.Join(setupConfig.TestDirAbsolutePath, "ca", testutils.RootCAFileName+".pem")},
		ReplicaSet: []*sdkconfig.Replica{
			{
				ID:       "node-1",
				Endpoint: c.Servers[0].URL(),
			},
			{
				ID:       "node-2",
				Endpoint: c.Servers[1].URL(),
			},
			{
				ID:       "node-3",
				Endpoint: c.Servers[2].URL(),
			},
		},
	}

	sessionConfig := &sdkconfig.SessionConfig{
		UserConfig: &sdkconfig.UserConfig{
			UserID:         "admin",
			CertPath:       path.Join(path.Join(setupConfig.TestDirAbsolutePath, "users"), "admin.pem"),
			PrivateKeyPath: path.Join(path.Join(setupConfig.TestDirAbsolutePath, "users"), "admin.key"),
		},
		TxTimeout:    10 * time.Second,
		QueryTimeout: 20 * time.Second,
	}

	cliConnectionConfig := &util.Config{
		ConnectionConfig: *connConfig,
		SessionConfig:    *sessionConfig,
	}

	marshaledCliConnConfig, err := yaml.Marshal(cliConnectionConfig)
	require.NoError(t, err)

	err = os.WriteFile(path.Join(dir, "connection_config.yml"), marshaledCliConnConfig, 0644)
	require.NoError(t, err)

	// get current cluster configuration
	rootCmd := InitializeOrionCli()

	pwd, err := os.Getwd()
	require.NoError(t, err)
	testConnConfigFilePath := path.Join(dir, "connection_config.yml")
	getConfigDirPath, err := os.MkdirTemp(os.TempDir(), "GetConfig_ClusterTest#1")
	defer os.RemoveAll(getConfigDirPath)
	require.NoError(t, err)
	getConfigDirRelativePath := path.Join("..", "..", getConfigDirPath)

	rootCmd.SetArgs([]string{"config", "get", "-d", testConnConfigFilePath, "-c", path.Join(pwd, getConfigDirRelativePath)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	sharedConfigYaml, err := readSharedConfigYaml(path.Join(getConfigDirRelativePath, "shared_cluster_config.yml"))
	require.NoError(t, err)
	require.Equal(t, 3, len(sharedConfigYaml.Nodes))

	// create the 4th node
	newServer, newPeer, newNode, err := createNewServer(c, setupConfig, 3)
	require.NoError(t, err)
	require.NotNil(t, newServer)
	require.NotNil(t, newPeer)
	require.NotNil(t, newNode)
	require.NoError(t, newServer.CreateConfigFile(&config.LocalConfiguration{}))

	newNodeConf := &NodeConf{
		NodeID:          newNode.GetId(),
		Host:            newNode.GetAddress(),
		Port:            newNode.GetPort(),
		CertificatePath: path.Join(dir, "node-4", "crypto", "server.pem"),
	}

	newPeerConf := &PeerConf{
		NodeId:   newPeer.GetNodeId(),
		RaftId:   newPeer.GetRaftId(),
		PeerHost: newPeer.GetPeerHost(),
		PeerPort: newPeer.GetPeerPort(),
	}

	// add the 4th node to the shared configuration, create a new shared configuration file
	newSharedConfiguration := &SharedConfiguration{
		Nodes: append(sharedConfigYaml.Nodes, newNodeConf),
		Consensus: &ConsensusConf{
			Algorithm:  sharedConfigYaml.Consensus.Algorithm,
			Members:    append(sharedConfigYaml.Consensus.Members, newPeerConf),
			Observers:  sharedConfigYaml.Consensus.Observers,
			RaftConfig: sharedConfigYaml.Consensus.RaftConfig,
		},
		CAConfig: sharedConfigYaml.CAConfig,
		Admin:    sharedConfigYaml.Admin,
		Ledger:   sharedConfigYaml.Ledger,
	}
	marshaledNewSharedConfiguration, err := yaml.Marshal(newSharedConfiguration)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(pwd, getConfigDirRelativePath, "new_cluster_config.yml"), marshaledNewSharedConfiguration, 0644)
	require.NoError(t, err)

	// set the new cluster config
	rootCmd.SetArgs([]string{"config", "set", "-d", testConnConfigFilePath, "-c", filepath.Join(pwd, getConfigDirRelativePath)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// Get cluster configuration again
	getConfigDirPath, err = os.MkdirTemp(os.TempDir(), "GetConfig_ClusterTest#2")
	defer os.RemoveAll(getConfigDirPath)
	require.NoError(t, err)
	getConfigDirRelativePath = path.Join("..", "..", getConfigDirPath)

	rootCmd.SetArgs([]string{"config", "get", "-d", testConnConfigFilePath, "-c", path.Join(pwd, getConfigDirRelativePath)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// check nodes and members have 4 elements
	newSharedConfigYaml, err := readSharedConfigYaml(path.Join(getConfigDirRelativePath, "shared_cluster_config.yml"))
	require.NoError(t, err)
	require.Equal(t, 4, len(newSharedConfigYaml.Nodes))
	require.Equal(t, 4, len(newSharedConfigYaml.Consensus.Members))

	// get last config block via the cli getLastConfigBlock command
	rootCmd.SetArgs([]string{"config", "getLastConfigBlock", "-d", testConnConfigFilePath, "-c", filepath.Join(pwd, getConfigDirRelativePath)})
	err = rootCmd.Execute()
	require.NoError(t, err)

	block, err := os.ReadFile(path.Join(getConfigDirRelativePath, "last_config_block.yml"))
	require.NoError(t, err)
	require.NotNil(t, block)

	// write the last config block to the boostrap file path of the 4th node and check the config.yml has the right properties
	err = os.WriteFile(newServer.BootstrapFilePath(), block, 0644)
	require.NoError(t, err)

	config, err := config.Read(newServer.ConfigFilePath())
	require.NoError(t, err)
	require.NotNil(t, config.LocalConfig)
	require.Nil(t, config.SharedConfig)
	require.NotNil(t, config.JoinBlock)

	c.AddNewServerToCluster(newServer)

	// start the 4th node
	c.StartServer(newServer)

	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2, 3)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	// get cluster status
	output := new(bytes.Buffer)
	rootCmd.SetOut(output)

	rootCmd.SetArgs([]string{"config", "getClusterStatus", "-d", testConnConfigFilePath})
	err = rootCmd.Execute()
	require.NoError(t, err)
	require.NotNil(t, output)

	idx := strings.Index(output.String(), "header:")
	extractedOutput := output.String()[idx : len(output.Bytes())-1]
	status := &types.GetClusterStatusResponse{}
	err = proto.UnmarshalText(extractedOutput, status)
	require.NoError(t, err)
	require.Equal(t, 4, len(status.GetNodes()))
	require.Equal(t, 4, len(status.GetActive()))
}

func createNewServer(c *setup.Cluster, conf *setup.Config, serverNum int) (*setup.Server, *types.PeerConfig, *types.NodeConfig, error) {
	newServer, err := setup.NewServer(uint64(serverNum), conf.TestDirAbsolutePath, conf.BaseNodePort, conf.BasePeerPort, conf.CheckRedirectFunc, c.GetLogger(), "join", math.MaxUint64)
	if err != nil {
		return nil, nil, nil, err
	}
	clusterCaPrivKey, clusterRootCAPemCert := c.GetKeyAndCA()
	decca, _ := pem.Decode(clusterRootCAPemCert)
	newServer.CreateCryptoMaterials(clusterRootCAPemCert, clusterCaPrivKey)
	if err != nil {
		return nil, nil, nil, err
	}
	server0 := c.Servers[0]
	newServer.SetAdmin(server0.AdminID(), server0.AdminCertPath(), server0.AdminKeyPath(), server0.AdminSigner())

	newPeer := &types.PeerConfig{
		NodeId:   "node-" + strconv.Itoa(serverNum+1),
		RaftId:   uint64(serverNum + 1),
		PeerHost: "127.0.0.1",
		PeerPort: conf.BasePeerPort + uint32(serverNum),
	}

	newNode := &types.NodeConfig{
		Id:          "node-" + strconv.Itoa(serverNum+1),
		Address:     "127.0.0.1",
		Port:        conf.BaseNodePort + uint32(serverNum),
		Certificate: decca.Bytes,
	}

	return newServer, newPeer, newNode, nil
}

func readConnConfig(localConfigFile string) (*cliConnectionConfig, error) {
	if localConfigFile == "" {
		return nil, errors.New("path to the local configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(localConfigFile)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "error reading local config file: %s", localConfigFile)
	}

	localConf := &cliConnectionConfig{}
	if err := v.UnmarshalExact(localConf); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal local config file: '%s' into struct", localConfigFile)
	}
	return localConf, nil
}

func compareFiles(filepath1 string, filepath2 string) error {
	data1, err := os.ReadFile(filepath1)
	if err != nil {
		return errors.Wrapf(err, "failed to read file: '%s'", filepath1)
	}
	data2, err := os.ReadFile(filepath2)
	if err != nil {
		return errors.Wrapf(err, "failed to read file: '%s'", filepath2)
	}
	if string(data1) != string(data2) {
		return errors.Wrapf(err, "files are different")
	}
	return nil
}

func buildSharedClusterConfigAndWriteConfigToYaml(clusterConfig *types.ClusterConfig, configYamlFilePath string, filename string) error {
	sharedConfiguration := buildSharedClusterConfig(clusterConfig, configYamlFilePath)
	c, err := yaml.Marshal(sharedConfiguration)
	if err != nil {
		return err
	}
	err = os.WriteFile(path.Join(configYamlFilePath, filename), c, 0644)
	if err != nil {
		return err
	}
	return nil
}
