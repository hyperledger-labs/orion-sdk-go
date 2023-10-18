package commands

import (
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"

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

	// 3. get cluster configration
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

	// 5. compare the generated certs with the certs recieved from the tx
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

	// 3. get cluster configration
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
