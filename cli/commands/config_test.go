package commands

import (
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

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
			expectedErrMsg: "failed to read CLI configuration file",
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

func TestSetConfigCommand(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := os.MkdirTemp(os.TempDir(), "Cli-Set-Config-Test")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Check cas command response
	rootCmd := InitializeOrionCli()
	testDbConnectionConfigFilePath := path.Join(tempDir, "config.yml")
	rootCmd.SetArgs([]string{"config", "set", "-d", testDbConnectionConfigFilePath})
	err = rootCmd.Execute()
	require.Error(t, err)
	require.Equal(t, err.Error(), "not implemented yet")
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
