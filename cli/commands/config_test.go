package commands

import (
	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"
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
			expectedErrMsg: "required flag(s) \"cli-config-path\", \"cluster-config-path\" not set",
		},
		{
			name:           "Missing Cli Connection Config Flag",
			args:           []string{"config", "get", "-p", "/path/to/cluster-config.yaml"},
			expectedErrMsg: "required flag(s) \"cli-config-path\" not set",
		},
		{
			name:           "Missing Cluster Config Flag",
			args:           []string{"config", "get", "-c", "/path/to/cli-config.yaml"},
			expectedErrMsg: "required flag(s) \"cluster-config-path\" not set",
		},
		{
			name:           "File path not found",
			args:           []string{"config", "get", "-c", "/path/to/cli-config.yaml", "-p", "/path/to/cluster-config.yaml"},
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
	tempDir, err := ioutil.TempDir(os.TempDir(), "ExampleTest")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Get cluster config from the server by the CLI GetConfig command
	rootCmd := InitializeOrionCli()
	pwd, err := os.Getwd()
	require.NoError(t, err)
	testConfigFilePath := path.Join(tempDir, "config.yml")
	rootCmd.SetArgs([]string{"config", "get", "-c", testConfigFilePath, "-p", filepath.Join(pwd, "..", "..")})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// 3. Check the server response
	// check that certs are equal to the expected certs
	createdDirName := "configTestRes"
	relativePathForCreatedDirName := path.Join("..", "..", createdDirName)
	err = compareFiles(path.Join(tempDir, "crypto", "admin", "admin.pem"), path.Join(relativePathForCreatedDirName, "admins", "admin.pem"))
	require.NoError(t, err)
	err = compareFiles(path.Join(tempDir, "crypto", "node", "node.pem"), path.Join(relativePathForCreatedDirName, "nodes", "server1.pem"))
	require.NoError(t, err)
	err = compareFiles(path.Join(tempDir, "crypto", "CA", "CA.pem"), path.Join(relativePathForCreatedDirName, "rootCAs", "rootCA0.pem"))
	require.NoError(t, err)

	// extract server endpoint and compare to the endpoint received by Get Config command
	expectedConfigRes, err := readConnConfig(testConfigFilePath)
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

	if err := os.RemoveAll(relativePathForCreatedDirName); err != nil {
		errors.Wrapf(err, "failed to cleanup directory: %s", relativePathForCreatedDirName)
	}
}

func TestSetConfigCommand(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := ioutil.TempDir(os.TempDir(), "ExampleTest")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Check cas command response
	rootCmd := InitializeOrionCli()
	testConfigFilePath := path.Join(tempDir, "config.yml")
	rootCmd.SetArgs([]string{"config", "set", "-c", testConfigFilePath})
	err = rootCmd.Execute()
	require.Error(t, err)
	require.Equal(t, err.Error(), "not implemented yet")
}

func readConnConfig(localConfigFile string) (*CliConnectionConfig, error) {
	if localConfigFile == "" {
		return nil, errors.New("path to the local configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(localConfigFile)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "error reading local config file: %s", localConfigFile)
	}

	localConf := &CliConnectionConfig{}
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
