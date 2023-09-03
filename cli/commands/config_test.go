package commands

import (
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/server"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"
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
	// 1. Create BCDBHTTPServer and start server
	testServer, err := SetupTestServer(t)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	// 2. Get cluster config from the server by the CLI GetConfig command
	rootCmd := InitializeOrionCli()
	pwd, err := os.Getwd()
	require.NoError(t, err)
	rootCmd.SetArgs([]string{"config", "get", "-c", "../testdata/connection-session-config.yml", "-p", filepath.Join(pwd, "..", "..")})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// 3. Check the server response - check expected certs, server config
	err = compareFiles("../testdata/crypto/admin/admin.pem", "../../configTestRes/admins/admin.pem")
	require.NoError(t, err)
	err = compareFiles("../testdata/crypto/server/server.pem", "../../configTestRes/nodes/orion-server1.pem")
	require.NoError(t, err)
	err = compareFiles("../testdata/crypto/CA/CA.pem", "../../configTestRes/rootCAs/rootCA0.pem")
	require.NoError(t, err)

	expectedConfigRes, err := readSharedConfig("../../configTestRes/shared_cluster_config.yml")
	if err != nil {
		errors.Wrapf(err, "failed to read expected shared configuration")
	}

	sharedConfigRes, err := readSharedConfig("../../configTestRes/shared_cluster_config.yml")
	if err != nil {
		errors.Wrapf(err, "failed to read shared configuration")
	}

	require.Equal(t, expectedConfigRes.Nodes[0].NodeID, sharedConfigRes.Nodes[0].NodeID)
	require.Equal(t, expectedConfigRes.Nodes[0].Port, sharedConfigRes.Nodes[0].Port)
	require.Equal(t, expectedConfigRes.Nodes[0].Host, sharedConfigRes.Nodes[0].Host)
}

func SetupTestServer(t *testing.T) (*server.BCDBHTTPServer, error) {
	serverLocalConfig, err := readLocalConfig(path.Join("../testdata/config-local/config.yml"))
	if err != nil {
		return nil, errors.Wrap(err, "error while unmarshaling local config")
	}

	serverSharedConfig, err := readSharedConfig(serverLocalConfig.Bootstrap.File)
	if err != nil {
		return nil, errors.Wrap(err, "error while unmarshaling shared config")
	}

	serverConfig := &config.Configurations{
		LocalConfig:  serverLocalConfig,
		SharedConfig: serverSharedConfig,
		JoinBlock:    nil,
	}

	server, err := server.New(serverConfig)
	return server, err
}

func StartTestServer(t *testing.T, s *server.BCDBHTTPServer) {
	err := s.Start()
	require.NoError(t, err)
	require.Eventually(t, func() bool { return s.IsLeader() == nil }, 30*time.Second, 100*time.Millisecond)
}

func readLocalConfig(localConfigFile string) (*config.LocalConfiguration, error) {
	if localConfigFile == "" {
		return nil, errors.New("path to the local configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(localConfigFile)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "error reading local config file: %s", localConfigFile)
	}

	localConf := &config.LocalConfiguration{}
	if err := v.UnmarshalExact(localConf); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal local config file: '%s' into struct", localConfigFile)
	}
	return localConf, nil
}

func readSharedConfig(sharedConfigFile string) (*config.SharedConfiguration, error) {
	if sharedConfigFile == "" {
		return nil, errors.New("path to the shared configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(sharedConfigFile)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "error reading shared config file: %s", sharedConfigFile)
	}

	sharedConf := &config.SharedConfiguration{}
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
