package commands

import (
	"os"
	"testing"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/stretchr/testify/require"
)

func TestCasCommand(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := os.MkdirTemp(os.TempDir(), "Cli-Cas-Test")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Check cas command response
	rootCmd := InitializeOrionCli()
	rootCmd.SetArgs([]string{"CAs"})
	err = rootCmd.Execute()
	require.Error(t, err)
	require.Equal(t, err.Error(), "not implemented yet")
}
