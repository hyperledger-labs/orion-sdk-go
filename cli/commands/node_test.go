package commands

import (
	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestNodeCommand(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := ioutil.TempDir(os.TempDir(), "ExampleTest")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Check cas command response
	rootCmd := InitializeOrionCli()
	rootCmd.SetArgs([]string{"node"})
	err = rootCmd.Execute()
	require.Error(t, err)
	require.Equal(t, err.Error(), "not implemented yet")
}
