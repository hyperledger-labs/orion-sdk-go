package commands

import (
	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestVersionCommand(t *testing.T) {
	// 1. Create crypto material and start server
	tempDir, err := ioutil.TempDir(os.TempDir(), "ExampleTest")
	require.NoError(t, err)

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6003))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	// 2. Get the version of the CLI by the CLI Version command
	rootCmd := InitializeOrionCli()
	rootCmd.SetArgs([]string{"version"})
	b := &strings.Builder{}
	rootCmd.SetOut(b)
	err = rootCmd.Execute()
	require.NoError(t, err)
	require.Contains(t, b.String(), "SDK version: ")
}
