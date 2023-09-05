package commands

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVersionCommand(t *testing.T) {
	// 1. Create BCDBHTTPServer and start server
	testServer, err := SetupTestServer(t)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	// 2. Get the version of the CLI by the CLI Version command
	rootCmd := InitializeOrionCli()
	rootCmd.SetArgs([]string{"version"})
	err = rootCmd.Execute()
	require.NoError(t, err)
}
