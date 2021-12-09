package main

import (
	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestDataContext_ExecuteJsonQueryExample(t *testing.T) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "ExampleTest")
	require.NoError(t, err)

	testConfigFile := path.Join(tempDir, "config.yml")

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6005))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	err = executeJsonQueryExample(testConfigFile)
	require.NoError(t, err)
}
