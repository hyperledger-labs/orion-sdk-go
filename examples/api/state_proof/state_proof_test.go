// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/stretchr/testify/require"
)

func TestDataContext_ExecuteStateTrieExample(t *testing.T) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "ExampleTest")
	require.NoError(t, err)

	testConfigFile := path.Join(tempDir, "config.yml")

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6009))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	err = ExecuteStateTrieExample(testConfigFile)
	require.NoError(t, err)
}

func TestDataContext_ExecuteStateTrieExampleNoServer(t *testing.T) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "ExampleTest")
	require.NoError(t, err)

	testConfigFile := path.Join(tempDir, "config.yml")

	_, err = util.CreateTestEnvFilesAndConfigs(t, tempDir, uint32(6001), uint32(7001), 500*time.Millisecond, 1)
	require.NoError(t, err)

	err = ExecuteStateTrieExample(testConfigFile)
	require.Error(t, err)
}
