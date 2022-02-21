// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/stretchr/testify/require"
)

func TestDataContext_ExecuteMultiSignExample(t *testing.T) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "ExampleTest")
	require.NoError(t, err)

	testConfigFile := path.Join(tempDir, "config.yml")

	testServer, _, _, err := util.SetupTestEnv(t, tempDir, uint32(6006))
	require.NoError(t, err)
	defer testServer.Stop()
	util.StartTestServer(t, testServer)

	err = executeMultiSignExample(path.Join(tempDir, "crypto"), testConfigFile)
	require.NoError(t, err)
}
