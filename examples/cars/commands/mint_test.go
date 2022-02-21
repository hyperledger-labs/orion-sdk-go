// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package commands

import (
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestMint(t *testing.T) {
	demoDir, err := ioutil.TempDir("/tmp", "cars-demo-test")
	require.NoError(t, err)
	defer os.RemoveAll(demoDir)

	err = Generate(demoDir)
	require.NoError(t, err)

	testServer, _, err := setupTestServer(t, demoDir)
	require.NoError(t, err)
	defer func() {
		if testServer != nil {
			err = testServer.Stop()
			require.NoError(t, err)
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)
	require.Eventually(t, func() bool { return testServer.IsLeader() == nil }, 30*time.Second, 100*time.Millisecond)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	serverUrl, err := url.Parse("http://127.0.0.1:" + serverPort)
	require.NoError(t, err)

	err = saveServerUrl(demoDir, serverUrl)
	require.NoError(t, err)

	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "cars-demo",
	}
	logger, err := logger.New(c)

	err = Init(demoDir, logger)
	require.NoError(t, err)

	out, err := MintRequest(demoDir, "dealer", "Test.Car.1", logger)
	require.NoError(t, err)
	require.Contains(t, out, "MintRequest: committed")

	index := strings.Index(out, "Key:")
	mintRequestKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(mintRequestKey, "mint-request~"))

	out, err = MintApprove(demoDir, "dmv", mintRequestKey, logger)
	require.NoError(t, err)
	require.Contains(t, out, "MintApprove: committed")

	index = strings.Index(out, "Key:")
	carKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(carKey, "car~"))

	out, err = MintApprove(demoDir, "dmv", mintRequestKey, logger)
	require.EqualError(t, err, "Car already exists: car~Test.Car.1")
	require.Equal(t, "", out)
}
