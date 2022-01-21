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

func TestTransfer(t *testing.T) {
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
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "cars-demo",
	}
	lg, err := logger.New(c)

	err = Init(demoDir, lg)
	require.NoError(t, err)

	carReg := "Test.Car.1"
	out, err := MintRequest(demoDir, "dealer", carReg, lg)
	require.NoError(t, err)
	require.Contains(t, out, "MintRequest: committed")

	index := strings.Index(out, "Key:")
	mintRequestKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(mintRequestKey, MintRequestRecordKeyPrefix))

	out, err = MintApprove(demoDir, "dmv", mintRequestKey, lg)
	require.NoError(t, err)
	require.Contains(t, out, "MintApprove: committed")

	index = strings.Index(out, "Key:")
	carKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(carKey, CarRecordKeyPrefix))

	out, err = ListCar(demoDir, "dmv", carReg, false, lg)
	require.NoError(t, err)
	require.Contains(t, out, "ListCar: executed")
	require.Contains(t, out, "Owner: dealer")

	out, err = Transfer(demoDir, "dmv", "dealer", "alice", carReg, lg)
	require.NoError(t, err)
	require.Contains(t, out, "Transfer: committed")

	index = strings.Index(out, "Key:")
	newOwnerKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(newOwnerKey, CarRecordKeyPrefix))
	indexID := strings.Index(out, "txID:")
	transferTxID := strings.TrimSuffix(strings.TrimSpace(out[indexID+5:index]), ",")

	out, err = ListCar(demoDir, "dmv", carReg, false, lg)
	require.NoError(t, err)
	require.Contains(t, out, "ListCar: executed")
	require.Contains(t, out, "Owner: alice")

	out, err = ListCar(demoDir, "dmv", carReg, true, lg)
	require.NoError(t, err)
	require.Contains(t, out, "ListCar: executed")
	require.Contains(t, out, "Owner: dealer")
	require.Contains(t, out, "Owner: alice")

	out, err = VerifyEvidence(demoDir, "bob", transferTxID, lg)
	require.NoError(t, err)
	require.Contains(t, out, "VerifyEvidence:")
}
