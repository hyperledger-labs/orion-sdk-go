package commands

import (
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/logger"
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
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	serverUrl, err := url.Parse("http://127.0.0.1:" + serverPort)
	require.NoError(t, err)

	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "bcdb-client",
	}
	logger, err := logger.New(c)

	err = Init(demoDir, serverUrl, logger)
	require.NoError(t, err)

	carReg := "Test.Car.1"
	out, err := MintRequest(demoDir, "dealer", carReg, logger)
	require.NoError(t, err)
	require.Contains(t, out, "MintRequest: committed")

	index := strings.Index(out, "Key:")
	mintRequestKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(mintRequestKey, MintRequestRecordKeyPrefix))

	out, err = MintApprove(demoDir, "dmv", mintRequestKey, logger)
	require.NoError(t, err)
	require.Contains(t, out, "MintApprove: committed")

	index = strings.Index(out, "Key:")
	carKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(carKey, CarRecordKeyPrefix))

	out, err = TransferTo(demoDir, "dealer", "alice", carReg, logger)
	require.NoError(t, err)
	require.Contains(t, out, "TransferTo: committed")

	index = strings.Index(out, "Key:")
	ttKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(ttKey, TransferToRecordKeyPrefix))

	out, err = TransferReceive(demoDir, "alice", carReg, ttKey, logger)
	require.NoError(t, err)
	require.Contains(t, out, "TransferReceive: committed")

	index = strings.Index(out, "Key:")
	trKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(trKey, TransferReceiveRecordKeyPrefix))

	out, err = Transfer(demoDir, "dmv", ttKey, trKey, logger)
	require.NoError(t, err)
	require.Contains(t, out, "Transfer: committed")

	index = strings.Index(out, "Key:")
	newOwnerKey := strings.TrimSpace(out[index+4:])
	require.True(t, strings.HasPrefix(newOwnerKey, CarRecordKeyPrefix))
}
