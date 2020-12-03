package commands

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"io/ioutil"
	"net/url"
	"os"
	"testing"
)

func TestMintRequest(t *testing.T) {
	t.Skip("server bug #296")

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

	out, err := MintRequest(demoDir, "dealer", "Test.Car.1")
	require.NoError(t, err)
	fmt.Println(out)
}
