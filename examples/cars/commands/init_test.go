package commands

import (
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/server"
)

func TestInit(t *testing.T) {
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
}

func TestInit_saveServerUrl(t *testing.T) {
	demoDir, err := ioutil.TempDir("/tmp", "cars-demo-test")
	require.NoError(t, err)
	defer os.RemoveAll(demoDir)

	serverUrl, err := url.Parse("http://127.0.0.1:8080")
	require.NoError(t, err)
	err = saveServerUrl(demoDir, serverUrl)
	require.NoError(t, err)
	loadedUrl, err := loadServerUrl(demoDir)
	require.NoError(t, err)
	require.Equal(t, serverUrl.String(), loadedUrl.String())
}

func setupTestServer(t *testing.T, demoDir string) (*server.BCDBHTTPServer, string, error) {
	tempDataDir, err := ioutil.TempDir("/tmp", "car-demo-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDataDir)
	})

	cryptoDir := path.Join(demoDir, "crypto")
	server, err := server.New(&config.Configurations{
		Node: config.NodeConf{
			Identity: config.IdentityConf{
				ID:              "demo",
				CertificatePath: path.Join(cryptoDir, "server", "server.pem"),
				KeyPath:         path.Join(cryptoDir, "server", "server.key"),
			},
			Database: config.DatabaseConf{
				Name:            "leveldb",
				LedgerDirectory: path.Join(tempDataDir, "ledger"),
			},
			Network: config.NetworkConf{
				Address: "127.0.0.1",
				Port:    0, // use ephemeral port for testing
			},
			QueueLength: config.QueueLengthConf{
				Block:                     1,
				Transaction:               1,
				ReorderedTransactionBatch: 1,
			},

			LogLevel: "info",
		},
		Admin: config.AdminConf{
			ID:              "admin",
			CertificatePath: path.Join(cryptoDir, "admin", "admin.pem"),
		},
		RootCA: config.RootCAConf{
			CertificatePath: path.Join(cryptoDir, "CA", "CA.pem"),
		},
		Consensus: config.ConsensusConf{
			Algorithm:                   "solo",
			BlockTimeout:                500 * time.Millisecond,
			MaxBlockSize:                1,
			MaxTransactionCountPerBlock: 1,
		},
	})
	return server, tempDataDir, err
}
