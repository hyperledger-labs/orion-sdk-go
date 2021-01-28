package bcdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	config2 "github.ibm.com/blockchaindb/sdk/pkg/config"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/server"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
)

func setupTestServer(t *testing.T, cryptoTempDir string) (*server.BCDBHTTPServer, error) {
	s, e := setupTestServerWithParams(t, cryptoTempDir, 500*time.Millisecond, 1)
	return s, e
}

func setupTestServerWithParams(t *testing.T, cryptoTempDir string, blockTime time.Duration, txPerBlock uint32) (*server.BCDBHTTPServer, error) {
	tempDir, err := ioutil.TempDir("/tmp", "userTxContextTest")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	caCertPEM, err := ioutil.ReadFile(path.Join(cryptoTempDir, testutils.RootCAFileName+".pem"))
	require.NoError(t, err)
	assert.NotNil(t, caCertPEM)

	server, err := server.New(&config.Configurations{
		Node: config.NodeConf{
			Identity: config.IdentityConf{
				ID:              "testNode1",
				CertificatePath: path.Join(cryptoTempDir, "server.pem"),
				KeyPath:         path.Join(cryptoTempDir, "server.key"),
			},
			Database: config.DatabaseConf{
				Name:            "leveldb",
				LedgerDirectory: path.Join(tempDir, "ledger"),
			},
			Network: config.NetworkConf{
				Address: "127.0.0.1",
				Port:    0, // use ephemeral port for testing
			},
			QueueLength: config.QueueLengthConf{
				Block:                     10,
				Transaction:               10,
				ReorderedTransactionBatch: 10,
			},

			LogLevel: "debug",
		},
		Admin: config.AdminConf{
			ID:              "admin",
			CertificatePath: path.Join(cryptoTempDir, "admin.pem"),
		},
		CAConfig: config.CAConfiguration{RootCACertsPath: []string{path.Join(cryptoTempDir, testutils.RootCAFileName+".pem")}},
		Consensus: config.ConsensusConf{
			Algorithm:                   "solo",
			BlockTimeout:                blockTime,
			MaxBlockSize:                1,
			MaxTransactionCountPerBlock: txPerBlock,
		},
	})
	return server, err
}

func createTestLogger(t *testing.T) *logger.SugarLogger {
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "bcdb-client",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)
	require.NotNil(t, logger)
	return logger
}

func openUserSession(t *testing.T, bcdb BCDB, user string, tempDir string) DBSession {
	// New session with alice user context
	session, err := bcdb.Session(&config2.SessionConfig{
		UserConfig: &config2.UserConfig{
			UserID:         user,
			CertPath:       path.Join(tempDir, user+".pem"),
			PrivateKeyPath: path.Join(tempDir, user+".key"),
		},
	})
	require.NoError(t, err)

	return session
}

func createDBInstance(t *testing.T, cryptoDir string, serverPort string) BCDB {
	// Create new connection
	bcdb, err := Create(&config2.ConnectionConfig{
		RootCAs: []string{path.Join(cryptoDir, testutils.RootCAFileName+".pem")},
		ReplicaSet: []*config2.Replica{
			{
				ID:       "testNode1",
				Endpoint: fmt.Sprintf("http://localhost:%s", serverPort),
			},
		},
	})
	require.NoError(t, err)

	return bcdb
}

func startServerConnectOpenAdminCreateUserAndUserSession(t *testing.T, testServer *server.BCDBHTTPServer, certTempDir string, user string) (BCDB, DBSession, DBSession) {
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, certTempDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(certTempDir, user+".pem"))
	require.NoError(t, err)
	addUser(t, user, adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, user, certTempDir)

	return bcdb, adminSession, userSession
}
