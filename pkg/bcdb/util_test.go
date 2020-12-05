package bcdb

import (
	"crypto/tls"
	"fmt"
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

func setupTestServer(t *testing.T, clientCertTempDir string) (*server.BCDBHTTPServer, tls.Certificate, string, error) {
	return setupTestServerWithParams(t, clientCertTempDir, 500 * time.Millisecond, 1)
}

func setupTestServerWithParams(t *testing.T, clientCertTempDir string, blockTime time.Duration, txPerBlock uint32) (*server.BCDBHTTPServer, tls.Certificate, string, error) {
	tempDir, err := ioutil.TempDir("/tmp", "userTxContextTest")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("BCDB RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, rootCAPemCert)
	require.NotNil(t, caPrivKey)

	keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	require.NoError(t, err)
	require.NotNil(t, keyPair)

	serverRootCACertFile, err := os.Create(path.Join(tempDir, "serverRootCACert.pem"))
	require.NoError(t, err)
	_, err = serverRootCACertFile.Write(rootCAPemCert)
	require.NoError(t, err)
	err = serverRootCACertFile.Close()
	require.NoError(t, err)

	pemCert, privKey, err := testutils.IssueCertificate("BCDB Instance", "127.0.0.1", keyPair)
	require.NoError(t, err)

	pemCertFile, err := os.Create(path.Join(tempDir, "server.pem"))
	require.NoError(t, err)
	_, err = pemCertFile.Write(pemCert)
	require.NoError(t, err)
	err = pemCertFile.Close()
	require.NoError(t, err)

	pemPrivKeyFile, err := os.Create(path.Join(tempDir, "server.key"))
	require.NoError(t, err)
	_, err = pemPrivKeyFile.Write(privKey)
	require.NoError(t, err)
	err = pemPrivKeyFile.Close()
	require.NoError(t, err)

	server, err := server.New(&config.Configurations{
		Node: config.NodeConf{
			Identity: config.IdentityConf{
				ID:              "testNode1",
				CertificatePath: path.Join(tempDir, "server.pem"),
				KeyPath:         path.Join(tempDir, "server.key"),
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
				Block:                     1,
				Transaction:               1,
				ReorderedTransactionBatch: 1,
			},

			LogLevel: "debug",
		},
		Admin: config.AdminConf{
			ID:              "admin",
			CertificatePath: path.Join(clientCertTempDir, "admin.pem"),
		},
		RootCA: config.RootCAConf{
			CertificatePath: path.Join(tempDir, "serverRootCACert.pem"),
		},
		Consensus: config.ConsensusConf{
			Algorithm:                   "solo",
			BlockTimeout:                blockTime,
			MaxBlockSize:                1,
			MaxTransactionCountPerBlock: txPerBlock,
		},
	})
	return server, keyPair, tempDir, err
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

func createDBInstance(t *testing.T, tempDir string, serverPort string) BCDB {
	// Create new connection
	bcdb, err := Create(&config2.ConnectionConfig{
		RootCAs: []string{path.Join(tempDir, "serverRootCACert.pem")},
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

func startServerConnectOpenAdminCreateUserAndUserSession(t *testing.T, testServer *server.BCDBHTTPServer, serverTempDir string, certTempDir string, user string) (BCDB, DBSession, DBSession){
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, serverTempDir, certTempDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(certTempDir, user + ".pem"))
	require.NoError(t, err)
	addUser(t, user, adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, user, certTempDir)

	return bcdb, adminSession, userSession
}

