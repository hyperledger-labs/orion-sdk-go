package bcdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	sdkconfig "github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/server"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
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
	require.NotNil(t, caCertPEM)

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
	session, err := bcdb.Session(&sdkconfig.SessionConfig{
		UserConfig: &sdkconfig.UserConfig{
			UserID:         user,
			CertPath:       path.Join(tempDir, user+".pem"),
			PrivateKeyPath: path.Join(tempDir, user+".key"),
		},
		TxTimeout: time.Second * 2,
	})
	require.NoError(t, err)

	return session
}

func createDBInstance(t *testing.T, cryptoDir string, serverPort string) BCDB {
	// Create new connection
	bcdb, err := Create(&sdkconfig.ConnectionConfig{
		RootCAs: []string{path.Join(cryptoDir, testutils.RootCAFileName+".pem")},
		ReplicaSet: []*sdkconfig.Replica{
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

type TxFinality int

const (
	TxFinalityCommitSync TxFinality = iota
	TxFinalityCommitAsync
	TxFinalityAbort
)

func assertTxFinality(t *testing.T, txFinality TxFinality, tx TxContext, userSession DBSession) {
	var txID string
	var err error

	switch txFinality {
	case TxFinalityCommitSync:
		txID, receipt, err := tx.Commit(true)
		require.NoError(t, err)
		require.True(t, len(txID) > 0)
		require.NotNil(t, receipt)
	case TxFinalityCommitAsync:
		txID, receipt, err := tx.Commit(false)
		require.NoError(t, err)
		require.True(t, len(txID) > 0)
		require.Nil(t, receipt)
		switch tx.(type) {
		case ConfigTxContext:
			// TODO remove once support for non data tx provenance added
			e, _ := tx.TxEnvelope()
			env := e.(*types.ConfigTxEnvelope)
			newConfig := env.GetPayload().GetNewConfig()
			require.Eventually(t, func() bool {
				// verify tx was successfully committed. "Get" works once per Tx.
				cfgTx, err := userSession.ConfigTx()
				if err != nil {
					return false
				}
				clusterConfig, err := cfgTx.GetClusterConfig()
				if err != nil || clusterConfig == nil {
					return false
				}
				return proto.Equal(newConfig, clusterConfig)
			}, 5*time.Second, 100*time.Millisecond)
		case DataTxContext:
			waitForTx(t, txID, userSession)
		case DBsTxContext:
			// TODO remove once support for non data tx provenance added
			e, _ := tx.TxEnvelope()
			env := e.(*types.DBAdministrationTxEnvelope)
			createdDBs := env.GetPayload().GetCreateDBs()
			deletedDBs := env.GetPayload().GetDeleteDBs()
			require.Eventually(t, func() bool {
				// verify tx was successfully committed. "Get" works once per Tx.
				res := true
				dbTx, err := userSession.DBsTx()
				if err != nil {
					return false
				}
				if len(createdDBs) > 0 {
					for _, db := range createdDBs {
						exists, err := dbTx.Exists(db)
						if err != nil {
							return false
						}
						res = res && exists
					}
				}
				if len(deletedDBs) > 0 {
					for _, db := range createdDBs {
						exists, err := dbTx.Exists(db)
						if err != nil {
							return false
						}
						res = res && !exists
					}
				}
				return res
			}, 30*time.Second, 100*time.Millisecond)

		case UsersTxContext:
			// TODO remove once support for non data tx provenance added
			e, _ := tx.TxEnvelope()
			env := e.(*types.UserAdministrationTxEnvelope)
			deleteUsers := env.GetPayload().GetUserDeletes()
			updateUsers := env.GetPayload().GetUserWrites()
			require.Eventually(t, func() bool {
				// verify tx was successfully committed. "Get" works once per Tx.
				res := true
				userTx, err := userSession.UsersTx()
				if err != nil {
					return false
				}
				if len(deleteUsers) > 0 {
					for _, userDelete := range deleteUsers {
						userDelete.GetUserID()
						dUser, err := userTx.GetUser(userDelete.GetUserID())
						if err != nil {
							return false
						}
						res = res && (dUser == nil)
					}
				}
				if len(updateUsers) > 0 {
					for _, userUpdate := range updateUsers {
						uUser, err := userTx.GetUser(userUpdate.User.ID)
						if err != nil {
							return false
						}
						res = res && proto.Equal(uUser, userUpdate.User)
					}
				}
				return res
			}, 30*time.Second, 100*time.Millisecond)
		}
	case TxFinalityAbort:
		err = tx.Abort()
		require.NoError(t, err)
	}

	// verify finality

	txID, _, err = tx.Commit(true)
	require.EqualError(t, err, ErrTxSpent.Error())
	require.True(t, len(txID) == 0)
	txID, _, err = tx.Commit(false)
	require.EqualError(t, err, ErrTxSpent.Error())
	require.True(t, len(txID) == 0)

	err = tx.Abort()
	require.EqualError(t, err, ErrTxSpent.Error())
}

func MarshalOrPanic(response interface{}) []byte {
	bytes, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}

	return bytes
}
