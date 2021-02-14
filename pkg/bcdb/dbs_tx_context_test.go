package bcdb

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb/mocks"
	sdkConfig "github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
)

func TestDBsContext_CheckStatusOfDefaultDB(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	tx, err := session.DBsTx()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		exist, err := tx.Exists("bdb")

		return err == nil && exist
	}, time.Minute, 200*time.Millisecond)
}

func TestDBsContext_CreateDBAndCheckStatus(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	// Start submission session to create a new database
	tx, err := session.DBsTx()
	require.NoError(t, err)

	err = tx.CreateDB("testDB")
	require.NoError(t, err)

	txId, receipt, err := tx.Commit(true)
	require.NoError(t, err)
	require.True(t, len(txId) > 0)
	require.NotNil(t, receipt)

	// Check database status, whenever created or not
	tx, err = session.DBsTx()
	require.NoError(t, err)
	exist, err := tx.Exists("testDB")
	require.NoError(t, err)
	require.True(t, exist)
}

func TestDBsContext_CheckStatusTimeout(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, session := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	// Start submission session to create a new database
	tx, err := session.DBsTx()
	require.NoError(t, err)

	err = tx.CreateDB("testDB")
	require.NoError(t, err)

	txId, receipt, err := tx.Commit(true)
	require.NoError(t, err)
	require.Greater(t, len(txId), 0)
	require.NotNil(t, receipt)

	sessionOneNano := openUserSessionWithQueryTimeout(t, bcdb, "admin", clientCertTemDir, time.Nanosecond)
	sessionTenSeconds := openUserSessionWithQueryTimeout(t, bcdb, "admin", clientCertTemDir, time.Second*10)

	// Check database status with timeout
	tx1, err := sessionOneNano.DBsTx()
	require.NoError(t, err)
	exist, err := tx1.Exists("testDB")
	require.Error(t, err)
	require.Contains(t, err.Error(), "queryTimeout error")

	tx2, err := sessionTenSeconds.DBsTx()
	require.NoError(t, err)
	exist, err = tx2.Exists("testDB")
	require.NoError(t, err)
	require.True(t, exist)
}

func TestDBsContext_CommitAbortFinality(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, clientCertTemDir)

	for i := 0; i < 3; i++ {
		// Start submission session to create a new database
		tx, err := session.DBsTx()
		require.NoError(t, err)

		err = tx.CreateDB(fmt.Sprintf("testDB-%d", i))
		require.NoError(t, err)

		assertTxFinality(t, TxFinality(i), tx, session)

		err = tx.CreateDB("some-db")
		require.EqualError(t, err, ErrTxSpent.Error())

		err = tx.DeleteDB("some-db")
		require.EqualError(t, err, ErrTxSpent.Error())

		exists, err := tx.Exists("some-db")
		require.EqualError(t, err, ErrTxSpent.Error())
		require.False(t, exists)

		if TxFinality(i) != TxFinalityAbort {
			tx, err = session.DBsTx()
			require.NoError(t, err)
			exists, err := tx.Exists(fmt.Sprintf("testDB-%d", i))
			require.NoError(t, err)
			require.True(t, exists)
		}
	}
}

func TestDBsContext_MalformedRequest(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb, err := Create(&sdkConfig.ConnectionConfig{
		RootCAs: []string{path.Join(clientCertTemDir, testutils.RootCAFileName+".pem")},
		ReplicaSet: []*sdkConfig.Replica{
			{
				ID:       "testNode1",
				Endpoint: fmt.Sprintf("http://localhost:%s", serverPort),
			},
		},
	})
	require.NoError(t, err)

	// New session with admin user context
	session, err := bcdb.Session(&sdkConfig.SessionConfig{
		UserConfig: &sdkConfig.UserConfig{
			UserID:         "adminX",
			CertPath:       path.Join(clientCertTemDir, "admin.pem"),
			PrivateKeyPath: path.Join(clientCertTemDir, "admin.key"),
		},
	})
	require.NoError(t, err)

	_, err = session.DBsTx()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to obtain server's certificate")
}

func TestDBsContext_ExistsFailureScenarios(t *testing.T) {
	testCases := []struct {
		name              string
		restClientFactory func() RestClient
		expectedError     string
	}{
		{
			name: "rest client internal error",
			restClientFactory: func() RestClient {
				restClient := &mocks.RestClient{}
				restClient.On("Query", mock.Anything, mock.Anything, mock.Anything).
					Return(nil, errors.New("cannot connect to replica"))
				return restClient
			},
			expectedError: "cannot connect to replica",
		},
		{
			name: "rest response error",
			restClientFactory: func() RestClient {
				restClient := &mocks.RestClient{}
				restClient.On("Query", mock.Anything, mock.Anything, mock.Anything).
					Return(&http.Response{
						StatusCode: http.StatusBadRequest,
						Status:     "malformed response",
					}, nil)
				return restClient
			},
			expectedError: "error handling request, server returned: status: malformed response, message: ",
		},
	}

	logger := createTestLogger(t)
	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			restClient := tt.restClientFactory()
			signer := &mocks.Signer{}
			dbsCtx := &dbsTxContext{
				commonTxContext: &commonTxContext{
					signer:     signer,
					userID:     "testUserId",
					restClient: restClient,
					logger:     logger,
					replicaSet: map[string]*url.URL{
						"node1": {
							Path: "http://localhost:8888",
						},
					},
				},
				createdDBs: map[string]bool{},
				deletedDBs: map[string]bool{},
			}

			exist, err := dbsCtx.Exists("bdb")
			require.Error(t, err)
			require.False(t, exist)
			require.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestDBsContext_MultipleOperations(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	// Start submission session to create a new database
	tx, err := session.DBsTx()
	require.NoError(t, err)

	err = tx.CreateDB("testDB")
	require.NoError(t, err)

	_, receipt, err := tx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, receipt)

	// Check database status, whenever created or not
	tx, err = session.DBsTx()
	require.NoError(t, err)
	exist, err := tx.Exists("testDB")
	require.NoError(t, err)
	require.True(t, exist)

	// create & delete
	tx, err = session.DBsTx()
	require.NoError(t, err)

	err = tx.CreateDB("db1")
	require.NoError(t, err)
	err = tx.DeleteDB("testDB")
	require.NoError(t, err)
	_, receipt, err = tx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, receipt)

	tx, err = session.DBsTx()
	require.NoError(t, err)
	exist, err = tx.Exists("testDB")
	require.NoError(t, err)
	require.False(t, exist)

	exist, err = tx.Exists("db1")
	require.NoError(t, err)
	require.True(t, exist)

	err = tx.Abort()
	require.NoError(t, err)
}

func TestDBsContext_AttemptDeleteSystemDatabase(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	// Start submission session to create a new database
	tx, err := session.DBsTx()
	require.NoError(t, err)

	err = tx.DeleteDB("bdb")
	require.NoError(t, err)

	_, _, err = tx.Commit(true)
	require.NoError(t, err)

	// Check database status, whenever created or not
	tx, err = session.DBsTx()
	require.NoError(t, err)

	exist, err := tx.Exists("bdb")
	require.NoError(t, err)
	require.True(t, exist)
}
