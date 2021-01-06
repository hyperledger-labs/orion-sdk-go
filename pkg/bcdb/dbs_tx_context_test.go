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
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, tempDir, clientCertTemDir)
	tx, err := session.DBsTx()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		exist, err := tx.Exists("bdb")

		return err == nil && exist
	}, time.Minute, 200*time.Millisecond)
}

func TestDBsContext_CreateDBAndCheckStatus(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, tempDir, clientCertTemDir)
	// Start submission session to create a new database
	tx, err := session.DBsTx()
	require.NoError(t, err)

	err = tx.CreateDB("testDB")
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	// Check database status, whenever created or not
	tx, err = session.DBsTx()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		exist, err := tx.Exists("testDB")

		return err == nil && exist
	}, time.Minute, 200*time.Millisecond)
}

func TestDBsContext_MalformedRequest(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb, err := Create(&sdkConfig.ConnectionConfig{
		RootCAs: []string{path.Join(tempDir, "serverRootCACert.pem")},
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
			expectedError: "server returned malformed response",
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
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, tempDir, clientCertTemDir)
	// Start submission session to create a new database
	tx, err := session.DBsTx()
	require.NoError(t, err)

	err = tx.CreateDB("testDB")
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	// Check database status, whenever created or not
	tx, err = session.DBsTx()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		exist, err := tx.Exists("testDB")

		return err == nil && exist
	}, time.Minute, 200*time.Millisecond)

	tx, err = session.DBsTx()
	require.NoError(t, err)

	err = tx.CreateDB("db1")
	require.NoError(t, err)
	err = tx.DeleteDB("testDB")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		exist, err := tx.Exists("testDB")

		return err == nil && !exist
	}, time.Minute, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		exist, err := tx.Exists("db1")

		return err == nil && exist
	}, time.Minute, 200*time.Millisecond)
}

func TestDBsContext_AttemptDeleteSystemDatabase(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, tempDir, clientCertTemDir)
	// Start submission session to create a new database
	tx, err := session.DBsTx()
	require.NoError(t, err)

	err = tx.DeleteDB("bdb")
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	// Check database status, whenever created or not
	tx, err = session.DBsTx()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		exist, err := tx.Exists("bdb")

		return err == nil && exist
	}, time.Minute, 200*time.Millisecond)
}
