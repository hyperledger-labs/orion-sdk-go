package bcdb

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb/mocks"
	sdkConfig "github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestUserContext_AddAndRetrieveUser(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	// Create new connection
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
			UserID:         "admin",
			CertPath:       path.Join(clientCertTemDir, "admin.pem"),
			PrivateKeyPath: path.Join(clientCertTemDir, "admin.key"),
		},
	})
	require.NoError(t, err)

	// Start submission session to introduce new user
	tx, err := session.UsersTx()
	require.NoError(t, err)

	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	certBlock, _ := pem.Decode(pemUserCert)
	err = tx.PutUser(&types.User{
		ID:          "alice",
		Certificate: certBlock.Bytes,
	}, nil)
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	// Start another session to query and make sure
	// results was successfully committed
	tx, err = session.UsersTx()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		alice, err := tx.GetUser("alice")

		return err == nil && alice != nil &&
			alice.ID == "alice" &&
			bytes.Equal(certBlock.Bytes, alice.Certificate)
	}, time.Minute, 200*time.Millisecond)
}

func TestUserContext_MalformedRequest(t *testing.T) {
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

	// transaction init should fail since wrong user id was configured
	// in the session config, therefore it will fail to fetch node
	// certificate and fail to start transaction
	_, err = session.UsersTx()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to obtain server's certificate")
}

func TestUserContext_GetUserFailureScenarios(t *testing.T) {
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
			usrCtx := &userTxContext{
				commonTxContext: commonTxContext{
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
			}

			user, err := usrCtx.GetUser("alice")
			require.Error(t, err)
			require.Nil(t, user)
			require.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestUserContext_TxSubmissionFullScenario(t *testing.T) {
	signer := &mocks.Signer{}
	signer.On("Sign", mock.Anything).Return([]byte{0}, nil)
	restClient := &mocks.RestClient{}

	queryResult := &types.GetUserResponseEnvelope{
		Payload: &types.GetUserResponse{
			Header: &types.ResponseHeader{
				NodeID: "node1",
			},
			User: &types.User{
				ID:          "alice",
				Certificate: []byte{1, 2, 3},
			},
			Metadata: &types.Metadata{
				Version: &types.Version{
					TxNum:    1,
					BlockNum: 1,
				},
			},
		},
		Signature: []byte{0},
	}
	queryResultBytes, err := json.Marshal(queryResult)
	require.NoError(t, err)
	require.NotNil(t, queryResultBytes)

	bodyReader := bytes.NewBuffer(queryResultBytes)
	restClient.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			uri := args.Get(1).(string)
			require.Equal(t, constants.URLForGetUser("alice"), uri)

			user := args.Get(2).(*types.GetUserQuery)
			require.Equal(t, "testUserId", user.UserID)
			require.Equal(t, "alice", user.TargetUserID)
		}).
		Return(&http.Response{
			StatusCode: http.StatusOK,
			Status:     http.StatusText(http.StatusOK),
			Body:       ioutil.NopCloser(bodyReader),
		}, nil)

	logger := createTestLogger(t)
	usrCtx := &userTxContext{
		commonTxContext: commonTxContext{
			signer:     signer,
			userID:     "testUserId",
			restClient: restClient,
			logger:     logger,
			replicaSet: map[string]*url.URL{
				"node1": &url.URL{
					Path: "http://localhost:8888",
				},
			},
		},
	}

	restClient.On("Submit", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			uri := args.Get(1).(string)
			require.Equal(t, constants.PostUserTx, uri)
			tx := args.Get(2).(*types.UserAdministrationTxEnvelope)
			require.NotNil(t, tx)
			require.NotNil(t, tx.Payload)
			require.Equal(t, "testUserId", tx.Payload.UserID)
			require.Equal(t, 1, len(tx.Payload.UserWrites))
			require.Equal(t, &types.UserWrite{
				User: &types.User{
					ID:          "carol",
					Certificate: []byte{1, 1, 1},
				},
			}, tx.Payload.UserWrites[0])

			require.Equal(t, 1, len(tx.Payload.UserReads))
			require.Equal(t, &types.UserRead{
				UserID: "alice",
				Version: &types.Version{
					TxNum:    1,
					BlockNum: 1,
				},
			}, tx.Payload.UserReads[0])

			require.Equal(t, 1, len(tx.Payload.UserDeletes))

			require.Equal(t, &types.UserDelete{
				UserID: "bob",
			}, tx.Payload.UserDeletes[0])
		}).
		Return(&http.Response{
			StatusCode: http.StatusOK,
			Status:     http.StatusText(http.StatusOK),
		}, nil)

	user, err := usrCtx.GetUser("alice")
	require.NoError(t, err)
	require.Equal(t, queryResult.Payload.User, user)

	err = usrCtx.RemoveUser("bob")
	require.NoError(t, err)

	err = usrCtx.PutUser(&types.User{
		ID:          "carol",
		Certificate: []byte{1, 1, 1},
	}, nil)
	require.NoError(t, err)

	_, err = usrCtx.Commit()
	require.NoError(t, err)
}
