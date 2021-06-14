// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/IBM-Blockchain/bcdb-sdk/pkg/bcdb/mocks"
	sdkConfig "github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestUserContext_AddAndRetrieveUserWithAndWithoutTimeout(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)

	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)

	sessionOneNano := openUserSessionWithQueryTimeout(t, bcdb, "admin", clientCertTemDir, time.Nanosecond)
	tx, err := sessionOneNano.UsersTx()
	require.NoError(t, err)
	require.NotNil(t, tx)
	alice, err := tx.GetUser("alice")
	require.Error(t, err)
	require.Contains(t, err.Error(), "queryTimeout error")
	require.Nil(t, alice)

}

func TestUserContext_CommitAbortFinality(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	_, session := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		// Start submission session to introduce new user
		tx, err := session.UsersTx()
		require.NoError(t, err)

		pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
		require.NoError(t, err)
		certBlock, _ := pem.Decode(pemUserCert)
		err = tx.PutUser(&types.User{ID: "alice", Certificate: certBlock.Bytes}, nil)
		require.NoError(t, err)

		assertTxFinality(t, TxFinality(i), tx, session)

		val, err := tx.GetUser("bob")
		require.EqualError(t, err, ErrTxSpent.Error())
		require.Nil(t, val)

		err = tx.PutUser(&types.User{ID: "bob", Certificate: certBlock.Bytes}, nil)
		require.EqualError(t, err, ErrTxSpent.Error())

		err = tx.RemoveUser("bob")
		require.EqualError(t, err, ErrTxSpent.Error())

		if TxFinality(i) != TxFinalityAbort {
			tx, err = session.UsersTx()
			require.NoError(t, err)
			val, err = tx.GetUser("alice")
			require.NoError(t, err)
			require.NotNil(t, val)
			require.True(t, proto.Equal(&types.User{ID: "alice", Certificate: certBlock.Bytes}, val))
		}
	}
}

func TestUserContext_MalformedRequest(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, _ := connectAndOpenAdminSession(t, testServer, clientCertTemDir)

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
			usrCtx := &userTxContext{
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

	expectedUser := &types.User{
		ID:          "alice",
		Certificate: []byte{1, 2, 3},
	}

	queryResult := &types.ResponseEnvelope{
		Payload: MarshalOrPanic(&types.Payload{
			Header: &types.ResponseHeader{
				NodeID: "node1",
			},
			Response: MarshalOrPanic(&types.GetUserResponse{
				User: expectedUser,
				Metadata: &types.Metadata{
					Version: &types.Version{
						TxNum:    1,
						BlockNum: 1,
					},
				},
			}),
		}),
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
	}

	restClient.On("Submit", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
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
		Return(okResponse(), nil)

	user, err := usrCtx.GetUser("alice")
	require.NoError(t, err)
	require.Equal(t, expectedUser, user)

	err = usrCtx.RemoveUser("bob")
	require.NoError(t, err)

	err = usrCtx.PutUser(&types.User{
		ID:          "carol",
		Certificate: []byte{1, 1, 1},
	}, nil)
	require.NoError(t, err)

	_, _, err = usrCtx.Commit(true)
	require.NoError(t, err)
}
