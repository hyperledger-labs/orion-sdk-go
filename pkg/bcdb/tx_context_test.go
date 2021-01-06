package bcdb

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb/mocks"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestTxEnvelope(t *testing.T) {

	emptyRestClient := &mocks.RestClient{}
	emptyRestClient.On("Submit", mock.Anything, mock.Anything, mock.Anything).
		Return(emptyOkResponse(), nil)
	emptySigner := &mocks.Signer{}
	emptySigner.On("Sign", mock.Anything).Return([]byte{1}, nil)

	logger := createTestLogger(t)

	tests := []struct {
		name    string
		txCtx   TxContext
		wantErr bool
	}{
		{
			name: "dataTx",
			txCtx: &dataTxContext{
				commonTxContext: &commonTxContext{
					userID:   "testUser",
					signer:   emptySigner,
					userCert: []byte{1, 2, 3},
					replicaSet: map[string]*url.URL{
						"node1": {
							Path: "http://localhost:8888",
						},
					},
					restClient: emptyRestClient,
					logger:     logger,
				},
			},
			wantErr: false,
		},
		{
			name: "configTx",
			txCtx: &configTxContext{
				commonTxContext: &commonTxContext{
					userID:   "testUser",
					signer:   emptySigner,
					userCert: []byte{1, 2, 3},
					replicaSet: map[string]*url.URL{
						"node1": {
							Path: "http://localhost:8888",
						},
					},
					restClient: emptyRestClient,
					logger:     logger,
				},
				oldConfig: &types.ClusterConfig{},
			},
			wantErr: false,
		},
		{
			name: "userTx",
			txCtx: &userTxContext{
				commonTxContext: &commonTxContext{
					userID:   "testUser",
					signer:   emptySigner,
					userCert: []byte{1, 2, 3},
					replicaSet: map[string]*url.URL{
						"node1": {
							Path: "http://localhost:8888",
						},
					},
					restClient: emptyRestClient,
					logger:     logger,
				},
			},
			wantErr: false,
		},
		{
			name: "dbsTx",
			txCtx: &dbsTxContext{
				commonTxContext: &commonTxContext{
					userID:   "testUser",
					signer:   emptySigner,
					userCert: []byte{1, 2, 3},
					replicaSet: map[string]*url.URL{
						"node1": {
							Path: "http://localhost:8888",
						},
					},
					restClient: emptyRestClient,
					logger:     logger,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := tt.txCtx.TxEnvelope()
			require.Error(t, err)
			require.Contains(t, "can't access tx envelope, transaction not finalized", err.Error())
			require.Nil(t, env)
			_, err = tt.txCtx.Commit()
			require.NoError(t, err)
			env, err = tt.txCtx.TxEnvelope()
			require.NoError(t, err)
			require.NotNil(t, env)
		})
	}
}

func emptyOkResponse() *http.Response {
	emptyPbJson, _ := json.Marshal(empty.Empty{})
	emptyRespReader := ioutil.NopCloser(bytes.NewReader([]byte(emptyPbJson)))
	return &http.Response{
		StatusCode: 200,
		Body:       emptyRespReader,
	}
}
