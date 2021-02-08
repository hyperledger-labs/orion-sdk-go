package bcdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.ibm.com/blockchaindb/server/pkg/constants"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb/mocks"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestTxCommit(t *testing.T) {

	emptySigner := &mocks.Signer{}
	emptySigner.On("Sign", mock.Anything).Return([]byte{1}, nil)

	logger := createTestLogger(t)

	tests := []struct {
		name       string
		txCtx      TxContext
		syncCommit bool
		wantErr    bool
		errMsg     string
	}{
		{
			name: "dataTx correct async",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: asyncSubmit,
						resp:     okResponseAsync(),
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: false,
		},
		{
			name: "dataTx correct sync",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: syncSubmit,
						resp:     okResponse(),
					}, emptySigner),
					timeout: time.Second * 2,
					logger:  logger,
				},
			},
			syncCommit: true,
			wantErr:    false,
		},
		{
			name: "dataTx sync server timeout",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: syncSubmit,
						resp:     serverTimeoutResponse(),
					}, emptySigner),
					timeout: time.Second * 2,
					logger:  logger,
				},
			},
			syncCommit: true,
			wantErr:    true,
			errMsg:     "timeout occurred on server side while submitting transaction, converted to asynchronous completion",
		},
		{
			name: "dataTx error submit",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: submitErr,
						resp:     nil,
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: true,
			errMsg:  "Submit error",
		},
		{
			name: "configTx correct async",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: asyncSubmit,
						resp:     okResponseAsync(),
					}, emptySigner),
					logger: logger,
				},
				oldConfig: &types.ClusterConfig{},
			},
			wantErr: false,
		},
		{
			name: "configTx correct sync",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: syncSubmit,
						resp:     okResponse(),
					}, emptySigner),
					timeout: time.Second * 2,
					logger:  logger,
				},
				oldConfig: &types.ClusterConfig{},
			},
			syncCommit: true,
			wantErr:    false,
		},
		{
			name: "configTx sync server timeout",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: syncSubmit,
						resp:     serverTimeoutResponse(),
					}, emptySigner),
					timeout: time.Second * 2,
					logger:  logger,
				},
				oldConfig: &types.ClusterConfig{},
			},
			syncCommit: true,
			wantErr:    true,
			errMsg:     "timeout occurred on server side while submitting transaction, converted to asynchronous completion",
		},
		{
			name: "userTx correct async",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: asyncSubmit,
						resp:     okResponseAsync(),
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: false,
		},
		{
			name: "userTx correct sync",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: syncSubmit,
						resp:     okResponse(),
					}, emptySigner),
					timeout: time.Second * 2,
					logger:  logger,
				},
			},
			syncCommit: true,
			wantErr:    false,
		},
		{
			name: "userTx sync server timeout",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: syncSubmit,
						resp:     serverTimeoutResponse(),
					}, emptySigner),
					timeout: time.Second * 2,
					logger:  logger,
				},
			},
			syncCommit: true,
			wantErr:    true,
			errMsg:     "timeout occurred on server side while submitting transaction, converted to asynchronous completion",
		},
		{
			name: "dbsTx correct async",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: asyncSubmit,
						resp:     okResponseAsync(),
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: false,
		},
		{
			name: "dbsTx correct sync",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: syncSubmit,
						resp:     okResponse(),
					}, emptySigner),
					timeout: time.Second,
					logger:  logger,
				},
			},
			syncCommit: true,
			wantErr:    false,
		},
		{
			name: "dbsTx sync server timeout",
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
					restClient: NewRestClient("testUser", &mockHttpClient{
						checkReq: syncSubmit,
						resp:     serverTimeoutResponse(),
					}, emptySigner),
					timeout: time.Second,
					logger:  logger,
				},
			},
			syncCommit: true,
			wantErr:    true,
			errMsg:     "timeout occurred on server side while submitting transaction, converted to asynchronous completion",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := tt.txCtx.TxEnvelope()
			require.Error(t, err)
			require.Contains(t, "can't access tx envelope, transaction not finalized", err.Error())
			require.Nil(t, env)
			_, receipt, err := tt.txCtx.Commit(tt.syncCommit)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, tt.errMsg, err.Error())
				return
			}
			require.NoError(t, err)
			env, err = tt.txCtx.TxEnvelope()
			require.NoError(t, err)
			require.NotNil(t, env)
			if tt.syncCommit {
				require.NotNil(t, receipt)
			} else {

			}
		})
	}
}

func okResponse() *http.Response {
	okResp := &types.TxResponseEnvelope{
		Payload: &types.TxResponse{
			Header: &types.ResponseHeader{
				NodeID: "node1",
			},
			Receipt: &types.TxReceipt{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 1,
					},
				},
				TxIndex: 1,
			},
		},
	}
	okPbJson, _ := json.Marshal(okResp)
	okRespReader := ioutil.NopCloser(bytes.NewReader([]byte(okPbJson)))
	return &http.Response{
		StatusCode: 200,
		Status:     http.StatusText(200),
		Body:       okRespReader,
	}
}

func okResponseAsync() *http.Response {
	okResp := &types.TxResponseEnvelope{
		Payload: &types.TxResponse{
			Header: &types.ResponseHeader{
				NodeID: "node1",
			},
		},
	}
	okPbJson, _ := json.Marshal(okResp)
	okRespReader := ioutil.NopCloser(bytes.NewReader([]byte(okPbJson)))
	return &http.Response{
		StatusCode: 200,
		Status:     http.StatusText(200),
		Body:       okRespReader,
	}
}

func serverTimeoutResponse() *http.Response {
	errResp := &types.HttpResponseErr{
		ErrMsg: "Transaction processing timeout",
	}
	errPbJson, _ := json.Marshal(errResp)
	errRespReader := ioutil.NopCloser(bytes.NewReader([]byte(errPbJson)))
	return &http.Response{
		StatusCode: http.StatusAccepted,
		Status:     http.StatusText(http.StatusAccepted),
		Body:       errRespReader,
	}
}

func serverBadRequestResponse() *http.Response {
	errResp := &types.HttpResponseErr{
		ErrMsg: "Bad request error",
	}
	errPbJson, _ := json.Marshal(errResp)
	errRespReader := ioutil.NopCloser(bytes.NewReader([]byte(errPbJson)))
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Body:       errRespReader,
	}
}

type checkRequestFunc func(req *http.Request, resp *http.Response) (*http.Response, error)

type mockHttpClient struct {
	checkReq checkRequestFunc
	resp     *http.Response
}

func (c *mockHttpClient) Do(req *http.Request) (*http.Response, error) {
	return c.checkReq(req, c.resp)
}

func asyncSubmit(req *http.Request, resp *http.Response) (*http.Response, error) {
	timeout, err := getTimeout(&req.Header)
	if err != nil {
		return serverBadRequestResponse(), nil
	}

	if timeout != 0 {
		return serverBadRequestResponse(), nil
	}
	return resp, nil
}

func syncSubmit(req *http.Request, resp *http.Response) (*http.Response, error) {
	timeout, err := getTimeout(&req.Header)
	if err != nil {
		return serverBadRequestResponse(), nil
	}

	if timeout == 0 {
		return serverBadRequestResponse(), nil
	}
	return resp, nil
}

func submitErr(req *http.Request, resp *http.Response) (*http.Response, error) {
	return nil, errors.New("Submit error")
}

func getTimeout(h *http.Header) (time.Duration, error) {
	timeoutStr := h.Get(constants.TimeoutHeader)
	if len(timeoutStr) == 0 {
		return 0, nil
	}

	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return 0, err
	}

	if timeout < 0 {
		return 0, errors.New("timeout can't be negative " + strconv.Quote(timeoutStr))
	}
	return timeout, nil
}
