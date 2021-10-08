// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
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

	"github.com/IBM-Blockchain/bcdb-sdk/pkg/bcdb/mocks"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTxCommit(t *testing.T) {
	emptySigner := &mocks.Signer{}
	emptySigner.On("Sign", mock.Anything).Return([]byte{1}, nil)

	verifier := &mocks.SignatureVerifier{}
	verifier.On("Verify", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	verifierFails := &mocks.SignatureVerifier{}
	verifierFails.On("Verify", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("bad-mock-signature"))

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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: asyncSubmit,
						resp:    okResponseAsync(),
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: false,
		},
		{
			name: "dataTx incorrect async",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: asyncSubmit,
						resp:    serverBadRequestResponse(),
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: true,
			errMsg:  "failed to submit transaction, server returned: status: Bad Request, message: Bad request error",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: syncSubmit,
						resp:    okResponse(),
					}, emptySigner),
					commitTimeout: time.Second * 2,
					logger:        logger,
				},
			},
			syncCommit: true,
			wantErr:    false,
		},
		{
			name: "dataTx sync invalid mvcc-conflict",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: syncSubmit,
						resp:    mvccResponse(),
					}, emptySigner),
					commitTimeout: time.Second * 2,
					logger:        logger,
				},
			},
			syncCommit: true,
			wantErr:    true,
			errMsg: "INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE",
		},
		{
			name: "dataTx sync server commitTimeout",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: syncSubmit,
						resp:    serverTimeoutResponse(),
					}, emptySigner),
					commitTimeout: time.Second * 2,
					logger:        logger,
				},
			},
			syncCommit: true,
			wantErr:    true,
			errMsg:     "timeout occurred on server side while submitting transaction, converted to asynchronous completion, TxID:",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: submitErr,
						resp:    nil,
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: true,
			errMsg:  "submit error",
		},
		{
			name: "dataTx sig verifier fails",
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
					verifier: verifierFails,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: asyncSubmit,
						resp:    okResponseAsync(),
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: true,
			errMsg:  "signature verification failed nodeID node1, due to bad-mock-signature",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: asyncSubmit,
						resp:    okResponseAsync(),
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: syncSubmit,
						resp:    okResponse(),
					}, emptySigner),
					commitTimeout: time.Second * 2,
					logger:        logger,
				},
				oldConfig: &types.ClusterConfig{},
			},
			syncCommit: true,
			wantErr:    false,
		},
		{
			name: "configTx sync server commitTimeout",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: syncSubmit,
						resp:    serverTimeoutResponse(),
					}, emptySigner),
					commitTimeout: time.Second * 2,
					logger:        logger,
				},
				oldConfig: &types.ClusterConfig{},
			},
			syncCommit: true,
			wantErr:    true,
			errMsg:     "timeout occurred on server side while submitting transaction, converted to asynchronous completion",
		},
		{
			name: "configTx sig verifier failed",
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
					verifier: verifierFails,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: asyncSubmit,
						resp:    okResponseAsync(),
					}, emptySigner),
					logger: logger,
				},
				oldConfig: &types.ClusterConfig{},
			},
			wantErr: true,
			errMsg:  "signature verification failed nodeID node1, due to bad-mock-signature",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: asyncSubmit,
						resp:    okResponseAsync(),
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: syncSubmit,
						resp:    okResponse(),
					}, emptySigner),
					commitTimeout: time.Second * 2,
					logger:        logger,
				},
			},
			syncCommit: true,
			wantErr:    false,
		},
		{
			name: "userTx sync server commitTimeout",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: syncSubmit,
						resp:    serverTimeoutResponse(),
					}, emptySigner),
					commitTimeout: time.Second * 2,
					logger:        logger,
				},
			},
			syncCommit: true,
			wantErr:    true,
			errMsg:     "timeout occurred on server side while submitting transaction, converted to asynchronous completion",
		},
		{
			name: "userTx sig verifier fails",
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
					verifier: verifierFails,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: asyncSubmit,
						resp:    okResponseAsync(),
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: true,
			errMsg:  "signature verification failed nodeID node1, due to bad-mock-signature",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: asyncSubmit,
						resp:    okResponseAsync(),
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: syncSubmit,
						resp:    okResponse(),
					}, emptySigner),
					commitTimeout: time.Second,
					logger:        logger,
				},
			},
			syncCommit: true,
			wantErr:    false,
		},
		{
			name: "dbsTx sync server commitTimeout",
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
					verifier: verifier,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: syncSubmit,
						resp:    serverTimeoutResponse(),
					}, emptySigner),
					commitTimeout: time.Second,
					logger:        logger,
				},
			},
			syncCommit: true,
			wantErr:    true,
			errMsg:     "timeout occurred on server side while submitting transaction, converted to asynchronous completion",
		},
		{
			name: "dbsTx sig verifier fails",
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
					verifier: verifierFails,
					restClient: NewRestClient("testUser", &mockHttpClient{
						process: asyncSubmit,
						resp:    okResponseAsync(),
					}, emptySigner),
					logger: logger,
				},
			},
			wantErr: true,
			errMsg:  "signature verification failed nodeID node1, due to bad-mock-signature",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := tt.txCtx.CommittedTxEnvelope()
			require.Error(t, err)
			require.Contains(t, "can't access tx envelope, transaction not finalized", err.Error())
			require.Nil(t, env)
			_, receipt, err := tt.txCtx.Commit(tt.syncCommit)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t,  err.Error(),tt.errMsg)
				return
			}
			require.NoError(t, err)
			env, err = tt.txCtx.CommittedTxEnvelope()
			require.NoError(t, err)
			require.NotNil(t, env)
			if tt.syncCommit {
				require.NotNil(t, receipt)
			}
			require.NoError(t, err)
		})
	}

}

func TestTxQuery(t *testing.T) {
	emptySigner := &mocks.Signer{}
	emptySigner.On("Sign", mock.Anything).Return([]byte{1}, nil)

	verifier := &mocks.SignatureVerifier{}
	verifier.On("Verify", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	logger := createTestLogger(t)

	tests := []struct {
		name    string
		txCtx   *commonTxContext
		wantErr bool
		errMsg  string
	}{
		{
			name: "dataTx Get, processing 100 ms, 0 timeout",
			txCtx: &commonTxContext{
				userID:   "testUser",
				signer:   emptySigner,
				userCert: []byte{1, 2, 3},
				replicaSet: map[string]*url.URL{
					"node1": {
						Path: "http://localhost:8888",
					},
				},
				verifier: verifier,
				restClient: NewRestClient("testUser", &mockHttpClient{
					process: querySleep100,
					resp:    okDataQueryResponse(),
				}, emptySigner),
				queryTimeout: 0,
				logger:       logger,
			},
			wantErr: false,
		},
		{
			name: "dataTx Get, processing 100 ms, 10 ms timeout",
			txCtx: &commonTxContext{
				userID:   "testUser",
				signer:   emptySigner,
				userCert: []byte{1, 2, 3},
				replicaSet: map[string]*url.URL{
					"node1": {
						Path: "http://localhost:8888",
					},
				},
				verifier: verifier,
				restClient: NewRestClient("testUser", &mockHttpClient{
					process: querySleep100,
					resp:    okDataQueryResponse(),
				}, emptySigner),
				queryTimeout: time.Millisecond * 10,
				logger:       logger,
			},
			wantErr: true,
			errMsg:  "queryTimeout error",
		},
		{
			name: "dataTx Get, processing 10 ms, 100 ms timeout",
			txCtx: &commonTxContext{
				userID:   "testUser",
				signer:   emptySigner,
				userCert: []byte{1, 2, 3},
				replicaSet: map[string]*url.URL{
					"node1": {
						Path: "http://localhost:8888",
					},
				},
				verifier: verifier,
				restClient: NewRestClient("testUser", &mockHttpClient{
					process: querySleep10,
					resp:    okDataQueryResponse(),
				}, emptySigner),
				queryTimeout: time.Millisecond * 100,
				logger:       logger,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := tt.txCtx.CommittedTxEnvelope()
			require.Error(t, err)
			require.Contains(t, "can't access tx envelope, transaction not finalized", err.Error())
			require.Nil(t, env)
			res := &types.GetDataResponse{}
			req := &types.GetDataQuery{
				UserId: "testUSer",
				DbName: "bdb",
				Key:    "key1",
			}
			err = tt.txCtx.handleRequest(constants.URLForGetData("bdb", "key1"), req, res)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
				return
			}
			require.NoError(t, err)
		})
	}

}

func okResponse() *http.Response {
	okResp := &types.TxReceiptResponseEnvelope{
		Response: &types.TxReceiptResponse{
			Header: &types.ResponseHeader{
				NodeId: "node1",
			},
			Receipt: &types.TxReceipt{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 1,
					},
					ValidationInfo:          []*types.ValidationInfo{
						{
							Flag:                 types.Flag_VALID,
							ReasonIfInvalid:      "",
						},
						{
							Flag:                 types.Flag_VALID,
							ReasonIfInvalid:      "",
						},
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
	okResp := &types.TxReceiptResponseEnvelope{
		Response: &types.TxReceiptResponse{
			Header: &types.ResponseHeader{
				NodeId: "node1",
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

func mvccResponse() *http.Response {
	okResp := &types.TxReceiptResponseEnvelope{
		Response: &types.TxReceiptResponse{
			Header: &types.ResponseHeader{
				NodeId: "node1",
			},
			Receipt: &types.TxReceipt{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 1,
					},
					ValidationInfo:          []*types.ValidationInfo{
						{
							Flag:                 types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
							ReasonIfInvalid:      "oops",
						},
					},
				},
				TxIndex: 0,
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

func okDataQueryResponse() *http.Response {
	okResp := &types.GetDataResponseEnvelope{
		Response: &types.GetDataResponse{
			Header: &types.ResponseHeader{
				NodeId: "node1",
			},
			Value:    []byte{1},
			Metadata: &types.Metadata{},
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
		ErrMsg: "Transaction processing commitTimeout",
	}
	errPbJson, _ := json.Marshal(errResp)
	errRespReader := ioutil.NopCloser(bytes.NewReader(errPbJson))
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
	errRespReader := ioutil.NopCloser(bytes.NewReader(errPbJson))
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     http.StatusText(http.StatusBadRequest),
		Body:       errRespReader,
	}
}

type processFunc func(req *http.Request, resp *http.Response) (*http.Response, error)

type mockHttpClient struct {
	process processFunc
	resp    *http.Response
}

func (c *mockHttpClient) Do(req *http.Request) (*http.Response, error) {
	return c.process(req, c.resp)
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

func submitErr(_ *http.Request, resp *http.Response) (*http.Response, error) {
	return nil, errors.New("submit error")
}

func querySleep100(req *http.Request, resp *http.Response) (*http.Response, error) {
	time.Sleep(time.Millisecond * 100)
	ctx := req.Context()
	if deadline, ok := ctx.Deadline(); ok {
		if deadline.Before(time.Now()) {
			return nil, &timeoutError{}
		}
	}
	return resp, nil
}

func querySleep10(req *http.Request, resp *http.Response) (*http.Response, error) {
	time.Sleep(time.Millisecond * 10)
	ctx := req.Context()
	if deadline, ok := ctx.Deadline(); ok {
		if deadline.Before(time.Now()) {
			return nil, &timeoutError{}
		}
	}
	return resp, nil
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

// Implements net.Error interface
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }
