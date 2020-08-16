package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/library/pkg/crypto_utils"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	server "github.ibm.com/blockchaindb/sdk/pkg/database/mock"
)

func TestClient_GetState(t *testing.T) {
	t.Parallel()
	s := server.NewTestServer()
	defer s.Stop()
	port, err := s.Port()
	require.NoError(t, err)
	opt := createOptions(port)
	signer, err := crypto.NewSigner(opt.User.Signer)
	require.NoError(t, err)
	rc, err := NewRESTClient(fmt.Sprintf("http://localhost:%s", port), http.DefaultClient)
	require.NoError(t, err)
	require.NotNil(t, rc)
	req := &types.GetStateQueryEnvelope{
		Payload: &types.GetStateQuery{
			UserID: "testUser",
			DBName: "testDb",
			Key:    "key1",
		},
		Signature: nil,
	}
	payloadBytes, err := json.Marshal(req.Payload)
	require.NoError(t, err)
	req.Signature, err = signer.Sign(payloadBytes)
	require.NoError(t, err)

	resp, err := rc.GetState(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
}

func TestClient_GetStateError(t *testing.T) {
	t.Parallel()
	s := server.NewTestServer()
	defer s.Stop()
	port, err := s.Port()
	require.NoError(t, err)
	rc, err := NewRESTClient(fmt.Sprintf("http://localhost:%s", port), http.DefaultClient)
	require.NoError(t, err)
	require.NotNil(t, rc)
	req := &types.GetStateQueryEnvelope{
		Payload: &types.GetStateQuery{
			UserID: "testUser",
			DBName: "testDb",
			Key:    "key1",
		},
		Signature: nil,
	}

	resp, err := rc.GetState(context.Background(), req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "empty signature")
}

func TestClient_GetStatus(t *testing.T) {
	t.Parallel()
	s := server.NewTestServer()
	defer s.Stop()
	port, err := s.Port()
	require.NoError(t, err)
	opt := createOptions(port)
	signer, err := crypto.NewSigner(opt.User.Signer)
	require.NoError(t, err)
	rc, err := NewRESTClient(fmt.Sprintf("http://localhost:%s", port), http.DefaultClient)
	require.NoError(t, err)
	require.NotNil(t, rc)
	req := &types.GetStatusQueryEnvelope{
		Payload: &types.GetStatusQuery{
			UserID: "testUser",
			DBName: "testDb",
		},
		Signature: nil,
	}
	payloadBytes, err := json.Marshal(req.Payload)
	require.NoError(t, err)
	req.Signature, err = signer.Sign(payloadBytes)
	require.NoError(t, err)

	resp, err := rc.GetStatus(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
}

func TestClient_GetStatusError(t *testing.T) {
	t.Parallel()
	s := server.NewTestServer()
	defer s.Stop()
	port, err := s.Port()
	require.NoError(t, err)
	rc, err := NewRESTClient(fmt.Sprintf("http://localhost:%s", port), http.DefaultClient)
	require.NoError(t, err)
	require.NotNil(t, rc)
	req := &types.GetStatusQueryEnvelope{
		Payload: &types.GetStatusQuery{
			UserID: "testUser",
			DBName: "testDb",
		},
		Signature: nil,
	}

	resp, err := rc.GetStatus(context.Background(), req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "empty signature")
}

func TestClient_SubmitTransaction(t *testing.T) {
	t.Parallel()
	s := server.NewTestServer()
	defer s.Stop()
	port, err := s.Port()
	require.NoError(t, err)
	opt := createOptions(port)
	signer, err := crypto.NewSigner(opt.User.Signer)
	require.NoError(t, err)
	rc, err := NewRESTClient(fmt.Sprintf("http://localhost:%s", port), http.DefaultClient)
	require.NoError(t, err)
	require.NotNil(t, rc)

	req := &types.TransactionEnvelope{
		Payload: &types.Transaction{
			UserID:     []byte("testUser"),
			DBName:     "testDb",
			TxID:       []byte("TX1"),
			DataModel:  types.Transaction_KV,
			Statements: nil,
			Reads:      nil,
			Writes:     nil,
		},
		Signature: nil,
	}
	payloadBytes, err := json.Marshal(req.Payload)
	require.NoError(t, err)
	req.Signature, err = signer.Sign(payloadBytes)
	require.NoError(t, err)

	resp, err := rc.SubmitTransaction(context.Background(), req)
	require.NoError(t, err)
	require.Nil(t, resp)
}

func TestNewRESTClient(t *testing.T) {
	t.Parallel()
	rc, err := NewRESTClient("http://localhost:9999", http.DefaultClient)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.EqualValues(t, "http://localhost:9999", rc.BaseURL.String())
}

func createOptions(port string) *config.Options {
	connOpt := &config.ConnectionOption{
		URL: fmt.Sprintf("http://localhost:%s/", port),
	}
	connOpts := make([]*config.ConnectionOption, 0)
	connOpts = append(connOpts, connOpt)
	userOpt := &config.IdentityOptions{
		UserID: "testUser",
		Signer: &crypto.SignerOptions{
			KeyFilePath: "../database/testdata/client.key",
		},
	}
	serversVerifyOpt := &crypto_utils.VerificationOptions{CAFilePath: "../database/testdata/ca_service.cert"}
	return &config.Options{
		ConnectionOptions: connOpts,
		User:              userOpt,
		ServersVerify:     serversVerifyOpt,
		TxOptions: &config.TxOptions{
			TxIsolation:   config.Serializable,
			ReadOptions:   &config.ReadOptions{QuorumSize: 1},
			CommitOptions: &config.CommitOptions{QuorumSize: 1},
		},
	}
}
