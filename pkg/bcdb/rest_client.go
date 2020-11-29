package bcdb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
)

//go:generate mockery --dir . --name RestClient --case underscore --output mocks/

// RestClient encapsulates http client with user identity
// signing capabilities to generalize ability to send requests
// to BCDB server
type RestClient interface {
	// Query sends REST request with query semantics
	Query(ctx context.Context, endpoint string, msg proto.Message) (*http.Response, error)

	// Submit send REST request with transaction submission semantics
	Submit(ctx context.Context, endpoint string, msg proto.Message) (*http.Response, error)
}

type restClient struct {
	userID     string
	httpClient *http.Client
	signer     Signer
}

func NewRestClient(userID string, httpClient *http.Client, signer Signer) RestClient {
	return &restClient{
		userID:     userID,
		httpClient: httpClient,
		signer:     signer,
	}
}

// Query sends REST request with query semantics
func (r *restClient) Query(ctx context.Context, endpoint string, msg proto.Message) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	signature, err := cryptoservice.SignQuery(r.signer, msg)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set(constants.UserHeader, r.userID)
	req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(signature))
	return r.httpClient.Do(req)
}

// Submit send REST request with transaction submission semantics
func (r *restClient) Submit(ctx context.Context, endpoint string, msg proto.Message) (*http.Response, error) {
	userTxEnvelope, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx,
		http.MethodPost,
		endpoint,
		bytes.NewReader(userTxEnvelope))

	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	return r.httpClient.Do(req)
}
