// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate mockery --dir . --name RestClient --case underscore --output mocks/

// RestClient encapsulates http client with user identity
// signing capabilities to generalize ability to send requests
// to BCDB server
type RestClient interface {
	// Query sends REST request with query semantics.
	// SDK will wait for `queryTimeout` for response from server and return error if no response received.
	// If commitTimeout set to 0, sdk will wait for http commitTimeout.
	Query(ctx context.Context, endpoint, httpMethod string, postData, signature []byte) (*http.Response, error)

	// Submit send REST request with transaction submission semantics and optional commitTimeout.
	// If commitTimeout set to 0, server will return immediately, without waiting for transaction processing
	// pipeline to complete and response will not contain transaction receipt, otherwise, server will wait
	// up to commitTimeout for transaction processing to complete and will return tx receipt as result.
	// In case of commitTimeout, http.StatusAccepted returned.
	Submit(ctx context.Context, endpoint string, msg proto.Message, serverTimeout time.Duration) (*http.Response, error)
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type restClient struct {
	userID     string
	httpClient HttpClient
}

func NewRestClient(userID string, httpClient HttpClient, signer Signer) RestClient {
	return &restClient{
		userID:     userID,
		httpClient: httpClient,
	}
}

// Query sends REST request with query semantics
func (r *restClient) Query(ctx context.Context, endpoint, httpMethod string, postData, signature []byte) (*http.Response, error) {
	var req *http.Request
	var err error

	switch httpMethod {
	case http.MethodPost:
		req, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(postData))
		if err != nil {
			return nil, err
		}
	case http.MethodGet:
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unexpected http method [" + httpMethod + "]. Either pass [" + http.MethodGet + "] or [" + http.MethodPost + "]")
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set(constants.UserHeader, r.userID)
	req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(signature))
	resp, err := r.httpClient.Do(req)

	if _, ok := err.(net.Error); ok {
		if err.(net.Error).Timeout() {
			err = errors.WithMessage(err, "queryTimeout error")
		}
	}

	return resp, err
}

// Submit send REST request with transaction submission semantics
func (r *restClient) Submit(ctx context.Context, endpoint string, msg proto.Message, serverTimeout time.Duration) (*http.Response, error) {
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
	if serverTimeout > 0 {
		req.Header.Set(constants.TimeoutHeader, serverTimeout.String())
	}

	resp, err := r.httpClient.Do(req)

	if _, ok := err.(net.Error); ok {
		if err.(net.Error).Timeout() {
			err = errors.WithMessage(err, "timeout error")
		}
	}
	return resp, err
}
