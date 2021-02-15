// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb/mocks"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestRestClient_Query(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		user := request.Header.Get(constants.UserHeader)
		sig := request.Header.Get(constants.SignatureHeader)

		require.Equal(t, "testUserID", user)
		require.Equal(t, base64.StdEncoding.EncodeToString([]byte{1, 2, 3}), sig)
		require.Equal(t, http.MethodGet, request.Method)

		time.Sleep(time.Millisecond * 50)

		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
	}))

	signer := &mocks.Signer{}
	signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3}, nil)

	client := NewRestClient("testUserID", server.Client(), signer)

	response, err := client.Query(context.Background(), server.URL, &types.GetDataQuery{
		UserID: "alice",
		DBName: "bdb",
		Key:    "foo",
	})

	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, http.StatusOK, response.StatusCode)

	ctx, cancelFnc := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelFnc()

	response, err = client.Query(ctx, server.URL, &types.GetDataQuery{
		UserID: "alice",
		DBName: "bdb",
		Key:    "foo",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "queryTimeout error")
	require.Nil(t, response)

	ctx, cancelFnc = context.WithTimeout(context.Background(), time.Second)
	defer cancelFnc()

	response, err = client.Query(ctx, server.URL, &types.GetDataQuery{
		UserID: "alice",
		DBName: "bdb",
		Key:    "foo",
	})

	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, http.StatusOK, response.StatusCode)
}

func TestRestClient_Submit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, http.MethodPost, request.Method)
		time.Sleep(time.Millisecond * 50)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
	}))

	signer := &mocks.Signer{}
	client := NewRestClient("testUserID", server.Client(), signer)

	response, err := client.Submit(context.Background(), server.URL, &types.DataTx{
		UserID: "alice",
		DBName: "bdb",
	}, 0)

	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, http.StatusOK, response.StatusCode)

	ctx, cancelFnc := context.WithTimeout(context.Background(), 0)
	defer cancelFnc()
	response, err = client.Submit(ctx, server.URL, &types.DataTx{
		UserID: "alice",
		DBName: "bdb",
	}, 0)

	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout error")
	require.Nil(t, response)

	ctx, cancelFnc = context.WithTimeout(context.Background(), time.Second)
	defer cancelFnc()
	response, err = client.Submit(ctx, server.URL, &types.DataTx{
		UserID: "alice",
		DBName: "bdb",
	}, 0)

	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, http.StatusOK, response.StatusCode)
}
