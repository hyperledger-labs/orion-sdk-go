package db

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUserOptions_LoadCrypto(t *testing.T) {
	userOpt := &UserOptions{
		UserID: []byte("testUser"),
		CA:     "cert/ca.cert",
		Cert:   "cert/service.pem",
		Key:    "cert/service.key",
	}
	userCrypto, err := userOpt.LoadCrypto()
	require.NoError(t, err)
	require.NotNil(t, userCrypto)
	require.NotNil(t, userCrypto.pool)
	require.NotNil(t, userCrypto.tlsPair)

	userOpt.CA = "cert/error_ca.cert"
	userCrypto, err = userOpt.LoadCrypto()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "could not read CA certificate")

	userOpt.CA = "cert/ca.cert"
	userOpt.Cert = "cert/error_service.pem"
	userCrypto, err = userOpt.LoadCrypto()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "could not load client Key pair")

	userOpt.CA = "cert/junk_ca.cert"
	userOpt.Cert = "cert/service.pem"
	userCrypto, err = userOpt.LoadCrypto()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to append CA certs")
}
