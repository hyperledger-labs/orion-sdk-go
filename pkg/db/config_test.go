package db

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUserOptions_LoadCrypto(t *testing.T) {
	userOpt := &UserOptions{
		UserID: []byte("testUser"),
		ca:     "cert/ca.cert",
		cert:   "cert/service.pem",
		key:    "cert/service.key",
	}
	userCrypto, err := userOpt.LoadCrypto()
	require.NoError(t, err)
	require.NotNil(t, userCrypto)
	require.NotNil(t, userCrypto.pool)
	require.NotNil(t, userCrypto.tlsPair)

	userOpt.ca = "cert/error_ca.cert"
	userCrypto, err = userOpt.LoadCrypto()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "could not read ca certificate")

	userOpt.ca = "cert/ca.cert"
	userOpt.cert = "cert/error_service.pem"
	userCrypto, err = userOpt.LoadCrypto()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "could not load client key pair")

	userOpt.ca = "cert/junk_ca.cert"
	userOpt.cert = "cert/service.pem"
	userCrypto, err = userOpt.LoadCrypto()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to append ca certs")
}
