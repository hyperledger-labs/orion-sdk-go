// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"fmt"
	"path"
	"testing"

	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/stretchr/testify/require"
)

func TestCreate(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	t.Run("wrong CA path", func(t *testing.T) {
		wrongCAPath := path.Join(clientCertTemDir, "will-fail", testutils.RootCAFileName+".pem")
		bcdb, err := Create(&sdkconfig.ConnectionConfig{
			RootCAs: []string{wrongCAPath},
			ReplicaSet: []*sdkconfig.Replica{
				{
					ID:       "testNode1",
					Endpoint: fmt.Sprintf("http://127.0.0.1:%s", serverPort),
				},
			},
			Logger: createTestLogger(t),
		})

		require.EqualError(t, err, fmt.Sprintf("failed to read root CA certificate: open %s: no such file or directory", wrongCAPath))
		require.Nil(t, bcdb)
	})

	t.Run("wrong CA cert - not a CA", func(t *testing.T) {
		wrongCAFile := path.Join(clientCertTemDir, "alice.pem")
		bcdb, err := Create(&sdkconfig.ConnectionConfig{
			RootCAs: []string{wrongCAFile},
			ReplicaSet: []*sdkconfig.Replica{
				{
					ID:       "testNode1",
					Endpoint: fmt.Sprintf("http://127.0.0.1:%s", serverPort),
				},
			},
			Logger: createTestLogger(t),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to create CACertCollection: certificate is missing the CA property, SN:")
		require.Nil(t, bcdb)
	})

	t.Run("wrong CA cert - not root", func(t *testing.T) {
		clientCertTemDirWrong := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"}, true)

		wrongCAFile := path.Join(clientCertTemDirWrong, testutils.IntermediateCAFileName+".pem")
		bcdb, err := Create(&sdkconfig.ConnectionConfig{
			RootCAs: []string{wrongCAFile},
			ReplicaSet: []*sdkconfig.Replica{
				{
					ID:       "testNode1",
					Endpoint: fmt.Sprintf("http://127.0.0.1:%s", serverPort),
				},
			},
			Logger: createTestLogger(t),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "verification of CA certs collection failed: root CA certificate is not self-signed, SN:")
		require.Nil(t, bcdb)
	})

}
