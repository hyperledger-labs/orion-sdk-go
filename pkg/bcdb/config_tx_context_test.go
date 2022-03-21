// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"path"
	"testing"
	"time"

	sdkConfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConfigTxContext_GetClusterConfig(t *testing.T) {
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"admin", "server"})
	testServer, nodePort, peerPort, err := SetupTestServer(t, cryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, cryptoDir, serverPort)
	session := openUserSession(t, bcdb, "admin", cryptoDir)

	tx, err := session.ConfigTx()
	require.NoError(t, err)

	clusterConfig, err := tx.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig)

	require.Equal(t, 1, len(clusterConfig.Nodes))
	require.Equal(t, "testNode1", clusterConfig.Nodes[0].Id)
	require.Equal(t, "127.0.0.1", clusterConfig.Nodes[0].Address)
	require.Equal(t, nodePort, clusterConfig.Nodes[0].Port)
	serverCertBytes, _ := testutils.LoadTestCrypto(t, cryptoDir, "server")
	require.Equal(t, serverCertBytes.Raw, clusterConfig.Nodes[0].Certificate)

	require.Equal(t, 1, len(clusterConfig.Admins))
	require.Equal(t, "admin", clusterConfig.Admins[0].Id)
	adminCertBytes, _ := testutils.LoadTestCrypto(t, cryptoDir, "admin")
	require.Equal(t, adminCertBytes.Raw, clusterConfig.Admins[0].Certificate)

	caCert, _ := testutils.LoadTestCA(t, cryptoDir, testutils.RootCAFileName)
	require.True(t, len(clusterConfig.CertAuthConfig.Roots) > 0)
	require.Equal(t, caCert.Raw, clusterConfig.CertAuthConfig.Roots[0])

	require.Equal(t, "raft", clusterConfig.ConsensusConfig.Algorithm)
	require.Equal(t, 1, len(clusterConfig.ConsensusConfig.Members))
	require.Equal(t, "testNode1", clusterConfig.ConsensusConfig.Members[0].NodeId)
	require.Equal(t, "127.0.0.1", clusterConfig.ConsensusConfig.Members[0].PeerHost)
	require.Equal(t, peerPort, clusterConfig.ConsensusConfig.Members[0].PeerPort)
	require.Equal(t, uint64(1), clusterConfig.ConsensusConfig.Members[0].RaftId)

	clusterConfig.Nodes = nil
	clusterConfig.Admins = nil
	clusterConfig.CertAuthConfig = nil
	clusterConfig.ConsensusConfig = nil
	clusterConfigAgain, err := tx.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfigAgain.Nodes, "it is a deep copy")
	require.NotNil(t, clusterConfigAgain.Admins, "it is a deep copy")
	require.NotNil(t, clusterConfigAgain.CertAuthConfig, "it is a deep copy")
	require.NotNil(t, clusterConfigAgain.ConsensusConfig, "it is a deep copy")
}

func TestConfigTxContext_GetClusterConfigTimeout(t *testing.T) {
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"admin", "server"})
	testServer, _, _, err := SetupTestServer(t, cryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, cryptoDir, serverPort)
	session := openUserSessionWithQueryTimeout(t, bcdb, "admin", cryptoDir, time.Nanosecond, false)

	tx, err := session.ConfigTx()
	require.Error(t, err)
	require.Contains(t, err.Error(), "queryTimeout error")
	require.Nil(t, tx)
}

func TestConfigTxContext_SetClusterConfig(t *testing.T) {
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"admin", "admin2", "server", "server2"})
	admin2Cert, _ := testutils.LoadTestCrypto(t, cryptoDir, "admin2")
	server2Cert, _ := testutils.LoadTestCrypto(t, cryptoDir, "server2")

	testServer, _, _, err := SetupTestServer(t, cryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, cryptoDir, serverPort)

	// Update the RaftConfig by setting a new config
	t.Run("update RaftConfig", func(t *testing.T) {
		session := openUserSession(t, bcdb, "admin", cryptoDir)

		tx1, err := session.ConfigTx()
		require.NoError(t, err)

		clusterConfig, err := tx1.GetClusterConfig()
		require.NoError(t, err)
		require.NotNil(t, clusterConfig)

		// These Raft parameters will not have an effect on the operation of the test node until it is restarted, but
		// the config will be committed.
		maxInflightBlocks := clusterConfig.ConsensusConfig.RaftConfig.MaxInflightBlocks + 10
		snapshotIntervalSize := clusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize - 10
		clusterConfig.ConsensusConfig.RaftConfig.MaxInflightBlocks = maxInflightBlocks
		clusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize = snapshotIntervalSize
		err = tx1.SetClusterConfig(clusterConfig)
		require.NoError(t, err)

		txID, receiptEnv, err := tx1.Commit(true)
		require.NoError(t, err)
		require.True(t, txID != "")
		receipt := receiptEnv.GetResponse().GetReceipt()
		require.NotNil(t, receipt)
		require.Equal(t, receipt.GetHeader().GetValidationInfo()[receipt.TxIndex].Flag, types.Flag_VALID)

		tx2, err := session.ConfigTx()
		clusterConfig2, err := tx2.GetClusterConfig()
		require.Equal(t, snapshotIntervalSize, clusterConfig2.GetConsensusConfig().GetRaftConfig().SnapshotIntervalSize)
		require.Equal(t, maxInflightBlocks, clusterConfig2.GetConsensusConfig().GetRaftConfig().MaxInflightBlocks)
	})

	// Setting a new config and update on it
	t.Run("set new config and update on it", func(t *testing.T) {
		session := openUserSession(t, bcdb, "admin", cryptoDir)

		tx, err := session.ConfigTx()
		require.NoError(t, err)

		config, err := tx.GetClusterConfig()
		require.NoError(t, err)
		require.NotNil(t, config)

		config.ConsensusConfig.RaftConfig.MaxInflightBlocks++
		err = tx.SetClusterConfig(config)
		require.NoError(t, err)

		err = tx.AddAdmin(&types.Admin{Id: "admin2", Certificate: admin2Cert.Raw})
		require.NoError(t, err)

		txID, receiptEnv, err := tx.Commit(true)
		require.NoError(t, err)
		require.True(t, txID != "")
		receipt := receiptEnv.GetResponse().GetReceipt()
		require.NotNil(t, receipt)
		require.Equal(t, receipt.GetHeader().GetValidationInfo()[receipt.TxIndex].Flag, types.Flag_VALID)
	})

	// A bad new config
	t.Run("error: bad RaftConfig", func(t *testing.T) {
		session := openUserSession(t, bcdb, "admin", cryptoDir)

		tx1, err := session.ConfigTx()
		require.NoError(t, err)

		clusterConfig, err := tx1.GetClusterConfig()
		require.NoError(t, err)
		require.NotNil(t, clusterConfig)

		// A node without a corresponding peer
		clusterConfig.Nodes = append(clusterConfig.Nodes,
			&types.NodeConfig{
				Id:          "node-2",
				Address:     "127.0.0.1",
				Port:        666,
				Certificate: server2Cert.Raw,
			},
		)

		err = tx1.SetClusterConfig(clusterConfig)
		require.NoError(t, err)

		txID, receiptEnv, err := tx1.Commit(true)
		require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: ClusterConfig.Nodes must be the same length as ClusterConfig.ConsensusConfig.Members, and Nodes set must include all Members")
		require.True(t, txID != "")
		receipt := receiptEnv.GetResponse().GetReceipt()
		require.Nil(t, receipt) // Rejected before ordering
	})

	// Submitting an empty new config results in an error
	t.Run("error: empty new config", func(t *testing.T) {
		session := openUserSession(t, bcdb, "admin", cryptoDir)

		tx, err := session.ConfigTx()
		require.NoError(t, err)

		txID, receiptEnv, err := tx.Commit(true)
		require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: new config is empty. There must be at least single node and an admin in the cluster")
		require.True(t, txID != "")
		require.Nil(t, receiptEnv)
	})

	// Setting a new config twice results in an error
	t.Run("error: set new config twice", func(t *testing.T) {
		session := openUserSession(t, bcdb, "admin", cryptoDir)

		tx, err := session.ConfigTx()
		require.NoError(t, err)

		config, err := tx.GetClusterConfig()
		require.NoError(t, err)
		require.NotNil(t, config)

		config.ConsensusConfig.RaftConfig.MaxInflightBlocks++
		err = tx.SetClusterConfig(config)
		require.NoError(t, err)

		config.ConsensusConfig.RaftConfig.MaxInflightBlocks++
		err = tx.SetClusterConfig(config)
		require.EqualError(t, err, "pending config already exists")

		err = tx.Abort()
		require.NoError(t, err)
	})

	// Setting a new config when a pending config exists results in an error
	t.Run("error: set new config on pending config", func(t *testing.T) {
		session := openUserSession(t, bcdb, "admin", cryptoDir)

		tx, err := session.ConfigTx()
		require.NoError(t, err)

		config, err := tx.GetClusterConfig()
		require.NoError(t, err)
		require.NotNil(t, config)

		err = tx.AddAdmin(&types.Admin{Id: "admin-alias", Certificate: admin2Cert.Raw})
		require.NoError(t, err)
		require.NotNil(t, config)

		config.ConsensusConfig.RaftConfig.MaxInflightBlocks++
		err = tx.SetClusterConfig(config)
		require.EqualError(t, err, "pending config already exists")

		err = tx.Abort()
		require.NoError(t, err)
	})
}

func TestConfigTxContext_AddAdmin(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestCrypto(t, []string{"admin", "admin2", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCryptoDir)

	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	adminCert, _ := testutils.LoadTestCrypto(t, clientCryptoDir, "admin")
	admin := &types.Admin{
		Id:          "admin",
		Certificate: adminCert.Raw,
	}

	admin2Cert, _ := testutils.LoadTestCrypto(t, clientCryptoDir, "admin2")
	admin2 := &types.Admin{Id: "admin2", Certificate: admin2Cert.Raw}

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session1 := openUserSession(t, bcdb, "admin", clientCryptoDir)

	// Add admin2
	tx, err := session1.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.AddAdmin(admin)
	require.EqualError(t, err, "admin already exists in current config: admin")

	err = tx.AddAdmin(admin2)
	require.NoError(t, err)

	err = tx.AddAdmin(admin2)
	require.EqualError(t, err, "admin already exists in pending config: admin2")

	txID, receiptEnv, err := tx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, txID)
	require.NotNil(t, receiptEnv)

	tx2, err := session1.ConfigTx()
	require.NoError(t, err)
	clusterConfig, err := tx2.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig)
	require.Len(t, clusterConfig.Admins, 2)

	found, index := AdminExists("admin2", clusterConfig.Admins)
	require.True(t, found)

	require.EqualValues(t, clusterConfig.Admins[index].Certificate, admin2Cert.Raw)

	// do something with the new admin
	session2 := openUserSession(t, bcdb, "admin2", clientCryptoDir)
	tx3, err := session2.ConfigTx()
	require.NoError(t, err)
	clusterConfig2, err := tx3.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig2)
}

func TestConfigTxContext_DeleteAdmin(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestCrypto(t, []string{"admin", "admin2", "admin3", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	adminCert, _ := testutils.LoadTestCrypto(t, clientCryptoDir, "admin")
	admin := &types.Admin{Id: "admin", Certificate: adminCert.Raw}

	admin2Cert, _ := testutils.LoadTestCrypto(t, clientCryptoDir, "admin2")
	admin3Cert, _ := testutils.LoadTestCrypto(t, clientCryptoDir, "admin3")

	admin2 := &types.Admin{Id: "admin2", Certificate: admin2Cert.Raw}
	admin3 := &types.Admin{Id: "admin3", Certificate: admin3Cert.Raw}

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session1 := openUserSession(t, bcdb, "admin", clientCryptoDir)

	// Add admin2 & admin3
	tx1, err := session1.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	err = tx1.AddAdmin(admin2)
	require.NoError(t, err)
	err = tx1.AddAdmin(admin3)
	require.NoError(t, err)

	txID, receiptEnv, err := tx1.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, txID)
	require.NotNil(t, receiptEnv)

	tx, err := session1.ConfigTx()
	require.NoError(t, err)
	clusterConfig, err := tx.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig)
	require.Len(t, clusterConfig.Admins, 3)

	// Remove an admin
	session2 := openUserSession(t, bcdb, "admin2", clientCryptoDir)
	tx2, err := session2.ConfigTx()
	require.NoError(t, err)
	err = tx2.DeleteAdmin(admin.Id)
	require.NoError(t, err)
	err = tx2.DeleteAdmin(admin.Id)
	require.EqualError(t, err, "admin does not exist in pending config: admin")
	err = tx2.DeleteAdmin("non-admin")
	require.EqualError(t, err, "admin does not exist in current config: non-admin")

	txID, receiptEnv, err = tx2.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, txID)
	require.NotNil(t, receiptEnv)

	// verify tx was successfully committed
	tx3, err := session2.ConfigTx()
	require.NoError(t, err)
	clusterConfig, err = tx3.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig)
	require.Len(t, clusterConfig.Admins, 2)
	found, index := AdminExists("admin2", clusterConfig.Admins)
	require.True(t, found)
	require.EqualValues(t, clusterConfig.Admins[index].Certificate, admin2Cert.Raw)

	found, index = AdminExists("admin3", clusterConfig.Admins)
	require.True(t, found)
	require.EqualValues(t, clusterConfig.Admins[index].Certificate, admin3Cert.Raw)

	// session1 by removed admin cannot execute additional transactions
	tx4, err := session1.ConfigTx()
	require.EqualError(t, err, "error handling request, server returned: status: 401 Unauthorized, status code: 401, message: signature verification failed")
	require.Nil(t, tx4)
}

func TestConfigTxContext_UpdateAdmin(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestCrypto(t, []string{"admin", "admin2", "adminUpdated", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	admin2Cert, _ := testutils.LoadTestCrypto(t, clientCryptoDir, "admin2")
	adminUpdatedCert, _ := testutils.LoadTestCrypto(t, clientCryptoDir, "adminUpdated")

	admin2 := &types.Admin{Id: "admin2", Certificate: admin2Cert.Raw}

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session1 := openUserSession(t, bcdb, "admin", clientCryptoDir)

	// Add admin2
	tx1, err := session1.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	err = tx1.AddAdmin(admin2)
	require.NoError(t, err)

	txID, receiptEnv, err := tx1.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, txID)
	require.NotNil(t, receiptEnv)

	// Update an admin
	session2 := openUserSession(t, bcdb, "admin2", clientCryptoDir)
	tx2, err := session2.ConfigTx()
	require.NoError(t, err)
	err = tx2.UpdateAdmin(&types.Admin{Id: "admin", Certificate: adminUpdatedCert.Raw})
	require.NoError(t, err)
	err = tx2.UpdateAdmin(&types.Admin{Id: "non-admin", Certificate: []byte("bad-cert")})
	require.EqualError(t, err, "admin does not exist in current config: non-admin")

	txID, receiptEnv, err = tx2.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, txID)
	require.NotNil(t, receiptEnv)

	tx, err := session2.ConfigTx()
	require.NoError(t, err)
	clusterConfig, err := tx.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig)
	require.Len(t, clusterConfig.Admins, 2)

	found, index := AdminExists("admin", clusterConfig.Admins)
	require.True(t, found)
	require.EqualValues(t, clusterConfig.Admins[index].Certificate, adminUpdatedCert.Raw)

	// session1 by updated admin cannot execute additional transactions, need to recreate session
	tx3, err := session1.ConfigTx()
	require.EqualError(t, err, "error handling request, server returned: status: 401 Unauthorized, status code: 401, message: signature verification failed")
	require.Nil(t, tx3)

	// need to recreate session with new credentials
	session3, err := bcdb.Session(&sdkConfig.SessionConfig{
		UserConfig: &sdkConfig.UserConfig{
			UserID:         "admin",
			CertPath:       path.Join(clientCryptoDir, "adminUpdated.pem"),
			PrivateKeyPath: path.Join(clientCryptoDir, "adminUpdated.key"),
		},
	})
	require.NoError(t, err)
	tx3, err = session3.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx3)
}

func TestConfigTxContext_UpdateCAConfig(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestCrypto(t, []string{"admin", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session := openUserSession(t, bcdb, "admin", clientCryptoDir)

	// 1. An empty CAConfig will return an error
	tx1, err := session.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	err = tx1.UpdateCAConfig(nil)
	require.NoError(t, err)

	txID, receiptEnv, err := tx1.Commit(true)
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: CA config is empty. At least one root CA is required")
	require.True(t, txID != "")
	require.Nil(t, receiptEnv)

	// 2. add a Root CA & Intermediate CA
	clientCryptoDir2 := testutils.GenerateTestCrypto(t, []string{"alice"}, true)
	certRootCA2, _ := testutils.LoadTestCA(t, clientCryptoDir2, testutils.RootCAFileName)
	certIntCA2, _ := testutils.LoadTestCA(t, clientCryptoDir2, testutils.IntermediateCAFileName)

	tx2, err := session.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	clusterConfig, err := tx2.GetClusterConfig()
	require.NoError(t, err)
	caConf := clusterConfig.GetCertAuthConfig()
	caConf.Roots = append(caConf.Roots, certRootCA2.Raw)
	caConf.Intermediates = append(caConf.Intermediates, certIntCA2.Raw)
	err = tx2.UpdateCAConfig(caConf)
	require.NoError(t, err)

	txID, receiptEnv, err = tx2.Commit(true)
	require.NoError(t, err)
	require.True(t, txID != "")
	require.NotNil(t, receiptEnv)

	tx3, err := session.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	clusterConfig2, err := tx3.GetClusterConfig()
	require.NoError(t, err)
	require.Len(t, clusterConfig2.GetCertAuthConfig().GetIntermediates(), 1)
	require.Len(t, clusterConfig2.GetCertAuthConfig().GetRoots(), 2)
}

func TestConfigTxContext_UpdateRaftConfig(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestCrypto(t, []string{"admin", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session := openUserSession(t, bcdb, "admin", clientCryptoDir)

	// 1. An empty RaftConfig will return an error
	tx1, err := session.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	err = tx1.UpdateRaftConfig(nil)
	require.NoError(t, err)

	txID, receiptEnv, err := tx1.Commit(true)
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: Consensus config RaftConfig is empty.")
	require.True(t, txID != "")
	require.Nil(t, receiptEnv)

	// 2. Update the RaftConfig
	tx2, err := session.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	clusterConfig, err := tx2.GetClusterConfig()
	require.NoError(t, err)
	raftConf := clusterConfig.GetConsensusConfig().GetRaftConfig()
	raftConf.MaxInflightBlocks++
	err = tx2.UpdateRaftConfig(raftConf)
	require.NoError(t, err)

	txID, receiptEnv, err = tx2.Commit(true)
	require.NoError(t, err)
	require.True(t, txID != "")
	require.NotNil(t, receiptEnv)

	tx3, err := session.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	clusterConfig2, err := tx3.GetClusterConfig()
	require.NoError(t, err)
	require.Equal(t, raftConf.MaxInflightBlocks, clusterConfig2.GetConsensusConfig().GetRaftConfig().GetMaxInflightBlocks())
}

func TestConfigTx_CommitAbortFinality(t *testing.T) {
	t.Skip("Add/Remove/Update node is a config update, TODO in issue: https://github.com/hyperledger-labs/orion-server/issues/40")

	clientCryptoDir := testutils.GenerateTestCrypto(t, []string{"admin", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	for i := 0; i < 3; i++ {
		session := openUserSession(t, bcdb, "admin", clientCryptoDir)
		tx, err := session.ConfigTx()
		require.NoError(t, err)

		config, err := tx.GetClusterConfig()
		require.NoError(t, err)
		node1 := config.Nodes[0]
		node1.Port++
		nodeId := node1.Id
		nodePort := node1.Port
		err = tx.UpdateClusterNode(config.Nodes[0], config.ConsensusConfig.Members[0])
		require.NoError(t, err)

		assertTxFinality(t, TxFinality(i), tx, session)

		config, err = tx.GetClusterConfig()
		require.EqualError(t, err, ErrTxSpent.Error())
		require.Nil(t, config)

		err = tx.AddClusterNode(&types.NodeConfig{}, nil)
		require.EqualError(t, err, ErrTxSpent.Error())
		err = tx.DeleteClusterNode("id")
		require.EqualError(t, err, ErrTxSpent.Error())
		err = tx.UpdateClusterNode(&types.NodeConfig{}, nil)
		require.EqualError(t, err, ErrTxSpent.Error())

		err = tx.AddAdmin(&types.Admin{})
		require.EqualError(t, err, ErrTxSpent.Error())
		err = tx.DeleteAdmin("id")
		require.EqualError(t, err, ErrTxSpent.Error())
		err = tx.UpdateAdmin(&types.Admin{})
		require.EqualError(t, err, ErrTxSpent.Error())

		if TxFinality(i) != TxFinalityAbort {
			tx, err = session.ConfigTx()
			require.NoError(t, err)

			config, err := tx.GetClusterConfig()
			require.NoError(t, err)
			node1 := config.Nodes[0]
			require.Equal(t, nodeId, node1.Id)
			require.Equal(t, nodePort, node1.Port)
		}
	}
}
