package bcdb

import (
	"bytes"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkConfig "github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestConfigTxContext_GetClusterConfig(t *testing.T) {
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "server"})
	testServer, err := setupTestServer(t, cryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

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
	require.Equal(t, "testNode1", clusterConfig.Nodes[0].ID)
	require.Equal(t, "127.0.0.1", clusterConfig.Nodes[0].Address)
	require.Equal(t, 0, int(clusterConfig.Nodes[0].Port))
	serverCertBytes, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "server")
	require.Equal(t, serverCertBytes.Raw, clusterConfig.Nodes[0].Certificate)

	require.Equal(t, 1, len(clusterConfig.Admins))
	require.Equal(t, "admin", clusterConfig.Admins[0].ID)
	adminCertBytes, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "admin")
	require.Equal(t, adminCertBytes.Raw, clusterConfig.Admins[0].Certificate)

	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)
	require.True(t, len(clusterConfig.CertAuthConfig.Roots)>0)
	require.Equal(t, caCert.Raw, clusterConfig.CertAuthConfig.Roots[0])

	clusterConfig.Nodes = nil
	clusterConfig.Admins = nil
	clusterConfig.CertAuthConfig = nil
	clusterConfigAgain, err := tx.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfigAgain.Nodes, "it is a deep copy")
	require.NotNil(t, clusterConfigAgain.Admins, "it is a deep copy")
	require.NotNil(t, clusterConfigAgain.CertAuthConfig, "it is a deep copy")
}

func TestConfigTxContext_AddAdmin(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "admin2", "server"})
	testServer, err := setupTestServer(t, clientCryptoDir)

	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	adminCert, _ := testutils.LoadTestClientCrypto(t, clientCryptoDir, "admin")
	admin := &types.Admin{
		ID:          "admin",
		Certificate: adminCert.Raw,
	}

	admin2Cert, _ := testutils.LoadTestClientCrypto(t, clientCryptoDir, "admin2")
	admin2 := &types.Admin{ID: "admin2", Certificate: admin2Cert.Raw}

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

	txID, err := tx.Commit()
	require.NoError(t, err)
	require.NotNil(t, txID)

	require.Eventually(t, func() bool {
		// verify tx was successfully committed. "Get" works once per Tx.
		tx2, err := session1.ConfigTx()
		require.NoError(t, err)
		clusterConfig, err := tx2.GetClusterConfig()
		if err != nil || clusterConfig == nil {
			return false
		}
		if len(clusterConfig.Admins) != 2 {
			return false
		}

		found, index := AdminExists("admin2", clusterConfig.Admins)
		if !found {
			return false
		}
		if !bytes.Equal(clusterConfig.Admins[index].Certificate, admin2Cert.Raw) {
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)

	//do something with the new admin
	session2 := openUserSession(t, bcdb, "admin2", clientCryptoDir)
	tx3, err := session2.ConfigTx()
	require.NoError(t, err)
	clusterConfig, err := tx3.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig)
}

func TestConfigTxContext_DeleteAdmin(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "admin2", "admin3", "server"})
	testServer, err := setupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	adminCert, _ := testutils.LoadTestClientCrypto(t, clientCryptoDir, "admin")
	admin := &types.Admin{
		ID:          "admin",
		Certificate: adminCert.Raw,
	}

	admin2Cert, _ := testutils.LoadTestClientCrypto(t, clientCryptoDir, "admin2")
	admin3Cert, _ := testutils.LoadTestClientCrypto(t, clientCryptoDir, "admin3")

	admin2 := &types.Admin{ID: "admin2", Certificate: admin2Cert.Raw}
	admin3 := &types.Admin{ID: "admin3", Certificate: admin3Cert.Raw}

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

	txID, err := tx1.Commit()
	require.NoError(t, err)
	require.NotNil(t, txID)

	require.Eventually(t, func() bool {
		// verify tx was successfully committed
		tx, err := session1.ConfigTx()
		require.NoError(t, err)
		clusterConfig, err := tx.GetClusterConfig()
		if err != nil || clusterConfig == nil {
			return false
		}
		if len(clusterConfig.Admins) != 3 {
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)

	// Remove an admin
	session2 := openUserSession(t, bcdb, "admin2", clientCryptoDir)
	tx2, err := session2.ConfigTx()
	require.NoError(t, err)
	err = tx2.DeleteAdmin(admin.ID)
	require.NoError(t, err)
	err = tx2.DeleteAdmin(admin.ID)
	require.EqualError(t, err, "admin does not exist in pending config: admin")
	err = tx2.DeleteAdmin("non-admin")
	require.EqualError(t, err, "admin does not exist in current config: non-admin")

	txID, err = tx2.Commit()
	require.NoError(t, err)
	require.NotNil(t, txID)

	require.Eventually(t, func() bool {
		// verify tx was successfully committed
		tx, err := session2.ConfigTx()
		require.NoError(t, err)
		clusterConfig, err := tx.GetClusterConfig()
		if err != nil || clusterConfig == nil {
			return false
		}
		if len(clusterConfig.Admins) != 2 {
			return false
		}
		found, index := AdminExists("admin2", clusterConfig.Admins)
		if !found {
			return false
		}
		if !bytes.Equal(clusterConfig.Admins[index].Certificate, admin2Cert.Raw) {
			return false
		}

		found, index = AdminExists("admin3", clusterConfig.Admins)
		if !found {
			return false
		}
		if !bytes.Equal(clusterConfig.Admins[index].Certificate, admin3Cert.Raw) {
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)

	// session1 by removed admin cannot execute additional transactions
	tx3, err := session1.ConfigTx()
	require.EqualError(t, err, "failed to obtain server's certificate")
	require.Nil(t, tx3)
}

func TestConfigTxContext_UpdateAdmin(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "admin2", "adminUpdated", "server"})
	testServer, err := setupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	admin2Cert, _ := testutils.LoadTestClientCrypto(t, clientCryptoDir, "admin2")
	adminUpdatedCert, _ := testutils.LoadTestClientCrypto(t, clientCryptoDir, "adminUpdated")

	admin2 := &types.Admin{ID: "admin2", Certificate: admin2Cert.Raw}

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session1 := openUserSession(t, bcdb, "admin", clientCryptoDir)

	// Add admin2
	tx1, err := session1.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	err = tx1.AddAdmin(admin2)
	require.NoError(t, err)

	txID, err := tx1.Commit()
	require.NoError(t, err)
	require.NotNil(t, txID)

	require.Eventually(t, func() bool {
		// verify tx was successfully committed
		tx, err := session1.ConfigTx()
		require.NoError(t, err)
		clusterConfig, err := tx.GetClusterConfig()
		if err != nil || clusterConfig == nil {
			return false
		}
		if len(clusterConfig.Admins) != 2 {
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)

	// Update an admin
	session2 := openUserSession(t, bcdb, "admin2", clientCryptoDir)
	tx2, err := session2.ConfigTx()
	require.NoError(t, err)
	err = tx2.UpdateAdmin(&types.Admin{ID: "admin", Certificate: adminUpdatedCert.Raw})
	require.NoError(t, err)
	err = tx2.UpdateAdmin(&types.Admin{ID: "non-admin", Certificate: []byte("bad-cert")})
	require.EqualError(t, err, "admin does not exist in current config: non-admin")

	txID, err = tx2.Commit()
	require.NoError(t, err)
	require.NotNil(t, txID)

	require.Eventually(t, func() bool {
		// verify tx was successfully committed
		tx, err := session2.ConfigTx()
		require.NoError(t, err)
		clusterConfig, err := tx.GetClusterConfig()
		if err != nil || clusterConfig == nil {
			return false
		}
		if len(clusterConfig.Admins) != 2 {
			return false
		}

		found, index := AdminExists("admin", clusterConfig.Admins)
		if !found {
			return false
		}
		if !bytes.Equal(clusterConfig.Admins[index].Certificate, adminUpdatedCert.Raw) {
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)

	// session1 by updated admin cannot execute additional transactions, need to recreate session
	tx3, err := session1.ConfigTx()
	require.EqualError(t, err, "failed to obtain server's certificate")
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

func TestConfigTxContext_AddClusterNode(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "server"})
	testServer, err := setupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session1 := openUserSession(t, bcdb, "admin", clientCryptoDir)
	tx, err := session1.ConfigTx()
	require.NoError(t, err)
	config, err := tx.GetClusterConfig()
	require.NoError(t, err)

	node2 := config.Nodes[0]
	node2.ID = "testNode2"
	node2.Port++
	err = tx.AddClusterNode(node2)
	require.NoError(t, err)

	txID, err := tx.Commit()
	require.NoError(t, err)
	require.NotNil(t, txID)

	require.Eventually(t, func() bool {
		// verify tx was successfully committed. "Get" works once per Tx.
		tx2, err := session1.ConfigTx()
		require.NoError(t, err)
		clusterConfig, err := tx2.GetClusterConfig()
		if err != nil || clusterConfig == nil {
			return false
		}
		if len(clusterConfig.Nodes) != 2 {
			return false
		}

		found, index := NodeExists("testNode2", clusterConfig.Nodes)
		if !found {
			return false
		}
		if clusterConfig.Nodes[index].Port != node2.Port {
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)

}

func TestConfigTxContext_DeleteClusterNode(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "server"})
	testServer, err := setupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session1 := openUserSession(t, bcdb, "admin", clientCryptoDir)
	tx1, err := session1.ConfigTx()
	require.NoError(t, err)
	config, err := tx1.GetClusterConfig()
	require.NoError(t, err)

	id1 := config.Nodes[0].ID
	node2 := config.Nodes[0]
	node2.ID = "testNode2"
	node2.Port++

	err = tx1.AddClusterNode(node2)
	require.NoError(t, err)
	err = tx1.DeleteClusterNode(id1)
	require.NoError(t, err)

	txID, err := tx1.Commit()
	require.NoError(t, err)
	require.NotNil(t, txID)

	require.Eventually(t, func() bool {
		// verify tx was successfully committed. "Get" works once per Tx.
		tx, err := session1.ConfigTx()
		require.NoError(t, err)
		clusterConfig, err := tx.GetClusterConfig()
		if err != nil || clusterConfig == nil {
			return false
		}
		if len(clusterConfig.Nodes) != 1 {
			return false
		}

		found, index := NodeExists("testNode2", clusterConfig.Nodes)
		if !found {
			return false
		}
		if clusterConfig.Nodes[index].Port != node2.Port {
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)
}

func TestConfigTxContext_UpdateClusterNode(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "server"})
	testServer, err := setupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session1 := openUserSession(t, bcdb, "admin", clientCryptoDir)
	tx1, err := session1.ConfigTx()
	require.NoError(t, err)
	config, err := tx1.GetClusterConfig()
	require.NoError(t, err)

	node1 := config.Nodes[0]
	node1.Port++
	err = tx1.UpdateClusterNode(node1)
	require.NoError(t, err)

	txID, err := tx1.Commit()
	require.NoError(t, err)
	require.NotNil(t, txID)

	require.Eventually(t, func() bool {
		// verify tx was successfully committed. "Get" works once per Tx.
		tx, err := session1.ConfigTx()
		require.NoError(t, err)
		clusterConfig, err := tx.GetClusterConfig()
		if err != nil || clusterConfig == nil {
			return false
		}
		if len(clusterConfig.Nodes) != 1 {
			return false
		}

		found, index := NodeExists("testNode1", clusterConfig.Nodes)
		if !found {
			return false
		}
		if clusterConfig.Nodes[index].Port != node1.Port {
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)
}

func TestConfigTxContext_Abort(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "server"})
	testServer, err := setupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session := openUserSession(t, bcdb, "admin", clientCryptoDir)

	tx, err := session.ConfigTx()
	require.NoError(t, err)

	_, err = tx.GetClusterConfig()
	require.NoError(t, err)

	err = tx.Abort()
	require.NoError(t, err)

	// after Abort() API calls return error
	_, err = tx.GetClusterConfig()
	require.EqualError(t, err, ErrTxSpent.Error())

	err = tx.AddClusterNode(&types.NodeConfig{})
	require.EqualError(t, err, ErrTxSpent.Error())
	err = tx.DeleteClusterNode("id")
	require.EqualError(t, err, ErrTxSpent.Error())
	err = tx.UpdateClusterNode(&types.NodeConfig{})
	require.EqualError(t, err, ErrTxSpent.Error())

	err = tx.AddAdmin(&types.Admin{})
	require.EqualError(t, err, ErrTxSpent.Error())
	err = tx.DeleteAdmin("id")
	require.EqualError(t, err, ErrTxSpent.Error())
	err = tx.UpdateAdmin(&types.Admin{})
	require.EqualError(t, err, ErrTxSpent.Error())
}

func TestConfigTxContext_Commit(t *testing.T) {
	clientCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "server"})
	testServer, err := setupTestServer(t, clientCryptoDir)
	defer func() {
		if testServer != nil {
			_ = testServer.Stop()
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	bcdb := createDBInstance(t, clientCryptoDir, serverPort)
	session := openUserSession(t, bcdb, "admin", clientCryptoDir)

	tx, err := session.ConfigTx()
	require.NoError(t, err)

	config, err := tx.GetClusterConfig()
	require.NoError(t, err)
	node1 := config.Nodes[0]
	node1.Port++
	err = tx.UpdateClusterNode(config.Nodes[0])
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// after Commit() API calls return error
	_, err = tx.GetClusterConfig()
	require.EqualError(t, err, ErrTxSpent.Error())

	err = tx.AddClusterNode(&types.NodeConfig{})
	require.EqualError(t, err, ErrTxSpent.Error())
	err = tx.DeleteClusterNode("id")
	require.EqualError(t, err, ErrTxSpent.Error())
	err = tx.UpdateClusterNode(&types.NodeConfig{})
	require.EqualError(t, err, ErrTxSpent.Error())

	err = tx.AddAdmin(&types.Admin{})
	require.EqualError(t, err, ErrTxSpent.Error())
	err = tx.DeleteAdmin("id")
	require.EqualError(t, err, ErrTxSpent.Error())
	err = tx.UpdateAdmin(&types.Admin{})
	require.EqualError(t, err, ErrTxSpent.Error())

	// TODO The server crashes if we close it when a tx is in the pipeline. See
	// https://github.ibm.com/blockchaindb/server/issues/282

	require.Eventually(t, func() bool {
		// verify tx was successfully committed. "Get" works once per Tx.
		tx, err := session.ConfigTx()
		require.NoError(t, err)
		clusterConfig, err := tx.GetClusterConfig()
		if err != nil || clusterConfig == nil {
			return false
		}
		if len(clusterConfig.Nodes) != 1 {
			return false
		}

		found, index := NodeExists("testNode1", clusterConfig.Nodes)
		if !found {
			return false
		}
		if clusterConfig.Nodes[index].Port != node1.Port {
			return false
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)
}
