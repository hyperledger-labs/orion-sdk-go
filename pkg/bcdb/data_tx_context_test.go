package bcdb

import (
	"bytes"
	"encoding/pem"
	"github.com/gogo/protobuf/proto"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/server"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestDataContext_PutAndGetKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, tempDir, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	addUser(t, "alice", adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeyAndValidate(t, "key1", "value1", "alice", userSession)
}

func TestDataContext_GetNonExistKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, tempDir, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	addUser(t, "alice", adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeyAndValidate(t, "key1", "value1", "alice", userSession)

	tx, err := userSession.DataTx("bdb")
	require.NoError(t, err)
	res, err := tx.Get("key2")
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestDataContext_MultipleUpdateForSameKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, tempDir, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	addUser(t, "alice", adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeyAndValidate(t, "key1", "value1", "alice", userSession)
	putKeyAndValidate(t, "key2", "value2", "alice", userSession)

	tx, err := userSession.DataTx("bdb")
	require.NoError(t, err)
	res1, err := tx.Get("key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), res1)

	res2, err := tx.Get("key2")
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), res2)

	err = tx.Put("key1", []byte("value3"), &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	})
	require.NoError(t, err)

	err = tx.Delete("key2")
	require.NoError(t, err)

	dataTx, ok := tx.(*dataTxContext)
	require.True(t, ok)
	_, key1WriteExist := dataTx.dataWrites["key1"]
	_, key2WriteExist := dataTx.dataWrites["key2"]
	_, key1DeleteExist := dataTx.dataDeletes["key1"]
	_, key2DeleteExist := dataTx.dataDeletes["key2"]
	require.True(t, key1WriteExist)
	require.False(t, key2WriteExist)
	require.False(t, key1DeleteExist)
	require.True(t, key2DeleteExist)

	err = tx.Put("key2", []byte("value4"), &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	})
	require.NoError(t, err)

	err = tx.Delete("key1")
	require.NoError(t, err)

	_, key1WriteExist = dataTx.dataWrites["key1"]
	_, key2WriteExist = dataTx.dataWrites["key2"]
	_, key1DeleteExist = dataTx.dataDeletes["key1"]
	_, key2DeleteExist = dataTx.dataDeletes["key2"]
	require.False(t, key1WriteExist)
	require.True(t, key2WriteExist)
	require.True(t, key1DeleteExist)
	require.False(t, key2DeleteExist)

	_, err = tx.Commit()
	require.NoError(t, err)

	// Start another tx to query and make sure
	// results was successfully committed
	tx, err = userSession.DataTx("bdb")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		val, err := tx.Get("key2")

		return err == nil && val != nil &&
			bytes.Equal(val, []byte("value4"))
	}, time.Minute, 200*time.Millisecond)

	res, err := tx.Get("key1")
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestDataContext_GetUserPermissions(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "bob"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, tempDir, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	addUser(t, "alice", adminSession, pemUserCert)
	aliceSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeyAndValidate(t, "key1", "value1", "alice", aliceSession)

	pemUserCert, err = ioutil.ReadFile(path.Join(clientCertTemDir, "bob.pem"))
	require.NoError(t, err)
	addUser(t, "bob", adminSession, pemUserCert)
	bobSession := openUserSession(t, bcdb, "bob", clientCertTemDir)
	tx, err := bobSession.DataTx("bdb")
	require.NoError(t, err)
	_, err = tx.Get("key1")
	require.Error(t, err)
	require.Contains(t, "error getting user's record, server returned 403 Forbidden", err.Error())
	err = tx.Abort()
	require.NoError(t, err)

	txUpdateUser, err := aliceSession.DataTx("bdb")
	require.NoError(t, err)
	err = txUpdateUser.Put("key1", []byte("value2"), &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true, "bob": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	})
	require.NoError(t, err)

	_, err = txUpdateUser.Commit()
	require.NoError(t, err)
	validateValue(t, "key1", "value2", aliceSession)

	tx, err = bobSession.DataTx("bdb")
	require.NoError(t, err)
	bobVal, err := tx.Get("key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("value2"), bobVal)
}

func connectAndOpenAdminSession(t *testing.T, testServer *server.BCDBHTTPServer, tempDir string, clientCertTempDir string) (BCDB, DBSession) {
	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstance(t, tempDir, serverPort)
	// New session with admin user context
	session := openUserSession(t, bcdb, "admin", clientCertTempDir)

	return bcdb, session
}

func addUser(t *testing.T, userName string, session DBSession, pemUserCert []byte) {
	tx, err := session.UsersTx()
	require.NoError(t, err)

	certBlock, _ := pem.Decode(pemUserCert)
	err = tx.PutUser(&types.User{
		ID:          userName,
		Certificate: certBlock.Bytes,
		Privilege: &types.Privilege{
			DBPermission: map[string]types.Privilege_Access{"bdb": 1},
		},
	}, nil)
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Start another session to query and make sure
	// results was successfully committed
	tx, err = session.UsersTx()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		alice, err := tx.GetUser(userName)

		return err == nil && alice != nil &&
			alice.ID == userName &&
			bytes.Equal(certBlock.Bytes, alice.Certificate)
	}, time.Minute, 200*time.Millisecond)
	err = tx.Abort()
	require.NoError(t, err)
}

func putKeyAndValidate(t *testing.T, key string, value string, user string, session DBSession) {
	putMultipleKeysAndValidate(t, []string{key}, []string{value}, user, session)
	return
}

func putMultipleKeysAndValidate(t *testing.T, key []string, value []string, user string, session DBSession) (txEnvelopes []proto.Message){
	// Creating new key
	for i := 0; i < len(key); i++ {
		tx, err := session.DataTx("bdb")
		require.NoError(t, err)

		err = tx.Put(key[i], []byte(value[i]), &types.AccessControl{
			ReadUsers:      map[string]bool{user: true},
			ReadWriteUsers: map[string]bool{user: true},
		})
		require.NoError(t, err)

		_, err = tx.Commit()
		require.NoError(t, err)
		txEnvelopes = append(txEnvelopes, tx.TxEnvelope())
	}

	// Start another tx to query and make sure
	// results was successfully committed
	tx, err := session.DataTx("bdb")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		val, err := tx.Get(key[len(key) - 1])

		return err == nil && val != nil &&
			bytes.Equal(val, []byte(value[len(key) - 1]))
	}, time.Minute, 200*time.Millisecond)
	return txEnvelopes
}


func validateValue(t *testing.T, key string, value string, session DBSession) {
	// Start another tx to query and make sure
	// results was successfully committed
	tx, err := session.DataTx("bdb")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		val, err := tx.Get(key)

		return err == nil && val != nil &&
			bytes.Equal(val, []byte(value))
	}, time.Minute, 200*time.Millisecond)
}
