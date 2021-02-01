package bcdb

import (
	"bytes"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/server"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestDataContext_PutAndGetKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	addUser(t, "alice", adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeyAndValidate(t, "key1", "value1", "alice", userSession)
}

func TestDataContext_GetNonExistKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	addUser(t, "alice", adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeyAndValidate(t, "key1", "value1", "alice", userSession)

	tx, err := userSession.DataTx("bdb")
	require.NoError(t, err)
	res, meta, err := tx.Get("key2")
	require.NoError(t, err)
	require.Nil(t, res)
	require.Nil(t, meta)
}

func TestDataContext_MultipleUpdateForSameKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	addUser(t, "alice", adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeyAndValidate(t, "key1", "value1", "alice", userSession)
	putKeyAndValidate(t, "key2", "value2", "alice", userSession)

	acl := &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	}
	tx, err := userSession.DataTx("bdb")
	require.NoError(t, err)
	res1, meta, err := tx.Get("key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), res1)
	require.True(t, proto.Equal(acl, meta.GetAccessControl()))

	res2, meta, err := tx.Get("key2")
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), res2)
	require.True(t, proto.Equal(acl, meta.GetAccessControl()))

	err = tx.Put("key1", []byte("value3"), acl)
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

	err = tx.Put("key2", []byte("value4"), acl)
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

	txID, err := tx.Commit()
	require.NoError(t, err)

	// Start another tx to query and make sure
	// results was successfully committed
	tx, err = userSession.DataTx("bdb")
	require.NoError(t, err)

	waitForTx(t, txID, userSession)

	res, _, err := tx.Get("key1")
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestDataContext_CommitAbortFinality(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	addUser(t, "alice", adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	acl := &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	}

	for i := 0; i < 2; i++ {
		tx, err := userSession.DataTx("bdb")
		err = tx.Put("key1", []byte("value1"), acl)
		require.NoError(t, err)

		assertFinalityOnCommitAbort(t, i == 0, tx)

		val, meta, err := tx.Get("key")
		require.EqualError(t, err, ErrTxSpent.Error())
		require.Nil(t, val)
		require.Nil(t, meta)

		err = tx.Put("key", []byte("value"), acl)
		require.EqualError(t, err, ErrTxSpent.Error())

		err = tx.Delete("key")
		require.EqualError(t, err, ErrTxSpent.Error())
	}
}

func assertFinalityOnCommitAbort(t *testing.T, commitOrAbort bool, tx TxContext) {
	var txID string
	var err error

	if commitOrAbort {
		txID, err = tx.Commit()
		require.NoError(t, err)
		require.True(t, len(txID) > 0)
	} else {
		err = tx.Abort()
		require.NoError(t, err)
	}

	// verify finality
	txID, err = tx.Commit()
	require.EqualError(t, err, ErrTxSpent.Error())
	require.True(t, len(txID) == 0)

	err = tx.Abort()
	require.EqualError(t, err, ErrTxSpent.Error())
}

func TestDataContext_MultipleGetForSameKeyInTxAndMVCCConflict(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	addUser(t, "alice", adminSession, pemUserCert)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeyAndValidate(t, "key1", "value1", "alice", userSession)

	tx, err := userSession.DataTx("bdb")
	require.NoError(t, err)
	res, meta, err := tx.Get("key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), res)
	storedRead, ok := tx.(*dataTxContext).dataReads["key1"]
	require.True(t, ok)
	require.Equal(t, res, storedRead.GetValue())
	require.Equal(t, meta, storedRead.GetMetadata())

	putKeyAndValidate(t, "key1", "value2", "alice", userSession)
	res, meta, err = tx.Get("key1")
	require.NoError(t, err)
	storedReadUpdated, ok := tx.(*dataTxContext).dataReads["key1"]
	require.True(t, ok)
	require.Equal(t, res, storedRead.GetValue())
	require.Equal(t, meta, storedRead.GetMetadata())
	require.Equal(t, storedReadUpdated, storedRead)
	require.NoError(t, err)
	txID, err := tx.Commit()
	waitForTx(t, txID, userSession)
	l, err := userSession.Ledger()
	require.NoError(t, err)
	r, err := l.GetTransactionReceipt(txID)
	require.NoError(t, err)
	require.Equal(t, r.GetHeader().GetValidationInfo()[int(r.GetTxIndex())].GetFlag(), types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE)
}

func TestDataContext_GetUserPermissions(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
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
	_, _, err = tx.Get("key1")
	require.Error(t, err)
	require.EqualError(t, err, "error handling request, server returned: status: 403 Forbidden, message: error while processing 'GET /data/bdb/key1' because the user [bob] has no permission to read key [key1] from database [bdb]")
	err = tx.Abort()
	require.NoError(t, err)

	txUpdateUser, err := aliceSession.DataTx("bdb")
	require.NoError(t, err)
	acl := &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true, "bob": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	}
	err = txUpdateUser.Put("key1", []byte("value2"), acl)
	require.NoError(t, err)

	txID, err := txUpdateUser.Commit()
	require.NoError(t, err)
	waitForTx(t, txID, aliceSession)
	validateValue(t, "key1", "value2", aliceSession)

	tx, err = bobSession.DataTx("bdb")
	require.NoError(t, err)
	bobVal, meta, err := tx.Get("key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("value2"), bobVal)
	require.True(t, proto.Equal(meta.GetAccessControl(), acl))
}

func connectAndOpenAdminSession(t *testing.T, testServer *server.BCDBHTTPServer, cryptoDir string) (BCDB, DBSession) {
	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstance(t, cryptoDir, serverPort)
	// New session with admin user context
	session := openUserSession(t, bcdb, "admin", cryptoDir)

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

func putMultipleKeysAndValidate(t *testing.T, key []string, value []string, user string, session DBSession) (txEnvelopes []proto.Message) {
	return putMultipleKeysAndValidateMultipleUsers(t, key, value, []string{user}, session)
}

func putMultipleKeysAndValidateMultipleUsers(t *testing.T, key []string, value []string, users []string, session DBSession) (txEnvelopes []proto.Message) {
	// Creating new key
	var txId string
	for i := 0; i < len(key); i++ {
		tx, err := session.DataTx("bdb")
		require.NoError(t, err)

		readUsers := make(map[string]bool)
		readWriteUsers := make(map[string]bool)
		for _, user := range users {
			readUsers[user] = true
			readWriteUsers[user] = true
		}
		err = tx.Put(key[i], []byte(value[i]), &types.AccessControl{
			ReadUsers:      readUsers,
			ReadWriteUsers: readWriteUsers,
		})
		require.NoError(t, err)

		txId, err = tx.Commit()
		require.NoError(t, err, fmt.Sprintf("Key = %s, value = %s", key[i], value[i]))
		txEnv, err := tx.TxEnvelope()
		require.NoError(t, err)
		txEnvelopes = append(txEnvelopes, txEnv)
	}

	waitForTx(t, txId, session)
	return txEnvelopes
}

func validateValue(t *testing.T, key string, value string, session DBSession) {
	// Start another tx to query and make sure
	// results was successfully committed
	tx, err := session.DataTx("bdb")
	require.NoError(t, err)
	val, _, err := tx.Get(key)
	require.NoError(t, err)
	require.Equal(t, val, []byte(value))
}

func waitForTx(t *testing.T, txID string, session DBSession) {
	l, err := session.Ledger()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		r, err := l.GetTransactionReceipt(txID)

		return err == nil && r != nil && r.GetHeader() != nil &&
			uint64(len(r.GetHeader().GetValidationInfo())) > r.GetTxIndex()
	}, time.Minute, 200*time.Millisecond)
}
