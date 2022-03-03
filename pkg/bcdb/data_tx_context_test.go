// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/server"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDataContext_PutAndGetKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeySync(t, "bdb", "key1", "value1", "alice", userSession)

	// Validate
	tx, err := userSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx)
	val, meta, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("value1"), val)
	require.NotNil(t, meta)
}

func TestDataContext_PutKey_WithTxID(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	tx, err := userSession.DataTx(WithTxID("external-TxID-1"))
	require.NoError(t, err)

	err = tx.Put("bdb", "key1", []byte("some-value"), nil)
	require.NoError(t, err)

	txID, receiptEnv, err := tx.Commit(true)
	require.NoError(t, err)
	require.Equal(t, "external-TxID-1", txID)
	require.NotNil(t, receiptEnv)

	// cannot reuse a TxID
	tx2, err := userSession.DataTx(WithTxID("external-TxID-1"))
	require.NoError(t, err)

	err = tx2.Put("bdb", "key1", []byte("some-other-value"), nil)
	require.NoError(t, err)

	txID2, receiptEnv2, err := tx2.Commit(true)
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: the transaction has a duplicate txID [external-TxID-1]")
	require.Equal(t, "external-TxID-1", txID2)
	require.Nil(t, receiptEnv2)

	// URL segment unsafe char `/`
	tx3, err := userSession.DataTx(WithTxID("external/TxID-1"))
	require.Nil(t, tx3)
	require.EqualError(t, err, "error while applying option: WithTxID: un-safe for a URL segment: \"external/TxID-1\"")

	tx4, err := userSession.DataTx(WithTxID(""))
	require.Nil(t, tx4)
	require.EqualError(t, err, "error while applying option: WithTxID: empty txID")
}

func TestDataContext_GetNonExistKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeySync(t, "bdb", "key1", "value1", "alice", userSession)

	tx, err := userSession.DataTx()
	require.NoError(t, err)
	res, meta, err := tx.Get("bdb", "key2")
	require.NoError(t, err)
	require.Nil(t, res)
	require.Nil(t, meta)
}

func TestDataContext_MultipleUpdateForSameKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	txDB, err := adminSession.DBsTx()
	require.NoError(t, err)

	err = txDB.CreateDB("testDB", nil)
	require.NoError(t, err)

	txId, receiptEnv, err := txDB.Commit(true)
	require.NoError(t, err)
	require.True(t, len(txId) > 0)
	require.NotNil(t, receiptEnv)

	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb":    1,
		"testDB": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)
	putKeySync(t, "bdb", "key1", "value1", "alice", userSession)
	putKeySync(t, "testDB", "key2", "value2", "alice", userSession)

	acl := &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	}
	tx, err := userSession.DataTx()
	require.NoError(t, err)
	res1, meta, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), res1)
	require.True(t, proto.Equal(acl, meta.GetAccessControl()))

	res2, meta, err := tx.Get("testDB", "key2")
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), res2)
	require.True(t, proto.Equal(acl, meta.GetAccessControl()))

	err = tx.Put("bdb", "key1", []byte("value3"), acl)
	require.NoError(t, err)

	err = tx.Delete("testDB", "key2")
	require.NoError(t, err)

	dataTx, ok := tx.(*dataTxContext)
	require.True(t, ok)
	_, key1WriteExist := dataTx.operations["bdb"].dataWrites["key1"]
	_, key2WriteExist := dataTx.operations["testDB"].dataWrites["key2"]
	_, key1DeleteExist := dataTx.operations["bdb"].dataDeletes["key1"]
	_, key2DeleteExist := dataTx.operations["testDB"].dataDeletes["key2"]
	require.True(t, key1WriteExist)
	require.False(t, key2WriteExist)
	require.False(t, key1DeleteExist)
	require.True(t, key2DeleteExist)

	err = tx.Put("testDB", "key2", []byte("value4"), acl)
	require.NoError(t, err)

	err = tx.Delete("bdb", "key1")
	require.NoError(t, err)

	_, key1WriteExist = dataTx.operations["bdb"].dataWrites["key1"]
	_, key2WriteExist = dataTx.operations["testDB"].dataWrites["key2"]
	_, key1DeleteExist = dataTx.operations["bdb"].dataDeletes["key1"]
	_, key2DeleteExist = dataTx.operations["testDB"].dataDeletes["key2"]
	require.False(t, key1WriteExist)
	require.True(t, key2WriteExist)
	require.True(t, key1DeleteExist)
	require.False(t, key2DeleteExist)

	txID, _, err := tx.Commit(false)
	require.NoError(t, err)

	// Start another tx to query and make sure
	// results was successfully committed
	tx, err = userSession.DataTx()
	require.NoError(t, err)

	waitForTx(t, txID, userSession)

	err = tx.Delete("testDB", "key2")
	require.NoError(t, err)

	res, _, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestDataContext_CommitAbortFinality(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	acl := &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	}

	for i := 0; i < 3; i++ {
		tx, err := userSession.DataTx()
		require.NoError(t, err)
		err = tx.Put("bdb", "key1", []byte("value1"), acl)
		require.NoError(t, err)

		assertTxFinality(t, TxFinality(i), tx, userSession)

		val, meta, err := tx.Get("bdb", "key")
		require.EqualError(t, err, ErrTxSpent.Error())
		require.Nil(t, val)
		require.Nil(t, meta)

		err = tx.Put("bdb", "key", []byte("value"), acl)
		require.EqualError(t, err, ErrTxSpent.Error())

		err = tx.Delete("bdb", "key")
		require.EqualError(t, err, ErrTxSpent.Error())

		if TxFinality(i) != TxFinalityAbort {
			tx, err := userSession.DataTx()
			require.NoError(t, err)
			val, meta, err := tx.Get("bdb", "key1")
			require.NoError(t, err)
			require.Equal(t, []byte("value1"), val)
			require.NotNil(t, meta)
		}
	}
}

func TestDataContext_MultipleGetForSameKeyInTxAndMVCCConflict(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeySync(t, "bdb", "key1", "value1", "alice", userSession)

	tx, err := userSession.DataTx()
	require.NoError(t, err)
	res, meta, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), res)
	storedRead, ok := tx.(*dataTxContext).operations["bdb"].dataReads["key1"]
	require.True(t, ok)
	require.Equal(t, res, storedRead.GetValue())
	require.Equal(t, meta, storedRead.GetMetadata())

	putKeySync(t, "bdb", "key1", "value2", "alice", userSession)
	res, meta, err = tx.Get("bdb", "key1")
	require.NoError(t, err)
	storedReadUpdated, ok := tx.(*dataTxContext).operations["bdb"].dataReads["key1"]
	require.True(t, ok)
	require.Equal(t, res, storedRead.GetValue())
	require.Equal(t, meta, storedRead.GetMetadata())
	require.Equal(t, storedReadUpdated, storedRead)
	require.NoError(t, err)
	txID, receiptEnv, err := tx.Commit(true)
	require.Error(t, err)
	require.NotNil(t, receiptEnv)
	receipt := receiptEnv.GetResponse().GetReceipt()
	require.Equal(t, types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, receipt.GetHeader().GetValidationInfo()[int(receipt.GetTxIndex())].GetFlag())
	require.Equal(t, "mvcc conflict has occurred as the committed state for the key [key1] in database [bdb] changed", receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()].GetReasonIfInvalid())
	require.Equal(t, "transaction txID = "+txID+" is not valid, flag: INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,"+
		" reason: mvcc conflict has occurred as the committed state for the key [key1] in database [bdb] changed", err.Error())
}

func TestDataContext_GetUserPermissions(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	aliceSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeySync(t, "bdb", "key1", "value1", "alice", aliceSession)

	pemUserCert, err = ioutil.ReadFile(path.Join(clientCertTemDir, "bob.pem"))
	require.NoError(t, err)
	addUser(t, "bob", adminSession, pemUserCert, dbPerm)
	bobSession := openUserSession(t, bcdb, "bob", clientCertTemDir)
	tx, err := bobSession.DataTx()
	require.NoError(t, err)
	_, _, err = tx.Get("bdb", "key1")
	require.Error(t, err)
	require.EqualError(t, err, "error handling request, server returned: status: 403 Forbidden, status code: 403, message: error while processing 'GET /data/bdb/key1' because the user [bob] has no permission to read key [key1] from database [bdb]")
	err = tx.Abort()
	require.NoError(t, err)

	txUpdateUser, err := aliceSession.DataTx()
	require.NoError(t, err)
	acl := &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true, "bob": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	}
	err = txUpdateUser.Put("bdb", "key1", []byte("value2"), acl)
	require.NoError(t, err)

	txID, _, err := txUpdateUser.Commit(false)
	require.NoError(t, err)
	waitForTx(t, txID, aliceSession)
	validateValue(t, "key1", "value2", aliceSession)

	tx, err = bobSession.DataTx()
	require.NoError(t, err)
	bobVal, meta, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("value2"), bobVal)
	require.True(t, proto.Equal(meta.GetAccessControl(), acl))
}

func TestDataContext_GetTimeout(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	sessionNoTimeout := openUserSession(t, bcdb, "alice", clientCertTemDir)
	sessionOneNanoTimeout := openUserSessionWithQueryTimeout(t, bcdb, "alice", clientCertTemDir, time.Nanosecond, false)
	sessionTenSecondTimeout := openUserSessionWithQueryTimeout(t, bcdb, "alice", clientCertTemDir, time.Second*10, false)

	putKeySync(t, "bdb", "key1", "value1", "alice", sessionNoTimeout)

	tx1, err := sessionOneNanoTimeout.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	val, _, err := tx1.Get("bdb", "key1")
	require.Error(t, err)
	require.Nil(t, val)
	require.Contains(t, err.Error(), "queryTimeout error")

	tx2, err := sessionTenSecondTimeout.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx2)
	val, _, err = tx2.Get("bdb", "key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("value1"), val)
}

func TestDataContext_AssertRead(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)
	_, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)

	tx1, err := adminSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)

	err = tx1.Put("bdb", "interestRate", []byte("0.01"), nil)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		key := "account" + strconv.Itoa(i)
		err = tx1.Put("bdb", key, []byte(strconv.Itoa(i)), nil)
		require.NoError(t, err)
	}
	_, _, err = tx1.Commit(true)
	require.NoError(t, err)

	tx2, err := adminSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx2)

	interestRateVal, metaData, err := tx2.Get("bdb", "interestRate")
	require.NoError(t, err)
	require.NotNil(t, metaData)
	version := metaData.GetVersion()

	_, _, err = tx2.Commit(true)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		tx3, err := adminSession.DataTx()
		require.NoError(t, err)
		require.NotNil(t, tx3)

		err = tx3.AssertRead("bdb", "interestRate", version)
		require.NoError(t, err)

		key := "account" + strconv.Itoa(i)
		accountVal, _, err := tx3.Get("bdb", key)
		require.NoError(t, err)
		accountValInt, _ := strconv.Atoi(string(accountVal))
		interestRateValInt, _ := strconv.Atoi(string(interestRateVal))
		err = tx3.Put("bdb", key, []byte(strconv.Itoa(accountValInt*(1+interestRateValInt))), nil)
		require.NoError(t, err)

		_, _, err = tx3.Commit(true)
		require.NoError(t, err)
	}
}

func TestDataContext_AssertReadOnZeroVersion(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)
	_, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)

	tx1, err := adminSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)

	err = tx1.AssertRead("bdb", "key1", nil)
	require.NoError(t, err)

	_, _, err = tx1.Commit(true)
	require.NoError(t, err) // committed successfully because 'key1' doesn't exist in the database => 'key1' version is nil

	tx2, err := adminSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx2)

	err = tx2.Put("bdb", "key1", []byte("val1"), nil)
	require.NoError(t, err)

	_, _, err = tx2.Commit(true)
	require.NoError(t, err)

	tx3, err := adminSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx3)

	err = tx3.AssertRead("bdb", "key1", nil)
	require.NoError(t, err)

	txID, receiptEnv, err := tx3.Commit(true)
	require.Error(t, err) // commit failed because 'key1' exists in the database => 'key1' version is not nil
	require.NotNil(t, receiptEnv)
	receipt := receiptEnv.GetResponse().GetReceipt()
	require.Equal(t, types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, receipt.GetHeader().GetValidationInfo()[int(receipt.GetTxIndex())].GetFlag())
	require.Equal(t, "mvcc conflict has occurred as the committed state for the key [key1] in database [bdb] changed", receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()].GetReasonIfInvalid())
	require.Equal(t, "transaction txID = "+txID+" is not valid, flag: INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,"+
		" reason: mvcc conflict has occurred as the committed state for the key [key1] in database [bdb] changed", err.Error())
}

func TestDataContext_AssertReadIncorrectVersionAndMVCCConflict(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)
	_, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)

	tx1, err := adminSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)

	err = tx1.Put("bdb", "key1", []byte("val1"), nil)
	require.NoError(t, err)

	_, _, err = tx1.Commit(true)
	require.NoError(t, err)

	tx2, err := adminSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx2)

	_, metaData, err := tx2.Get("bdb", "key1")
	require.NoError(t, err)
	require.NotNil(t, metaData)
	version := metaData.GetVersion()

	_, _, err = tx2.Commit(true)
	require.NoError(t, err)

	tx3, err := adminSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx3)

	incorrectVersion := types.Version{BlockNum: version.GetBlockNum() + 1, TxNum: version.GetTxNum() + 1}
	err = tx3.AssertRead("bdb", "key1", &incorrectVersion)
	require.NoError(t, err)

	txID, receiptEnv, err := tx3.Commit(true)
	require.Error(t, err)
	require.NotNil(t, receiptEnv)
	receipt := receiptEnv.GetResponse().GetReceipt()
	require.Equal(t, types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, receipt.GetHeader().GetValidationInfo()[int(receipt.GetTxIndex())].GetFlag())
	require.Equal(t, "mvcc conflict has occurred as the committed state for the key [key1] in database [bdb] changed", receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()].GetReasonIfInvalid())
	require.Equal(t, "transaction txID = "+txID+" is not valid, flag: INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,"+
		" reason: mvcc conflict has occurred as the committed state for the key [key1] in database [bdb] changed", err.Error())
}

func TestDataContext_GetAndAssertReadForTheSameKeyInTx(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)
	_, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)

	tx1, err := adminSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)

	_, _, err = tx1.Get("bdb", "key1")
	require.NoError(t, err)
	err = tx1.AssertRead("bdb", "key1", nil)
	require.NotNil(t, err)
	require.Equal(t, "can not execute Get and AssertRead for the same key 'key1' in the same transaction", err.Error())

	_, _, err = tx1.Commit(true)
	require.NoError(t, err)
}

func TestDataContext_ConstructEnvelopeForMultiSign(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeySync(t, "bdb", "key1", "value1", "alice", userSession)

	tx, err := userSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx)

	val, meta, err := tx.Get("bdb", "key1")
	require.EqualValues(t, []byte("value1"), val)
	require.NoError(t, err)

	val, meta, err = tx.Get("bdb", "key2")
	require.NoError(t, err)
	require.Nil(t, meta)
	require.Nil(t, val)

	require.NoError(t, tx.Put("bdb", "key2", []byte("value2"), nil))
	require.NoError(t, tx.Put("bdb", "key3", []byte("value3"), nil))
	require.NoError(t, tx.Delete("bdb", "key4"))
	require.NoError(t, tx.Put("cde", "key1", []byte("value1"), nil))
	require.NoError(t, tx.Put("cde", "key2", []byte("value2"), nil))
	require.NoError(t, tx.Delete("cde", "key3"))

	tx.AddMustSignUser("user1")
	tx.AddMustSignUser("user2")

	txEnv, err := tx.SignConstructedTxEnvelopeAndCloseTx()
	require.NoError(t, err)
	require.NotNil(t, txEnv)
	require.Error(t, tx.Delete("fgh", "key1"))
	dataTxEnv := txEnv.(*types.DataTxEnvelope)

	require.ElementsMatch(t, []string{"alice", "user1", "user2"}, dataTxEnv.Payload.MustSignUserIds)
	require.NotEmpty(t, dataTxEnv.Payload.TxId)
	require.NotEmpty(t, dataTxEnv.Payload.DbOperations)
	expectedOps := []*types.DBOperation{
		{
			DbName: "bdb",
			DataReads: []*types.DataRead{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 3,
						TxNum:    0,
					},
				},
				{
					Key:     "key2",
					Version: nil,
				},
			},
			DataWrites: []*types.DataWrite{
				{
					Key:   "key2",
					Value: []byte("value2"),
				},
				{
					Key:   "key3",
					Value: []byte("value3"),
				},
			},
			DataDeletes: []*types.DataDelete{
				{
					Key: "key4",
				},
			},
		},
		{
			DbName: "cde",
			DataWrites: []*types.DataWrite{
				{
					Key:   "key1",
					Value: []byte("value1"),
				},
				{
					Key:   "key2",
					Value: []byte("value2"),
				},
			},
			DataDeletes: []*types.DataDelete{
				{
					Key: "key3",
				},
			},
		},
	}

	require.Len(t, dataTxEnv.Payload.DbOperations, len(expectedOps))
	for _, expectedDBOps := range expectedOps {
		for _, actualDBOps := range dataTxEnv.Payload.DbOperations {
			if actualDBOps.DbName == expectedDBOps.DbName {
				require.ElementsMatch(t, expectedDBOps.DataReads, actualDBOps.DataReads)
				require.ElementsMatch(t, expectedDBOps.DataWrites, actualDBOps.DataWrites)
				require.ElementsMatch(t, expectedDBOps.DataDeletes, actualDBOps.DataDeletes)
			}
		}
	}
	require.Len(t, dataTxEnv.Signatures, 1)
	require.NotNil(t, dataTxEnv.Signatures["alice"])
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

func addUser(t *testing.T, userName string, session DBSession, pemUserCert []byte, dbPerm map[string]types.Privilege_Access) {
	tx, err := session.UsersTx()
	require.NoError(t, err)

	certBlock, _ := pem.Decode(pemUserCert)
	err = tx.PutUser(&types.User{
		Id:          userName,
		Certificate: certBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: dbPerm,
		},
	}, nil)
	require.NoError(t, err)
	_, receiptEnv, err := tx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, receiptEnv)

	tx, err = session.UsersTx()
	require.NoError(t, err)
	user, err := tx.GetUser(userName)
	require.NoError(t, err)
	require.Equal(t, userName, user.GetId())
}

func createDB(t *testing.T, dbName string, session DBSession) {
	tx, err := session.DBsTx()
	require.NoError(t, err)

	err = tx.CreateDB(dbName, nil)
	require.NoError(t, err)

	txId, receiptEnv, err := tx.Commit(true)
	require.NoError(t, err)
	require.True(t, len(txId) > 0)
	require.NotNil(t, receiptEnv)
	receipt := receiptEnv.GetResponse().GetReceipt()
	require.True(t, len(receipt.GetHeader().GetValidationInfo()) > 0)
	require.True(t, receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()].Flag == types.Flag_VALID)

	// Check database status, whenever created or not
	tx, err = session.DBsTx()
	require.NoError(t, err)
	exist, err := tx.Exists(dbName)
	require.NoError(t, err)
	require.True(t, exist)
}

func putKeySync(t *testing.T, dbName, key string, value string, user string, session DBSession) (*types.TxReceipt, string, proto.Message) {
	tx, err := session.DataTx()
	require.NoError(t, err)

	readUsers := make(map[string]bool)
	readWriteUsers := make(map[string]bool)

	readUsers[user] = true
	readWriteUsers[user] = true

	err = tx.Put(dbName, key, []byte(value), &types.AccessControl{
		ReadUsers:      readUsers,
		ReadWriteUsers: readWriteUsers,
	})
	require.NoError(t, err)

	txID, receiptEnv, err := tx.Commit(true)
	require.NoError(t, err, fmt.Sprintf("Key = %s, value = %s", key, value))
	require.NotNil(t, txID)
	require.NotNil(t, receiptEnv)
	env, err := tx.CommittedTxEnvelope()
	require.NoError(t, err)
	return receiptEnv.GetResponse().GetReceipt(), txID, env
}

func putMultipleKeysAndValues(t *testing.T, key []string, value []string, user string, session DBSession) (txEnvelopes []proto.Message) {
	return putMultipleKeysAndValidateMultipleUsers(t, key, value, []string{user}, session)
}

func putMultipleKeysAndValidateMultipleUsers(t *testing.T, key []string, value []string, users []string, session DBSession) (txEnvelopes []proto.Message) {
	// Creating new key
	var txId string
	for i := 0; i < len(key); i++ {
		tx, err := session.DataTx()
		require.NoError(t, err)

		readUsers := make(map[string]bool)
		readWriteUsers := make(map[string]bool)
		for _, user := range users {
			readUsers[user] = true
			readWriteUsers[user] = true
		}
		err = tx.Put("bdb", key[i], []byte(value[i]), &types.AccessControl{
			ReadUsers:      readUsers,
			ReadWriteUsers: readWriteUsers,
		})
		require.NoError(t, err)

		txId, _, err = tx.Commit(false)
		require.NoError(t, err, fmt.Sprintf("Key = %s, value = %s", key[i], value[i]))
		txEnv, err := tx.CommittedTxEnvelope()
		require.NoError(t, err)
		txEnvelopes = append(txEnvelopes, txEnv)
	}

	waitForTx(t, txId, session)
	return txEnvelopes
}

func validateValue(t *testing.T, key string, value string, session DBSession) {
	// Start another tx to query and make sure
	// results was successfully committed
	tx, err := session.DataTx()
	require.NoError(t, err)
	val, _, err := tx.Get("bdb", key)
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
