// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLoadedDataContext_CommitConstructedMultiSign(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	createDB(t, "db1", adminSession)
	createDB(t, "db2", adminSession)
	aliceCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"db1": 1,
		"db2": 1,
	}
	addUser(t, "alice", adminSession, aliceCert, dbPerm)

	bobCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "bob.pem"))
	require.NoError(t, err)
	addUser(t, "bob", adminSession, bobCert, dbPerm)

	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)
	putKeySync(t, "db1", "key3", "value3", "alice", userSession)
	putKeySync(t, "db1", "key4", "value4", "alice", userSession)
	putKeySync(t, "db2", "key3", "value3", "alice", userSession)
	putKeySync(t, "db2", "key4", "value4", "alice", userSession)

	tx, err := userSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx)
	for _, dbName := range []string{"db1", "db2"} {
		for _, keyPostfix := range []string{"3", "4"} {
			val, meta, err := tx.Get(dbName, "key"+keyPostfix)
			require.NoError(t, err)
			require.NotNil(t, meta)
			require.Equal(t, []byte("value"+keyPostfix), val)
		}
	}
	require.NoError(t, tx.Abort())

	tx, err = userSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx)

	require.NoError(t, tx.AssertRead("db1", "key1", nil))
	require.NoError(t, tx.AssertRead("db1", "key2", nil))
	require.NoError(t, tx.AssertRead("db2", "key1", nil))
	require.NoError(t, tx.AssertRead("db2", "key2", nil))
	require.NoError(t, tx.Put("db1", "key1", []byte("value1"), nil))
	require.NoError(t, tx.Put("db1", "key2", []byte("value2"), nil))
	require.NoError(t, tx.Put("db2", "key1", []byte("value1"), nil))
	require.NoError(t, tx.Put("db2", "key2", []byte("value2"), nil))
	require.NoError(t, tx.Delete("db1", "key3"))
	require.NoError(t, tx.Delete("db1", "key4"))
	require.NoError(t, tx.Delete("db2", "key3"))
	require.NoError(t, tx.Delete("db2", "key4"))

	tx.AddMustSignUser("bob")

	txEnv, err := tx.SignConstructedTxEnvelopeAndCloseTx()
	require.NoError(t, err)
	require.NotNil(t, txEnv)
	require.Len(t, txEnv.(*types.DataTxEnvelope).Signatures, 1)

	t.Run("commit constructed envelope after signing", func(t *testing.T) {
		txEnv := proto.Clone(txEnv)
		userSession = openUserSession(t, bcdb, "bob", clientCertTemDir)

		dataTxEnv := txEnv.(*types.DataTxEnvelope)
		loadedTx, err := userSession.LoadDataTx(dataTxEnv)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"alice", "bob"}, loadedTx.MustSignUsers())
		require.ElementsMatch(t, []string{"alice"}, loadedTx.SignedUsers())

		txID, receiptEnv, err := loadedTx.Commit(true)
		require.NoError(t, err)
		require.NotNil(t, receiptEnv)
		require.Equal(t, dataTxEnv.Payload.TxId, txID)

		tx, err = userSession.DataTx()
		require.NoError(t, err)

		for _, dbName := range []string{"db1", "db2"} {
			for _, keyPostfix := range []string{"3", "4"} {
				val, meta, err := tx.Get(dbName, "key"+keyPostfix)
				require.NoError(t, err)
				require.Nil(t, val)
				require.Nil(t, meta)
			}
		}
		for _, dbName := range []string{"db1", "db2"} {
			for _, keyPostfix := range []string{"1", "2"} {
				val, meta, err := tx.Get(dbName, "key"+keyPostfix)
				require.NoError(t, err)
				require.NotNil(t, meta)
				require.Equal(t, []byte("value"+keyPostfix), val)
			}
		}

		require.NoError(t, tx.Abort())
	})

	t.Run("fetch constructed envelope after signing", func(t *testing.T) {
		txEnv := proto.Clone(txEnv)
		userSession = openUserSession(t, bcdb, "bob", clientCertTemDir)

		dataTxEnv := txEnv.(*types.DataTxEnvelope)
		loadedTx, err := userSession.LoadDataTx(dataTxEnv)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"alice", "bob"}, loadedTx.MustSignUsers())
		require.ElementsMatch(t, []string{"alice"}, loadedTx.SignedUsers())

		reads := loadedTx.Reads()
		expectedReads := map[string][]*types.DataRead{
			"db1": {
				{
					Key:     "key1",
					Version: nil,
				},
				{
					Key:     "key2",
					Version: nil,
				},
			},
			"db2": {
				{
					Key:     "key1",
					Version: nil,
				},
				{
					Key:     "key2",
					Version: nil,
				},
			},
		}
		require.Len(t, reads, len(expectedReads))
		for dbName := range expectedReads {
			require.ElementsMatch(t, expectedReads[dbName], reads[dbName])
		}

		writes := loadedTx.Writes()
		expectedWrites := map[string][]*types.DataWrite{
			"db1": {
				{
					Key:   "key1",
					Value: []byte("value1"),
				},
				{
					Key:   "key2",
					Value: []byte("value2"),
				},
			},
			"db2": {
				{
					Key:   "key1",
					Value: []byte("value1"),
				},
				{
					Key:   "key2",
					Value: []byte("value2"),
				},
			},
		}
		require.Len(t, writes, len(expectedWrites))
		for dbName := range expectedWrites {
			require.ElementsMatch(t, expectedWrites[dbName], writes[dbName])
		}

		deletes := loadedTx.Deletes()
		expectedDeletes := map[string][]*types.DataDelete{
			"db1": {
				{
					Key: "key3",
				},
				{
					Key: "key4",
				},
			},
			"db2": {
				{
					Key: "key3",
				},
				{
					Key: "key4",
				},
			},
		}
		require.Len(t, deletes, len(expectedDeletes))
		for dbName := range expectedDeletes {
			require.ElementsMatch(t, expectedDeletes[dbName], deletes[dbName])
		}

		newTxEnv, err := loadedTx.CoSignTxEnvelopeAndCloseTx()
		require.NoError(t, err)
		require.NotNil(t, newTxEnv)
		newDataTxEnv := newTxEnv.(*types.DataTxEnvelope)
		require.Equal(t, dataTxEnv.Payload, newDataTxEnv.Payload)
		require.Len(t, newDataTxEnv.Signatures, 2)
		bobSign, ok := newDataTxEnv.Signatures["bob"]
		require.True(t, ok)
		require.NotNil(t, bobSign)
	})
}

func TestLoadedDataContext_ErrorCase(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	bcdb, _ := connectAndOpenAdminSession(t, testServer, clientCertTemDir)

	userSession := openUserSession(t, bcdb, "admin", clientCertTemDir)

	loadedTxCtx, err := userSession.LoadDataTx(nil)
	require.EqualError(t, err, "transaction envelope is nil")
	require.Nil(t, loadedTxCtx)

	loadedTxCtx, err = userSession.LoadDataTx(
		&types.DataTxEnvelope{
			Payload: nil,
		},
	)
	require.EqualError(t, err, "payload in the transaction envelope is nil")
	require.Nil(t, loadedTxCtx)

	loadedTxCtx, err = userSession.LoadDataTx(
		&types.DataTxEnvelope{
			Payload: &types.DataTx{
				MustSignUserIds: []string{"alice"},
			},
		},
	)
	require.EqualError(t, err, "transaction envelope does not have a signature")
	require.Nil(t, loadedTxCtx)

	loadedTxCtx, err = userSession.LoadDataTx(
		&types.DataTxEnvelope{
			Payload: &types.DataTx{
				MustSignUserIds: []string{"alice"},
			},
			Signatures: map[string][]byte{
				"alice": []byte("sign"),
			},
		},
	)
	require.EqualError(t, err, "transaction ID in the transaction envelope is empty")
	require.Nil(t, loadedTxCtx)

	loadedTxCtx, err = userSession.LoadDataTx(
		&types.DataTxEnvelope{
			Payload: &types.DataTx{
				MustSignUserIds: []string{},
				TxId:            "tx1",
			},
			Signatures: map[string][]byte{
				"alice": []byte("sign"),
			},
		},
	)
	require.EqualError(t, err, "no user ID in the transaction envelope")
	require.Nil(t, loadedTxCtx)
}
