package database

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/sdk/pkg/cryptoprovider"
	server "github.ibm.com/blockchaindb/sdk/pkg/database/mock"
	"github.ibm.com/blockchaindb/server/api"
)

func TestOpen(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)
	require.Equal(t, len(db.(*blockchainDB).connections), 1)
	require.Equal(t, len(dbConnections), 1)
	require.False(t, db.(*blockchainDB).isClosed)
	require.EqualValues(t, db.(*blockchainDB).userID, options.User.UserID, "user ids are not equal")
	val, err := db.Get("key1")
	require.NotNil(t, val)
	require.Nil(t, err)

	db, err = Open("testDB2", options)
	require.Nil(t, db)
	require.Error(t, err)

	options.ConnectionOptions[0].URL = fmt.Sprintf("http://localhost:%d/", 1999)
	db, err = Open("testDB", options)
	require.Nil(t, db)
	require.Error(t, err)

	invalidOpt := createOptions()
	invalidOpt.User.CAFilePath = "nonexist.crt"
	db, err = Open("testDb", invalidOpt)
	require.Nil(t, db)
	require.Error(t, err)
}

func TestBlockchainDB_Close(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)
	err = db.Close()
	require.NoError(t, err)
	_, err = db.Get("key1")
	require.Contains(t, err.Error(), "closed")
}

func TestBlockchainDB_Get(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	key1res, err := db.Get("key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("Testvalue11"), key1res)

	key2res, err := db.Get("key2")
	require.NoError(t, err)
	require.EqualValues(t, []byte("Testvalue21"), key2res)
}

func TestBlockchainDB_Begin(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &config.TxOptions{
		TxIsolation:   config.RepeatableRead,
		ReadOptions:   &config.ReadOptions{QuorumSize: 1},
		CommitOptions: &config.CommitOptions{QuorumSize: 1},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)
	require.NotNil(t, txCtx)
	require.NotNil(t, txCtx.(*transactionContext).txID)
	require.NotNil(t, txCtx.(*transactionContext).db)
	require.EqualValues(t, txCtx.(*transactionContext).db, db)
	found := false
	for k, _ := range db.(*blockchainDB).openTx {
		if strings.Compare(k, hex.EncodeToString(txCtx.(*transactionContext).txID)) == 0 {
			found = true
			break
		}
	}
	require.True(t, found, "transaction context not found")

	db.Close()
	require.NoError(t, err)
	require.Empty(t, db.(*blockchainDB).openTx)
	_, err = db.Begin(txOptions)
	require.Contains(t, err.Error(), "closed")
}

func TestTxContext_Get(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &config.TxOptions{
		TxIsolation:   config.RepeatableRead,
		ReadOptions:   &config.ReadOptions{QuorumSize: 1},
		CommitOptions: &config.CommitOptions{QuorumSize: 1},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)

	key1res, err := txCtx.Get("key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("Testvalue11"), key1res)

	key2res, err := txCtx.Get("key2")
	require.NoError(t, err)
	require.EqualValues(t, []byte("Testvalue21"), key2res)

	stmt1 := &api.Statement{
		Operation: "GET",
		Arguments: make([][]byte, 0),
	}
	stmt1.Arguments = append(stmt1.Arguments, []byte("key1"))
	stmt2 := &api.Statement{
		Operation: "GET",
		Arguments: make([][]byte, 0),
	}
	stmt2.Arguments = append(stmt2.Arguments, []byte("key2"))

	require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt1)
	require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt2)

	rset1 := &api.KVRead{
		Key: "key1",
		Version: &api.Version{
			BlockNum: 0,
			TxNum:    0,
		},
	}
	rset2 := &api.KVRead{
		Key: "key2",
		Version: &api.Version{
			BlockNum: 0,
			TxNum:    1,
		},
	}
	require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.rset["key1"], rset1))
	require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.rset["key2"], rset2))

	db.Close()
	require.NoError(t, err)
	_, err = txCtx.Get("key2")
	require.Contains(t, err.Error(), "valid")
}

func TestTxContext_PutDelete(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &config.TxOptions{
		TxIsolation:   config.Serializable,
		ReadOptions:   &config.ReadOptions{QuorumSize: 1},
		CommitOptions: &config.CommitOptions{QuorumSize: 1},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)

	err = txCtx.Put("key3", []byte("Testvalue31"))
	require.NoError(t, err)
	require.Empty(t, txCtx.(*transactionContext).rwset.rset)

	err = txCtx.Put("key4", []byte("Testvalue41"))
	require.NoError(t, err)
	require.Empty(t, txCtx.(*transactionContext).rwset.rset)

	stmt1 := &api.Statement{
		Operation: "PUT",
		Arguments: make([][]byte, 0),
	}
	stmt1.Arguments = append(stmt1.Arguments, []byte("key3"), []byte("Testvalue31"))
	stmt2 := &api.Statement{
		Operation: "PUT",
		Arguments: make([][]byte, 0),
	}
	stmt2.Arguments = append(stmt2.Arguments, []byte("key4"), []byte("Testvalue41"))
	require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt1)
	require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt2)

	require.Contains(t, txCtx.(*transactionContext).rwset.wset, "key3")
	require.Contains(t, txCtx.(*transactionContext).rwset.wset, "key4")

	require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.wset["key3"], &api.KVWrite{
		Key:      "key3",
		IsDelete: false,
		Value:    []byte("Testvalue31"),
	}))

	require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.wset["key4"], &api.KVWrite{
		Key:      "key4",
		IsDelete: false,
		Value:    []byte("Testvalue41"),
	}))

	err = txCtx.Delete("key3")
	require.NoError(t, err)
	require.Empty(t, txCtx.(*transactionContext).rwset.rset)
	stmt3 := &api.Statement{
		Operation: "DELETE",
		Arguments: make([][]byte, 0),
	}
	stmt3.Arguments = append(stmt3.Arguments, []byte("key3"))
	require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt3)
	require.Contains(t, txCtx.(*transactionContext).rwset.wset, "key3")
	require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.wset["key3"], &api.KVWrite{
		Key:      "key3",
		IsDelete: true,
		Value:    nil,
	}))

	db.Close()
	require.NoError(t, err)
	err = txCtx.Put("key2", []byte("Val2"))
	require.Contains(t, err.Error(), "valid")
	err = txCtx.Delete("key2")
	require.Contains(t, err.Error(), "valid")

}

func TestTxContext_Commit(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &config.TxOptions{
		TxIsolation:   config.Serializable,
		ReadOptions:   &config.ReadOptions{QuorumSize: 1},
		CommitOptions: &config.CommitOptions{QuorumSize: 1},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)

	key1res, err := txCtx.Get("key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("Testvalue11"), key1res)

	key2res, err := txCtx.Get("key2")
	require.NoError(t, err)
	require.EqualValues(t, []byte("Testvalue21"), key2res)

	err = txCtx.Put("key3", []byte("Testvalue31"))
	require.NoError(t, err)

	err = txCtx.Put("key4", []byte("Testvalue41"))
	require.NoError(t, err)

	err = txCtx.Delete("key3")
	require.NoError(t, err)

	_, err = txCtx.Commit()
	require.NoError(t, err)

	_, err = txCtx.Commit()
	require.Contains(t, err.Error(), "valid")
}

func TestTxContext_Abort(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &config.TxOptions{
		TxIsolation:   config.RepeatableRead,
		ReadOptions:   &config.ReadOptions{QuorumSize: 1},
		CommitOptions: &config.CommitOptions{QuorumSize: 1},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)

	err = txCtx.Abort()
	require.NoError(t, err)

	err = txCtx.Abort()
	require.Error(t, err)
	require.Contains(t, err.Error(), "valid")
}

func TestTxContext_TxIsolation_DifferentReads(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &config.TxOptions{
		TxIsolation:   config.Serializable,
		ReadOptions:   &config.ReadOptions{QuorumSize: 1},
		CommitOptions: &config.CommitOptions{QuorumSize: 1},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)

	key1res, err := txCtx.Get("key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("Testvalue11"), key1res)

	key1res, err = txCtx.Get("key1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "KeyFilePath value version changed during tx")
}

func TestTxContext_TxIsolation_DirtyReads(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &config.TxOptions{
		TxIsolation:   config.Serializable,
		ReadOptions:   &config.ReadOptions{QuorumSize: 1},
		CommitOptions: &config.CommitOptions{QuorumSize: 1},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)

	err = txCtx.Put("key1", []byte("NewValue"))
	require.NoError(t, err)

	_, err = txCtx.Get("key1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "KeyFilePath value already changed inside this tx")
}

func TestTxContext_GetMultipleValues(t *testing.T) {
	server.StartTestServer()
	defer StopTestServer()
	options := createOptions()
	// Connect twice to same Server, as it another Server
	options.ConnectionOptions = append(options.ConnectionOptions, &config.ConnectionOption{
		URL: "http://localhost:9999/",
	})
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &config.TxOptions{
		TxIsolation:   config.Serializable,
		ReadOptions:   &config.ReadOptions{QuorumSize: 2},
		CommitOptions: &config.CommitOptions{QuorumSize: 2},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)

	val, err := txCtx.Get("key1")
	require.Error(t, err)
	require.Nil(t, val)
	require.Contains(t, err.Error(), "can't get value: can't read 2 copies of same value")

	txCtx, err = db.Begin(txOptions)
	require.NoError(t, err)
	require.Equal(t, 2, len(db.(*blockchainDB).openTx))
	val, err = txCtx.Get("key5")
	require.NoError(t, err)
	require.NotNil(t, val)

	val, err = txCtx.Get("keynil")
	require.NoError(t, err)
	require.Nil(t, val)
}

func StopTestServer() {
	for _, conn := range dbConnections {
		delete(dbConnections, conn.Client.RawURL)
	}
	server.StopServer()
}

func createOptions() *config.Options {
	connOpts := []*config.ConnectionOption{
		{
			URL: "http://localhost:9999/",
		},
	}
	userOpt := &cryptoprovider.UserOptions{
		UserID:       "testUser",
		CAFilePath:   "../database/cert/ca_service.cert",
		CertFilePath: "../database/cert/client.pem",
		KeyFilePath:  "../database/cert/client.key",
	}
	return &config.Options{
		ConnectionOptions: connOpts,
		User:              userOpt,
		TxOptions: &config.TxOptions{
			TxIsolation:   config.Serializable,
			ReadOptions:   &config.ReadOptions{QuorumSize: 1},
			CommitOptions: &config.CommitOptions{QuorumSize: 1},
		},
	}
}
