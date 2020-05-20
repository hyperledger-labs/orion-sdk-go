package db

import (
	"encoding/hex"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	server "github.ibm.com/blockchaindb/sdk/pkg/db/mock"
	"github.ibm.com/blockchaindb/server/api"
	"net"
	"strings"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	startServer()
	defer stopServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)
	require.Equal(t, len(db.(*blockchainDB).connections), 1)
	require.Equal(t, len(dbConnections), 1)
	require.False(t, db.(*blockchainDB).isClosed)
	require.EqualValues(t, db.(*blockchainDB).userId, options.user.UserID, "user ids are not equal")
	val, err := db.Get("key1")
	require.NotNil(t, val)
	require.Nil(t, err)

	db, err = Open("testDB2", options)
	require.Nil(t, db)
	require.Error(t, err)

	options.connectionOptions[0].port = 19999
	db, err = Open("testDB", options)
	require.Nil(t, db)
	require.Error(t, err)

	invalidOpt := createOptions()
	invalidOpt.user.ca = "nonexist.crt"
	db, err = Open("testDb", invalidOpt)
	require.Nil(t, db)
	require.Error(t, err)
}

func TestBlockchainDB_Close(t *testing.T) {
	startServer()
	defer stopServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)
	err = db.Close()
	require.NoError(t, err)
	_, err = db.Get("key1")
	require.Contains(t, err.Error(), "closed")
}

func TestBlockchainDB_Get(t *testing.T) {
	startServer()
	defer stopServer()
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
	startServer()
	defer server.StopServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &TxOptions{
		txIsolation: RepeatableRead,
		ro:          &ReadOptions{QuorumSize: 1},
		co:          &CommitOptions{QuorumSize: 1},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)
	require.NotNil(t, txCtx)
	require.NotNil(t, txCtx.(*txContext).txId)
	require.NotNil(t, txCtx.(*txContext).db)
	require.EqualValues(t, txCtx.(*txContext).db, db)
	found := false
	for k, _ := range db.(*blockchainDB).openTx {
		if strings.Compare(k, hex.EncodeToString(txCtx.(*txContext).txId)) == 0 {
			found = true
			break
		}
	}
	require.True(t, found, "transaction context not found")

	require.Equal(t, txCtx.(*txContext).snapshotBlock, uint64(0))

	db.Close()
	require.NoError(t, err)
	require.Empty(t, db.(*blockchainDB).openTx)
	_, err = db.Begin(txOptions)
	require.Contains(t, err.Error(), "closed")
}

func TestTxContext_Get(t *testing.T) {
	startServer()
	defer server.StopServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &TxOptions{
		txIsolation: RepeatableRead,
		ro:          &ReadOptions{QuorumSize: 1},
		co:          &CommitOptions{QuorumSize: 1},
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

	require.Contains(t, txCtx.(*txContext).statements, stmt1)
	require.Contains(t, txCtx.(*txContext).statements, stmt2)

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
	require.True(t, proto.Equal(txCtx.(*txContext).rset["key1"], rset1))
	require.True(t, proto.Equal(txCtx.(*txContext).rset["key2"], rset2))

	db.Close()
	require.NoError(t, err)
	_, err = txCtx.Get("key2")
	require.Contains(t, err.Error(), "valid")
}

func TestTxContext_Abort(t *testing.T) {
	startServer()
	defer server.StopServer()
	options := createOptions()
	db, err := Open("testDb", options)
	require.NoError(t, err)

	txOptions := &TxOptions{
		txIsolation: RepeatableRead,
		ro:          &ReadOptions{QuorumSize: 1},
		co:          &CommitOptions{QuorumSize: 1},
	}

	txCtx, err := db.Begin(txOptions)
	require.NoError(t, err)

	err = txCtx.Abort()
	require.NoError(t, err)

	err = txCtx.Abort()
	require.Error(t, err)
	require.Contains(t, err.Error(), "valid")
}

func checkConnect(host string, port string, timeoutMillis int) bool {
	timeout := time.Millisecond * time.Duration(timeoutMillis)
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
	if err != nil {
		fmt.Println("Connecting error:", err)
	}
	if conn != nil {
		defer conn.Close()
		fmt.Println("Opened", net.JoinHostPort(host, port))
		return true
	}
	return false
}

func startServer() {
	go server.StartServer(9999)
	for {
		if !checkConnect("localhost", "9999", 100) {
			time.Sleep(time.Millisecond * 100)
		} else {
			break
		}
	}
}

func stopServer() {
	for _, conn := range dbConnections {
		conn.conn.Close()
		delete(dbConnections, conn.addr)
	}
	server.StopServer()
}

func createOptions() *Options {
	connOpt := &ConnectionOption{
		server: "localhost",
		port:   9999,
	}
	connOpts := make([]*ConnectionOption, 0)
	connOpts = append(connOpts, connOpt)
	userOpt := &UserOptions{
		UserID: []byte("testUser"),
		ca:     "cert/ca.cert",
		cert:   "cert/service.pem",
		key:    "cert/service.key",
	}
	return &Options{
		connectionOptions: connOpts,
		user:              userOpt,
		TxOptions: &TxOptions{
			txIsolation: Serializable,
			ro:          &ReadOptions{QuorumSize: 1},
			co:          &CommitOptions{QuorumSize: 1},
		},
	}
}
