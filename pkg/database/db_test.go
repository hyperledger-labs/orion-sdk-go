package database

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.ibm.com/blockchaindb/library/pkg/crypto_utils"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	server "github.ibm.com/blockchaindb/sdk/pkg/database/mock"
)

func TestDBOpen(t *testing.T) {
	t.Run("test-open", func(t *testing.T) {
		t.Parallel()
		db, options, s := openTestDB(t)
		defer s.Stop()

		require.Equal(t, len(db.(*blockchainDB).connections), 1)
		require.False(t, db.(*blockchainDB).isClosed)
		require.EqualValues(t, db.(*blockchainDB).userID, options.User.UserID, "user ids are not equal")
		val, err := db.Get("key1")
		require.NotNil(t, val)
		require.Nil(t, err)
	})

	t.Run("test-open-non-exits-db", func(t *testing.T) {
		t.Parallel()
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		require.NoError(t, err)

		options := createOptions(port)
		db, err := Open("testDB2", options)
		require.Nil(t, db)
		require.Error(t, err)
	})

	t.Run("test-open-wrong-connection-url", func(t *testing.T) {
		t.Parallel()
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		require.NoError(t, err)

		options := createOptions(port)
		options.ConnectionOptions[0].URL = fmt.Sprintf("http://localhost:%d/", 1999)
		db, err := Open("testDB", options)
		require.Nil(t, db)
		require.Error(t, err)
	})

	t.Run("test-open-wrong-cert", func(t *testing.T) {
		t.Parallel()
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		require.NoError(t, err)

		invalidOpt := createOptions(port)
		invalidOpt.ServersVerify.CAFilePath = "nonexist.crt"
		db, err := Open("testDb", invalidOpt)
		require.Nil(t, db)
		require.Error(t, err)
	})
}

func TestBlockchainDB_Close(t *testing.T) {
	t.Run("test-close", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

		err := db.Close()
		require.NoError(t, err)
		_, err = db.Get("key1")
		require.Contains(t, err.Error(), "closed")
	})
}

func TestBlockchainDB_Get(t *testing.T) {
	t.Run("test-db-get", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

		key1res, err := db.Get("key1")
		require.NoError(t, err)
		require.EqualValues(t, []byte("Testvalue11"), key1res)

		key2res, err := db.Get("key2")
		require.NoError(t, err)
		require.EqualValues(t, []byte("Testvalue21"), key2res)
	})
}

func TestDBegin(t *testing.T) {
	t.Run("test-db-begin-ok", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

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
		for k := range db.(*blockchainDB).openTx {
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
	})
}

func TestCreateDelete(t *testing.T) {
	t.Run("test-db-create-ok", func(t *testing.T) {
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		opt := createOptions(port)

		err = Create("testXYZ", opt, []string{}, []string{})
		require.NoError(t, err)
		require.Contains(t, s.GetAllDBNames(), "_users")
		require.Contains(t, s.GetAllDBNames(), "_dbs")

		dbfound := false
		for k, v := range s.GetAllKeysForDB("_dbs") {
			if k == "testXYZ" {
				dbfound = true
				dbConf := &types.DatabaseConfig{}
				err := json.Unmarshal(v.Value, dbConf)
				require.NoError(t, err)
				require.Equal(t, "testXYZ", dbConf.Name)
			}
		}
		require.True(t, dbfound)
	})

	t.Run("test-db-create-existing-db", func(t *testing.T) {
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		opt := createOptions(port)

		err = Create("testXYZ", opt, []string{}, []string{})

		err = Create("testXYZ", opt, []string{}, []string{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "can't create db")

		err = Create("testDb", opt, []string{}, []string{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "can't create db")
	})

	t.Run("test-db-delete-ok", func(t *testing.T) {
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		opt := createOptions(port)

		err = Create("testXYZ", opt, []string{}, []string{})

		err = Delete("testXYZ", opt)
		require.NoError(t, err)

		dbfound := false
		for k, v := range s.GetAllKeysForDB("_dbs") {
			if k == "testXYZ" {
				if v.Value == nil {
					continue
				}
				dbfound = true
				dbConf := &types.DatabaseConfig{}
				err := json.Unmarshal(v.Value, dbConf)
				require.NoError(t, err)
				require.Equal(t, "testXYZ", dbConf.Name)
			}
		}
		require.False(t, dbfound)
	})

	t.Run("test-db-double-double-non-exist", func(t *testing.T) {
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		opt := createOptions(port)

		err = Create("testXYZ", opt, []string{}, []string{})

		err = Delete("testXYZ", opt)
		require.NoError(t, err)

		err = Delete("testXYZ", opt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "can't remove db")

		err = Delete("testXYZ2", opt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "can't remove db")
	})
}

func TestTxContextGet(t *testing.T) {
	t.Run("test-txcontext-get-ok", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

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

		stmt1 := &types.Statement{
			Operation: "GET",
			Arguments: make([][]byte, 0),
		}
		stmt1.Arguments = append(stmt1.Arguments, []byte("key1"))
		stmt2 := &types.Statement{
			Operation: "GET",
			Arguments: make([][]byte, 0),
		}
		stmt2.Arguments = append(stmt2.Arguments, []byte("key2"))

		require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt1)
		require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt2)

		rset1 := &types.KVRead{
			Key: "key1",
			Version: &types.Version{
				BlockNum: 0,
				TxNum:    0,
			},
		}
		rset2 := &types.KVRead{
			Key: "key2",
			Version: &types.Version{
				BlockNum: 0,
				TxNum:    1,
			},
		}
		require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.rset["key1"], rset1))
		require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.rset["key2"], rset2))

	})

	t.Run("test-txcontext-get-after-close", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

		txOptions := &config.TxOptions{
			TxIsolation:   config.RepeatableRead,
			ReadOptions:   &config.ReadOptions{QuorumSize: 1},
			CommitOptions: &config.CommitOptions{QuorumSize: 1},
		}

		txCtx, err := db.Begin(txOptions)
		require.NoError(t, err)

		db.Close()
		require.NoError(t, err)
		_, err = txCtx.Get("key2")
		require.Contains(t, err.Error(), "valid")
	})

}

func TestTxContextPutDelete(t *testing.T) {
	t.Run("test-txcontext-put-delete", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

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

		stmt1 := &types.Statement{
			Operation: "PUT",
			Arguments: make([][]byte, 0),
		}
		stmt1.Arguments = append(stmt1.Arguments, []byte("key3"), []byte("Testvalue31"))
		stmt2 := &types.Statement{
			Operation: "PUT",
			Arguments: make([][]byte, 0),
		}
		stmt2.Arguments = append(stmt2.Arguments, []byte("key4"), []byte("Testvalue41"))
		require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt1)
		require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt2)

		require.Contains(t, txCtx.(*transactionContext).rwset.wset, "key3")
		require.Contains(t, txCtx.(*transactionContext).rwset.wset, "key4")

		require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.wset["key3"], &types.KVWrite{
			Key:      "key3",
			IsDelete: false,
			Value:    []byte("Testvalue31"),
		}))

		require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.wset["key4"], &types.KVWrite{
			Key:      "key4",
			IsDelete: false,
			Value:    []byte("Testvalue41"),
		}))

		err = txCtx.Delete("key3")
		require.NoError(t, err)
		require.Empty(t, txCtx.(*transactionContext).rwset.rset)
		stmt3 := &types.Statement{
			Operation: "DELETE",
			Arguments: make([][]byte, 0),
		}
		stmt3.Arguments = append(stmt3.Arguments, []byte("key3"))
		require.Contains(t, txCtx.(*transactionContext).rwset.statements, stmt3)
		require.Contains(t, txCtx.(*transactionContext).rwset.wset, "key3")
		require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.wset["key3"], &types.KVWrite{
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
	})
}

func TestTxContextCommitAbort(t *testing.T) {
	t.Run("test-txcontext-commit-ok", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

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

	})

	t.Run("test-txcontext-commit-double", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

		txOptions := &config.TxOptions{
			TxIsolation:   config.Serializable,
			ReadOptions:   &config.ReadOptions{QuorumSize: 1},
			CommitOptions: &config.CommitOptions{QuorumSize: 1},
		}

		txCtx, err := db.Begin(txOptions)

		_, err = txCtx.Commit()
		require.NoError(t, err)

		_, err = txCtx.Commit()
		require.Contains(t, err.Error(), "valid")
	})

	t.Run("test-txcontext-abort-ok", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

		txOptions := &config.TxOptions{
			TxIsolation:   config.RepeatableRead,
			ReadOptions:   &config.ReadOptions{QuorumSize: 1},
			CommitOptions: &config.CommitOptions{QuorumSize: 1},
		}

		txCtx, err := db.Begin(txOptions)
		require.NoError(t, err)

		err = txCtx.Abort()
		require.NoError(t, err)
	})

	t.Run("test-txcontext-abort-double", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

		txOptions := &config.TxOptions{
			TxIsolation:   config.RepeatableRead,
			ReadOptions:   &config.ReadOptions{QuorumSize: 1},
			CommitOptions: &config.CommitOptions{QuorumSize: 1},
		}

		txCtx, err := db.Begin(txOptions)

		err = txCtx.Abort()

		err = txCtx.Abort()
		require.Error(t, err)
		require.Contains(t, err.Error(), "valid")
	})
}

func TestTxContext_TxIsolation(t *testing.T) {
	t.Run("test-txisolation-different-reads", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

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
		require.Contains(t, err.Error(), "Key value version changed during tx")
	})

	t.Run("test-txisolation-dirty-reads", func(t *testing.T) {
		t.Parallel()
		db, _, s := openTestDB(t)
		defer s.Stop()

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
		require.Contains(t, err.Error(), "Key value already changed inside this tx")

	})
}

func TestTxContext_GetMultipleValues(t *testing.T) {
	t.Run("test-multiple-values-read", func(t *testing.T) {
		t.Parallel()
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		require.NoError(t, err)
		options := createOptions(port)
		// Connect twice to same Server, as it another Server
		options.ConnectionOptions = append(options.ConnectionOptions, &config.ConnectionOption{
			URL: fmt.Sprintf("http://localhost:%s/", port),
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
		require.Nil(t, val)

		val, err = txCtx.Get("keynil")
		require.NoError(t, err)
		require.Nil(t, val)
	})
}

func createOptions(port string) *config.Options {
	connOpts := []*config.ConnectionOption{
		{
			URL: fmt.Sprintf("http://localhost:%s/", port),
		},
	}
	userOpt := &config.IdentityOptions{
		UserID: "testUser",
		Signer: &crypto.SignerOptions{
			KeyFilePath: "../database/testdata/client.key",
		},
	}
	serverVerifyOpt := &crypto_utils.VerificationOptions{
		CAFilePath: "../database/testdata/ca_service.cert",
	}
	return &config.Options{
		ConnectionOptions: connOpts,
		User:              userOpt,
		ServersVerify:     serverVerifyOpt,
		TxOptions: &config.TxOptions{
			TxIsolation:   config.Serializable,
			ReadOptions:   &config.ReadOptions{QuorumSize: 1},
			CommitOptions: &config.CommitOptions{QuorumSize: 1},
		},
	}
}

func openTestDB(t *testing.T) (DB, *config.Options, *server.TestServer) {
	s := server.NewTestServer()
	port, err := s.Port()
	require.NoError(t, err)
	options := createOptions(port)
	db, err := Open("testDb", options)
	require.NoError(t, err)
	return db, options, s
}
