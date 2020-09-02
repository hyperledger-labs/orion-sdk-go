package database

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/library/pkg/crypto_utils"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	server "github.ibm.com/blockchaindb/sdk/pkg/database/mock"
)

func TestDBOpen(t *testing.T) {
	t.Run("test-open", func(t *testing.T) {
		t.Parallel()
		db, options, s := openTestDB(t)
		defer s.Stop()

		require.NotNil(t, db.(*blockchainDB).Client, 1)
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
		connector := createDBConnector(t, options)
		db, err := connector.OpenDBSession("testDB2", options.TxOptions)
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
		connector := createDBConnector(t, options)
		options.ConnectionOptions[0].URL = fmt.Sprintf("http://localhost:%d/", 1999)
		db, err := connector.OpenDBSession("testDB", options.TxOptions)
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
		connector := createDBConnector(t, invalidOpt)
		invalidOpt.ServersVerify.CAFilePath = "nonexist.crt"
		db, err := connector.OpenDBSession("testDb", invalidOpt.TxOptions)
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

		connector := createDBConnector(t, opt)
		err = connector.GetDBManagement().CreateDB("testXYZ", []string{}, []string{})
		require.NoError(t, err)
		require.Contains(t, s.GetAllDBNames(), "_users")
		require.Contains(t, s.GetAllDBNames(), "_dbs")

		dbfound := false
		for k, v := range s.GetAllKeysForDB("_dbs") {
			if k == "testXYZ" {
				dbfound = true
				dbConf := &types.DatabaseConfig{}
				err := json.Unmarshal(v, dbConf)
				require.NoError(t, err)
				require.Equal(t, "testXYZ", dbConf.Name)
			}
		}
		require.True(t, dbfound)

		require.Equal(t, types.Transaction_DB, s.LastTxType())
	})

	t.Run("test-db-create-existing-db", func(t *testing.T) {
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		opt := createOptions(port)

		connector := createDBConnector(t, opt)
		err = connector.GetDBManagement().CreateDB("testXYZ", []string{}, []string{})

		err = connector.GetDBManagement().CreateDB("testXYZ", []string{}, []string{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "can't create db")

		err = connector.GetDBManagement().CreateDB("testDb", []string{}, []string{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "can't create db")
	})

	t.Run("test-db-delete-ok", func(t *testing.T) {
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		opt := createOptions(port)

		connector := createDBConnector(t, opt)
		err = connector.GetDBManagement().CreateDB("testXYZ", []string{}, []string{})

		err = connector.GetDBManagement().DeleteDB("testXYZ")
		require.NoError(t, err)

		dbfound := false
		for k, v := range s.GetAllKeysForDB("_dbs") {
			if k == "testXYZ" {
				if v == nil {
					continue
				}
				dbfound = true
				dbConf := &types.DatabaseConfig{}
				err := json.Unmarshal(v, dbConf)
				require.NoError(t, err)
				require.Equal(t, "testXYZ", dbConf.Name)
			}
		}
		require.False(t, dbfound)
		require.Equal(t, types.Transaction_DB, s.LastTxType())
	})

	t.Run("test-db-double-double-non-exist", func(t *testing.T) {
		s := server.NewTestServer()
		defer s.Stop()
		port, err := s.Port()
		opt := createOptions(port)

		connector := createDBConnector(t, opt)
		err = connector.GetDBManagement().CreateDB("testXYZ", []string{}, []string{})

		err = connector.GetDBManagement().DeleteDB("testXYZ")
		require.NoError(t, err)

		err = connector.GetDBManagement().DeleteDB("testXYZ")
		require.Error(t, err)
		require.Contains(t, err.Error(), "can't remove db")

		err = connector.GetDBManagement().DeleteDB("testXYZ2")
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

		err = txCtx.Put("key3", []byte("Testvalue31"), nil)
		require.NoError(t, err)
		require.Empty(t, txCtx.(*transactionContext).rwset.rset)

		err = txCtx.Put("key4", []byte("Testvalue41"), nil)
		require.NoError(t, err)
		require.Empty(t, txCtx.(*transactionContext).rwset.rset)

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
		require.Contains(t, txCtx.(*transactionContext).rwset.wset, "key3")
		require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.wset["key3"], &types.KVWrite{
			Key:      "key3",
			IsDelete: true,
			Value:    nil,
		}))

		db.Close()
		require.NoError(t, err)
		err = txCtx.Put("key2", []byte("Val2"), nil)
		require.Contains(t, err.Error(), "valid")
		err = txCtx.Delete("key2")
		require.Contains(t, err.Error(), "valid")
	})

	t.Run("test-txcontext-putwithacl", func(t *testing.T) {
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

		acl := &types.AccessControl{
			ReadUsers: map[string]bool{
				"u1": true,
				"u2": true,
			},
			ReadWriteUsers: map[string]bool{
				"u1": true,
			},
		}
		err = txCtx.Put("key3", []byte("Testvalue31"), acl)
		require.NoError(t, err)
		require.Empty(t, txCtx.(*transactionContext).rwset.rset)

		err = txCtx.Put("key4", []byte("Testvalue41"), nil)
		require.NoError(t, err)
		require.Empty(t, txCtx.(*transactionContext).rwset.rset)

		require.Contains(t, txCtx.(*transactionContext).rwset.wset, "key3")
		require.Contains(t, txCtx.(*transactionContext).rwset.wset, "key4")

		require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.wset["key3"], &types.KVWrite{
			Key:      "key3",
			IsDelete: false,
			Value:    []byte("Testvalue31"),
			ACL: &types.AccessControl{
				ReadUsers: map[string]bool{
					"u1": true,
					"u2": true,
				},
				ReadWriteUsers: map[string]bool{
					"u1": true,
				},
			},
		}))

		require.True(t, proto.Equal(txCtx.(*transactionContext).rwset.wset["key4"], &types.KVWrite{
			Key:      "key4",
			IsDelete: false,
			Value:    []byte("Testvalue41"),
		}))

		txCtx.Commit()
		dataStored := func() bool {
			meta := s.GetAllMetadataForDB("testDb")["key3"]
			return proto.Equal(meta.GetAccessControl(), &types.AccessControl{
				ReadUsers: map[string]bool{
					"u1": true,
					"u2": true,
				},
				ReadWriteUsers: map[string]bool{
					"u1": true,
				},
			})
		}
		require.Eventually(t, dataStored, 1000*time.Millisecond, 10*time.Millisecond)
		db.Close()
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

		err = txCtx.Put("key3", []byte("Testvalue31"), nil)
		require.NoError(t, err)

		err = txCtx.Put("key4", []byte("Testvalue41"), nil)
		require.NoError(t, err)

		err = txCtx.Delete("key3")
		require.NoError(t, err)

		_, err = txCtx.Commit()
		require.NoError(t, err)

		require.Equal(t, types.Transaction_DATA, s.LastTxType())
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

		err = txCtx.Put("key1", []byte("NewValue"), nil)
		require.NoError(t, err)

		_, err = txCtx.Get("key1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Key value already changed inside this tx")

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

func openTestDB(t *testing.T) (DBSession, *config.Options, *server.TestServer) {
	s := server.NewTestServer()
	port, err := s.Port()
	require.NoError(t, err)
	options := createOptions(port)
	connector := createDBConnector(t, options)
	db, err := connector.OpenDBSession("testDb", options.TxOptions)
	require.NoError(t, err)
	return db, options, s
}

func createDBConnector(t *testing.T, options *config.Options) DBConnector {
	connector, err := NewConnector(options)
	require.NoError(t, err)
	require.NotNil(t, connector)
	return connector
}
