package database

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/library/pkg/crypto_utils"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/sdk/pkg/rest"
	"log"
	"sync"
)

type BDB struct {
	// Internal database that stores all users' databases
	_dbs                              *blockchainDB
	internalDBManagementDatabaseMutex sync.RWMutex
	certificateFetcher                crypto_utils.CertificateFetcher
}

func NewBDB() (*BDB, error) {
	// TODO: remove hardcoded certificate fetcher
	certificateFetcher , err := CreateHardcodedFetcher()
	if err != nil {
		return nil, err
	}
	return &BDB{
		certificateFetcher: certificateFetcher,
	}, nil
}

// Create new database with default read and write acl
func (b *BDB) Create(dbName string, opt *config.Options, readACL, writeALC []string) error {
	if err := b.openInternalDBManagementDatabase(opt); err != nil {
		return err
	}

	tx, err := b._dbs.Begin(opt.TxOptions)
	if err != nil {
		return err
	}
	defer tx.Abort()

	dbConfig := &types.DatabaseConfig{
		Name:             dbName,
		ReadAccessUsers:  readACL,
		WriteAccessUsers: writeALC,
	}

	orgDBconfig, err := tx.Get(dbName)
	if err != nil {
		return err
	}

	if orgDBconfig != nil {
		return errors.Errorf("can't create db %s, it already exist", dbName)
	}
	dbConfigBytes, err := json.Marshal(dbConfig)
	tx.Put(dbName, dbConfigBytes)
	if _, err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Delete existing database
func (b *BDB) Delete(dbName string, opt *config.Options) error {
	if err := b.openInternalDBManagementDatabase(opt); err != nil {
		return err
	}

	tx, err := b._dbs.Begin(opt.TxOptions)
	if err != nil {
		return err
	}
	defer tx.Abort()

	orgDBconfig, err := tx.Get(dbName)
	if err != nil {
		return nil
	}

	if orgDBconfig == nil {
		return errors.Errorf("can't remove db %s, it does not exist", dbName)
	}
	tx.Delete(dbName)
	if _, err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Open opens an existing database associated with the dbName
// Options may specify:
// 1. Required transaction isolation level
// 2. Read QuorumSize - number of servers used to read data
// 3. Commit QuorumSize - number of responses should be collected by Client SDK during Commit() call
//    to return success to Client
// 4. User crypto materials
func (b *BDB) Open(dbName string, options *config.Options) (DB, error) {
	db := &blockchainDB{
		dbName:      dbName,
		connections: make([]*rest.Client, 0),
		openTx:      make(map[string]*transactionContext, 0),
		isClosed:    false,
		TxOptions:   options.TxOptions,
	}
	if options.User != nil {
		cryptoRegistry, err := crypto_utils.NewVerifiersRegistry(options.ServersVerify, b.certificateFetcher)
		if err != nil {
			return nil, err
		}
		db.verifiers = cryptoRegistry
		db.signer, err = crypto.NewSigner(options.User.Signer)
		if err != nil {
			return nil, err
		}
		db.userID = options.User.UserID

	}
	for _, serverOption := range options.ConnectionOptions {
		conn, err := b.openConnection(serverOption)
		if err != nil {
			return nil, err
		}
		db.connections = append(db.connections, conn)

		query := &types.GetStatusQuery{
			UserID: db.userID,
			DBName: dbName,
		}
		envelope := &types.GetStatusQueryEnvelope{
			Payload:   query,
			Signature: nil,
		}
		queryBytes, err := json.Marshal(query)
		if err != nil {
			return nil, err
		}
		envelope.Signature, err = db.signer.Sign(queryBytes)
		if err != nil {
			return nil, err
		}
		dbStatusEnvelope, err := conn.GetStatus(context.Background(), envelope)
		if err != nil {
			return nil, err
		}
		if !dbStatusEnvelope.Payload.Exist {
			return nil, errors.Errorf("database %s doesn't exist", dbName)
		}
	}

	return db, nil
}

// Single threaded
func (b *BDB) openConnection(options *config.ConnectionOption) (*rest.Client, error) {
	log.Println(fmt.Sprintf("Connecting to Server %s", options.URL))
	rc, err := rest.NewRESTClient(options.URL)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create REST client for %s", options.URL)
	}

	return rc, nil
}

// Single threaded
func (b *BDB) openInternalDBManagementDatabase(options *config.Options) error {
	b.internalDBManagementDatabaseMutex.Lock()
	defer b.internalDBManagementDatabaseMutex.Unlock()
	if b._dbs != nil {
		return nil
	}
	db, err := b.Open("_dbs", options)
	if err != nil {
		return err
	}
	b._dbs = db.(*blockchainDB)

	return nil
}
