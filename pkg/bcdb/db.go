// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"encoding/pem"
	"io/ioutil"
	"net/url"

	"github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/certificateauthority"
	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// BCDB Blockchain Database interface, defines set of APIs
// required to operate with BCDB instance
type BCDB interface {
	// Session instantiates session to the database
	Session(config *config.SessionConfig) (DBSession, error)
}

// DBSession captures user's session
type DBSession interface {
	UsersTx() (UsersTxContext, error)
	DataTx() (DataTxContext, error)
	LoadDataTx(*types.DataTxEnvelope) (LoadedDataTxContext, error)
	DBsTx() (DBsTxContext, error)
	ConfigTx() (ConfigTxContext, error)
	Provenance() (Provenance, error)
	Ledger() (Ledger, error)
}

var ErrTxSpent = errors.New("transaction committed or aborted")

// TxContet an abstract API to capture general purpose
// functionality for all types of transactions context
type TxContext interface {
	// Commit submits transaction to the server, can be sync or async.
	// Sync option returns tx id and tx receipt and
	// in case of error, commitTimeout error is one of possible errors to return.
	// Async returns tx id, always nil as tx receipt or error
	Commit(sync bool) (string, *types.TxReceipt, error)
	// Abort cancel submission and abandon all changes
	// within given transaction context
	Abort() error
	// CommittedTxEnvelope returns transaction envelope, can be called only after Commit(), otherwise will return nil
	CommittedTxEnvelope() (proto.Message, error)
}

type Ledger interface {
	// GetBlockHeader returns block header from ledger
	GetBlockHeader(blockNum uint64) (*types.BlockHeader, error)
	// GetLedgerPath returns cryptographically verifiable path between any block pairs in ledger skip list
	GetLedgerPath(startBlock, endBlock uint64) ([]*types.BlockHeader, error)
	// GetTransactionProof returns intermediate hashes from hash(tx, validating info) to root of
	// tx merkle tree stored in block header
	GetTransactionProof(blockNum uint64, txIndex int) (*TxProof, error)
	// GetTransactionReceipt return block header where tx is stored and tx index inside block
	GetTransactionReceipt(txId string) (*types.TxReceipt, error)
}

type Provenance interface {
	// GetHistoricalData return all historical values for specific dn and key
	// Value returned with its associated metadata, including block number, tx index, etc
	GetHistoricalData(dbName, key string) ([]*types.ValueWithMetadata, error)
	// GetHistoricalDataAt returns value for specific version, if exist
	GetHistoricalDataAt(dbName, key string, version *types.Version) (*types.ValueWithMetadata, error)
	// GetPreviousHistoricalData returns value precedes given version, including its metadata, i.e version
	GetPreviousHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error)
	// GetNextHistoricalData returns value succeeds given version, including its metadata
	GetNextHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error)
	// GetDataReadByUser returns all user reads
	GetDataReadByUser(userID string) ([]*types.KVWithMetadata, error)
	// GetDataWrittenByUser returns all user writes
	GetDataWrittenByUser(userID string) ([]*types.KVWithMetadata, error)
	// GetReaders returns all users who read value associated with the key
	GetReaders(dbName, key string) ([]string, error)
	// GetWriters returns all users who wrote value associated with the key
	GetWriters(dbName, key string) ([]string, error)
	// GetTxIDsSubmittedByUser IDs of all tx submitted by user
	GetTxIDsSubmittedByUser(userID string) ([]string, error)
}

//go:generate mockery --dir . --name Signer --case underscore --output mocks/

type Signer interface {
	crypto.Signer
}

// Create prepares connection context to work with BCDB instance
// loads root CA certificates
func Create(config *config.ConnectionConfig) (BCDB, error) {
	dbLogger := config.Logger
	if dbLogger == nil {
		c := &logger.Config{
			Level:         "info",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		}
		var err error
		dbLogger, err = logger.New(c)
		if err != nil {
			return nil, err
		}
	}

	var rootCAs [][]byte
	for _, rootCAPath := range config.RootCAs {
		rootCABytes, err := ioutil.ReadFile(rootCAPath)
		if err != nil {
			dbLogger.Errorf("failed to read root CA certificate, due to %s", err)
			return nil, errors.Wrap(err, "failed to read root CA certificate")
		}
		asn1Data, _ := pem.Decode(rootCABytes)
		rootCAs = append(rootCAs, asn1Data.Bytes)
	}
	rootCACerts, err := certificateauthority.NewCACertCollection(rootCAs, nil)
	if err != nil {
		dbLogger.Errorf("failed to create CACertCollection, due to %s", err)
		return nil, err
	}
	if err = rootCACerts.VerifyCollection(); err != nil {
		dbLogger.Errorf("verification of CA certs collection is failed, due to %s", err)
		return nil, err
	}

	// Validate replica set URIs
	urls := map[string]*url.URL{}
	for _, uri := range config.ReplicaSet {
		replicaURL, err := url.Parse(uri.Endpoint)
		if err != nil {
			dbLogger.Errorf("error parsing replica URI, %s", uri.Endpoint)
			return nil, errors.Wrapf(err, "error parsing replica URI, %s", uri.Endpoint)
		}
		urls[uri.ID] = replicaURL
	}

	return &bDB{
		replicaSet: urls,
		rootCAs:    rootCACerts,
		logger:     dbLogger,
	}, nil
}

type bDB struct {
	replicaSet map[string]*url.URL
	rootCAs    *certificateauthority.CACertCollection
	logger     *logger.SugarLogger
}

// Session parses sessions configuration and opens session to BCDB, takes
// care to read user
func (b *bDB) Session(cfg *config.SessionConfig) (DBSession, error) {
	signer, err := crypto.NewSigner(&crypto.SignerOptions{
		KeyFilePath: cfg.UserConfig.PrivateKeyPath,
	})
	if err != nil {
		b.logger.Errorf("cannot create signer with user's private key, from %s, due to %s",
			cfg.UserConfig.PrivateKeyPath, err)
		return nil, errors.Wrap(err, "cannot create signer with user's private key")
	}

	certBytes, err := ioutil.ReadFile(cfg.UserConfig.CertPath)
	if err != nil {
		b.logger.Errorf("cannot read user's certificate with user's private key, from %s, due to %s",
			cfg.UserConfig.CertPath, err)
		return nil, errors.Wrap(err, "cannot read user's certificate with user's private key")
	}

	session := &dbSession{
		userID:       cfg.UserConfig.UserID,
		signer:       signer,
		userCert:     certBytes,
		replicaSet:   b.replicaSet,
		rootCAs:      b.rootCAs,
		txTimeout:    cfg.TxTimeout,
		queryTimeout: cfg.QueryTimeout,
		logger:       b.logger,
	}
	httpClient := newHTTPClient()
	session.verifier, err = session.sigVerifier(httpClient)
	if err != nil {
		b.logger.Errorf("cannot create a signature verifier, error: %s", err)
		return nil, errors.Wrap(err, "cannot create a signature verifier")
	}

	return session, nil
}
