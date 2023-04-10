// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"crypto/tls"
	"encoding/pem"
	"io/ioutil"
	"net/url"
	"os"
	"sync"

	"github.com/hyperledger-labs/orion-sdk-go/internal"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
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
	DataTx(options ...TxContextOption) (DataTxContext, error)
	LoadDataTx(*types.DataTxEnvelope) (LoadedDataTxContext, error)
	DBsTx() (DBsTxContext, error)
	ConfigTx() (ConfigTxContext, error)
	Provenance() (Provenance, error)
	Ledger() (Ledger, error)
	Query() (Query, error)
	// ReplicaSet returns the set of replicas the session is currently using. If `refresh` is `true`, the session will
	// also query the cluster for the most recent replica set before returning.
	// Note that when a DBSession is first created, it queries the cluster for the most recent replica set.
	ReplicaSet(refresh bool) ([]*config.Replica, error)
}

var ErrTxSpent = errors.New("transaction committed or aborted")

// TxContext is an abstract API to capture general purpose functionality for all types of transactions context.
type TxContext interface {
	// Commit submits transaction to the server, can be sync or async.
	// Sync option returns tx id and tx receipt envelope and
	// in case of error, commitTimeout error is one of possible errors to return.
	// Async returns tx id, always nil as tx receipt or error
	Commit(sync bool) (string, *types.TxReceiptResponseEnvelope, error)
	// Abort cancel submission and abandon all changes
	// within given transaction context
	Abort() error
	// CommittedTxEnvelope returns transaction envelope, can be called only after Commit(), otherwise will return nil
	CommittedTxEnvelope() (proto.Message, error)
	// TxID gives the transaction id
	TxID() string
}

type Ledger interface {
	// GetBlockHeader returns block header from ledger
	GetBlockHeader(blockNum uint64) (*types.BlockHeader, error)
	// GetLastBlockHeader returns last block from ledger
	GetLastBlockHeader() (*types.BlockHeader, error)
	// GetLedgerPath returns cryptographically verifiable path between any block pairs in ledger skip list
	GetLedgerPath(startBlock, endBlock uint64) (*LedgerPath, error)
	// GetTransactionProof returns intermediate hashes from hash(tx, validating info) to root of
	// tx merkle tree stored in block header
	GetTransactionProof(blockNum uint64, txIndex int) (*TxProof, error)
	// GetTransactionReceipt return block header where tx is stored and tx index inside block
	GetTransactionReceipt(txId string) (*types.TxReceipt, error)
	// GetDataProof returns proof of existence of value associated with key in block Merkle-Patricia Trie
	// Proof itself is a path from node that contains value to root node in MPTrie
	GetDataProof(blockNum uint64, dbName, key string, isDeleted bool) (*state.Proof, error)
	// GetFullTxProofAndVerify do full tx existence and validity proof by fetching and validating two ledger skip list paths and one Merkle tree path.
	// First, it fetches the Merkle tree path within the block with the transaction.
	// Next, the ledger path from the block with the transaction to the genesis block is fetched.
	// Then, the ledger path from the last know (a-priori) block to the block with the transaction is fetched.
	// Finally, these three proofs are validated.
	// Returns
	// TxProof - the Merkle tree path within the block with the transaction.
	// LedgerPath - two concatenated ledger paths [last... block... genesis]
	// error - in case if verification failed, nil otherwise
	GetFullTxProofAndVerify(txReceipt *types.TxReceipt, lastKnownBlockHeader *types.BlockHeader, tx proto.Message) (*TxProof, *LedgerPath, error)
	// NewBlockHeaderDeliveryService creates a delivery service to deliver block header
	// from a given starting block number present in the config to all the future block
	// till the service is stopped
	NewBlockHeaderDeliveryService(conf *BlockHeaderDeliveryConfig) BlockHeaderDelivererService
	// GetTxContent returns the transaction envelope associated with the block number and transaction index, along
	// with the validation info and version. Only users that had signed the transaction correctly can get the
	// transaction content.
	GetTxContent(blockNum, txIndex uint64) (*types.GetTxResponse, error)
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
	// GetDataReadByUser returns all user reads grouped by databases
	GetDataReadByUser(userID string) (map[string]*types.KVsWithMetadata, error)
	// GetDataWrittenByUser returns all user writes grouped by databases
	GetDataWrittenByUser(userID string) (map[string]*types.KVsWithMetadata, error)
	// GetReaders returns all users who read value associated with the key
	GetReaders(dbName, key string) ([]string, error)
	// GetWriters returns all users who wrote value associated with the key
	GetWriters(dbName, key string) ([]string, error)
	// GetTxIDsSubmittedByUser IDs of all tx submitted by user
	GetTxIDsSubmittedByUser(userID string) ([]string, error)
}

type RangeQueryResponse struct {
	KVs            []*types.KVWithMetadata
	PendingResults bool
	NextStartKey   string
}

// Query provides method to execute json query and range query on a
// given database.
type Query interface {
	// ExecuteJSONQuery executes a given JSON query on a given database.
	// The JSON query is a json string which must contain predicates under the field
	// selector. The first field in the selector can be a combinational operator
	// such as "$and" or "$or" followed by a list of attributes and a list of
	// conditions per attributes. A query example is shown below
	//
	// {
	//   "selector": {
	// 		"$and": {            -- top level combinational operator
	// 			"attr1": {          -- a field in the json document
	// 				"$gte": "a",    -- value criteria for the field
	// 				"$lt": "b"      -- value criteria for the field
	// 			},
	// 			"attr2": {          -- a field in the json document
	// 				"$eq": true     -- value criteria for the field
	// 			},
	// 			"attr3": {          -- a field in the json document
	// 				"$lt": "a2"     -- a field in the json document
	// 			}
	// 		}
	//   }
	// }
	ExecuteJSONQuery(dbName, query string) ([]*types.KVWithMetadata, error)
	// GetDataByRange executes a range query on a given database. The startKey is
	// inclusive but endKey is not. When the startKey is an empty string, it denotes
	// `fetch keys from the beginning` while an empty endKey denotes `fetch keys till the
	// the end`. The limit denotes the number of records to be fetched in total. However,
	// when the limit is set to 0, it denotes no limit. The iterator returned by
	// GetDataByRange is used to retrieve the records.
	GetDataByRange(dbName, startKey, endKey string, limit uint64) (Iterator, error)
}

// Iterator implements methods to iterate over a set records
type Iterator interface {
	// Next returns the next record. If there is no more records, it would return a nil value
	// and a false value.
	Next() (*types.KVWithMetadata, bool, error)
}

//go:generate mockery --dir . --name Signer --case underscore --output mocks/

type Signer interface {
	crypto.Signer
}

// Create prepares connection context to work with BCDB instance
// loads root CA certificates
func Create(connectionConfig *config.ConnectionConfig) (BCDB, error) {
	dbLogger := connectionConfig.Logger
	if dbLogger == nil {
		c := &logger.Config{
			Level:         "info",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "orion-client",
		}
		var err error
		dbLogger, err = logger.New(c)
		if err != nil {
			return nil, err
		}
	}

	rootCAs, err := loadCACertificates(connectionConfig.RootCAs, dbLogger)
	if err != nil {
		return nil, err
	}
	rootCACerts, err := certificateauthority.NewCACertCollection(rootCAs, nil)
	if err != nil {
		dbLogger.Errorf("failed to create CACertCollection, due to %s", err)
		return nil, errors.Wrap(err, "failed to create CACertCollection")
	}
	if err = rootCACerts.VerifyCollection(); err != nil {
		dbLogger.Errorf("verification of CA certs collection failed, due to %s", err)
		return nil, errors.Wrap(err, "verification of CA certs collection failed")
	}

	// Verify replica set URIs
	urls := map[string]*url.URL{}
	for _, uri := range connectionConfig.ReplicaSet {
		replicaURL, err := url.Parse(uri.Endpoint)
		if err != nil {
			dbLogger.Errorf("error parsing replica URI, %s", uri.Endpoint)
			return nil, errors.Wrapf(err, "error parsing replica URI, %s", uri.Endpoint)
		}
		urls[uri.ID] = replicaURL

		if connectionConfig.TLSConfig.Enabled {
			if replicaURL.Scheme != "https" {
				dbLogger.Errorf("configuration error, tls in use, but url is %s", uri.Endpoint)
				return nil, errors.Wrapf(err, "configuration error, tls in use, but url is %s", uri.Endpoint)
			}
		} else {
			if replicaURL.Scheme != "http" {
				dbLogger.Errorf("configuration error, tls disabled, but url is %s", uri.Endpoint)
				return nil, errors.Wrapf(err, "configuration error, tls disabled, but url is  %s", uri.Endpoint)
			}
		}
	}

	db := &bDB{
		bootstrapReplicaMap: urls,
		rootCAs:             rootCACerts,
		logger:              dbLogger,
	}

	// Loading TLS CA root and intermediate certificates
	if connectionConfig.TLSConfig.Enabled {
		tlsRootCAs, err := loadCACertificates(connectionConfig.TLSConfig.CaConfig.RootCACertsPath, dbLogger)
		if err != nil {
			return nil, err
		}
		tlsIntermediateCAs, err := loadCACertificates(connectionConfig.TLSConfig.CaConfig.IntermediateCACertsPath, dbLogger)
		tlsCACertCollection, err := certificateauthority.NewCACertCollection(tlsRootCAs, tlsIntermediateCAs)
		if err != nil {
			dbLogger.Errorf("failed to create CACertCollection, due to %s", err)
			return nil, err
		}
		if err = tlsCACertCollection.VerifyCollection(); err != nil {
			dbLogger.Errorf("verification of CA certs collection is failed, due to %s", err)
			return nil, err
		}
		if err != nil {
			return nil, err
		}
		db.tlsRootCAs = tlsCACertCollection
		db.tlsEnabled = true
		db.tlsClientAuthRequire = connectionConfig.TLSConfig.ClientAuthRequired
	}

	return db, nil
}

type bDB struct {
	mutex                sync.Mutex
	bootstrapReplicaMap  map[string]*url.URL
	rootCAs              *certificateauthority.CACertCollection
	tlsEnabled           bool
	tlsRootCAs           *certificateauthority.CACertCollection
	tlsClientAuthRequire bool
	logger               *logger.SugarLogger
}

// Session parses the session configuration and opens a user session to the Orion cluster.
// When a session is created, the cluster is queried for the latest cluster status using the BCDB existing replica set.
// The returned cluster status is used to update the replica set of the session and the BCDB instance.
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
		rootCAs:      b.rootCAs,
		tlsEnabled:   b.tlsEnabled,
		tlsRootCAs:   b.tlsRootCAs,
		txTimeout:    cfg.TxTimeout,
		queryTimeout: cfg.QueryTimeout,
		logger:       b.logger,
	}

	for id, url := range b.bootstrapReplicaMap {
		session.replicaSet = append(session.replicaSet, &internal.ReplicaWithRole{
			Id:   id,
			URL:  url,
			Role: internal.ReplicaRole_UNKNOWN,
		})
	}

	if b.tlsEnabled {
		clientTlsConfig := &tls.Config{
			RootCAs:    b.tlsRootCAs.GetCertPool(),
			ClientCAs:  b.tlsRootCAs.GetCertPool(),
			MinVersion: tls.VersionTLS12,
		}
		if b.tlsClientAuthRequire {
			clientKeyBytes, err := os.ReadFile(cfg.ClientTLS.ClientKeyPath)
			if err != nil {
				b.logger.Errorf("cannot read user's tls certificate, from %s, due to %s",
					cfg.ClientTLS.ClientKeyPath, err)
				return nil, errors.Wrap(err, "cannot read user's tls certificate")
			}
			clientCertBytes, err := os.ReadFile(cfg.ClientTLS.ClientCertificatePath)
			if err != nil {
				b.logger.Errorf("cannot read user's tls private key, from %s, due to %s",
					cfg.ClientTLS.ClientCertificatePath, err)
				return nil, errors.Wrap(err, "cannot read user's tls private key")
			}
			clientKeyPair, err := tls.X509KeyPair(clientCertBytes, clientKeyBytes)
			if err != nil {
				b.logger.Error("cannot create x509 key pair", err)
				return nil, errors.Wrap(err, "cannot create x509 key pair")
			}
			clientTlsConfig.Certificates = []tls.Certificate{clientKeyPair}
			session.clientAuthRequired = true
		}
		session.clientTlsConfig = clientTlsConfig
	}
	httpClient := newHTTPClient(session.tlsEnabled, session.clientTlsConfig, nil)
	err = session.updateReplicaSetAndVerifier(httpClient, session.tlsEnabled)
	if err != nil {
		b.logger.Errorf("cannot update the replica set and signature verifier, error: %s", err)
		return nil, errors.Wrap(err, "cannot update the replica set and signature verifier")
	}

	b.updateReplicaMap(session.replicaSet.ToReplicaMap())

	return session, nil
}

func (b *bDB) updateReplicaMap(replicaMap map[string]*url.URL) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.bootstrapReplicaMap = replicaMap
}

func loadCACertificates(certPaths []string, dbLogger *logger.SugarLogger) ([][]byte, error) {
	var rootCAs [][]byte
	for _, rootCAPath := range certPaths {
		rootCABytes, err := ioutil.ReadFile(rootCAPath)
		if err != nil {
			dbLogger.Errorf("failed to read root CA certificate, due to %s", err)
			return nil, errors.Wrap(err, "failed to read root CA certificate")
		}
		asn1Data, _ := pem.Decode(rootCABytes)
		rootCAs = append(rootCAs, asn1Data.Bytes)
	}
	return rootCAs, nil
}
