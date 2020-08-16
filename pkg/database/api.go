package database

import (
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
)

// DBConnector handle connectivity between sdk and Blockchain Database cluster
type DBConnector interface {
	// OpenDBSession creates logical connection to database and returns DBSession interface
	OpenDBSession(dbName string, options *config.TxOptions) (DBSession, error)
	// GetDBAdmin returns blockchain db management interface
	GetDBManagement() DBManagement
}

type DBManagement interface {
	// CreateDB create new database in cluster
	CreateDB(dbName string, readACL, readWriteALC []string) error
	// DeleteDB deletes database from cluster
	DeleteDB(dbName string) error
}

// DBSession represents one of logical databases in Blockchain Database cluster and provides APIs to begin
// a transaction, to perform provenance queries, and close the session and release all resources.
type DBSession interface {
	// Begin initializes a transaction context
	// TxOptions may override Options:
	// 1. Required transaction isolation level
	// 2. Number of servers used to read data during Tx creation/execution
	// 3. Number of responses should be collected by Client SDK during Commit() call
	//    to return success to Client
	Begin(options *config.TxOptions) (TxContext, error)
	// Close closes the connection to DBSession
	Close() error
	// DataQuerier provides APIs to query states from the database
	DataQuerier
	// Provenance provides APIs to access historical data
	Provenance
	// UserQuerier provides APIs to query existing users
	UserQuerier
}

// TxContext provides APIs to both query and modify states
// Please note that TxContext is not thread safe - access same TxContext object from
// different go routines on you own risk
type TxContext interface {
	DataQuerier
	// Put stores the given KeyFilePath and value
	Put(key string, value []byte) error
	// Delete deletes the given Key
	Delete(key string) error
	// Users provides APIs for user management
	Users
	// Commit commits the transaction and return the
	// block merkel tree root and the block number at which
	// the transaction got added
	Commit() (*types.Digest, error)
	// Cancel transaction context, discard all transaction
	// data
	Abort() error
}

// Users provide API to operate database users
type Users interface {
	UserQuerier
	// AddUsers adds a new user to the DB
	AddUser(user *types.User) error
	// UpdateUser updates an existing user in the DB
	UpdateUser(user *types.User) error
	// DeleteUser deletes an existing user in the DB
	DeleteUser(user *types.User) error
}

// DataQuerier provides API to query states from the DB
type DataQuerier interface {
	Get(key string) ([]byte, error)
}

// Provenance access to historical data and dat integrity proofs
type Provenance interface {
	// GetMerkleRoot returns the current block merkle root hash and the last committed
	// block number
	GetMerkleRoot() (*types.Digest, error)
}

// UserQuerier access database user data
type UserQuerier interface {
	// GetUsers returns all users in the DB
	GetUsers() []*types.User
}

// Encapsulate hash bytes
type Hash []byte

// Encapsulate signature bytes
type Signature []byte

type QueryOption interface {
}
