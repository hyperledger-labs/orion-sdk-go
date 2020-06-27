package db

import (
	"github.ibm.com/blockchaindb/server/api"
	"time"
)

// DB provides APIs to begin a transaction context, to perform provenance queries, and close the db connection.
type DB interface {
	// Begin initializes a transaction context
	// TxOptions may override Options:
	// 1. Required transaction isolation level
	// 2. Number of servers used to read data during Tx creation/execution
	// 3. Number of responses should be collected by Client SDK during Commit() call
	//    to return success to Client
	Begin(options *TxOptions) (TxContext, error)
	// Close closes the connection to DB
	Close() error
	// DataQuerier provides APIs to query states from the database
	DataQuerier
	// Provenance provides APIs to access historical data
	Provenance
	// UserQuerier provides APIs to query existing users
	UserQuerier
}

// TxContext provides APIs to both query and modify states
type TxContext interface {
	DataQuerier
	// Put stores the given Key and value
	Put(key string, value []byte) error
	// Delete deletes the given Key
	Delete(key string) error
	// Users provides APIs for user management
	Users
	// Commit commits the transaction and return the
	// block merkel tree root and the block number at which
	// the transaction got added
	Commit() (*api.Digest, error)
	// Cancel transaction context, discard all transaction
	// data
	Abort() error
}

type Users interface {
	UserQuerier
	// AddUsers adds a new user to the DB
	AddUser(user *api.User) error
	// UpdateUser updates an existing user in the DB
	UpdateUser(user *api.User) error
	// DeleteUser deletes an existing user in the DB
	DeleteUser(user *api.User) error
}

// DataQuerier provides API to query states from the DB
type DataQuerier interface {
	Get(key string) ([]byte, error)
}

// Provide access to historical data and dat integrity proofs
type Provenance interface {
	// GetValueAtTime returns the value of a Key at a given time
	GetValueAtTime(key string, date time.Time) (*api.HistoricalData, error)
	// GetHistoryIterator returns an iterator to iter over the historical
	// value of a given Key.
	GetHistoryIterator(key string, opt QueryOption) (HistoryIterator, error)
	// GetTxProof return a proof for a given txId so that the caller can
	// validate the authenticity of the transaction.
	GetTxProof(txId string) (*api.Proof, error)
	// GetMerkleRoot returns the current block merkle root hash and the last committed
	// block number
	GetMerkleRoot() (*api.Digest, error)
}

type HistoryIterator interface {
	Next() (*api.HistoricalData, error)
	Close()
}

// Access db user data
type UserQuerier interface {
	// GetUsers returns all users in the DB
	GetUsers() []*api.User
	// GetUsersForRole returns all users in the DB with a given role
	GetUsersForRole(role string) []*api.User
}

// Encapsulate hash bytes
type Hash []byte

// Encapsulate signature bytes
type Signature []byte

type QueryOption interface {
}
