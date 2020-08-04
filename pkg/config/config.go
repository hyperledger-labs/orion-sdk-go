package config

import (
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/library/pkg/crypto_utils"
)

// Options - database options, including connection options, user crypto materials locations and transaction options
type Options struct {
	ConnectionOptions []*ConnectionOption
	User              *IdentityOptions
	ServersVerify     *crypto_utils.VerificationOptions
	*TxOptions
}

// IdentityOptions contains client is and path to its signing entity
type IdentityOptions struct {
	Signer *crypto.SignerOptions
	UserID string
}

// TransactionIsolation - database transaction isolation level
type TransactionIsolation int

const (
	Serializable TransactionIsolation = iota + 1
	PhantomRead
	RepeatableRead
)

// TxOptions - transaction execution options, including tx isolation level, number of consistent reads and commits
type TxOptions struct {
	TxIsolation   TransactionIsolation
	ReadOptions   *ReadOptions
	CommitOptions *CommitOptions
}

//ConnectionOption - how to connect to database single server
type ConnectionOption struct {
	URL string
}

// ReadOptions - transaction read quorum
type ReadOptions struct {
	QuorumSize int
}

// CommitOptions - transaction commit quorum
type CommitOptions struct {
	QuorumSize int
}
