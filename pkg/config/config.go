package config

import "github.ibm.com/blockchaindb/server/pkg/logger"

// Replica
type Replica struct {
	// ID replica's ID
	ID string
	// Endpoint the URI of the replica to connect to
	Endpoint string
}

// ConnectionConfig required configuration in order to
// open session with BCDB instance, replica set informations
// servers root CAs
type ConnectionConfig struct {
	// List of replicas URIs client can connect to
	ReplicaSet []*Replica
	// Keeps path to the server's root CA
	RootCAs []string
	// Logger instance, if nil an internal logger is created
	Logger *logger.SugarLogger
}

// SessionConfig keeps per database session
// configuration information
type SessionConfig struct {
	UserConfig *UserConfig
}

// UserConfig user related information
// maintains wallet with public and private keys
type UserConfig struct {
	// UserID the identity of the user
	UserID string
	// CertPath path to the user's certificate
	CertPath string
	// PrivateKeyPath path to the user's private key
	PrivateKeyPath string
}
