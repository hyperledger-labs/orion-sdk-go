package db

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
)

type Options struct {
	ConnectionOptions []*ConnectionOption
	User              *UserOptions
	*TxOptions
}

type TransactionIsolation int

const (
	Serializable TransactionIsolation = iota + 1
	PhantomRead
	RepeatableRead
)

type TxOptions struct {
	TxIsolation   TransactionIsolation
	ReadOptions   *ReadOptions
	CommitOptions *CommitOptions
}

type ConnectionOption struct {
	Server string
	Port   int
}

type ReadOptions struct {
	QuorumSize int
}

type CommitOptions struct {
	QuorumSize int
}

// User and crypto data
type UserOptions struct {
	UserID []byte
	CA     string
	Cert   string
	Key    string
}

type cryptoMaterials struct {
	pool    *x509.CertPool
	tlsPair tls.Certificate
}

func (uo *UserOptions) LoadCrypto() (*cryptoMaterials, error) {
	certificate, err := tls.LoadX509KeyPair(uo.Cert, uo.Key)
	if err != nil {
		return nil, fmt.Errorf("could not load client Key pair: %s", err)
	}

	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(uo.CA)
	if err != nil {
		return nil, fmt.Errorf("could not read CA certificate: %s", err)
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to append CA certs")
	}

	return &cryptoMaterials{
		pool:    certPool,
		tlsPair: certificate,
	}, nil
}
