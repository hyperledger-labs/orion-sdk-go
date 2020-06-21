package db

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
)

type Options struct {
	connectionOptions []*ConnectionOption
	user              *UserOptions
	*TxOptions
}

type TransactionIsolation int

const (
	Serializable TransactionIsolation = iota + 1
	PhantomRead
	RepeatableRead
)

type TxOptions struct {
	txIsolation TransactionIsolation
	ro          *ReadOptions
	co          *CommitOptions
}

type ConnectionOption struct {
	server string
	port   int
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
	ca     string
	cert   string
	key    string
}

type cryptoMaterials struct {
	pool    *x509.CertPool
	tlsPair tls.Certificate
}

func (uo *UserOptions) LoadCrypto() (*cryptoMaterials, error) {
	certificate, err := tls.LoadX509KeyPair(uo.cert, uo.key)
	if err != nil {
		return nil, fmt.Errorf("could not load client key pair: %s", err)
	}

	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(uo.ca)
	if err != nil {
		return nil, fmt.Errorf("could not read ca certificate: %s", err)
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to append ca certs")
	}

	return &cryptoMaterials{
		pool:    certPool,
		tlsPair: certificate,
	}, nil
}
