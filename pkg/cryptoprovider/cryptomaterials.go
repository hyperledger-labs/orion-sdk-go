package cryptoprovider

import (
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"

	"github.ibm.com/blockchaindb/server/api"
)

// UserOptions - user and crypto data
type UserOptions struct {
	UserID       string
	CAFilePath   string
	CertFilePath string
	KeyFilePath  string
}

type CryptoMaterials struct {
	caPool       *x509.CertPool
	certPool     *x509.CertPool
	tlsPair      tls.Certificate
	cert         *x509.Certificate
	nodeProvider NodeCryptoProvider
}

type NodeCryptoProvider interface {
	GetNodeCrypto(nodeID []byte) (*api.Node, error)
}

func (o *UserOptions) LoadCrypto(nodeCryptoProvider NodeCryptoProvider) (*CryptoMaterials, error) {
	certPair, err := tls.LoadX509KeyPair(o.CertFilePath, o.KeyFilePath)
	if err != nil {
		return nil, fmt.Errorf("could not load client key pair: %s", err)
	}

	x509certificate, err := x509.ParseCertificate(certPair.Certificate[0])
	if err != nil {
		log.Printf("can't extract x509 certificate: %s", err)
		return nil, fmt.Errorf("can't extract x509 certificate: %s", err)
	}

	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(o.CAFilePath)
	if err != nil {
		return nil, fmt.Errorf("could not read ca certificate: %s", err)
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to append ca certs")
	}

	return &CryptoMaterials{
		caPool:       certPool,
		certPool:     x509.NewCertPool(),
		tlsPair:      certPair,
		cert:         x509certificate,
		nodeProvider: nodeCryptoProvider,
	}, nil
}

func (c *CryptoMaterials) Sign(msg interface{}) ([]byte, error) {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	digest := sha256.New()
	_, err = digest.Write(msgBytes)
	if err != nil {
		return nil, err
	}

	singer, ok := c.tlsPair.PrivateKey.(crypto.Signer)
	if !ok {
		return nil, errors.New("can't sign using private key, not implement signer interface")
	}

	return singer.Sign(rand.Reader, digest.Sum(nil), crypto.SHA256)
}

func (c *CryptoMaterials) Validate(nodeID []byte, msg interface{}, signature []byte) error {
	err := c.validate(nodeID, msg, signature)
	if err != nil {
		return nil
	}
	return err
}


func (c *CryptoMaterials) validate(nodeID []byte, msg interface{}, signature []byte) error {
	node, err := c.nodeProvider.GetNodeCrypto(nodeID)
	if err != nil {
		return err
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	nodeCert, err := x509.ParseCertificate(node.NodeCertificate)
	if err != nil {
		return err
	}

	chains, err := nodeCert.Verify(x509.VerifyOptions{
		Intermediates: c.certPool,
		Roots:         c.caPool,
	})
	if err != nil {
		return err
	}
	if chains == nil || len(chains) == 0 {
		return errors.New("server certificate didn't pass verification")
	}

	c.certPool.AddCert(nodeCert)

	return nodeCert.CheckSignature(nodeCert.SignatureAlgorithm, msgBytes, signature)
}

func (c *CryptoMaterials) GetRawCertificate() []byte {
	return c.cert.Raw
}
