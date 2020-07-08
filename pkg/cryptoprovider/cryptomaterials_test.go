package cryptoprovider

import (
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/api"
)

var testNodeCrypto *testNodeCryptoProvider

func Init() {
	testNodeCrypto = &testNodeCryptoProvider{}
	nodeOpt := &UserOptions{
		UserID:       "node1",
		CAFilePath:   "../database/cert/ca_client.cert",
		CertFilePath: "../database/cert/service.pem",
		KeyFilePath:  "../database/cert/service.key",
	}

	cm, err := nodeOpt.LoadCrypto(testNodeCrypto)
	if err != nil {
		log.Fatalf("can't load hardcoded node configuration, %s", err.Error())
	}
	testNodeCrypto.node = &api.Node{
		NodeID:          []byte(nodeOpt.UserID),
		NodeCertificate: cm.GetRawCertificate(),
	}
}

func TestMain(m *testing.M) {
	Init()
	os.Exit(m.Run())
}

func TestUserOptions_LoadCrypto(t *testing.T) {
	userOpt := &UserOptions{
		UserID:       "testUser",
		CAFilePath:   "../database/cert/ca_service.cert",
		CertFilePath: "../database/cert/client.pem",
		KeyFilePath:  "../database/cert/client.key",
	}
	userCrypto, err := userOpt.LoadCrypto(nil)
	require.NoError(t, err)
	require.NotNil(t, userCrypto)
	require.NotNil(t, userCrypto.caPool)
	require.NotNil(t, userCrypto.certPool)
	require.NotNil(t, userCrypto.tlsPair)
	require.NotNil(t, userCrypto.cert)

	userOpt.CAFilePath = "../database/cert/error_ca.cert"
	userCrypto, err = userOpt.LoadCrypto(nil)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "could not read ca certificate")

	userOpt.CAFilePath = "../database/cert/ca_service.cert"
	userOpt.CertFilePath = "../database/cert/error_client.pem"
	userCrypto, err = userOpt.LoadCrypto(nil)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "could not load client key pair")

	userOpt.CAFilePath = "../database/cert/junk_ca.cert"
	userOpt.CertFilePath = "../database/cert/client.pem"
	userCrypto, err = userOpt.LoadCrypto(nil)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to append ca certs")
}

func TestCryptoMaterials_Sign(t *testing.T) {
	userOpt := &UserOptions{
		UserID:       "testUser",
		CAFilePath:   "../database/cert/ca_service.cert",
		CertFilePath: "../database/cert/client.pem",
		KeyFilePath:  "../database/cert/client.key",
	}

	userCrypto, err := userOpt.LoadCrypto(testNodeCrypto)
	require.NoError(t, err)
	require.NotNil(t, userCrypto)
	require.NotNil(t, userCrypto.caPool)
	require.NotNil(t, userCrypto.certPool)
	require.NotNil(t, userCrypto.tlsPair)
	require.NotNil(t, userCrypto.cert)

	msg := &api.GetStateQuery{
		UserID: "testuser",
		DBName: "testdb",
		Key:    "testkey",
	}
	signature, err := userCrypto.Sign(msg)
	require.NoError(t, err)
	require.NotNil(t, signature)
	digest := sha256.New()
	msgBytes, err := json.Marshal(msg)
	require.NoError(t, err)
	digest.Write(msgBytes)
	singer := userCrypto.tlsPair.PrivateKey.(crypto.Signer)
	expectedSignature, err := singer.Sign(rand.Reader, digest.Sum(nil), crypto.SHA256)
	require.NoError(t, err)
	require.EqualValues(t, expectedSignature, signature)
}

func TestCryptoMaterials_Validate(t *testing.T) {
	userOpt := &UserOptions{
		UserID:       "testUser",
		CAFilePath:   "../database/cert/ca_service.cert",
		CertFilePath: "../database/cert/client.pem",
		KeyFilePath:  "../database/cert/client.key",
	}

	userCrypto, err := userOpt.LoadCrypto(testNodeCrypto)
	require.NoError(t, err)
	require.NotNil(t, userCrypto)
	require.NotNil(t, userCrypto.caPool)
	require.NotNil(t, userCrypto.certPool)
	require.NotNil(t, userCrypto.tlsPair)
	require.NotNil(t, userCrypto.cert)

	nodeOpt := &UserOptions{
		UserID:       "node1",
		CAFilePath:   "../database/cert/ca_client.cert",
		CertFilePath: "../database/cert/service.pem",
		KeyFilePath:  "../database/cert/service.key",
	}

	nodeCrypto, err := nodeOpt.LoadCrypto(testNodeCrypto)
	require.NoError(t, err)
	require.NotNil(t, nodeCrypto)
	require.NotNil(t, nodeCrypto.caPool)
	require.NotNil(t, nodeCrypto.certPool)
	require.NotNil(t, nodeCrypto.tlsPair)
	require.NotNil(t, nodeCrypto.cert)

	msg := &api.GetStateResponse{
		Header: &api.ResponseHeader{
			NodeID: []byte("node1"),
		},
		Value: &api.Value{
			Value: []byte("this is test value string"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		},
	}

	signature, err := nodeCrypto.Sign(msg)
	require.NoError(t, err)
	require.NotNil(t, signature)
	err = userCrypto.Validate(msg.Header.NodeID, msg, signature)
	require.NoError(t, err)
}

type testNodeCryptoProvider struct {
	node *api.Node
}

func (t *testNodeCryptoProvider) GetNodeCrypto(nodeID []byte) (*api.Node, error) {
	return t.node, nil
}
