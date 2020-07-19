package cryptoprovider

import (
	"errors"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/api"
)

var testNodeCrypto *testNodeCryptoProvider

func Init() {
	testNodeCrypto = &testNodeCryptoProvider{node: make(map[string]*api.Node, 0)}
	nodeOpts := []*UserOptions{
		createTestNodeOptions(),
		createTestNodeNoCAOptions(),
	}

	for _, nodeOpt := range nodeOpts {
		cm, err := nodeOpt.LoadCrypto(testNodeCrypto)
		if err != nil {
			log.Fatalf("can't load hardcoded node configuration, %s", err.Error())
		}
		testNodeCrypto.node[nodeOpt.UserID] = &api.Node{
			NodeID:          []byte(nodeOpt.UserID),
			NodeCertificate: cm.GetRawCertificate(),
		}
	}
}

func TestMain(m *testing.M) {
	Init()
	os.Exit(m.Run())
}

func TestUserOptions(t *testing.T) {
	userOpt := createTestUserOptions()
	t.Run("LoadCrypto", func (t *testing.T){
		userCrypto, err := userOpt.LoadCrypto(nil)
		validateLoadedCrypto(t, userCrypto, err)

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
	})
}

func TestCryptoMaterials(t *testing.T) {
	userOpt := createTestUserOptions()

	t.Run("ValidateCorrect", func(t *testing.T) {
		nodeOpt := createTestNodeOptions()
		msg := createTestStateResponseMsg(nodeOpt.UserID)
		_, _, _, err := loadSignAndValidate(t, userOpt, nodeOpt, msg)
		require.NoError(t, err)
	})

	t.Run("ValidateNoCA", func(t *testing.T) {
		nodeOpt := createTestNodeNoCAOptions()
		msg := createTestStateResponseMsg(nodeOpt.UserID)
		userCrypto, nodeCrypto, signature, err := loadSignAndValidate(t, userOpt, nodeOpt, msg)
		require.Error(t, err)

		// Even pushing server certificate to list of already validated doesn't solve the problem
		userCrypto.certPool.AddCert(nodeCrypto.cert)
		err = userCrypto.Validate(msg.Header.NodeID, msg, signature)
		require.Error(t, err)

		// But adding it as CA cert may solve the issue
		userCrypto.caPool.AddCert(nodeCrypto.cert)
		err = userCrypto.Validate(msg.Header.NodeID, msg, signature)
		require.NoError(t, err)

	})

	t.Run("ValidateWrongServerCertificate", func(t *testing.T) {
		nodeOpt := createTestNodeOptions()
		msg := createTestStateResponseMsg(nodeOpt.UserID)
		userCrypto, err := userOpt.LoadCrypto(testNodeCrypto)
		nodeCrypto, err := nodeOpt.LoadCrypto(testNodeCrypto)
		signature, err := nodeCrypto.Sign(msg)

		// Set wrong server certificate on user side
		testNodeCrypto.node[nodeOpt.UserID] = &api.Node{
			NodeID:          []byte(nodeOpt.UserID),
			NodeCertificate: []byte("Wrong cert"),
		}
		err = userCrypto.Validate(msg.Header.NodeID, msg, signature)
		require.Error(t, err)

		// No server certificate
		delete(testNodeCrypto.node, nodeOpt.UserID)
		err = userCrypto.Validate(msg.Header.NodeID, msg, signature)
		require.Error(t, err)

	})

}

func validateLoadedCrypto(t *testing.T, cm *CryptoMaterials, err error) {
	require.NoError(t, err)
	require.NotNil(t, cm)
	require.NotNil(t, cm.caPool)
	require.NotNil(t, cm.certPool)
	require.NotNil(t, cm.tlsPair)
	require.NotNil(t, cm.cert)
}

func createTestStateResponseMsg(userId string) *api.GetStateResponse {
	return &api.GetStateResponse{
		Header: &api.ResponseHeader{
			NodeID: []byte(userId),
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
}

func createTestUserOptions() *UserOptions {
	return &UserOptions{
		UserID:       "testUser",
		CAFilePath:   "../database/cert/ca_service.cert",
		CertFilePath: "../database/cert/client.pem",
		KeyFilePath:  "../database/cert/client.key",
	}
}

func createTestNodeOptions() *UserOptions {
	return &UserOptions{
		UserID:       "node1",
		CAFilePath:   "../database/cert/ca_client.cert",
		CertFilePath: "../database/cert/service.pem",
		KeyFilePath:  "../database/cert/service.key",
	}
}

func createTestNodeNoCAOptions() *UserOptions {
	return &UserOptions{
		UserID:       "node1_noca",
		CAFilePath:   "../database/cert/ca_client.cert",
		CertFilePath: "../database/cert/noca_service.pem",
		KeyFilePath:  "../database/cert/noca_service.key",
	}
}

func loadSignAndValidate(t *testing.T, userOpt *UserOptions, nodeOpt *UserOptions, msg *api.GetStateResponse) (*CryptoMaterials, *CryptoMaterials, []byte, error) {
	userCrypto, err := userOpt.LoadCrypto(testNodeCrypto)
	validateLoadedCrypto(t, userCrypto, err)

	nodeCrypto, err := nodeOpt.LoadCrypto(testNodeCrypto)
	validateLoadedCrypto(t, nodeCrypto, err)

	signature, err := nodeCrypto.Sign(msg)
	require.NoError(t, err)
	require.NotNil(t, signature)

	return userCrypto, nodeCrypto, signature, userCrypto.Validate(msg.Header.NodeID, msg, signature)

}

type testNodeCryptoProvider struct {
	node map[string]*api.Node
}

func (t *testNodeCryptoProvider) GetNodeCrypto(nodeID []byte) (*api.Node, error) {
	node, ok := t.node[string(nodeID)]
	if !ok {
		return nil, errors.New("can't find node crypto")
	}

	return node, nil
}
