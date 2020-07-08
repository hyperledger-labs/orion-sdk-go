package database

import (
	"log"

	"github.ibm.com/blockchaindb/sdk/pkg/cryptoprovider"
	"github.ibm.com/blockchaindb/server/api"
)

// TODO: Replace with NodeCryptoProvider that access node info
var nodeProvider *hardcodedNodeCryptoProvider

func init() {
	nodeProvider = &hardcodedNodeCryptoProvider{}
	nodeOptions := createNodeUserOptions()
	cm, err := nodeOptions.LoadCrypto(nodeProvider)
	if err != nil {
		log.Fatalf("can't load hardcoded node configuration, %s", err.Error())
	}
	nodeProvider.node = &api.Node{
		NodeID:          []byte(nodeOptions.UserID),
		NodeCertificate: cm.GetRawCertificate(),
	}
}

func createNodeUserOptions() *cryptoprovider.UserOptions {
	return &cryptoprovider.UserOptions{
		UserID:       "node1",
		CAFilePath:   "../database/cert/ca_client.cert",
		CertFilePath: "../database/cert/service.pem",
		KeyFilePath:  "../database/cert/service.key",
	}
}

type hardcodedNodeCryptoProvider struct {
	node *api.Node
}

func (hncp *hardcodedNodeCryptoProvider) GetNodeCrypto(nodeID []byte) (*api.Node, error) {
	return hncp.node, nil
}
