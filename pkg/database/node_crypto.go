package database

import (
	"log"

	"github.ibm.com/blockchaindb/sdk/pkg/cryptoprovider"
	"github.ibm.com/blockchaindb/server/api"
)

// TODO: Replace with NodeCryptoProvider that access node info
var (
	nodeProvider *hardcodedNodeCryptoProvider
	certPossibleLocations = []string {"../database/cert/", "pkg/database/cert/", "../pkg/database/cert/"}
)
func init() {
	nodeProvider = &hardcodedNodeCryptoProvider{}

	for _, loc := range certPossibleLocations {
		nodeOptions := createNodeUserOptions(loc)
		cm, err := nodeOptions.LoadCrypto(nil)
		if err != nil {
			continue
		}
		nodeProvider.node = &api.Node{
			NodeID:          []byte(nodeOptions.UserID),
			NodeCertificate: cm.GetRawCertificate(),
		}
		return
	}
	log.Panicln("can't load hardcoded node configuration")
}

func createNodeUserOptions(location string) *cryptoprovider.UserOptions {
	return &cryptoprovider.UserOptions{
		UserID:       "node1",
		CAFilePath:   location + "ca_client.cert",
		CertFilePath: location + "service.pem",
		KeyFilePath:  location + "service.key",
	}
}

type hardcodedNodeCryptoProvider struct {
	node *api.Node
}

func (hncp *hardcodedNodeCryptoProvider) GetNodeCrypto(nodeID []byte) (*api.Node, error) {
	return hncp.node, nil
}
