package database

import (
	"log"

	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/protos/types"
)

// TODO: Replace with NodeCryptoProvider that access node info
var (
	nodeProvider          *hardcodedNodeCryptoProvider
	certPossibleLocations = []string{"../database/testdata/", "pkg/database/testdata/", "../pkg/database/testdata/"}
)

func init() {
	nodeProvider = &hardcodedNodeCryptoProvider{}

	for _, loc := range certPossibleLocations {
		nodeOptions := createNodeIdentityOptions(loc)
		cm, err := nodeOptions.LoadCrypto(nil)
		if err != nil {
			continue
		}
		nodeProvider.node = &types.Node{
			NodeID:          []byte(nodeOptions.UserID),
			NodeCertificate: cm.GetRawCertificate(),
		}
		return
	}
	log.Panicln("can't load hardcoded node configuration")
}

func createNodeIdentityOptions(location string) *crypto.IdentityOptions {
	return &crypto.IdentityOptions{
		UserID:       "node1",
		CAFilePath:   location + "ca_client.cert",
		CertFilePath: location + "service.pem",
		KeyFilePath:  location + "service.key",
	}
}

type hardcodedNodeCryptoProvider struct {
	node *types.Node
}

func (hncp *hardcodedNodeCryptoProvider) GetNodeCrypto(nodeID []byte) (*types.Node, error) {
	return hncp.node, nil
}
