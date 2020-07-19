package server

import (
	"github.ibm.com/blockchaindb/library/pkg/crypto"
)

var nodeCrypto *crypto.CryptoMaterials
var nodeID []byte

func init() {
	nodeOptions := createNodeIdentityOptions()
	nodeID = []byte(nodeOptions.UserID)
	nodeCrypto, _ = nodeOptions.LoadCrypto(nil)

}

func createNodeIdentityOptions() *crypto.IdentityOptions {
	return &crypto.IdentityOptions{
		UserID:       "node1",
		CAFilePath:   "../database/testdata/ca_client.cert",
		CertFilePath: "../database/testdata/service.pem",
		KeyFilePath:  "../database/testdata/service.key",
	}
}
