package server

import (
	"github.ibm.com/blockchaindb/sdk/pkg/cryptoprovider"
)

var nodeCrypto *cryptoprovider.CryptoMaterials
var nodeID []byte

func init() {
	nodeOptions := createNodeUserOptions()
	nodeID = []byte(nodeOptions.UserID)
	nodeCrypto, _ = nodeOptions.LoadCrypto(nil)

}

func createNodeUserOptions() *cryptoprovider.UserOptions {
	return &cryptoprovider.UserOptions{
		UserID:       "node1",
		CAFilePath:   "../database/cert/ca_client.cert",
		CertFilePath: "../database/cert/service.pem",
		KeyFilePath:  "../database/cert/service.key",
	}
}
