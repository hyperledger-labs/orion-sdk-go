package server

import (
	"log"

	"github.ibm.com/blockchaindb/library/pkg/crypto"
)

var nodeSigner *crypto.Signer
var nodeID []byte

func init() {
	nodeOptions := createNodeSignerOptions()
	nodeID = []byte("node1")
	var err error
	nodeSigner, err = crypto.NewSigner(nodeOptions)
	if err != nil {
		log.Panicf("can't initiate server side Signer %v", err)
	}
}

func createNodeSignerOptions() *crypto.SignerOptions {
	return &crypto.SignerOptions{
		KeyFilePath: "../database/testdata/service.key",
	}
}
