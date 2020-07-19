module github.ibm.com/blockchaindb/sdk

go 1.14

require (
	github.com/golang/protobuf v1.4.2
	github.com/gorilla/mux v1.7.4
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.6.1
	github.ibm.com/blockchaindb/library v0.0.0
	github.ibm.com/blockchaindb/protos v0.0.0
)

replace github.ibm.com/blockchaindb/library v0.0.0 => ../library

replace github.ibm.com/blockchaindb/protos v0.0.0 => ../protos-go
