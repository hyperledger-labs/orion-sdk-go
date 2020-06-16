module github.ibm.com/blockchaindb/sdk

go 1.14

require (
	github.com/golang/protobuf v1.4.2
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.6.1
	github.ibm.com/blockchaindb/server v0.0.0
	google.golang.org/grpc v1.29.1
)

replace github.ibm.com/blockchaindb/server v0.0.0 => ../server
