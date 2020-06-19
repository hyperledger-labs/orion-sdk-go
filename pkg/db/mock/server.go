package server

import (
	"fmt"
	"github.ibm.com/blockchaindb/server/api"
	"google.golang.org/grpc"
	"log"
	"net"
)

var s *grpc.Server

func StartServer(port int) {
	// create listiner
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// create grpc server
	s = grpc.NewServer()
	qs, err := NewQueryServer()
	if err != nil {
		log.Fatalf("failed to create query service: %v", err)

	}

	ts, err := NewTransactionServer()
	if err != nil {
		log.Fatalf("failed to create transaction service: %v", err)

	}
	api.RegisterQueryServer(s, qs)
	api.RegisterTransactionSvcServer(s, ts)

	// and start...
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func StopServer() {
	s.Stop()
}
