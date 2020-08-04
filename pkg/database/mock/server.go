package server

import (
	"log"
	"net"
	"net/http"
)

type TestServer struct {
	l net.Listener
}

func NewTestServer() *TestServer {
	restServer, err := NewDBServer()
	if err != nil {
		log.Fatalf("failed to start rest server: %v", err)
	}

	listen, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	go func() {
		http.Serve(listen, restServer.router)
	}()

	return &TestServer{
		l: listen,
	}
}

func (t *TestServer) Stop() {
	t.l.Close()
}

func (t *TestServer) Port() (string, error) {
	_, port, err := net.SplitHostPort(t.l.Addr().String())
	if err != nil {
		return ":0", err
	}
	return port, nil
}
