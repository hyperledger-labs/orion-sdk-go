package server

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

var s *http.Server

func StartServer(port int) {
	restServer, err := NewDBServer()
	if err != nil {
		log.Fatalf("failed to start rest server: %v", err)
	}

	go func() {
		s = &http.Server{
			Addr:    fmt.Sprintf("localhost:%d", port),
			Handler: restServer.router,
		}

		s.ListenAndServe()
	}()
}

func StopServer() {
	s.Close()
}

func StartTestServer() {
	StartServer(9999)
	time.Sleep(time.Millisecond * 100)
}
