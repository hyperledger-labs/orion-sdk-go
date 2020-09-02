package server

import (
	"log"
	"net"
	"net/http"

	"github.ibm.com/blockchaindb/protos/types"
)

type TestServer struct {
	l        net.Listener
	dbServer *DBServer
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
		l:        listen,
		dbServer: restServer,
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

func (t *TestServer) GetAllDBNames() []string {
	res := make([]string, 0)
	for dbname, _ := range t.dbServer.mockserver.dbs {
		res = append(res, dbname)
	}
	return res
}

func (t *TestServer) GetAllKeysForDB(name string) map[string][]byte {
	res := make(map[string][]byte, 0)
	db, ok := t.dbServer.mockserver.dbs[name]
	if ok {
		for k, v := range db.values {
			if v.index >= len(v.values) {
				res[k] = v.values[len(v.values)-1]
			} else {
				res[k] = v.values[v.index]
			}
		}
	}
	return res
}

func (t *TestServer) GetAllMetadataForDB(name string) map[string]*types.Metadata {
	res := make(map[string]*types.Metadata, 0)
	db, ok := t.dbServer.mockserver.dbs[name]
	if ok {
		for k, v := range db.values {
			if v.index >= len(v.values) {
				res[k] = v.metas[len(v.values)-1]
			} else {
				res[k] = v.metas[v.index]
			}
		}
	}
	return res
}

func (t *TestServer) LastTxType() types.Transaction_Type {
	return t.dbServer.ts.lastTxType
}
