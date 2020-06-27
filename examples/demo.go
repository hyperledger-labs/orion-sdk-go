package main

import (
	"fmt"
	"github.ibm.com/blockchaindb/sdk/pkg/db"
	server "github.ibm.com/blockchaindb/sdk/pkg/db/mock"
	"log"
	"time"
)

func CreateOptins() *db.Options {
	connOpt := &db.ConnectionOption{
		Server: "localhost",
		Port:   6001,
	}
	connOpts := make([]*db.ConnectionOption, 0)
	connOpts = append(connOpts, connOpt)
	userOpt := &db.UserOptions{
		UserID: []byte("testUser"),
		CA:     "pkg/db/cert/ca.cert",
		Cert:   "pkg/db/cert/service.pem",
		Key:    "pkg/db/cert/service.key",
	}
	return &db.Options{
		ConnectionOptions: connOpts,
		User:              userOpt,
		TxOptions: &db.TxOptions{
			TxIsolation:   db.Serializable,
			ReadOptions:   &db.ReadOptions{QuorumSize: 1},
			CommitOptions: &db.CommitOptions{QuorumSize: 1},
		},
	}
}

func main() {
	go func() {
		server.StartServer(6001)
	}()
	time.Sleep(time.Second)
	opt := CreateOptins()
	db, err := db.Open("testDb", opt)
	if err != nil {
		log.Fatalln(fmt.Sprintf("can't connect to database: %v", err))
	}
	val1, err := db.Get("key1")
	if err != nil {
		log.Fatalln("can't get value for key key1 from database")
	}
	fmt.Println(fmt.Sprintf("[key1] = %s", string(val1)))

	tx, err := db.Begin(opt.TxOptions)
	if err != nil {
		log.Fatalln("can't begin transaction")
	}
	val2, err := tx.Get("key2")
	if err != nil {
		log.Fatalln("can't get value for key key1 from database")
	}
	fmt.Println(fmt.Sprintf("[key2] = %s", string(val2)))

	val3 := fmt.Sprintf("%s%s", string(val1), string(val2))
	if err = tx.Put("key3", []byte(val3)); err != nil {
		log.Fatalln("can't put value for key key3 to database")
	}

	if _, err = tx.Commit(); err != nil {
		log.Fatalln("can't commit transaction to database")
	}

}
