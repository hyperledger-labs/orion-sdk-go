package main

import (
	"fmt"
	"log"
	"time"

	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/sdk/pkg/cryptoprovider"
	"github.ibm.com/blockchaindb/sdk/pkg/database"
)


func main() {
	time.Sleep(time.Second)
	opt := createOptins()

	db, err := database.Open("testdb", opt)
	if err != nil {
		log.Fatalln(fmt.Sprintf("can't connect to database: %v", err))
	}

	val1, err := db.Get("key1")
	if err != nil {
		log.Fatalln("can't get value for key key1 from database")
	}
	fmt.Printf("[key1] = %s\n", string(val1))

	tx, err := db.Begin(opt.TxOptions)
	if err != nil {
		log.Fatalln("can't begin transaction")
	}

	val2, err := tx.Get("key2")
	if err != nil {
		log.Fatalln("can't get value for key key1 from database")
	}
	fmt.Printf("[key2] = %s\n", string(val2))

	if err = tx.Put("key3", []byte(string(val1) + string(val2))); err != nil {
		log.Fatalln("can't put value for key key3 to database")
	}

	if _, err = tx.Commit(); err != nil {
		log.Fatalln("can't commit transaction to database")
	}
}

func createOptins() *config.Options {
	connOpts := []*config.ConnectionOption{
		{
			URL: "http://localhost:6001/",
		},
	}
	userOpt := &cryptoprovider.UserOptions{
		UserID:       "testUser",
		CAFilePath:   "pkg/database/cert/ca_service.cert",
		CertFilePath: "pkg/database/cert/client.pem",
		KeyFilePath:  "pkg/database/cert/client.key",
	}
	return &config.Options{
		ConnectionOptions: connOpts,
		User:              userOpt,
		TxOptions: &config.TxOptions{
			TxIsolation:   config.Serializable,
			ReadOptions:   &config.ReadOptions{QuorumSize: 1},
			CommitOptions: &config.CommitOptions{QuorumSize: 1},
		},
	}
}

