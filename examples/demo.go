package main

import (
	"fmt"
	"log"
	"time"

	"github.ibm.com/blockchaindb/library/pkg/crypto_utils"

	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/sdk/pkg/database"
)

func main() {
	time.Sleep(time.Second)
	opt := createOptions("6001")

	fmt.Println("Connecting to database test...")
	db, err := database.Open("test", opt)
	if err != nil {
		log.Fatalln(fmt.Sprintf("can't connect to database: %v", err))
	}
	fmt.Println("Connected to database test")

	fmt.Println("Staring blind write transaction, to create some keys in db")
	tx, err := db.Begin(opt.TxOptions)
	if err != nil {
		log.Fatalln("can't begin transaction")
	}

	fmt.Println("Adding [key1] = \"value1\"")
	if err = tx.Put("key1", []byte("value1")); err != nil {
		log.Fatalln("can't put value for key key1 to database")
	}

	fmt.Println("Adding [key2] = \"value2\"")
	if err = tx.Put("key2", []byte("value2")); err != nil {
		log.Fatalln("can't put value for key key2 to database")
	}

	fmt.Println("Committing transaction")
	if _, err = tx.Commit(); err != nil {
		log.Fatalln("can't commit transaction to database")
	}

	time.Sleep(time.Millisecond + 10)

	fmt.Println("Reading key1 directly from database")
	val1, err := db.Get("key1")
	if err != nil {
		log.Fatalln("can't get value for key key1 from database")
	}
	fmt.Printf("[key1] = %s\n", string(val1))

	fmt.Println("Starting second transaction")
	tx, err = db.Begin(opt.TxOptions)
	if err != nil {
		log.Fatalln("can't begin transaction")
	}

	fmt.Println("Reading key2 in tx context")
	val2, err := tx.Get("key2")
	if err != nil {
		log.Fatalln("can't get value for key key1 from database")
	}
	fmt.Printf("[key2] = %s\n", string(val2))

	fmt.Printf("Storing [key3] = \"%s\" + \"%s\"\n", string(val1), string(val2))
	if err = tx.Put("key3", []byte(string(val1)+string(val2))); err != nil {
		log.Fatalln("can't put value for key key3 to database")
	}

	fmt.Println("Committing transaction")
	if _, err = tx.Commit(); err != nil {
		log.Fatalln("can't commit transaction to database")
	}
}

func createOptions(port string) *config.Options {
	connOpts := []*config.ConnectionOption{
		{
			URL: fmt.Sprintf("http://localhost:%s/", port),
		},
	}
	userOpt := &config.IdentityOptions{
		UserID: "testUser",
		Signer: &crypto.SignerOptions{
			KeyFilePath: "pkg/database/testdata/client.key",
		},
	}
	serverVerifyOpt := &crypto_utils.VerificationOptions{
		CAFilePath: "pkg/database/testdata/ca_service.cert",
	}
	return &config.Options{
		ConnectionOptions: connOpts,
		User:              userOpt,
		ServersVerify:     serverVerifyOpt,
		TxOptions: &config.TxOptions{
			TxIsolation:   config.Serializable,
			ReadOptions:   &config.ReadOptions{QuorumSize: 1},
			CommitOptions: &config.CommitOptions{QuorumSize: 1},
		},
	}
}
