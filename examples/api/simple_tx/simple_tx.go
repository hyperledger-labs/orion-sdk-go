package main

import (
	"fmt"

	"github.com/IBM-Blockchain/bcdb-sdk/pkg/bcdb"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
)

func main() {
	c, err := ReadConfig("./config.yml")
	if err != nil {
		fmt.Printf(err.Error())
	}

	logger, err := logger.New(
		&logger.Config{
			Level:         "debug",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		},
	)

	conConf := &config.ConnectionConfig{
		ReplicaSet: c.ConnectionConfig.ReplicaSet,
		RootCAs:    c.ConnectionConfig.RootCAs,
		Logger:     logger,
	}

	fmt.Println("Opening connection to database, configuration: ", c.ConnectionConfig)
	db, err := bcdb.Create(conConf)
	if err != nil {
		fmt.Printf("Database connection creating failed, reason: %s\n", err.Error())
		return
	}

	sessionConf := &config.SessionConfig{
		UserConfig:   c.SessionConfig.UserConfig,
		TxTimeout:    c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	fmt.Println("Opening session to database, configuration: ", c.SessionConfig)
	session, err := db.Session(sessionConf)
	if err != nil {
		fmt.Printf("Database session creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Opening data transaction")
	tx, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Printf("Adding key, value: key1, val1 to the database")
	err = tx.Put("bdb", "key1", []byte("val1"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Printf("Adding key, value: key2, val2 to the database")
	err = tx.Put("bdb", "key2", []byte("val2"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Printf("Committing transaction")
	txID, _, err := tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number " + txID + " committed successfully")

	fmt.Println("Opening data transaction")
	tx, err = session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Printf("Getting key1 value")
	val, _, err := tx.Get("bdb", "key1")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("key1 value is " + string(val))

	fmt.Printf("Updating key1 value to val0")
	err = tx.Put("bdb", "key1", []byte("val0"), nil)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return
	}

	fmt.Printf("Deleting key2 from the database")
	err = tx.Delete("bdb", "key2")
	if err != nil {
		fmt.Printf("Deleting key from database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Printf("Committing transaction")
	txID, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number " + txID + " committed successfully")
}
