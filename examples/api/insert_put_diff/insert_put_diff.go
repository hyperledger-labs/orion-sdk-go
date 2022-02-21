// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

/*
	Showing the difference between insert and put key-value pairs to the database
	tx1 - inserting key1, val1 to the database, using Get to check if key1 already exists
	tx2 - inserting key2, val2 to the database, using AssertRead to check if key2 already exists
	tx3 - putting key3, val3 in the database
*/
func main() {
	if err := executeInsertPutExample("../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeInsertPutExample(configFile string) error {
	session, err := prepareData(configFile)
	if session == nil || err != nil {
		return err
	}

	err = clearDB(session)
	if err != nil {
		fmt.Printf("Clearing database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Opening data transaction")
	tx1, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Inserting new key, value: key1, val1 to the database")
	/* reading a non-existing value in a transaction injects a dependency on version 0,0 for key1,
	which ensures the put will be an insert, that is, it will succeed only if key1 does not exist */
	recordBytes, _, err := tx1.Get("bdb", "key1")
	if err != nil {
		fmt.Printf("Getting key1 value failed, reason: %s\n", err.Error())
		return err
	}
	if recordBytes != nil {
		fmt.Println("key1 already exists in the database")
		return err
	}
	err = tx1.Put("bdb", "key1", []byte("val1"), nil)
	if err != nil {
		fmt.Printf("Inserting a new key to the database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err := tx1.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	fmt.Println("Opening data transaction")
	tx2, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Inserting new key, value: key2, val2 to the database")
	/* AssertRead with a nil version in a transaction injects a dependency on version 0,0 for key2,
	which ensures the put will be an insert, that is, it will succeed only if key2 does not exist */
	err = tx2.AssertRead("bdb", "key2", nil)
	if err != nil {
		fmt.Printf("AssertRead failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("key2 version is nil => key2 doesn't exist in the database")

	err = tx2.Put("bdb", "key2", []byte("val2"), nil)
	if err != nil {
		fmt.Printf("Inserting a new key to the database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err = tx2.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	fmt.Println("Opening data transaction")
	tx3, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Adding key, value: key3, val3 to the database")
	/* using Put without checking if key3 already exists in the database will either insert key3 to the database or
	update the value of key3 */
	err = tx3.Put("bdb", "key3", []byte("val3"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err = tx3.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

func prepareData(configFile string) (bcdb.DBSession, error) {
	c, err := util.ReadConfig(configFile)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
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
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}

	conConf := &config.ConnectionConfig{
		ReplicaSet: c.ConnectionConfig.ReplicaSet,
		RootCAs:    c.ConnectionConfig.RootCAs,
		Logger:     logger,
	}

	fmt.Println("Opening connection to database, configuration: ", c.ConnectionConfig)
	db, err := bcdb.Create(conConf)
	if err != nil {
		fmt.Printf("Database connection creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	sessionConf := &config.SessionConfig{
		UserConfig:   c.SessionConfig.UserConfig,
		TxTimeout:    c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	fmt.Println("Opening session to database, configuration: ", c.SessionConfig)
	session, err := db.Session(sessionConf)
	if err != nil {
		fmt.Printf("Database session creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	return session, nil
}

func clearDB(session bcdb.DBSession) error {
	fmt.Println("Opening data transaction")
	tx, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	val, _, err := tx.Get("bdb", "key1")
	if err != nil {
		fmt.Printf("Getting key1 value failed, reason: %s\n", err.Error())
		return err
	}
	if val != nil {
		fmt.Println("Deleting key1 from the database")
		err = tx.Delete("bdb", "key1")
		if err != nil {
			fmt.Printf("Deleting key from database failed, reason: %s\n", err.Error())
			return err
		}
	}

	val, _, err = tx.Get("bdb", "key2")
	if err != nil {
		fmt.Printf("Getting key2 value failed, reason: %s\n", err.Error())
		return err
	}
	if val != nil {
		fmt.Println("Deleting key2 from the database")
		err = tx.Delete("bdb", "key2")
		if err != nil {
			fmt.Printf("Deleting key from database failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Committing transaction")
	txID, _, err := tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)
	return nil
}
