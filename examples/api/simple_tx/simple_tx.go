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
	Add, get, update and delete key-value pairs on the database
*/
func main() {
	if err := executeSimpleTxExample("../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeSimpleTxExample(configFile string) error {
	c, err := util.ReadConfig(configFile)
	if err != nil {
		fmt.Printf(err.Error())
		return err
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
		return err
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
		return err
	}

	sessionConf := &config.SessionConfig{
		UserConfig:   c.SessionConfig.UserConfig,
		TxTimeout:    c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	fmt.Println("Opening session to database, configuration: ", c.SessionConfig)
	session, err := db.Session(sessionConf)
	if err != nil {
		fmt.Printf("Database session creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Opening data transaction")
	tx, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Adding key, value: key1, val1 to the database")
	err = tx.Put("bdb", "key1", []byte("val1"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Adding key, value: key2, val2 to the database")
	err = tx.Put("bdb", "key2", []byte("val2"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err := tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	fmt.Println("Opening data transaction")
	tx, err = session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Getting key1 value")
	val, _, err := tx.Get("bdb", "key1")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("key1 value is %s\n", string(val))

	fmt.Println("Updating key1 value to val0")
	err = tx.Put("bdb", "key1", []byte("val0"), nil)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Deleting key2 from the database")
	err = tx.Delete("bdb", "key2")
	if err != nil {
		fmt.Printf("Deleting key from database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}
