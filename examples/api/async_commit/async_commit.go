// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

/*
	Example of using async commit
*/
func main() {
	if err := executeAsyncCommitExample("../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeAsyncCommitExample(configFile string) error {
	session, err := prepareData(configFile)
	if session == nil || err != nil {
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

	fmt.Println("Committing transaction")
	txID, receiptEnv, err := tx.Commit(false)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	//async commit always return receipt = nil
	txReceipt := receiptEnv.GetResponse().GetReceipt()
	fmt.Printf("Transaction receipt = %s\n", txReceipt)

	l, err := session.Ledger()
	if err != nil {
		fmt.Printf(err.Error())
		return err
	}

	fmt.Println("Getting transaction receipt")
LOOP:
	for {
		timeout := time.After(5 * time.Second)
		select {
		case <-time.After(10 * time.Millisecond):
			txReceipt, err = l.GetTransactionReceipt(txID)
			if err != nil {
				if _, notFoundErr := err.(*bcdb.ErrorNotFound); notFoundErr {
					continue
				}
				fmt.Printf("Getting transaction receipt failed, reason: %s\n", err.Error())
				return err
			}
			if txReceipt == nil {
				continue
			} else {
				break LOOP
			}
		case <-timeout:
			fmt.Println("Getting transaction receipt failed")
			return err
		}
	}

	fmt.Printf("The transaction is stored on block header number %d, index %d, with validiation flag %s\n", txReceipt.GetHeader().GetBaseHeader().GetNumber(),
		txReceipt.GetTxIndex(), txReceipt.GetHeader().GetValidationInfo()[txReceipt.GetTxIndex()].GetFlag())
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

	return session, err
}
