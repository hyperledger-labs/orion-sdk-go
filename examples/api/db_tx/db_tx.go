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
	Create and delete databases
*/
func main() {
	if err := executeDbTxExample("../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeDbTxExample(configFile string) error {
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

	err = clearData(session)
	if err != nil {
		return err
	}

	//creating db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Creating new database db1")
	err = dbTx.CreateDB("db1", nil)
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("Creating new database db2")
	err = dbTx.CreateDB("db2", nil)
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//checking the existence of db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Checking the existence of db1")
	exist, err := dbTx.Exists("db1")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exist {
		fmt.Println("Database db1 exists")
	} else {
		fmt.Println("Database db1 does not exist")
	}

	fmt.Println("Checking the existence of db2")
	exist, err = dbTx.Exists("db2")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exist {
		fmt.Println("Database db2 exists")
	} else {
		fmt.Println("Database db2 does not exist")
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//deleting db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Deleting db1")
	err = dbTx.DeleteDB("db1")
	if err != nil {
		fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("Deleting db2")
	err = dbTx.DeleteDB("db2")
	if err != nil {
		fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//checking the existence of db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Checking the existence of db1")
	exist, err = dbTx.Exists("db1")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exist {
		fmt.Println("Database db1 exists")
	} else {
		fmt.Println("Database db1 does not exist")
	}

	fmt.Println("Checking the existence of db2")
	exist, err = dbTx.Exists("db2")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exist {
		fmt.Println("Database db2 exists")
	} else {
		fmt.Println("Database db2 does not exist")
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//creating db1, db2 again
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Creating new database db1")
	err = dbTx.CreateDB("db1", nil)
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("Creating new database db2")
	err = dbTx.CreateDB("db2", nil)
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//creating db3, db4 and deleting db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Creating new database db3")
	err = dbTx.CreateDB("db3", nil)
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("Creating new database db4")
	err = dbTx.CreateDB("db4", nil)
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Deleting db1")
	err = dbTx.DeleteDB("db1")
	if err != nil {
		fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Deleting db2")
	err = dbTx.DeleteDB("db2")
	if err != nil {
		fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

func clearData(session bcdb.DBSession) error {
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Checking whenever database db1 already exists")
	exist, err := dbTx.Exists("db1")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exist {
		fmt.Println("Deleting db1")
		err = dbTx.DeleteDB("db1")
		if err != nil {
			fmt.Printf("Deleting db1 failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Checking whenever database db2 already exists")
	exist, err = dbTx.Exists("db2")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exist {
		fmt.Println("Deleting db2")
		err = dbTx.DeleteDB("db2")
		if err != nil {
			fmt.Printf("Deleting db2 failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Checking whenever database db3 already exists")
	exist, err = dbTx.Exists("db3")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exist {
		fmt.Println("Deleting db3")
		err = dbTx.DeleteDB("db3")
		if err != nil {
			fmt.Printf("Deleting db3 failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Checking whenever database db4 already exists")
	exist, err = dbTx.Exists("db4")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exist {
		fmt.Println("Deleting db4")
		err = dbTx.DeleteDB("db4")
		if err != nil {
			fmt.Printf("Deleting db4 failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}
