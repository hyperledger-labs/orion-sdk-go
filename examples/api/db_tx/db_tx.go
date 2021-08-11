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

	//creating db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Creating new database db1")
	err = dbTx.CreateDB("db1")
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return
	}
	fmt.Println("Creating new database db2")
	err = dbTx.CreateDB("db2")
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//checking the existence of db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Checking the existence of db1")
	exist, err := dbTx.Exists("db1")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return
	}
	if exist{
		fmt.Println("Database db1 exists")

	} else {
		fmt.Println("Database db1 does not exist")
	}

	fmt.Println("Checking the existence of db2")
	exist, err = dbTx.Exists("db2")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return
	}
	if exist{
		fmt.Println("Database db2 exists")

	} else {
		fmt.Println("Database db2 does not exist")
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//deleting db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Deleting db1")
	err = dbTx.DeleteDB("db1")
	if err != nil {
		fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
		return
	}
	fmt.Println("Deleting db2")
	err = dbTx.DeleteDB("db2")
	if err != nil {
		fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//checking the existence of db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Checking the existence of db1")
	exist, err = dbTx.Exists("db1")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return
	}
	if exist{
		fmt.Println("Database db1 exists")

	} else {
		fmt.Println("Database db1 does not exist")
	}

	fmt.Println("Checking the existence of db2")
	exist, err = dbTx.Exists("db2")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return
	}
	if exist{
		fmt.Println("Database db2 exists")

	} else {
		fmt.Println("Database db2 does not exist")
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//creating db1, db2 again
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Creating new database db1")
	err = dbTx.CreateDB("db1")
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return
	}
	fmt.Println("Creating new database db2")
	err = dbTx.CreateDB("db2")
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//creating db3, db4 and deleting db1, db2
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Creating new database db3")
	err = dbTx.CreateDB("db3")
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return
	}
	fmt.Println("Creating new database db4")
	err = dbTx.CreateDB("db4")
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Deleting db1")
	err = dbTx.DeleteDB("db1")
	if err != nil {
		fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Deleting db2")
	err = dbTx.DeleteDB("db2")
	if err != nil {
		fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)
}