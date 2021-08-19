package main

import (
	"fmt"
	"strconv"

	"github.com/IBM-Blockchain/bcdb-sdk/examples/util"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/bcdb"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

/*
	Two transactions try to modify the value of the same key, one transaction will be valid and the other will not.
	It's not possible to know in advance which one of the transactions will be valid, as the server may reorder them.
*/
func main() {
	c, err := util.ReadConfig("../../util/config.yml")
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

	fmt.Println("Adding key, value: key1, 1 to the database")
	err = tx.Put("bdb", "key1", []byte("1"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing transaction")
	txID, _, err := tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	fmt.Println("Opening data transaction tx1")
	tx1, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Opening data transaction tx2")
	tx2, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("tx1 - Getting key1 value")
	val1, _, err := tx1.Get("bdb", "key1")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("key1 value is %s\n", string(val1))
	newVal1, _ := strconv.Atoi(string(val1))
	newVal1 += 1

	fmt.Println("tx 2 - Getting key1 value")
	val2, _, err := tx2.Get("bdb", "key1")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("key1 value is %s\n", string(val2))
	newVal2, _ := strconv.Atoi(string(val2))
	newVal2 += 2

	fmt.Printf("tx1 - Updating key1 value to %s\n", strconv.Itoa(newVal1))
	err = tx1.Put("bdb", "key1", []byte(strconv.Itoa(newVal1)), nil)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return
	}

	fmt.Printf("tx2 - Updating key1 value to %s\n", strconv.Itoa(newVal2))
	err = tx2.Put("bdb", "key1", []byte(strconv.Itoa(newVal2)), nil)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing transaction tx1")
	tx1_ID, tx1_receipt, err := tx1.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	if tx1_receipt.Header.ValidationInfo[tx1_receipt.TxIndex].Flag != types.Flag_VALID {
		fmt.Printf("Transaction tx1 is invalid, reason: %s\n", tx1_receipt.Header.ValidationInfo[tx1_receipt.TxIndex].ReasonIfInvalid)
	}
	fmt.Printf("Transaction number %s committed successfully\n", tx1_ID)

	fmt.Println("Committing transaction tx2")
	tx2_ID, tx2_receipt, err := tx2.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	if tx2_receipt.Header.ValidationInfo[tx2_receipt.TxIndex].Flag != types.Flag_VALID {
		fmt.Printf("Transaction tx2 is invalid, reason: %s\n", tx2_receipt.Header.ValidationInfo[tx2_receipt.TxIndex].ReasonIfInvalid)
	}
	fmt.Printf("Transaction number %s committed successfully\n", tx2_ID)
}
