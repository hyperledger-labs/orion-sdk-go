package main

import (
	"fmt"
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
	session, err := prepareData()
	if session == nil || err != nil {
		return
	}

	fmt.Println("Opening data transaction")
	tx, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Adding key, value: key1, val1 to the database")
	err = tx.Put("bdb", "key1", []byte("val1"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing transaction")
	txID, txReceipt, err := tx.Commit(false)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	//async commit always return receipt = nil
	fmt.Printf("Transaction receipt = %s\n", txReceipt)

	l, err := session.Ledger()
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	fmt.Println("Getting transaction receipt")
LOOP:
	for {
		timeout := time.After(5 * time.Second)
		select {
		case <-time.After(10 * time.Millisecond):
			txReceipt, err = l.GetTransactionReceipt(txID)
			if err != nil {
				fmt.Printf("Getting transaction receipt failed, reason: %s\n", err.Error())
				return
			}
			if txReceipt == nil {
				continue
			} else {
				break LOOP
			}
		case <-timeout:
			fmt.Println("Getting transaction receipt failed")
			return
		}
	}

	fmt.Printf("The transaction is stored on block header number %d, index %d, with validiation flag %s\n", txReceipt.Header.GetBaseHeader().GetNumber(),
		txReceipt.GetTxIndex(), txReceipt.Header.ValidationInfo[txReceipt.TxIndex].GetFlag())
	fmt.Printf("Transaction number %s committed successfully\n", txID)
}

func prepareData() (bcdb.DBSession, error) {
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
