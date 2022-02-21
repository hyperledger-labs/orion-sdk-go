// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

/*
	In this example, two transactions try to modify the value of the same key, one transaction will be valid and the other will not.
	In the case of sync-commit, the first transaction will be valid and the second will not.
	In the case of async-commit, it's not possible to know in advance which one of the transactions will be valid, as the server may reorder them.
*/
func main() {
	if err := executeTxValidityExample("../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeTxValidityExample(configFile string) error {
	session, err := prepareData(configFile)
	if session == nil || err != nil {
		return err
	}

	fmt.Println("Opening initialization data transaction")
	tx, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Adding key, value: key1, 1 to the database")
	err = tx.Put("bdb", "key1", []byte("1"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Adding key, value: key2, 2 to the database")
	err = tx.Put("bdb", "key2", []byte("2"), nil)
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

	fmt.Println("Opening data transaction tx1")
	tx1, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Opening data transaction tx2")
	tx2, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("tx1 - Getting key1 value")
	val1, metaData1, err := tx1.Get("bdb", "key1")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("key1 value is %s, version is %s\n", string(val1), metaData1.Version)
	newVal1, _ := strconv.Atoi(string(val1))
	newVal1 += 1

	fmt.Println("tx2 - Getting key1 value")
	val2, metaData2, err := tx2.Get("bdb", "key1")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("key1 value is %s, version is %s\n", string(val1), metaData2.Version)
	newVal2, _ := strconv.Atoi(string(val2))
	newVal2 += 2

	fmt.Printf("tx1 and tx2 have the same read set: dbName: bdb, key: key1, version: %s\n", metaData1.Version)

	fmt.Printf("tx1 - Updating key1 value to %s\n", strconv.Itoa(newVal1))
	err = tx1.Put("bdb", "key1", []byte(strconv.Itoa(newVal1)), nil)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Printf("tx2 - Updating key1 value to %s\n", strconv.Itoa(newVal2))
	err = tx2.Put("bdb", "key1", []byte(strconv.Itoa(newVal2)), nil)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction tx1")
	tx1Id, receiptEnv, err := tx1.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		if receiptEnv == nil {
			return err
		}
	} else {
		fmt.Printf("Transaction number %s committed successfully\n", tx1Id)
	}

	tx1Receipt := receiptEnv.GetResponse().GetReceipt()
	tx1Flag := tx1Receipt.GetHeader().GetValidationInfo()[tx1Receipt.GetTxIndex()].GetFlag()
	if err == nil {
		fmt.Printf("Transaction number %s validation flag is %s\n", tx1Id, tx1Flag)
	}
	fmt.Printf("Transaction number %s committed successfully\n", tx1Id)

	fmt.Println("Committing transaction tx2")
	tx2Id, receiptEnv, err := tx2.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		if receiptEnv == nil {
			return err
		}
	} else {
		fmt.Printf("Transaction number %s committed successfully\n", tx2Id)
	}

	var tx2Flag types.Flag
	if receiptEnv != nil {
		tx2Receipt := receiptEnv.GetResponse().GetReceipt()
		tx2Flag = tx2Receipt.Header.ValidationInfo[tx2Receipt.TxIndex].Flag
		fmt.Printf("Transaction number %s validation flag is %s, reason: %s\n", tx2Id, tx2Flag,
			tx2Receipt.Header.ValidationInfo[tx2Receipt.TxIndex].ReasonIfInvalid)
	}

	if tx1Flag == types.Flag_VALID && tx2Flag == types.Flag_VALID {
		println("Error - both tx1 and tx2 are valid")
		return errors.New("both tx1 and tx2 are valid")
	}
	if tx1Flag != types.Flag_VALID && tx2Flag != types.Flag_VALID {
		println("Error - both tx1 and tx2 are invalid")
		return errors.New("both tx1 and tx2 are invalid")
	}

	fmt.Println("Opening data transaction tx3")
	tx3, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Opening data transaction tx4")
	tx4, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("tx3 - Getting key2 value")
	val3, metaData3, err := tx3.Get("bdb", "key2")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("key2 value is %s, version is %s\n", string(val3), metaData3.Version)
	newVal3, _ := strconv.Atoi(string(val3))
	newVal3 += 1

	fmt.Println("tx4 - Getting key2 value")
	val4, metaData4, err := tx4.Get("bdb", "key2")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("key2 value is %s, version is %s\n", string(val4), metaData4.Version)
	newVal4, _ := strconv.Atoi(string(val4))
	newVal4 += 2

	fmt.Printf("tx3 and tx4 have the same read set: dbName: bdb, key: key2, version: %s\n", metaData3.Version)

	fmt.Printf("tx3 - Updating key2 value to %s\n", strconv.Itoa(newVal3))
	err = tx3.Put("bdb", "key2", []byte(strconv.Itoa(newVal3)), nil)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Printf("tx4 - Updating key2 value to %s\n", strconv.Itoa(newVal4))
	err = tx4.Put("bdb", "key2", []byte(strconv.Itoa(newVal4)), nil)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction tx3")
	tx3Id, receiptEnv, err := tx3.Commit(false)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
	}
	tx3Receipt := receiptEnv.GetResponse().GetReceipt()

	fmt.Println("Committing transaction tx4")
	tx4Id, receiptEnv, err := tx4.Commit(false)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
	}
	tx4Receipt := receiptEnv.GetResponse().GetReceipt()

	l, err := session.Ledger()
	if err != nil {
		fmt.Printf(err.Error())
		return err
	}

	fmt.Println("Getting transaction tx3 receipt")
LOOP:
	for {
		timeout := time.After(5 * time.Second)
		select {
		case <-time.After(10 * time.Millisecond):
			tx3Receipt, err = l.GetTransactionReceipt(tx3Id)
			if err != nil {
				if _, notFoundErr := err.(*bcdb.ErrorNotFound); notFoundErr {
					continue
				}
				fmt.Printf("Getting transaction receipt failed, reason: %s\n", err.Error())
				return err
			}
			if tx3Receipt == nil {
				continue
			} else {
				break LOOP
			}
		case <-timeout:
			fmt.Println("Getting transaction receipt failed")
			return errors.New("tx timeout")
		}
	}

	tx3Flag := tx3Receipt.Header.ValidationInfo[tx3Receipt.TxIndex].Flag
	fmt.Printf("Transaction number %s validation flag is %s\n", tx3Id, tx3Flag)
	if tx3Flag != types.Flag_VALID {
		fmt.Printf("Transaction number %s is invalid, reason: %s\n", tx3Id, tx3Receipt.Header.ValidationInfo[tx3Receipt.TxIndex].ReasonIfInvalid)
	} else {
		fmt.Printf("Transaction number %s committed successfully\n", tx3Id)
	}

	l, err = session.Ledger()
	if err != nil {
		fmt.Printf(err.Error())
		return err
	}
	fmt.Println("Getting transaction tx4 receipt")
LOOP2:
	for {
		timeout := time.After(5 * time.Second)
		select {
		case <-time.After(10 * time.Millisecond):
			tx4Receipt, err = l.GetTransactionReceipt(tx4Id)
			if err != nil {
				if _, notFoundErr := err.(*bcdb.ErrorNotFound); notFoundErr {
					continue
				}
				fmt.Printf("Getting transaction receipt failed, reason: %s\n", err.Error())
				return err
			}
			if tx4Receipt == nil {
				continue
			} else {
				break LOOP2
			}
		case <-timeout:
			fmt.Println("Getting transaction receipt failed")
			return errors.New("tx timeout")
		}
	}

	tx4Flag := tx4Receipt.Header.ValidationInfo[tx4Receipt.TxIndex].Flag
	fmt.Printf("Transaction number %s validation flag is %s\n", tx4Id, tx4Flag)
	if tx4Flag != types.Flag_VALID {
		fmt.Printf("Transaction number %s is invalid, reason: %s\n", tx4Id, tx4Receipt.Header.ValidationInfo[tx4Receipt.TxIndex].ReasonIfInvalid)
	} else {
		fmt.Printf("Transaction number %s committed successfully\n", tx4Id)
	}

	if tx3Flag == types.Flag_VALID && tx4Flag == types.Flag_VALID {
		println("Error - both tx3 and tx4 are valid")
		return errors.New("both tx3 and tx4 are valid")
	}
	if tx3Flag != types.Flag_VALID && tx4Flag != types.Flag_VALID {
		println("Error - both tx3 and tx4 are invalid")
		return errors.New("both tx3 and tx4 are invalid")
	}

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
		fmt.Printf("Database connection creation failed, reason: %s\n", err.Error())
		return nil, err
	}

	sessionConf := &config.SessionConfig{
		UserConfig:   c.SessionConfig.UserConfig,
		TxTimeout:    c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	fmt.Println("Opening session to database, configuration: ", c.SessionConfig)
	session, err := db.Session(sessionConf)
	if err != nil {
		fmt.Printf("Database session creation failed, reason: %s\n", err.Error())
		return nil, err
	}

	return session, err
}
