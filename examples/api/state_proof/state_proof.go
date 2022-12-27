// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

func main() {
	if err := ExecuteStateTrieExample("../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func ExecuteStateTrieExample(configLocation string) error {
	session, err := OpenSession(configLocation)
	if err != nil {
		fmt.Printf("Database session creating failed, reason: %s\n", err.Error())
		return err
	}

	_, receipt1, err := SyncDataUpdate(session, []string{"key1", "key2"}, []string{"val1", "val2"}, []string{})
	if err != nil {
		fmt.Printf("Syncronious update data failed, reason: %s\n", err.Error())
		return err
	}

	_, receipt2, err := SyncDataUpdate(session, []string{"key1"}, []string{"val0"}, []string{"key2"})
	if err != nil {
		fmt.Printf("Syncronious update data failed, reason: %s\n", err.Error())
		return err
	}

	ledger, err := session.Ledger()
	if err != nil {
		fmt.Printf("Can't get access to ledger, reason: %s\n", err.Error())
		return err
	}

	// key1 has value val1 after the first transaction (receipt1), lets prove it
	// Value to verify
	dbName := "bdb"
	key := "key1"
	value := "val1"
	deleted := false

	// Lets query server for proof for value associated with key key1 in the data block pointed to by receipt1
	fmt.Printf("Getting proof from server for db %s, key %s, value %s in block %d\n", dbName, key, value, receipt1.GetHeader().GetBaseHeader().GetNumber())
	stateProof, err := ledger.GetDataProof(receipt1.GetHeader().GetBaseHeader().GetNumber(), dbName, key, deleted)
	if err != nil {
		fmt.Printf("Can't get state proof ledger, reason: %s\n", err.Error())
		return err
	}

	fmt.Printf("Calculate value hash in Merkle-Patricia trie (stored as field in Branch or Value nodes) for db %s, key %s, value %s in state trie, by concatinating db, key and value hashes\n", dbName, key, value)
	valueHash, err := bcdb.CalculateValueHash(dbName, key, []byte(value))
	if err != nil {
		fmt.Printf("Failed to calculate value hash in Merkle-Patricia trie , reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Verify proof using value hash and Merkle-Patricia trie root stored in block header")
	isCorrect, err := stateProof.Verify(valueHash, receipt1.GetHeader().GetStateMerkleTreeRootHash(), deleted)
	if err != nil {
		fmt.Printf("Can't verify value in state trie, reason: %s\n", err.Error())
		return err
	}

	if isCorrect {
		fmt.Printf("Value [%s, %s, %s] is part of state trie at block %d\n", dbName, key, value, receipt1.GetHeader().GetBaseHeader().GetNumber())
	} else {
		errors.Errorf("Value [%s, %s, %s] is not part of state trie at block %d\n", dbName, key, value, receipt1.GetHeader().GetBaseHeader().GetNumber())
	}

	// key1 has value val0 after the second transaction (receipt2), lets prove it
	// Value to verify
	value = "val0"
	// Lets query server for proof for value associated with key key1 in the data block pointed to by receipt2
	fmt.Printf("Getting proof from server for db %s, key %s, value %s in block %d\n", dbName, key, value, receipt2.GetHeader().GetBaseHeader().GetNumber())
	stateProof, err = ledger.GetDataProof(receipt2.GetHeader().GetBaseHeader().GetNumber(), dbName, key, deleted)
	if err != nil {
		fmt.Printf("Can't get state proof ledger, reason: %s\n", err.Error())
		return err
	}

	fmt.Printf("Calculate value hash in Merkle-Patricia trie (stored as field in Branch or Value nodes) for db %s, key %s, value %s in state trie, by concatinating db, key and value hashes\n", dbName, key, value)
	valueHash, err = bcdb.CalculateValueHash(dbName, key, []byte(value))
	if err != nil {
		fmt.Printf("Failed to calculate value hash in Merkle-Patricia trie , reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Verify proof using value hash and Merkle-Patricia trie root stored in block header")
	isCorrect, err = stateProof.Verify(valueHash, receipt2.GetHeader().GetStateMerkleTreeRootHash(), deleted)
	if err != nil {
		fmt.Printf("Can't verify value in state trie, reason: %s\n", err.Error())
		return err
	}

	if isCorrect {
		fmt.Printf("Value [%s, %s, %s] is part of state trie at block %d\n", dbName, key, value, receipt2.GetHeader().GetBaseHeader().GetNumber())
	} else {
		return errors.Errorf("Value [%s, %s, %s] is not part of state trie at block %d\n", dbName, key, value, receipt2.GetHeader().GetBaseHeader().GetNumber())
	}

	// Old value of key1 is overwritten in second transaction
	// Because proof query from server doesn't contains value as argument, we can use proof from above
	value = "val1"
	fmt.Printf("Calculate value hash in Merkle-Patricia trie (stored as field in Branch or Value nodes) for db %s, key %s, value %s in state trie, by concatinating db, key and value hashes\n", dbName, key, value)
	valueHash, err = bcdb.CalculateValueHash(dbName, key, []byte(value))
	if err != nil {
		fmt.Printf("Failed to calculate value hash in Merkle-Patricia trie , reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Verify proof using value hash and Merkle-Patricia trie root stored in block header")
	isCorrect, err = stateProof.Verify(valueHash, receipt2.GetHeader().GetStateMerkleTreeRootHash(), deleted)
	if err != nil {
		fmt.Printf("Can't verify value in state trie, reason: %s\n", err.Error())
		return err
	}

	if isCorrect {
		return errors.Errorf("Value [%s, %s, %s] is part of state trie at block %d\n", dbName, key, value, receipt2.Header.BaseHeader.Number)
	} else {
		fmt.Printf("Value [%s, %s, %s] is not part of state trie at block %d\n", dbName, key, value, receipt2.Header.BaseHeader.Number)
	}

	// Key key2 value val2 was deleted in the second transaction, lets prove it
	// by querying server for proof with deleted flag set to true
	key = "key2"
	deleted = true
	value = "val2"
	fmt.Printf("Verify key is deleted: db %s, key %s, value %s in block %d\n", dbName, key, value, receipt2.GetHeader().GetBaseHeader().GetNumber())
	stateProof, err = ledger.GetDataProof(receipt2.GetHeader().GetBaseHeader().GetNumber(), dbName, key, deleted)
	if err != nil {
		fmt.Printf("Can't get state proof ledger, reason: %s\n", err.Error())
		return err
	}

	fmt.Printf("Calculate value hash in Merkle-Patricia trie (stored as field in Branch or Value nodes) for db %s, key %s, value %s in state trie, by concatinating db, key and value hashes\n", dbName, key, value)
	valueHash, err = bcdb.CalculateValueHash(dbName, key, []byte(value))
	if err != nil {
		fmt.Printf("Failed to calculate value hash in Merkle-Patricia trie , reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Verify proof using value hash and Merkle-Patricia trie root stored in block header")
	isCorrect, err = stateProof.Verify(valueHash, receipt2.GetHeader().GetStateMerkleTreeRootHash(), deleted)
	if err != nil {
		fmt.Printf("Can't verify value in state trie, reason: %s\n", err.Error())
		return err
	}

	if isCorrect {
		fmt.Printf("Value [%s, %s, %s] was deleted in state trie at block %d\n", dbName, key, value, receipt2.GetHeader().GetBaseHeader().GetNumber())
	} else {
		errors.Errorf("Value [%s, %s, %s] wasn't deleted in state trie at block %d\n", dbName, key, value, receipt2.GetHeader().GetBaseHeader().GetNumber())
	}
	return nil
}

func OpenSession(configLocation string) (bcdb.DBSession, error) {
	c, err := util.ReadConfig(configLocation)
	if err != nil {
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
		return nil, err
	}

	sessionConf := &config.SessionConfig{
		UserConfig:   c.SessionConfig.UserConfig,
		TxTimeout:    c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	fmt.Println("Opening session to database, configuration: ", c.SessionConfig)
	session, err := db.Session(sessionConf)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func SyncDataUpdate(session bcdb.DBSession, updateKeys, updateValues, deleteKeys []string) (string, *types.TxReceipt, error) {
	fmt.Println("Opening data transaction")
	tx, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return "", nil, err
	}

	for i := 0; i < len(updateKeys); i++ {
		key := updateKeys[i]
		value := updateValues[i]
		fmt.Printf("Updating key value (%s, %s) in the database\n", key, value)
		err = tx.Put("bdb", key, []byte(value), nil)
		if err != nil {
			fmt.Printf("Update key (%s) value in the database failed, reason: %s\n", key, err.Error())
			return "", nil, err
		}
	}

	for _, key := range deleteKeys {
		fmt.Printf("Deleting key (%s) from the database\n", key)
		err = tx.Delete("bdb", key)
		if err != nil {
			fmt.Printf("Deleting key (%s) from the database failed, reason: %s\n", key, err.Error())
			return "", nil, err
		}

	}

	fmt.Println("Committing transaction synchronously")
	txID, receiptEnv, err := tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return "", nil, err
	}
	receipt := receiptEnv.GetResponse().GetReceipt()
	fmt.Printf("Transaction ID %s committed successfully in block %d\n", txID, receipt.GetHeader().GetBaseHeader().GetNumber())
	return txID, receipt, nil
}
