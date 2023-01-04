// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

/*
   Create, update and delete database users
*/
func main() {
	if err := executeUserTxExample("../../../../orion-server/deployment/crypto/", "../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeUserTxExample(cryptoDir string, configFile string) error {
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

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

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

	fmt.Println("Opening user transaction")
	tx, err := session.UsersTx()
	if err != nil {
		fmt.Printf("User transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	dbPerm := map[string]types.Privilege_Access{
		"db1": types.Privilege_Read,
		"db2": types.Privilege_ReadWrite,
	}
	//reading and decoding alice's certificate
	alicePemUserCert, err := ioutil.ReadFile(path.Join(cryptoDir, "alice", "alice.pem"))
	if err != nil {
		fmt.Printf(err.Error())
		return err
	}
	aliceCertBlock, _ := pem.Decode(alicePemUserCert)

	alice := &types.User{
		Id:          "alice",
		Certificate: aliceCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: dbPerm,
		}}

	fmt.Println("Adding alice to the database")
	err = tx.PutUser(alice, nil)
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return err
	}

	dbPerm = map[string]types.Privilege_Access{
		"db1": types.Privilege_Read,
		"db2": types.Privilege_Read,
	}
	//reading and decoding bob's certificate
	bobPemUserCert, err := ioutil.ReadFile(path.Join(cryptoDir, "bob", "bob.pem"))
	if err != nil {
		fmt.Printf(err.Error())
		return err
	}
	bobCertBlock, _ := pem.Decode(bobPemUserCert)

	bob := &types.User{
		Id:          "bob",
		Certificate: bobCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: dbPerm,
		}}

	fmt.Println("Adding bob to the database")
	err = tx.PutUser(bob, nil)
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	fmt.Println("Opening user transaction")
	tx, err = session.UsersTx()
	if err != nil {
		fmt.Printf("User transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Getting alice's record from database")
	user, _, err := tx.GetUser("alice")
	if err != nil || user.GetId() != "alice" {
		fmt.Printf("Getting user's record from database failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("alice information: id: %s, privilege: %s\n", user.GetId(), user.GetPrivilege().String())

	fmt.Println("Getting bob's record from database")
	user, _, err = tx.GetUser("bob")
	if err != nil || user.GetId() != "bob" {
		fmt.Printf("Getting user's record from database failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("bob information: id: %s, privilege: %s\n", user.GetId(), user.GetPrivilege().String())

	fmt.Println("Committing transaction")
	txID, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	fmt.Println("Opening user transaction")
	tx, err = session.UsersTx()
	if err != nil {
		fmt.Printf("User transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	alice = &types.User{
		Id:          "alice",
		Certificate: aliceCertBlock.Bytes,
		Privilege:   nil}

	fmt.Println("Removing privileges given to alice")
	err = tx.PutUser(alice, nil)
	if err != nil {
		fmt.Printf("Updating user failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	fmt.Println("Opening user transaction")
	tx, err = session.UsersTx()
	if err != nil {
		fmt.Printf("User transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Deleting alice from the database")
	err = tx.RemoveUser("alice")
	if err != nil {
		fmt.Printf("Deleting user from database failed, reason: %s\n", err.Error())
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
