// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

/*
	Example of non-valid transaction because of ACL violation,
	alice has read-write privilege on database db, but bob only has read privilege,
	each user opens a transaction that writes to the database, alice tx is valid and bob tx is non-valid
*/
func main() {
	if err := executeAclViolationExample("../../../../orion-server/deployment/crypto/", "../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeAclViolationExample(cryptoDir string, configFile string) error {
	session, db, err := openSessionAndCreateDB(configFile)
	if session == nil || db == nil || err != nil {
		return err
	}

	err = addUser(session, "alice", path.Join(cryptoDir, "alice", "alice.pem"),
		nil, []string{"db"})
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return err
	}

	err = addUser(session, "bob", path.Join(cryptoDir, "bob", "bob.pem"),
		[]string{"db"}, nil)
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return err
	}

	//alice tx
	aliceConfig := config.SessionConfig{
		UserConfig: &config.UserConfig{
			UserID:         "alice",
			CertPath:       path.Join(cryptoDir, "alice", "alice.pem"),
			PrivateKeyPath: path.Join(cryptoDir, "alice", "alice.key"),
		},
		TxTimeout: time.Second * 5,
	}
	fmt.Println("Opening alice session to database, configuration: ", aliceConfig)
	aliceSession, err := db.Session(&aliceConfig)
	if err != nil {
		fmt.Printf("Session creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Opening alice data transaction")
	aliceTx, err := aliceSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Alice - adding key, value: key1, val1 to the database")
	err = aliceTx.Put("db", "key1", []byte("val1"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing alice transaction")
	txID, receiptEnv, err := aliceTx.Commit(true)
	if err != nil {
		if receiptEnv != nil {
			receipt := receiptEnv.GetResponse().GetReceipt()
			if receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()].GetFlag() != types.Flag_VALID {
				fmt.Printf("Alice transaction is invalid, reason: %s\n", receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()].GetReasonIfInvalid())
			}
		}
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//bob tx
	bobConfig := config.SessionConfig{
		UserConfig: &config.UserConfig{
			UserID:         "bob",
			CertPath:       path.Join(cryptoDir, "bob", "bob.pem"),
			PrivateKeyPath: path.Join(cryptoDir, "bob", "bob.key"),
		},
		TxTimeout: time.Second * 5,
	}
	fmt.Println("Opening bob session to database, configuration: ", bobConfig)
	bobSession, err := db.Session(&bobConfig)
	if err != nil {
		fmt.Printf("Session creating failed, reason: %s\n", err.Error())
		return err
	}

	bobTx, err := bobSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Bob - adding key, value: key2, val2 to the database")
	err = bobTx.Put("db", "key2", []byte("val2"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing bob transaction")
	txID, receiptEnv, err = bobTx.Commit(true)
	if err == nil {
		return errors.Errorf("Unexpectedly transaction number %s committed successfully\n", txID)
	}
	if receiptEnv != nil {
		receipt := receiptEnv.GetResponse().GetReceipt()
		if receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()].GetFlag() != types.Flag_VALID {
			fmt.Printf("Bob transaction is invalid, reason: %s\n", receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()].GetReasonIfInvalid())
		}
	}
	fmt.Printf("As expected, commit failed, reason: %s\n", err.Error())
	return nil
}

func clearData(session bcdb.DBSession) error {
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Checking if database 'db' already exists")
	exists, err := dbTx.Exists("db")
	if exists {
		fmt.Println("Deleting database 'db'")
		err = dbTx.DeleteDB("db")
		if err != nil {
			fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
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

func openSessionAndCreateDB(configFile string) (bcdb.DBSession, bcdb.BCDB, error) {
	c, err := util.ReadConfig(configFile)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, nil, err
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
		return nil, nil, err
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
		return nil, nil, err
	}

	sessionConf := &config.SessionConfig{
		UserConfig:   c.SessionConfig.UserConfig,
		TxTimeout:    c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	fmt.Println("Opening session to database, configuration: ", c.SessionConfig)
	session, err := db.Session(sessionConf)
	if err != nil {
		fmt.Printf("Database session creation failed, reason: %s\n", err.Error())
		return nil, nil, err
	}

	err = clearData(session)
	if err != nil {
		return nil, nil, err
	}

	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creation failed, reason: %s\n", err.Error())
		return nil, nil, err
	}

	fmt.Println("Creating new database 'db'")
	err = dbTx.CreateDB("db", nil)
	if err != nil {
		fmt.Printf("New database creation failed, reason: %s\n", err.Error())
		return nil, nil, err
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return nil, nil, err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return session, db, err
}

func addUser(session bcdb.DBSession, userName string, pemUserCert string, readPrivilege []string, readWritePrivilege []string) error {
	fmt.Println("Opening user transaction")
	tx, err := session.UsersTx()
	if err != nil {
		fmt.Printf("User transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	//reading and decoding user's certificate
	userPemUserCert, err := ioutil.ReadFile(pemUserCert)
	if err != nil {
		fmt.Printf(err.Error())
		return err
	}
	userCertBlock, _ := pem.Decode(userPemUserCert)

	dbPerm := make(map[string]types.Privilege_Access)
	for i := 0; i < len(readPrivilege); i++ {
		dbPerm[readPrivilege[i]] = types.Privilege_Read
	}
	for i := 0; i < len(readWritePrivilege); i++ {
		dbPerm[readWritePrivilege[i]] = types.Privilege_ReadWrite
	}

	user := &types.User{
		Id:          userName,
		Certificate: userCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: dbPerm,
		}}

	fmt.Printf("Adding %s to the database\n", userName)
	err = tx.PutUser(user, nil)
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return err
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
