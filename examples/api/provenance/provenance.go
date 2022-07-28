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

	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

/*
	An example that shows how to get to the following data:
	   - key->value history, in different views and directions
	   - info about users who accessed specific pieces of data
	   - info, including history, about data accessed by user
	   - history of user transactions
*/
func main() {
	if err := executeProvenanceExample("../../../../orion-server/deployment/crypto/", "../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeProvenanceExample(cryptoDir string, configFile string) error {
	session, err := prepareData(cryptoDir, configFile)
	if session == nil || err != nil {
		return err
	}

	fmt.Println("Getting provenance access object")
	provenance, err := session.Provenance()
	if err != nil {
		fmt.Printf("Getting provenance access object failed, reason: %s\n", err.Error())
		return err
	}

	//Getting full history of changes in value of key in database.
	fmt.Println("Getting all historical values for database db2 and key key1")
	res1, err := provenance.GetHistoricalData("db2", "key1")
	if err != nil {
		fmt.Printf("Getting all historical values failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("historical values for database db2 and key key1 are:")
	for i := 0; i < len(res1); i++ {
		fmt.Printf("value: %s, version: %s, read users: %v, read-write users: %v\n", res1[i].GetValue(),
			res1[i].GetMetadata().GetVersion(), res1[i].GetMetadata().GetAccessControl().GetReadUsers(),
			res1[i].GetMetadata().GetAccessControl().GetReadWriteUsers())
	}

	fmt.Println("Getting all historical values for database db2 and key key3")
	res1, err = provenance.GetHistoricalData("db2", "key3")
	if err != nil {
		fmt.Printf("Getting all historical values failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("historical values for database db2 and key key3 are:")
	for i := 0; i < len(res1); i++ {
		fmt.Printf("value: %s, version: %s, read users: %v, read-write users: %v\n", res1[i].GetValue(),
			res1[i].GetMetadata().GetVersion(), res1[i].GetMetadata().GetAccessControl().GetReadUsers(),
			res1[i].GetMetadata().GetAccessControl().GetReadWriteUsers())
	}

	//Getting history for specific version
	fmt.Println("Getting all historical values for database db2 and key key2 starting from block 6")
	res2, err := provenance.GetNextHistoricalData("db2", "key2", &types.Version{
		BlockNum: 6,
		TxNum:    0,
	})
	if err != nil {
		fmt.Printf("Getting all historical values failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("historical values for database db2 and key key2 starting from block 6 are:")
	for i := 0; i < len(res2); i++ {
		fmt.Printf("value: %s, version: %s, read users: %v, read-write users: %v\n", res2[i].GetValue(),
			res2[i].GetMetadata().GetVersion(), res2[i].GetMetadata().GetAccessControl().GetReadUsers(),
			res2[i].GetMetadata().GetAccessControl().GetReadWriteUsers())
	}

	fmt.Println("Getting all historical values for database db2 and key key3 going down from block 5")
	res2, err = provenance.GetPreviousHistoricalData("db2", "key3", &types.Version{
		BlockNum: 5,
		TxNum:    0,
	})
	if err != nil {
		fmt.Printf("Getting all historical values failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("historical values for database db2 and key key3 going down from block 5 are:")
	for i := 0; i < len(res2); i++ {
		fmt.Printf("value: %s, version: %s, read users: %v, read-write users: %v\n", res2[i].GetValue(),
			res2[i].GetMetadata().GetVersion(), res2[i].GetMetadata().GetAccessControl().GetReadUsers(),
			res2[i].GetMetadata().GetAccessControl().GetReadWriteUsers())
	}

	fmt.Println("Getting all historical values for database db2 and key key1 at block 6")
	res3, err := provenance.GetHistoricalDataAt("db2", "key1", &types.Version{
		BlockNum: 6,
		TxNum:    0,
	})
	if err != nil {
		fmt.Printf("Getting all historical values failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("historical values for database db2 and key key1 at block 6 are:")
	fmt.Printf("value: %s, version: %s, read users: %v, read-write users: %v\n", res3.GetValue(),
		res3.GetMetadata().GetVersion(), res3.GetMetadata().GetAccessControl().GetReadUsers(),
		res3.GetMetadata().GetAccessControl().GetReadWriteUsers())

	//Getting history of user transactions
	fmt.Println("Getting IDs of all tx submitted by user alice")
	res4, err := provenance.GetTxIDsSubmittedByUser("alice")
	if err != nil {
		fmt.Printf("Getting IDs of all tx submitted by user alice failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("IDs of all tx submitted by user alice are:")
	for i := 0; i < len(res4); i++ {
		fmt.Printf("%s\n", res4[i])
	}

	fmt.Println("Getting all user reads for user alice")
	res5, err := provenance.GetDataReadByUser("alice")
	if err != nil {
		fmt.Printf("Getting all user reads for user alice failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("All user reads for user alice are:")
	for dbName, reads := range res5 {
		for _, kv := range reads.KVs {
			fmt.Printf("dbname: %s, key: %s, value: %s, version: %s\n", dbName, kv.GetKey(), kv.GetValue(), kv.GetMetadata().GetVersion())
		}
	}

	fmt.Println("Getting all user writes for user alice")
	res6, err := provenance.GetDataWrittenByUser("alice")
	if err != nil {
		fmt.Printf("Getting all user writes for user alice failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Println("All user writes for user alice are:")
	for dbName, writes := range res6 {
		for _, kv := range writes.KVs {
			fmt.Printf("dbname: %s, key: %s, value: %s, version: %s\n", dbName, kv.GetKey(), kv.GetValue(), kv.GetMetadata().GetVersion())
		}
	}

	return nil
}

/*
	- Create database db2
	- Create and add user alice to db2
	- Execute 2 synchronous data transactions by user alice
	- "key1" and "key2" added in first transaction, read and updated in second transaction
	- "key3" added in first transaction, deleted in second transaction
*/
func prepareData(cryptoDir string, configFile string) (bcdb.DBSession, error) {
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

	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Checking whenever database db2 already exists")
	exist, err := dbTx.Exists("db2")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return nil, err
	}
	if exist {
		fmt.Println("Deleting db2")
		err = dbTx.DeleteDB("db2")
		if err != nil {
			fmt.Printf("Deleting db2 failed, reason: %s\n", err.Error())
			return nil, err
		}
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return nil, err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//create database db2
	fmt.Println("Opening database transaction")
	dbTx, err = session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Creating new database db2")
	err = dbTx.CreateDB("db2", nil)
	if err != nil {
		fmt.Printf("New database creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Committing transaction")
	txID, _, err = dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return nil, err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//create user alice
	fmt.Println("Opening user transaction")
	tx, err := session.UsersTx()
	if err != nil {
		fmt.Printf("User transaction creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	//reading and decoding alice's certificate
	alicePemUserCert, err := ioutil.ReadFile(path.Join(cryptoDir, "alice", "alice.pem"))
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}
	aliceCertBlock, _ := pem.Decode(alicePemUserCert)

	dbPerm := map[string]types.Privilege_Access{
		"db2": types.Privilege_ReadWrite,
	}
	alice := &types.User{
		Id:          "alice",
		Certificate: aliceCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: dbPerm,
		}}

	acl := types.AccessControl{
		ReadUsers: map[string]bool{
			"alice": true,
		},
		ReadWriteUsers: map[string]bool{
			"alice": true,
		},
	}

	fmt.Println("Adding alice to the database")
	err = tx.PutUser(alice, nil)
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Committing transaction")
	txID, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return nil, err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//create alice session
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
		return nil, err
	}

	//Alice adding multiple keys: key1, key2 and key3
	fmt.Println("Opening alice data transaction")
	aliceTx1, err := aliceSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Adding key, value: key1, val1 to the database")
	err = aliceTx1.Put("db2", "key1", []byte("val1"), &acl)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Adding key, value: key2, val2 to the database")
	err = aliceTx1.Put("db2", "key2", []byte("val2"), &acl)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Adding key, value: key3, val3 to the database")
	err = aliceTx1.Put("db2", "key3", []byte("val3"), &acl)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Committing alice transaction")
	txID, _, err = aliceTx1.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return nil, err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//Alice reading multiple keys: key1 and key2, writing to multiple keys: key1 and key2 and deleting key3.
	fmt.Println("Opening alice data transaction")
	aliceTx2, err := aliceSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Getting key1 value")
	val, _, err := aliceTx2.Get("db2", "key1")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return nil, err
	}
	fmt.Printf("key1 value is %s\n", string(val))

	fmt.Println("Getting key2 value")
	val, _, err = aliceTx2.Get("db2", "key2")
	if err != nil {
		fmt.Printf("Getting existing key value failed, reason: %s\n", err.Error())
		return nil, err
	}
	fmt.Printf("key2 value is %s\n", string(val))

	fmt.Println("Updating key1 value to newVal1")
	err = aliceTx2.Put("db2", "key1", []byte("newVal1"), &acl)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Updating key2 value to newVal2")
	err = aliceTx2.Put("db2", "key2", []byte("newVal2"), &acl)
	if err != nil {
		fmt.Printf("Updating value failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Deleting key3 from the database")
	err = aliceTx2.Delete("db2", "key3")
	if err != nil {
		fmt.Printf("Deleting key from database failed, reason: %s\n", err.Error())
		return nil, err
	}

	fmt.Println("Committing transaction")
	txID, _, err = aliceTx2.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return nil, err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return session, err
}
