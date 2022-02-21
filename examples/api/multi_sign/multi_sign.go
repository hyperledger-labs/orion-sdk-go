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
	Example of using multi-sign data transaction.
	Required conditions for a multi-sign transaction to be valid:
	1. All users in the MustSignUserIDs set must co-sign the transaction.
	2. When a transaction modifies keys which have multiple users in the write ACL, or when a transaction modifies
       keys where each key has different user in the write ACL, the signature of additional users may be required.
*/
func main() {
	if err := executeMultiSignExample("../../../../orion-server/deployment/crypto/", "../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeMultiSignExample(cryptoDir string, configFile string) error {
	// creating new database 'db'
	session, db, err := openSessionAndCreateDB(configFile)
	if session == nil || db == nil || err != nil {
		return err
	}

	// creating 3 users: alice, bob and charlie, with read-write permission on the database 'db'
	aliceSession, bobSession, charlieSession, err := addUsersAndOpenUsersSessions(session, db, cryptoDir)
	if err != nil {
		return err
	}

	if err = allMustSignersSignTheTx(aliceSession, bobSession, charlieSession); err != nil {
		return err
	}
	if err = userInTheMustSignListHasNotSigned(aliceSession, bobSession); err != nil {
		return err
	}
	if err = writeUserHasNotSigned(aliceSession, bobSession); err != nil {
		return err
	}
	if err = mustSignAndReadOnlyUserHasNotSigned(aliceSession, bobSession); err != nil {
		return err
	}

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
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
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

func openUserSession(db bcdb.BCDB, userName string, cryptoDir string) (bcdb.DBSession, error) {
	userConfig := config.SessionConfig{
		UserConfig: &config.UserConfig{
			UserID:         userName,
			CertPath:       path.Join(cryptoDir, userName, userName+".pem"),
			PrivateKeyPath: path.Join(cryptoDir, userName, userName+".key"),
		},
		TxTimeout: time.Second * 5,
	}

	fmt.Println("Opening "+userName+" session to database, configuration: ", userConfig)
	userSession, err := db.Session(&userConfig)
	if err != nil {
		fmt.Printf("Session creating failed, reason: %s\n", err.Error())
		return nil, err
	}
	return userSession, nil
}

func addUsersAndOpenUsersSessions(session bcdb.DBSession, db bcdb.BCDB, cryptoDir string) (bcdb.DBSession, bcdb.DBSession, bcdb.DBSession, error) {
	err := addUser(session, "alice", path.Join(cryptoDir, "alice", "alice.pem"),
		nil, []string{"db"})
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return nil, nil, nil, err
	}

	err = addUser(session, "bob", path.Join(cryptoDir, "bob", "bob.pem"),
		nil, []string{"db"})
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return nil, nil, nil, err
	}

	err = addUser(session, "charlie", path.Join(cryptoDir, "charlie", "charlie.pem"),
		nil, []string{"db"})
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return nil, nil, nil, err
	}

	aliceSession, err := openUserSession(db, "alice", cryptoDir)
	if err != nil {
		fmt.Printf("Session creating failed, reason: %s\n", err.Error())
		return nil, nil, nil, err
	}

	bobSession, err := openUserSession(db, "bob", cryptoDir)
	if err != nil {
		fmt.Printf("Session creating failed, reason: %s\n", err.Error())
		return nil, nil, nil, err
	}

	charlieSession, err := openUserSession(db, "charlie", cryptoDir)
	if err != nil {
		fmt.Printf("Session creating failed, reason: %s\n", err.Error())
		return nil, nil, nil, err
	}

	return aliceSession, bobSession, charlieSession, err
}

/*
	- alice adds key1 to the database with nil acl, which means it inherited the ACL of the DB
	- alice adds bob and charlie to the Must sign set
	- bob co-sign, charlie signs and commits the transaction
	- the tx will be valid
*/
func allMustSignersSignTheTx(aliceSession bcdb.DBSession, bobSession bcdb.DBSession, charlieSession bcdb.DBSession) error {
	fmt.Println("")
	fmt.Println("=== Alice adds bob and charlie to the Must sign set, and they all sign the tx ===")

	fmt.Println("Alice opens data transaction")
	aliceTx, err := aliceSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Alice adds key, value: key1, val1 to the database with nil acl")
	/* key1 does not exist, therefore normally it could have been written by alice alone,
	but she insists on adding additional signers */
	err = aliceTx.Put("db", "key1", []byte("val1"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Adding bob and charlie to the multi-sign data transaction's MustSignUserIDs set")
	aliceTx.AddMustSignUser("bob")
	aliceTx.AddMustSignUser("charlie")

	fmt.Println("Alice signs the transaction and construct the envelope")
	txEnv, err := aliceTx.SignConstructedTxEnvelopeAndCloseTx()
	if err != nil {
		fmt.Printf("Failed to compose transaction envelope, reason: %s\n", err.Error())
		return err
	}
	dataTxEnv := txEnv.(*types.DataTxEnvelope)

	fmt.Println("Bob loads the transaction envelope")
	bobLoadedTx, err := bobSession.LoadDataTx(dataTxEnv)
	if err != nil {
		fmt.Printf("Loaded data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Bob inspects the transaction by:")
	fmt.Printf("  reading the Must sign users: %v\n", bobLoadedTx.MustSignUsers())
	fmt.Printf("  reading who already signed the TX: %v\n", bobLoadedTx.SignedUsers())
	fmt.Printf("  reading the data operation:\n   read operations: %v\n   write operations: %v\n   "+
		"delete operations: %v\n", bobLoadedTx.Reads(), bobLoadedTx.Writes(), bobLoadedTx.Deletes())

	fmt.Println("Bob co-signs the transaction and closes it")
	txEnv, err = bobLoadedTx.CoSignTxEnvelopeAndCloseTx()
	if err != nil {
		fmt.Printf("Failed to compose transaction envelope, reason: %s\n", err.Error())
		return err
	}
	dataTxEnv = txEnv.(*types.DataTxEnvelope)

	fmt.Println("Charlie loads the transaction envelope")
	charlieLoadedTx, err := charlieSession.LoadDataTx(dataTxEnv)
	if err != nil {
		fmt.Printf("Loaded data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Charlie inspects the transaction by:")
	fmt.Printf("  reading the Must sign users: %v\n", charlieLoadedTx.MustSignUsers())
	fmt.Printf("  reading who already signed the TX: %v\n", charlieLoadedTx.SignedUsers())
	fmt.Printf("  reading the data operation:\n   read operations: %v\n   write operations: %v\n   "+
		"delete operations: %v\n", charlieLoadedTx.Reads(), charlieLoadedTx.Writes(), charlieLoadedTx.Deletes())

	fmt.Println("Charlie signs and commits the transaction")
	txID, _, err := charlieLoadedTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

/*
	- alice adds key2 to the database with nil acl, which means it inherited the ACL of the DB
	- alice adds bob and charlie to the Must sign set
	- bob co-sign and commits the transaction, but charlie doesn't sign
	- the tx will fail because charlie should sign as well because he is a member of the Must sign set //
*/
func userInTheMustSignListHasNotSigned(aliceSession bcdb.DBSession, bobSession bcdb.DBSession) error {
	fmt.Println("")
	fmt.Println("=== Alice adds bob and charlie to the Must sign set, but only bob signs the tx ===")

	fmt.Println("Alice opens data transaction")
	aliceTx, err := aliceSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Alice adds key, value: key2, val2 to the database")
	err = aliceTx.Put("db", "key2", []byte("val2"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Adding bob and charlie to the multi-sign data transaction's MustSignUserIDs set")
	aliceTx.AddMustSignUser("bob")
	aliceTx.AddMustSignUser("charlie")

	fmt.Println("Alice signs the transaction and construct the envelope")
	txEnv, err := aliceTx.SignConstructedTxEnvelopeAndCloseTx()
	if err != nil {
		fmt.Printf("Failed to compose transaction envelope, reason: %s\n", err.Error())
		return err
	}
	dataTxEnv := txEnv.(*types.DataTxEnvelope)

	fmt.Println("Bob loads the transaction envelope")
	bobLoadedTx, err := bobSession.LoadDataTx(dataTxEnv)
	if err != nil {
		fmt.Printf("Loaded data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Bob commits the transaction, but Charlie should have signed as well because: " +
		"he is a member of the Must sign set")
	txID, _, err := bobLoadedTx.Commit(true)
	if err == nil {
		return errors.Errorf("Unexpectedly transaction number %s committed successfully\n", txID)
	}
	fmt.Printf("As expected, commit failed, reason: %s\n", err.Error())
	return nil
}

/*
	- alice adds key3 to the database, with alice, bob and charlie as ReadWriteUsers, and the SignPolicyForWrite is types.AccessControl_ALL
	- alice adds bob to the Must sign set
	- bob co-sign and commits the transaction, but charlie doesn't sign
	- the tx will be invalid because charlie should sign as well because he is a member of ReadWriteUsers set,
		and the SignPolicyForWrite is types.AccessControl_ALL
*/
func writeUserHasNotSigned(aliceSession bcdb.DBSession, bobSession bcdb.DBSession) error {
	fmt.Println("")
	fmt.Println("=== alice adds key3 to the database with charlie as a ReadWriteUser and adds bob to the Must sign set," +
		" but only bob co-sign the tx ===")

	fmt.Println("Alice opens data transaction")
	aliceTx, err := aliceSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	acl := &types.AccessControl{
		ReadWriteUsers: map[string]bool{
			"alice":   true,
			"bob":     true,
			"charlie": true,
		},
		SignPolicyForWrite: types.AccessControl_ALL,
	}
	fmt.Println("Alice adds key, value: key3, val3 to the database")
	err = aliceTx.Put("db", "key3", []byte("val3"), acl)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Alice commits the transaction")
	txID, _, err := aliceTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	fmt.Println("Alice opens data transaction")
	aliceTx, err = aliceSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Alice updates key3 value to val4")
	err = aliceTx.Put("db", "key3", []byte("val4"), acl)
	if err != nil {
		fmt.Printf("Updating key failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Adding bob to the multi-sign data transaction's MustSignUserIDs set")
	aliceTx.AddMustSignUser("bob")

	fmt.Println("Alice signs the transaction and construct the envelope")
	txEnv, err := aliceTx.SignConstructedTxEnvelopeAndCloseTx()
	if err != nil {
		fmt.Printf("Failed to compose transaction envelope, reason: %s\n", err.Error())
		return err
	}
	dataTxEnv := txEnv.(*types.DataTxEnvelope)

	fmt.Println("Bob loads the transaction envelope")
	bobLoadedTx, err := bobSession.LoadDataTx(dataTxEnv)
	if err != nil {
		fmt.Printf("Loaded data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Bob commits the transaction, but Charlie should have signed as well because: " +
		"he is a member of ReadWriteUsers set, and the SignPolicyForWrite is types.AccessControl_ALL")
	txID, _, err = bobLoadedTx.Commit(true)
	if err == nil {
		return errors.Errorf("Unexpectedly transaction number %s committed successfully\n", txID)
	}
	fmt.Printf("As expected, the commit failed, reason: %s\n", err.Error())
	return nil
}

/*
	- alice adds key4 to the database, with alice, bob as ReadWriteUsers and charlie as ReadUser
	- alice adds charlie to the Must sign set
	- bob co-sign and commits the transaction, but charlie doesn't sign
	- the tx will fail because charlie should sign as well because he is a member of Must sign set//
*/
func mustSignAndReadOnlyUserHasNotSigned(aliceSession bcdb.DBSession, bobSession bcdb.DBSession) error {
	fmt.Println("")
	fmt.Println("=== alice adds key4 to the database with charlie as a readUser and adds charlie to the Must sign set," +
		" but only bob co-sign the tx ===")

	fmt.Println("Alice opens data transaction")
	aliceTx, err := aliceSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	acl := &types.AccessControl{
		ReadWriteUsers: map[string]bool{
			"alice": true,
			"bob":   true,
		},
		ReadUsers: map[string]bool{
			"charlie": true,
		},
	}

	fmt.Println("Alice adds key, value: key4, val4 to the database")
	err = aliceTx.Put("db", "key4", []byte("val4"), acl)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Adding charlie to the multi-sign data transaction's MustSignUserIDs set")
	aliceTx.AddMustSignUser("charlie")

	fmt.Println("Alice signs the transaction and construct the envelope")
	txEnv, err := aliceTx.SignConstructedTxEnvelopeAndCloseTx()
	if err != nil {
		fmt.Printf("Failed to compose transaction envelope, reason: %s\n", err.Error())
		return err
	}
	dataTxEnv := txEnv.(*types.DataTxEnvelope)

	fmt.Println("Bob loads the transaction envelope")
	bobLoadedTx, err := bobSession.LoadDataTx(dataTxEnv)
	if err != nil {
		fmt.Printf("Loaded data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Bob commits the transaction, but Charlie should have signed as well because: " +
		"he is a member of the Must sign set")
	txID, _, err := bobLoadedTx.Commit(true)
	if err == nil {
		return errors.Errorf("Unexpectedly transaction number %s committed successfully\n", txID)
	}
	fmt.Printf("As expected, the commit failed, reason: %s\n", err.Error())
	return nil
}
