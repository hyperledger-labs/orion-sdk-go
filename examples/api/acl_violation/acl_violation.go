package main

import (
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/IBM-Blockchain/bcdb-sdk/examples/util"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/bcdb"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

/*
	Example of non-valid transaction because of ACL violation,
	alice has read-write privilege on database db, but bob only has read privilege,
	each user opens a transaction that writes to the database, alice tx is valid and bob tx is non-valid
*/
func main() {
	session, db, err := openSessionAndCreateDB()
	if session == nil || db == nil || err != nil {
		return
	}

	err = addUser(session, "alice", "../../../../bcdb-server/sampleconfig/crypto/alice/alice.pem",
		nil, []string{"db"})
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return
	}

	err = addUser(session, "bob", "../../../../bcdb-server/sampleconfig/crypto/bob/bob.pem",
		[]string{"db"}, nil)
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return
	}

	//alice tx
	aliceConfig := config.SessionConfig{
		UserConfig: &config.UserConfig{
			UserID:         "alice",
			CertPath:       "../../../../bcdb-server/sampleconfig/crypto/alice/alice.pem",
			PrivateKeyPath: "../../../../bcdb-server/sampleconfig/crypto/alice/alice.key",
		},
		TxTimeout: time.Second * 5,
	}
	fmt.Println("Opening alice session to database, configuration: ", aliceConfig)
	aliceSession, err := db.Session(&aliceConfig)
	if err != nil {
		fmt.Printf("Session creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Opening alice data transaction")
	aliceTx, err := aliceSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Alice - adding key, value: key1, val1 to the database")
	err = aliceTx.Put("db", "key1", []byte("val1"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing alice transaction")
	txID, aliceReceipt, err := aliceTx.Commit(true)
	if err != nil {
		if aliceReceipt != nil && aliceReceipt.Header.ValidationInfo[aliceReceipt.TxIndex].Flag != types.Flag_VALID {
			fmt.Printf("Alice transaction is invalid, reason: %s\n", aliceReceipt.Header.ValidationInfo[aliceReceipt.TxIndex].ReasonIfInvalid)
		}
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//bob tx
	bobConfig := config.SessionConfig{
		UserConfig: &config.UserConfig{
			UserID:         "bob",
			CertPath:       "../../../../bcdb-server/sampleconfig/crypto/bob/bob.pem",
			PrivateKeyPath: "../../../../bcdb-server/sampleconfig/crypto/bob/bob.key",
		},
		TxTimeout: time.Second * 5,
	}
	fmt.Println("Opening bob session to database, configuration: ", bobConfig)
	bobSession, err := db.Session(&bobConfig)
	if err != nil {
		fmt.Printf("Session creating failed, reason: %s\n", err.Error())
		return
	}

	bobTx, err := bobSession.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Bob - adding key, value: key2, val2 to the database")
	err = bobTx.Put("db", "key2", []byte("val2"), nil)
	if err != nil {
		fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing bob transaction")
	txID, bobReceipt, err := bobTx.Commit(true)
	if err != nil {
		if bobReceipt != nil && bobReceipt.Header.ValidationInfo[bobReceipt.TxIndex].Flag != types.Flag_VALID {
			fmt.Printf("Bob transaction is invalid, reason: %s\n", bobReceipt.Header.ValidationInfo[bobReceipt.TxIndex].ReasonIfInvalid)
		}
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)
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

func openSessionAndCreateDB() (bcdb.DBSession, bcdb.BCDB, error) {
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
	err = dbTx.CreateDB("db")
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
