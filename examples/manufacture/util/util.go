package util

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"os"
)

func OpenConnection(c *Config) (bcdb.BCDB, error) {

	logger, err := logger.New(
		&logger.Config{
			Level:         "info",
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
	return db, nil

}

func OpenSession(c *Config, db bcdb.BCDB) (bcdb.DBSession, error) {
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
	return session, nil
}

func OpenDBSession(configLocation string) (bcdb.DBSession, error) {
	c, err := ReadConfig(configLocation)
	if err != nil {
		return nil, err
	}

	fmt.Println("\n===Creating a Connection to Orion===")
	fmt.Println()
	db, err := OpenConnection(c)
	if err != nil {
		return nil, err
	}

	fmt.Println("\n===Creating a Session to Orion===")
	fmt.Println()
	session, err := OpenSession(c, db)
	if err != nil {
		return nil, err
	}

	return session, nil
}

func StoreProtoAsJsonOrPanic(msg interface{}, fileName string) {
	msgJson, err := json.Marshal(msg)
	if err != nil {
		panic("can't marshal to json")
	}

	err = os.WriteFile(fileName, msgJson, 0644)
	if err != nil {
		panic("can't store to file: " + err.Error())
	}
}

func LoadProtoFromFileOrPanic(msg interface{}, fileName string) {
	storedMsgJson, err := os.ReadFile(fileName)
	if err != nil {
		panic("can't read from file")
	}

	err = json.Unmarshal(storedMsgJson, msg)
	if err != nil {
		panic("can't unmarshall from json")
	}
}

