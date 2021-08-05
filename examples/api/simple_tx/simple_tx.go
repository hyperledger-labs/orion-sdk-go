package main

import (
	"fmt"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/bcdb"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"

)

func main() {
	c, err := ReadConfig("./config.yml")
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

	db, err := bcdb.Create(conConf)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	sessionConf := &config.SessionConfig{
		UserConfig: c.SessionConfig.UserConfig,
		TxTimeout: c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	session, err := db.Session(sessionConf)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	tx, err := session.DataTx()
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	err = tx.Put("bdb", "key1", []byte("val"),nil)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	_, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	val, _, err := tx.Get("bdb","key1")
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	err = tx.Put("bdb", "key1", []byte("val1"),nil)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	_, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	err = tx.Put("bdb", "key2", []byte("val2"),nil)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	_, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	err = tx.Delete("bdb", "key1")
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	_, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
}
