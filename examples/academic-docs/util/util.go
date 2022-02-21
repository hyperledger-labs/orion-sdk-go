// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"time"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

func CreateConnection() (bcdb.BCDB, error) {
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
		ReplicaSet: []*config.Replica{
			{
				ID:       "orion-server1",
				Endpoint: "http://127.0.0.1:6001",
			},
		},
		RootCAs: []string{
			"../crypto/CA/CA.pem",
		},
		Logger: logger,
	}

	db, err := bcdb.Create(conConf)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func OpenSession(db bcdb.BCDB, userID string) (bcdb.DBSession, error) {
	sessionConf := &config.SessionConfig{
		UserConfig: &config.UserConfig{
			UserID:         userID,
			CertPath:       "../crypto/" + userID + "/" + userID + ".pem",
			PrivateKeyPath: "../crypto/" + userID + "/" + userID + ".key",
		},
		TxTimeout:    20 * time.Second,
		QueryTimeout: 10 * time.Second,
	}

	session, err := db.Session(sessionConf)
	if err != nil {
		return nil, err
	}

	return session, nil
}
