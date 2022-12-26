// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package commands

import (
	"encoding/pem"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

const CarDBName = "carDB"

// Init the server, load users, create databases, set permissions
func Init(demoDir string, lg *logger.SugarLogger) error {
	serverUrl, err := loadServerUrl(demoDir)
	if err != nil {
		return errors.Wrap(err, "error loading server URL")
	}

	bcDB, err := createDBInstance(demoDir, serverUrl)
	if err != nil {
		lg.Errorf("error creating database instance, due to %s", err)
		return err
	}

	lg.Debugf("initialize database session with admin user in context")
	session, err := createUserSession(demoDir, bcDB, "admin")
	if err != nil {
		lg.Errorf("error creating database session, due to %s", err)
		return err
	}

	if err = initDB(session, lg); err != nil {
		return err
	}

	if err = initUsers(demoDir, session, lg); err != nil {
		return err
	}

	return nil
}

func createUserSession(demoDir string, bcdb bcdb.BCDB, user string) (bcdb.DBSession, error) {
	session, err := bcdb.Session(&config.SessionConfig{
		UserConfig: &config.UserConfig{
			UserID:         user,
			CertPath:       path.Join(demoDir, "crypto", user, user+".pem"),
			PrivateKeyPath: path.Join(demoDir, "crypto", user, user+".key"),
		},
		TxTimeout: time.Second * 5,
	})
	return session, err
}

func createDBInstance(demoDir string, url *url.URL) (bcdb.BCDB, error) {
	c := &logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "bcdb-client",
	}
	clientLogger, err := logger.New(c)
	if err != nil {
		return nil, err
	}

	bcDB, err := bcdb.Create(&config.ConnectionConfig{
		RootCAs: []string{
			path.Join(demoDir, "crypto", "CA", "CA.pem"),
		},
		ReplicaSet: []*config.Replica{
			{
				ID:       "demo",
				Endpoint: url.String(),
			},
		},
		Logger: clientLogger,
	})

	return bcDB, err
}

func saveServerUrl(demoDir string, url *url.URL) error {
	serverUrlFile, err := os.Create(path.Join(demoDir, "server.url"))
	if err != nil {
		return err
	}
	_, err = serverUrlFile.WriteString(url.String())
	if err != nil {
		return err
	}
	return serverUrlFile.Close()
}

func loadServerUrl(demoDir string) (*url.URL, error) {
	urlBytes, err := ioutil.ReadFile(path.Join(demoDir, "server.url"))
	serverUrl, err := url.Parse(string(urlBytes))
	if err != nil {
		return nil, err
	}
	return serverUrl, nil
}

func initDB(session bcdb.DBSession, lg *logger.SugarLogger) error {
	tx, err := session.DBsTx()
	if err != nil {
		return err
	}

	err = tx.CreateDB(CarDBName, nil)
	if err != nil {
		return err
	}
	txID, receiptEnv, err := tx.Commit(true)
	if err != nil {
		lg.Errorf("cannot commit transaction to create cars db, due to %s", err)
		return err
	}
	lg.Debugf("transaction to create carDB has been submitted, txID = %s, txReceipt = %s", txID, receiptEnv.GetResponse().GetReceipt().String())

	lg.Info("database carDB has been created")
	return nil
}

func initUsers(demoDir string, session bcdb.DBSession, logger *logger.SugarLogger) error {
	for _, role := range []string{"dmv", "dealer", "alice", "bob"} {
		usersTx, err := session.UsersTx()
		if err != nil {
			return err
		}

		certPath := path.Join(demoDir, "crypto", role, role+".pem")
		certFile, err := ioutil.ReadFile(certPath)
		if err != nil {
			logger.Errorf("error reading certificate of %s, due to %s", role, err)
			return err
		}
		certBlock, _ := pem.Decode(certFile)
		err = usersTx.PutUser(
			&types.User{
				Id:          role,
				Certificate: certBlock.Bytes,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{CarDBName: 1},
				},
			}, nil)
		if err != nil {
			usersTx.Abort()
			return err
		}

		txID, receiptEnv, err := usersTx.Commit(true)
		if err != nil {
			return err
		}
		receipt := receiptEnv.GetResponse().GetReceipt()
		logger.Infof("transaction to provision user record has been committed, user-ID: %s, txID: %s, block: %d, txIndex: %d",
			role, txID, receipt.GetHeader().GetBaseHeader().GetNumber(), receipt.GetTxIndex())
	}

	return nil
}
