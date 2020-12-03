package commands

import (
	"encoding/pem"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"sync"
	"time"
)

const CarDBName = "carDB"

// Init the server, load users, create databases, set permissions
func Init(demoDir string, url *url.URL, lg *logger.SugarLogger) error {

	bcDB, err := createDBInstance(demoDir, url)
	if err != nil {
		lg.Errorf("error creating database instance, due to %s", err)
		return err
	}

	if err = saveServerUrl(demoDir, url); err != nil {
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
	})
	return session, err
}

func createDBInstance(demoDir string, url *url.URL) (bcdb.BCDB, error) {
	bcDB, err := bcdb.Create(&config.ConnectionConfig{
		RootCAs: []string{
			path.Join(demoDir, "crypto", "CA", "CA.pem"),
		},
		ReplicaSet: []*config.Replica{
			{
				ID:       "node",
				Endpoint: url.String(),
			},
		},
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

	err = tx.CreateDB(CarDBName)
	if err != nil {
		return err
	}
	txID, err := tx.Commit()
	if err != nil {
		lg.Errorf("cannot commit transaction to create cars db, due to %s", err)
		return err
	}
	lg.Debugf("transaction to create carDB has been submitted, txID = %s", txID)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			exist, err := tx.Exists(CarDBName)
			if exist && err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		wg.Done()
	}()
	wg.Wait()

	lg.Debug("database carDB has been created")
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
				ID:          role,
				Certificate: certBlock.Bytes,
				Privilege:   &types.Privilege{
					DBPermission:          map[string]types.Privilege_Access{CarDBName: 1},
				},
			}, &types.AccessControl{
				ReadWriteUsers: bcdb.UsersMap("admin"),
				ReadUsers:      bcdb.UsersMap("admin"),
			})
		if err != nil {
			usersTx.Abort()
			return err
		}

		txID, err := usersTx.Commit()
		if err != nil {
			logger.Errorf("cannot commit transaction to add users, due to %s", err)
			return err
		}
		logger.Debugf("transaction to provision user record has been submitted, ID: %s, txID = %s", role, txID)

		err = waitForUserTxCommit(session, role, txID)
		if err != nil {
			return err
		}
		logger.Debugf("transaction to provision user record has been committed, ID: %s, txID = %s", role, txID)
	}

	return nil
}

func waitForUserTxCommit(session bcdb.DBSession, key, txID string) error {
wait_for_commit:
	for {
		select {
		case <-time.After(1 * time.Second):
			return errors.Errorf("timeout while waiting for transaction %s to commit to BCDB", txID)

		case <-time.After(50 * time.Millisecond):
			userTx, err := session.UsersTx()
			if err != nil {
				return errors.Wrap(err, "error creating data transaction")
			}

			recordBytes, err := userTx.GetUser(key)
			if err != nil {
				return errors.Wrapf(err, "error while waiting for transaction %s to commit to BCDB", txID)
			}
			if recordBytes != nil {
				break wait_for_commit
			}
		}
	}

	return nil
}
