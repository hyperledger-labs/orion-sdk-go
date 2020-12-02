package commands

import (
	"encoding/pem"
	"io/ioutil"
	"net/url"
	"path"
	"sync"
	"time"

	"github.ibm.com/blockchaindb/sdk/pkg/bcdb"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// Init the server, load users, create databases, set permissions
func Init(demoDir string, url *url.URL, logger *logger.SugarLogger) error {

	bcdb, err := bcdb.Create(&config.ConnectionConfig{
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
	if err != nil {
		logger.Errorf("error creating database instance, due to %s", err)
		return err
	}

	logger.Debugf("initialize database session with admin user in context")
	session, err := bcdb.Session(&config.SessionConfig{
		UserConfig: &config.UserConfig{
			UserID:         "admin",
			CertPath:       path.Join(demoDir, "crypto", "admin", "admin.pem"),
			PrivateKeyPath: path.Join(demoDir, "crypto", "admin", "admin.key"),
		},
	})
	if err != nil {
		logger.Errorf("error creating database session, due to %s", err)
		return err
	}

	tx, err := session.DBsTx()
	if err != nil {
		return err
	}

	err = tx.CreateDB("carDB")
	if err != nil {
		return err
	}
	txID, err := tx.Commit()
	if err != nil {
		logger.Errorf("cannot commit transaction to create cars db, due to %s", err)
		return err
	}
	logger.Debugf("transaction to create carDB has been submitted, txID = %s", txID)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			exist, err := tx.Exists("carDB")
			if exist && err != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		wg.Done()
	}()
	wg.Wait()
	logger.Debug("databse carDB has been created")

	usersTx, err := session.UsersTx()
	if err != nil {
		return err
	}

	for _, role := range []string{"dmv", "dealer", "alice", "bob"} {
		certPath := path.Join(demoDir, "crypto", role, role+".pem")
		certFile, err := ioutil.ReadFile(certPath)
		if err != nil {
			logger.Errorf("error reading certificate of %s, due to %s", role, err)
			return err
		}
		certBlock, _ := pem.Decode(certFile)
		err = usersTx.PutUser(&types.User{
			ID:          role,
			Certificate: certBlock.Bytes,
		}, &types.AccessControl{
			ReadWriteUsers: map[string]bool{
				"carDB": true,
			},
			ReadUsers: map[string]bool{
				"carDB": true,
			},
		})
		if err != nil {
			tx.Abort()
			return err
		}
	}

	txID, err = usersTx.Commit()
	if err != nil {
		logger.Errorf("cannot commit transaction to add users, due to %s", err)
		return err
	}
	logger.Debugf("transaction to provision users records has been submitted, txID = %s", txID)

	return nil
}
