package database

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/library/pkg/crypto_utils"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/sdk/pkg/rest"
)

type dbConnector struct {
	// Internal database that stores all users' databases
	internalDBManagementDatabase      *blockchainDB
	internalDBManagementDatabaseMutex sync.RWMutex
	certificateFetcher                crypto_utils.CertificateFetcher
	options                           *config.Options
	clients                           []*rest.Client
	httpClient                        *http.Client
	userManagement                    *dbUserManagement
}

func NewConnector(opt *config.Options) (DBConnector, error) {
	// TODO: remove hardcoded certificate fetcher
	certificateFetcher, err := CreateHardcodedFetcher()
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	connector := &dbConnector{
		certificateFetcher: certificateFetcher,
		options:            opt,
		clients:            make([]*rest.Client, 0),
		httpClient:         httpClient,
		userManagement:     &dbUserManagement{},
	}
	connector.userManagement.connector = connector

	for _, clientOpt := range opt.ConnectionOptions {
		restClient, err := connector.createRESTClient(clientOpt)
		if err == nil && restClient != nil {
			connector.clients = append(connector.clients, restClient)
		}
	}
	if len(connector.clients) == 0 {
		return nil, errors.Errorf("no rest clients to communicate with db servers, available options were: %v ", opt.ConnectionOptions)
	}

	return connector, nil
}

func (c *dbConnector) GetDBManagement() DBManagement {
	return c
}

func (c *dbConnector) GetUserManagement() UserManagement {
	return c.userManagement
}

// CreateDB creates new database with default read and read-write ACLs
func (c *dbConnector) CreateDB(dbName string, readACL, writeALC []string) error {
	if err := c.initInternalDBManagementDatabase(); err != nil {
		return err
	}

	tx, err := c.internalDBManagementDatabase.Begin(c.options.TxOptions)
	if err != nil {
		return err
	}
	defer tx.Abort()

	dbConfig := &types.DatabaseConfig{
		Name:             dbName,
		ReadAccessUsers:  readACL,
		WriteAccessUsers: writeALC,
	}

	orgDBconfig, err := tx.Get(dbName)
	if err != nil {
		return err
	}

	if orgDBconfig != nil {
		return errors.Errorf("can't create db %s, it already exist", dbName)
	}
	dbConfigBytes, err := json.Marshal(dbConfig)
	if err != nil {
		return err
	}
	if err = tx.Put(dbName, dbConfigBytes, nil); err != nil {
		return err
	}
	if _, err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// DeleteDB deletes existing database
func (c *dbConnector) DeleteDB(dbName string) error {
	if err := c.initInternalDBManagementDatabase(); err != nil {
		return err
	}

	tx, err := c.internalDBManagementDatabase.Begin(c.options.TxOptions)
	if err != nil {
		return err
	}
	defer tx.Abort()

	orgDBconfig, err := tx.Get(dbName)
	if err != nil {
		return nil
	}

	if orgDBconfig == nil {
		return errors.Errorf("can't remove db %s, it does not exist", dbName)
	}
	if err = tx.Delete(dbName); err != nil {
		return err
	}
	if _, err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// OpenDBSession opens an existing database associated with the dbName
func (c *dbConnector) OpenDBSession(dbName string, options *config.TxOptions) (DBSession, error) {
	db := &blockchainDB{
		dbConnector: c,
		dbName:      dbName,
		openTx:      make(map[string]*transactionContext, 0),
		isClosed:    false,
		TxOptions:   options,
	}
	if c.options.User != nil {
		cryptoRegistry, err := crypto_utils.NewVerifiersRegistry(c.options.ServersVerify, c.certificateFetcher)
		if err != nil {
			return nil, err
		}
		db.verifiers = cryptoRegistry
		db.Signer, err = crypto.NewSigner(c.options.User.Signer)
		if err != nil {
			return nil, err
		}
		db.userID = c.options.User.UserID
	}

	var err error
	db.Client, err = c.getClientForDB(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (c *dbConnector) createRESTClient(options *config.ConnectionOption) (*rest.Client, error) {
	rc, err := rest.NewRESTClient(options.URL, c.httpClient)
	if err != nil {
		log.Println(fmt.Sprintf("could not create REST client for %s, %v", options.URL, err))
		return nil, errors.Wrapf(err, "could not create REST client for %s", options.URL)
	}

	return rc, nil
}

func (c *dbConnector) getClientForDB(db *blockchainDB) (*rest.Client, error) {
	clientStartIndex := rand.Intn(len(c.clients))

	var totalError error
	for i := 0; i < len(c.clients); i++ {
		currentClient := c.clients[(clientStartIndex+i)%len(c.clients)]
		exist, err := checkDB(currentClient, db)
		if err != nil || !exist {
			if !exist {
				err = errors.Errorf("server %v doesn't contains db %s", currentClient.BaseURL, db.dbName)
			}
			totalError = errors.Wrapf(err, "%v", totalError)
			continue
		}
		return currentClient, nil
	}
	return nil, totalError
}

func checkDB(client *rest.Client, db *blockchainDB) (bool, error) {
	query := &types.GetDBStatusQuery{
		UserID: db.userID,
		DBName: db.dbName,
	}
	envelope := &types.GetDBStatusQueryEnvelope{
		Payload:   query,
		Signature: nil,
	}
	queryBytes, err := json.Marshal(query)
	if err != nil {
		return false, err
	}
	envelope.Signature, err = db.Sign(queryBytes)
	if err != nil {
		return false, err
	}
	dbStatusEnvelope, err := client.GetStatus(context.Background(), envelope)
	if err != nil {
		return false, err
	}
	if !dbStatusEnvelope.Payload.Exist {
		return false, errors.Errorf("database %s doesn't exist", db.dbName)
	}
	return true, nil
}

// Single threaded
func (c *dbConnector) initInternalDBManagementDatabase() error {
	c.internalDBManagementDatabaseMutex.Lock()
	defer c.internalDBManagementDatabaseMutex.Unlock()
	if c.internalDBManagementDatabase != nil {
		return nil
	}
	db, err := c.OpenDBSession("_dbs", c.options.TxOptions)
	if err != nil {
		return err
	}
	c.internalDBManagementDatabase = db.(*blockchainDB)

	return nil
}
