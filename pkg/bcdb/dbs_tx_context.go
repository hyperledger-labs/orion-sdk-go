// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"encoding/json"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"google.golang.org/protobuf/proto"
)

// DBsTxContext abstraction for database management transaction context
type DBsTxContext interface {
	TxContext
	// CreateDB creates new database along with index definition for the query.
	// The index is a map of attributes/fields in json document, i.e., value associated
	// with the key, to its value type. For example, map["name"]types.IndexAttributeType_STRING
	// denotes that "name" attribute in all json documents to be stored in the given
	// database to be indexed for queries. Note that only indexed attributes can be
	// used as predicates in the query string. Currently, we support the following three
	// value types: STRING, BOOLEAN, and INT64
	CreateDB(dbName string, index map[string]types.IndexAttributeType) error
	// DeleteDB deletes database
	DeleteDB(dbName string) error
	// Exists checks whenever database is already created
	Exists(dbName string) (bool, error)
	// GetDBIndex returns the index definition associated with the given database.
	// The index definition is of form map["name"]types.IndexAttributeType where
	// name denotes the field name in the JSON document and types.IndexAttributeType
	// denotes one of the three value types: STRING, BOOLEAN, and INT64. When a database
	// does not have an index definition, GetDBIndex would return a nil map
	GetDBIndex(dbName string) (map[string]types.IndexAttributeType, error)
}

type dbsTxContext struct {
	*commonTxContext
	createdDBs map[string]*types.DBIndex
	deletedDBs map[string]bool
}

func (d *dbsTxContext) Commit(sync bool) (string, *types.TxReceiptResponseEnvelope, error) {
	return d.commit(d, constants.PostDBTx, sync)
}

func (d *dbsTxContext) Abort() error {
	return d.commonTxContext.abort(d)
}

func (d *dbsTxContext) CreateDB(dbName string, index map[string]types.IndexAttributeType) error {
	if d.txSpent {
		return ErrTxSpent
	}

	d.createdDBs[dbName] = &types.DBIndex{
		AttributeAndType: index,
	}
	return nil
}

func (d *dbsTxContext) DeleteDB(dbName string) error {
	if d.txSpent {
		return ErrTxSpent
	}

	d.deletedDBs[dbName] = true
	return nil
}

func (d *dbsTxContext) Exists(dbName string) (bool, error) {
	if d.txSpent {
		return false, ErrTxSpent
	}

	path := constants.URLForGetDBStatus(dbName)
	resEnv := &types.GetDBStatusResponseEnvelope{}
	err := d.handleRequest(
		path,
		&types.GetDBStatusQuery{
			UserId: d.userID,
			DbName: dbName,
		}, resEnv,
	)
	if err != nil {
		d.logger.Errorf("failed to execute database status query, path = %s, due to %s", path, err)
		return false, err
	}

	return resEnv.GetResponse().GetExist(), nil
}

func (d *dbsTxContext) GetDBIndex(dbName string) (map[string]types.IndexAttributeType, error) {
	if d.txSpent {
		return nil, ErrTxSpent
	}

	path := constants.URLForGetDBIndex(dbName)
	resEnv := &types.GetDBIndexResponseEnvelope{}
	err := d.handleRequest(
		path,
		&types.GetDBIndexQuery{
			UserId: d.userID,
			DbName: dbName,
		},
		resEnv,
	)
	if err != nil {
		d.logger.Errorf("failed to execute database index query, path = %s, due to %s", path, err)
		return nil, err
	}

	if resEnv.GetResponse().GetIndex() == "" {
		return nil, nil
	}

	index := map[string]types.IndexAttributeType{}
	if err = json.Unmarshal([]byte(resEnv.GetResponse().GetIndex()), &index); err != nil {
		return nil, err
	}

	return index, nil
}

func (d *dbsTxContext) cleanCtx() {
	d.createdDBs = map[string]*types.DBIndex{}
	d.deletedDBs = map[string]bool{}
}

func (d *dbsTxContext) composeEnvelope(txID string) (proto.Message, error) {
	payload := &types.DBAdministrationTx{
		UserId:   d.userID,
		TxId:     txID,
		DbsIndex: make(map[string]*types.DBIndex),
	}

	for db, index := range d.createdDBs {
		payload.CreateDbs = append(payload.CreateDbs, db)
		if index != nil {
			payload.DbsIndex[db] = index
		}
	}

	for db := range d.deletedDBs {
		payload.DeleteDbs = append(payload.DeleteDbs, db)
	}

	signature, err := cryptoservice.SignTx(d.signer, payload)
	if err != nil {
		return nil, err
	}

	return &types.DBAdministrationTxEnvelope{
		Payload:   payload,
		Signature: signature,
	}, nil
}
