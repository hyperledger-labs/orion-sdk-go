// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
)

type DataTxContext interface {
	// Embed general abstraction
	TxContext
	// Put new value to key
	Put(dbName string, key string, value []byte, acl *types.AccessControl) error
	// Get existing key value
	Get(dbName, key string) ([]byte, *types.Metadata, error)
	// Delete value for key
	Delete(dbName, key string) error
	// AssertRead insert a key-version to the transaction assert map
	AssertRead(dbName string, key string, version *types.Version) error
}

type dataTxContext struct {
	*commonTxContext
	operations map[string]*dbOperations
}

func (d *dataTxContext) Commit(sync bool) (string, *types.TxReceipt, error) {
	return d.commit(d, constants.PostDataTx, sync)
}

func (d *dataTxContext) Abort() error {
	return d.abort(d)
}

// Put new value to key
func (d *dataTxContext) Put(dbName, key string, value []byte, acl *types.AccessControl) error {
	if d.txSpent {
		return ErrTxSpent
	}

	ops, ok := d.operations[dbName]
	if !ok {
		ops = newDBOperations()
		d.operations[dbName] = ops
	}

	_, deleteExist := ops.dataDeletes[key]
	if deleteExist {
		delete(ops.dataDeletes, key)
	}
	ops.dataWrites[key] = &types.DataWrite{
		Key:   key,
		Value: value,
		Acl:   acl,
	}
	return nil
}

// Get existing key value
func (d *dataTxContext) Get(dbName, key string) ([]byte, *types.Metadata, error) {
	if d.txSpent {
		return nil, nil, ErrTxSpent
	}

	// TODO For this version, we support only single version read, each sequential read to same key will return same value
	// TODO We ignore dirty reads for now - no check if key is already part of  d.dataWrites and/or d.dataDeletes, should be handled later
	// Is key already read?
	ops, ok := d.operations[dbName]
	if ok {
		if _, ok := ops.dataAsserts[key]; ok {
			return nil, nil, &ErrorGetAndAssertReadForTheSameKey{Key: key}
		}
		if storedValue, ok := ops.dataReads[key]; ok {
			return storedValue.GetValue(), storedValue.GetMetadata(), nil
		}
	}

	path := constants.URLForGetData(dbName, key)
	resEnv := &types.GetDataResponseEnvelope{}
	err := d.handleRequest(path, &types.GetDataQuery{
		UserId: d.userID,
		DbName: dbName,
		Key:    key,
	}, resEnv)
	if err != nil {
		d.logger.Errorf("failed to execute ledger data query path %s, due to %s", path, err)
		return nil, nil, err
	}

	// TODO: signature verification

	if !ok {
		ops = newDBOperations()
		d.operations[dbName] = ops
	}

	res := resEnv.GetResponse()
	ops.dataReads[key] = res
	return res.GetValue(), res.GetMetadata(), nil
}

// Delete value for key
func (d *dataTxContext) Delete(dbName, key string) error {
	if d.txSpent {
		return ErrTxSpent
	}

	ops, ok := d.operations[dbName]
	if !ok {
		ops = newDBOperations()
		d.operations[dbName] = ops
	}

	_, writeExist := ops.dataWrites[key]
	if writeExist {
		delete(ops.dataWrites, key)
	}
	ops.dataDeletes[key] = &types.DataDelete{
		Key: key,
	}
	return nil
}

// AssertRead insert a key-version to the transaction assert map
func (d *dataTxContext) AssertRead(dbName string, key string, version *types.Version) error {
	if d.txSpent {
		return ErrTxSpent
	}

	ops, ok := d.operations[dbName]
	if ok {
		if _, ok := ops.dataReads[key]; ok {
			return &ErrorGetAndAssertReadForTheSameKey{Key: key}
		}
		if _, ok := ops.dataAsserts[key]; ok {
			return nil
		}
	}

	if !ok {
		ops = newDBOperations()
		d.operations[dbName] = ops
	}
	ops.dataAsserts[key] = version

	return nil
}

func (d *dataTxContext) composeEnvelope(txID string) (proto.Message, error) {
	var dbOperations []*types.DBOperation

	for name, ops := range d.operations {
		dbOp := &types.DBOperation{
			DbName: name,
		}

		for _, v := range ops.dataWrites {
			dbOp.DataWrites = append(dbOp.DataWrites, v)
		}

		for _, v := range ops.dataDeletes {
			dbOp.DataDeletes = append(dbOp.DataDeletes, v)
		}

		for k, v := range ops.dataReads {
			dbOp.DataReads = append(dbOp.DataReads, &types.DataRead{
				Key:     k,
				Version: v.GetMetadata().GetVersion(),
			})
		}

		for k, v := range ops.dataAsserts {
			dbOp.DataReads = append(dbOp.DataReads, &types.DataRead{
				Key:     k,
				Version: v,
			})
		}

		dbOperations = append(dbOperations, dbOp)
	}

	payload := &types.DataTx{
		MustSignUserIds: []string{d.userID},
		TxId:            txID,
		DbOperations:    dbOperations,
	}

	signature, err := cryptoservice.SignTx(d.signer, payload)
	if err != nil {
		return nil, err
	}

	return &types.DataTxEnvelope{
		Payload: payload,
		Signatures: map[string][]byte{
			d.userID: signature,
		},
	}, nil
}

func (d *dataTxContext) cleanCtx() {
	d.operations = map[string]*dbOperations{}
}

type dbOperations struct {
	dataReads   map[string]*types.GetDataResponse
	dataWrites  map[string]*types.DataWrite
	dataDeletes map[string]*types.DataDelete
	dataAsserts map[string]*types.Version
}

func newDBOperations() *dbOperations {
	return &dbOperations{
		dataReads:   map[string]*types.GetDataResponse{},
		dataWrites:  map[string]*types.DataWrite{},
		dataDeletes: map[string]*types.DataDelete{},
		dataAsserts: map[string]*types.Version{},
	}
}

type ErrorGetAndAssertReadForTheSameKey struct {
	Key string
}

func (e *ErrorGetAndAssertReadForTheSameKey) Error() string {
	return "can not execute Get and AssertRead for the same key '" + e.Key +  "' in the same transaction"
}
