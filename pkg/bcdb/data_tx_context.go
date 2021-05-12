// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"github.com/golang/protobuf/proto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
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
		ACL:   acl,
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
		if storedValue, ok := ops.dataReads[key]; ok {
			return storedValue.GetValue(), storedValue.GetMetadata(), nil
		}
	}

	path := constants.URLForGetData(dbName, key)
	res := &types.GetDataResponse{}
	err := d.handleRequest(path, &types.GetDataQuery{
		UserID: d.userID,
		DBName: dbName,
		Key:    key,
	}, res)
	if err != nil {
		d.logger.Errorf("failed to execute ledger data query path %s, due to %s", path, err)
		return nil, nil, err
	}

	if !ok {
		ops = newDBOperations()
		d.operations[dbName] = ops
	}
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

func (d *dataTxContext) composeEnvelope(txID string) (proto.Message, error) {
	var dbOperations []*types.DBOperation

	for name, ops := range d.operations {
		dbOp := &types.DBOperation{
			DBName: name,
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

		dbOperations = append(dbOperations, dbOp)
	}

	payload := &types.DataTx{
		MustSignUserIDs: []string{d.userID},
		TxID:            txID,
		DBOperations:    dbOperations,
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
}

func newDBOperations() *dbOperations {
	return &dbOperations{
		dataReads:   map[string]*types.GetDataResponse{},
		dataWrites:  map[string]*types.DataWrite{},
		dataDeletes: map[string]*types.DataDelete{},
	}
}
