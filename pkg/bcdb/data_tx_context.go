// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
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
	// AddMustSignUser adds userID to the multi-sign data transaction's
	// MustSignUserIDs set. All users in the MustSignUserIDs set must co-sign
	// the transaction for it to be valid. Note that, in addition, when a
	// transaction modifies keys which have multiple users in the write ACL,
	// or when a transaction modifies keys where each key has different user
	// in the write ACL, the signature of additional users may be required.
	// AddMustSignUser can be used to add users whose signatures is required,
	// on top of those mandates by the ACLs of the keys in the write-set of
	// the transaction. The userID of the initiating client is always in
	// the MustSignUserIDs set."
	AddMustSignUser(userID string)
	// SignConstructedTxEnvelopeAndCloseTx returns a signed transaction envelope and
	// also closes the transaction context. When a transaction requires
	// signatures from multiple users, an initiating user prepares the
	// transaction and calls SignConstructedTxEnvelopeAndCloseTx in order to
	// sign it and construct the envelope. The envelope must then be
	// circulated among all the users that need to co-sign it."
	SignConstructedTxEnvelopeAndCloseTx() (proto.Message, error)
}

type dataTxContext struct {
	*commonTxContext
	operations map[string]*dbOperations
	txUsers    map[string]bool
}

func (d *dataTxContext) Commit(sync bool) (string, *types.TxReceiptResponseEnvelope, error) {
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
			return nil, nil, errors.Errorf("can not execute Get and AssertRead for the same key '" + key + "' in the same transaction")
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
			return errors.Errorf("can not execute Get and AssertRead for the same key '" + key + "' in the same transaction")
		}
		if currentVersion, ok := ops.dataAsserts[key]; ok {
			if currentVersion != version {
				return errors.Errorf("the received version is different from the existing version")
			}
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

func (d *dataTxContext) AddMustSignUser(userID string) {
	d.txUsers[userID] = true
}

// SignConstructedTxEnvelopeAndCloseTx returns a signed transaction envelope and
// also closes the transaction context. When the transaction requires signature from
// multiple users, SignConstructedTxEnvelopeAndCloseTx can be used by
// any one of the participating users after adding userID of each of the
// participating users in the multi-sign transaction and executing the transaction.
func (d *dataTxContext) SignConstructedTxEnvelopeAndCloseTx() (proto.Message, error) {
	d.logger.Debugf("compose transaction enveloped with txID = %s", d.txID)

	var err error
	d.txEnvelope, err = d.composeEnvelope(d.txID)
	if err != nil {
		d.logger.Errorf("failed to compose transaction envelope, due to %s", err)
		return nil, err
	}

	d.txSpent = true
	d.cleanCtx()
	return d.txEnvelope, nil
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

	var mustSignUserIDs []string
	for user := range d.txUsers {
		mustSignUserIDs = append(mustSignUserIDs, user)
	}

	payload := &types.DataTx{
		MustSignUserIds: mustSignUserIDs,
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
