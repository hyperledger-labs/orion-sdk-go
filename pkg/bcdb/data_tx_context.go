package bcdb

import (
	"github.com/golang/protobuf/proto"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type DataTxContext interface {
	// Embed general abstraction
	TxContext
	// Put new value to key
	Put(key string, value []byte, acl *types.AccessControl) error
	// Get existing key value
	Get(key string) ([]byte, *types.Metadata, error)
	// Delete value for key
	Delete(key string) error
}

type dataTxContext struct {
	*commonTxContext
	database    string
	dataReads   []*types.DataRead
	dataWrites  map[string]*types.DataWrite
	dataDeletes map[string]*types.DataDelete
}

func (d *dataTxContext) Commit() (string, error) {
	return d.commit(d, constants.PostDataTx)
}

func (d *dataTxContext) Abort() error {
	return d.abort(d)
}

// Put new value to key
func (d *dataTxContext) Put(key string, value []byte, acl *types.AccessControl) error {
	_, deleteExist := d.dataDeletes[key]
	if deleteExist {
		delete(d.dataDeletes, key)
	}
	d.dataWrites[key] = &types.DataWrite{
		Key:   key,
		Value: value,
		ACL:   acl,
	}
	return nil
}

// Get existing key value
func (d *dataTxContext) Get(key string) ([]byte, *types.Metadata, error) {
	// TODO Should we check if key already part of d.dataWrites and/or d.dataDeletes? Dirty reads case...
	path := constants.URLForGetData(d.database, key)
	res := &types.GetDataResponseEnvelope{}
	err := d.handleRequest(path, &types.GetDataQuery{
		UserID: d.userID,
		DBName: d.database,
		Key:    key,
	}, res)
	if err != nil {
		d.logger.Errorf("failed to execute ledger data query path %s, due to %s", path, err)
		return nil, nil, err
	}

	// TODO Should we check existence of key in d.dataReads with different version before appending? To fail early...
	d.dataReads = append(d.dataReads, &types.DataRead{
		Key:     key,
		Version: res.GetPayload().GetMetadata().GetVersion(),
	})

	return res.GetPayload().GetValue(), res.GetPayload().GetMetadata(), nil
}

// Delete value for key
func (d *dataTxContext) Delete(key string) error {
	_, writeExist := d.dataWrites[key]
	if writeExist {
		delete(d.dataWrites, key)
	}
	d.dataDeletes[key] = &types.DataDelete{
		Key: key,
	}
	return nil
}

func (d *dataTxContext) composeEnvelope(txID string) (proto.Message, error) {
	var dataWrites []*types.DataWrite
	var dataDeletes []*types.DataDelete

	for _, v := range d.dataWrites {
		dataWrites = append(dataWrites, v)
	}

	for _, v := range d.dataDeletes {
		dataDeletes = append(dataDeletes, v)
	}

	payload := &types.DataTx{
		UserID:      d.userID,
		TxID:        txID,
		DBName:      d.database,
		DataReads:   d.dataReads,
		DataWrites:  dataWrites,
		DataDeletes: dataDeletes,
	}

	signature, err := cryptoservice.SignTx(d.signer, payload)
	if err != nil {
		return nil, err
	}

	return &types.DataTxEnvelope{
		Payload:   payload,
		Signature: signature,
	}, nil
}

func (d *dataTxContext) cleanCtx() {
	d.dataDeletes = map[string]*types.DataDelete{}
	d.dataWrites = map[string]*types.DataWrite{}
	d.dataReads = []*types.DataRead{}
}
