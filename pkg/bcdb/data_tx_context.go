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
	dataReads   map[string]*types.GetDataResponse
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
	// TODO For this version, we support only single version read, each sequential read to same key will return same value
	// TODO We ignore dirty reads for now - no check if key is already part of  d.dataWrites and/or d.dataDeletes, should be handled later
	// Is key already read?
	if storedValue, ok := d.dataReads[key]; ok {
		return storedValue.GetValue(), storedValue.GetMetadata(), nil
	}
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

	d.dataReads[key] = res.GetPayload()
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
	var dataReads []*types.DataRead

	for _, v := range d.dataWrites {
		dataWrites = append(dataWrites, v)
	}

	for _, v := range d.dataDeletes {
		dataDeletes = append(dataDeletes, v)
	}

	for k, v := range d.dataReads {
		dataReads = append(dataReads, &types.DataRead{
			Key:     k,
			Version: v.GetMetadata().GetVersion(),
		})
	}

	payload := &types.DataTx{
		UserID:      d.userID,
		TxID:        txID,
		DBName:      d.database,
		DataReads:   dataReads,
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
	d.dataReads = map[string]*types.GetDataResponse{}
}
