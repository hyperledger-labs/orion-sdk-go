package bcdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

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
	Get(key string) ([]byte, error)
	// Delete value for key
	Delete(key string) error
}

type dataTxContext struct {
	commonTxContext
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
func (d *dataTxContext) Get(key string) ([]byte, error) {
	// TODO Should we check if key already part of d.dataWrites and/or d.dataDeletes? Dirty reads case...
	getData := &url.URL{
		Path: constants.URLForGetData(d.database, key),
	}
	replica := d.selectReplica()
	configREST := replica.ResolveReference(getData)

	ctx := context.TODO() // TODO: Replace with timeout
	response, err := d.restClient.Query(ctx, configREST.String(), &types.GetDataQuery{
		UserID: d.userID,
		DBName: d.database,
		Key:    key,
	})
	if err != nil {
		d.logger.Errorf("failed to send query transaction to obtain record for (db, key) = (%s, %s), due to %s", d.database, key, err)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		d.logger.Errorf("error getting user's record, server returned %s", response.Status)
		return nil, errors.New(fmt.Sprintf("error getting user's record, server returned %s", response.Status))
	}

	res := &types.GetDataResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		d.logger.Errorf("failed to decode json response, due to", err)
		return nil, err
	}

	// TODO Should we check existence of key in d.dataReads with different version before appending? To fail early...
	d.dataReads = append(d.dataReads, &types.DataRead{
		Key:     key,
		Version: res.GetPayload().GetMetadata().GetVersion(),
	})

	return res.GetPayload().GetValue(), nil
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