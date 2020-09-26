package server

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
)

type queryProcessor struct {
	dbserver *mockdbserver
}

type value struct {
	values [][]byte
	metas  []*types.Metadata
	index  int
}

type height struct {
	results []*types.Digest
	index   int
}

type dbStatus struct {
	values []*types.GetDBStatusResponse
	index  int
}

func (qp *queryProcessor) GetState(ctx context.Context, req *types.GetDataQueryEnvelope) (*types.GetDataResponseEnvelope, error) {
	db, ok := qp.dbserver.dbs[req.Payload.DBName]
	var val []byte
	var meta *types.Metadata
	if !ok {
		return nil, errors.Errorf("db not exist %s", req.Payload.DBName)
	} else {
		val, meta = db.GetState(req)
	}
	return valueToEnv(val, meta)
}

func (qp *queryProcessor) GetStatus(ctx context.Context, req *types.GetDBStatusQueryEnvelope) (*types.GetDBStatusResponseEnvelope, error) {
	_, ok := qp.dbserver.dbs[req.Payload.DBName]
	return dbStatusToEnv(&types.GetDBStatusResponse{
		Exist: ok,
	})
}

func NewQueryServer(dbserver *mockdbserver) (*queryProcessor, error) {
	return &queryProcessor{
		dbserver: dbserver,
	}, nil
}

func valueToEnv(val []byte, meta *types.Metadata) (*types.GetDataResponseEnvelope, error) {
	response := &types.GetDataResponse{
		Header: &types.ResponseHeader{
			NodeID: nodeID,
		},
		Value:    val,
		Metadata: meta,
	}
	responseBytes, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	signature, err := nodeSigner.Sign(responseBytes)
	if err != nil {
		return nil, err
	}
	return &types.GetDataResponseEnvelope{
		Payload:   response,
		Signature: signature,
	}, nil
}

func dbStatusToEnv(dbStatus *types.GetDBStatusResponse) (*types.GetDBStatusResponseEnvelope, error) {
	dbStatusBytes, err := json.Marshal(dbStatus)
	if err != nil {
		return nil, err
	}
	signature, err := nodeSigner.Sign(dbStatusBytes)
	if err != nil {
		return nil, err
	}
	return &types.GetDBStatusResponseEnvelope{
		Payload:   dbStatus,
		Signature: signature,
	}, nil
}
