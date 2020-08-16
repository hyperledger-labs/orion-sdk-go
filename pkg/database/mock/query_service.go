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
	values []*types.GetStatusResponse
	index  int
}

func (qp *queryProcessor) GetState(ctx context.Context, req *types.GetStateQueryEnvelope) (*types.GetStateResponseEnvelope, error) {
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

func (qp *queryProcessor) GetStatus(ctx context.Context, req *types.GetStatusQueryEnvelope) (*types.GetStatusResponseEnvelope, error) {
	_, ok := qp.dbserver.dbs[req.Payload.DBName]
	return dbStatusToEnv(&types.GetStatusResponse{
		Exist: ok,
	})
}

func NewQueryServer(dbserver *mockdbserver) (*queryProcessor, error) {
	return &queryProcessor{
		dbserver: dbserver,
	}, nil
}

func valueToEnv(val []byte, meta *types.Metadata) (*types.GetStateResponseEnvelope, error) {
	response := &types.GetStateResponse{
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
	return &types.GetStateResponseEnvelope{
		Payload:   response,
		Signature: signature,
	}, nil
}

func dbStatusToEnv(dbStatus *types.GetStatusResponse) (*types.GetStatusResponseEnvelope, error) {
	dbStatusBytes, err := json.Marshal(dbStatus)
	if err != nil {
		return nil, err
	}
	signature, err := nodeSigner.Sign(dbStatusBytes)
	if err != nil {
		return nil, err
	}
	return &types.GetStatusResponseEnvelope{
		Payload:   dbStatus,
		Signature: signature,
	}, nil
}
