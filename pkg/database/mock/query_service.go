package server

import (
	"context"

	"github.ibm.com/blockchaindb/protos/types"
)

type queryProcessor struct {
	values       map[string]*value
	defaultValue *value
	dbStatuses   map[string]*dbStatus
	ledgerHeight *height
}

type value struct {
	values []*types.Value
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
	val, ok := qp.values[req.Payload.Key]
	if !ok {
		val = qp.defaultValue
	}
	if val.index < len(val.values) {
		res := val.values[val.index]
		val.index += 1
		return valueToEnv(res)
	}
	return valueToEnv(val.values[len(val.values)-1])
}

func (qp *queryProcessor) GetStatus(ctx context.Context, req *types.GetStatusQueryEnvelope) (*types.GetStatusResponseEnvelope, error) {
	val, ok := qp.dbStatuses[req.Payload.DBName]
	if !ok {
		return dbStatusToEnv(&types.GetStatusResponse{
			Exist: false,
		})
	}
	if val.index < len(val.values) {
		res := val.values[val.index]
		val.index += 1
		return dbStatusToEnv(res)
	}
	return dbStatusToEnv(val.values[len(val.values)-1])
}

func NewQueryServer() (*queryProcessor, error) {
	key1result := &value{
		values: make([]*types.Value, 0),
		index:  0,
	}
	key1result.values = append(key1result.values, &types.Value{
		Value: []byte("Testvalue11"),
		Metadata: &types.Metadata{
			Version: &types.Version{
				BlockNum: 0,
				TxNum:    0,
			},
		},
	})
	key1result.values = append(key1result.values, &types.Value{
		Value: []byte("Testvalue12"),
		Metadata: &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    0,
			},
		},
	})

	key2result := &value{
		values: make([]*types.Value, 0),
		index:  0,
	}
	key2result.values = append(key2result.values, &types.Value{
		Value: []byte("Testvalue21"),
		Metadata: &types.Metadata{
			Version: &types.Version{
				BlockNum: 0,
				TxNum:    1,
			},
		},
	})

	keyNilResult := &value{
		values: make([]*types.Value, 0),
		index:  0,
	}
	keyNilResult.values = append(keyNilResult.values, &types.Value{
		Value: nil,
		Metadata: &types.Metadata{
			Version: &types.Version{
				BlockNum: 0,
				TxNum:    1,
			},
		},
	})

	defaultResult := &value{
		values: make([]*types.Value, 0),
		index:  0,
	}
	defaultResult.values = append(defaultResult.values, &types.Value{
		Value: []byte("Default1"),
		Metadata: &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
		},
	})
	ledgerHeight := &height{
		results: make([]*types.Digest, 0),
		index:   0,
	}
	ledgerHeight.results = append(ledgerHeight.results, &types.Digest{
		Height: 0,
	})
	ledgerHeight.results = append(ledgerHeight.results, &types.Digest{
		Height: 1,
	})

	results := make(map[string]*value)
	results["key1"] = key1result
	results["key2"] = key2result
	results["keynil"] = keyNilResult

	dbStatusResults := make(map[string]*dbStatus)
	testDBResult := &dbStatus{
		values: make([]*types.GetStatusResponse, 0),
		index:  0,
	}

	testDBResult.values = append(testDBResult.values, &types.GetStatusResponse{
		Header: &types.ResponseHeader{
			NodeID: nodeID,
		},
		Exist: true,
	})
	dbStatusResults["testDb"] = testDBResult

	return &queryProcessor{
		values:       results,
		defaultValue: defaultResult,
		dbStatuses:   dbStatusResults,
		ledgerHeight: ledgerHeight,
	}, nil
}

func valueToEnv(val *types.Value) (*types.GetStateResponseEnvelope, error) {
	response := &types.GetStateResponse{
		Header: &types.ResponseHeader{
			NodeID: nodeID,
		},
		Value: val,
	}
	signature, err := nodeCrypto.Sign(response)
	if err != nil {
		return nil, err
	}
	return &types.GetStateResponseEnvelope{
		Payload:   response,
		Signature: signature,
	}, nil
}

func dbStatusToEnv(dbStatus *types.GetStatusResponse) (*types.GetStatusResponseEnvelope, error) {
	signature, err := nodeCrypto.Sign(dbStatus)
	if err != nil {
		return nil, err
	}
	return &types.GetStatusResponseEnvelope{
		Payload:   dbStatus,
		Signature: signature,
	}, nil
}
