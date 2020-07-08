package server

import (
	"context"

	"github.ibm.com/blockchaindb/server/api"
)

type queryProcessor struct {
	values       map[string]*value
	defaultValue *value
	dbStatuses   map[string]*dbStatus
	ledgerHeight *height
}

type value struct {
	values []*api.Value
	index  int
}

type height struct {
	results []*api.Digest
	index   int
}

type dbStatus struct {
	values []*api.GetStatusResponse
	index  int
}

func (qp *queryProcessor) GetState(ctx context.Context, req *api.GetStateQueryEnvelope) (*api.GetStateResponseEnvelope, error) {
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

func (qp *queryProcessor) GetStatus(ctx context.Context, req *api.GetStatusQueryEnvelope) (*api.GetStatusResponseEnvelope, error) {
	val, ok := qp.dbStatuses[req.Payload.DBName]
	if !ok {
		return dbStatusToEnv(&api.GetStatusResponse{
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
		values: make([]*api.Value, 0),
		index:  0,
	}
	key1result.values = append(key1result.values, &api.Value{
		Value: []byte("Testvalue11"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 0,
				TxNum:    0,
			},
		},
	})
	key1result.values = append(key1result.values, &api.Value{
		Value: []byte("Testvalue12"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 1,
				TxNum:    0,
			},
		},
	})

	key2result := &value{
		values: make([]*api.Value, 0),
		index:  0,
	}
	key2result.values = append(key2result.values, &api.Value{
		Value: []byte("Testvalue21"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 0,
				TxNum:    1,
			},
		},
	})

	keyNilResult := &value{
		values: make([]*api.Value, 0),
		index:  0,
	}
	keyNilResult.values = append(keyNilResult.values, &api.Value{
		Value: nil,
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 0,
				TxNum:    1,
			},
		},
	})

	defaultResult := &value{
		values: make([]*api.Value, 0),
		index:  0,
	}
	defaultResult.values = append(defaultResult.values, &api.Value{
		Value: []byte("Default1"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 1,
				TxNum:    1,
			},
		},
	})
	ledgerHeight := &height{
		results: make([]*api.Digest, 0),
		index:   0,
	}
	ledgerHeight.results = append(ledgerHeight.results, &api.Digest{
		Height: 0,
	})
	ledgerHeight.results = append(ledgerHeight.results, &api.Digest{
		Height: 1,
	})

	results := make(map[string]*value)
	results["key1"] = key1result
	results["key2"] = key2result
	results["keynil"] = keyNilResult

	dbStatusResults := make(map[string]*dbStatus)
	testDBResult := &dbStatus{
		values: make([]*api.GetStatusResponse, 0),
		index:  0,
	}

	testDBResult.values = append(testDBResult.values, &api.GetStatusResponse{
		Header: &api.ResponseHeader{
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

func valueToEnv(val *api.Value) (*api.GetStateResponseEnvelope, error) {
	response := &api.GetStateResponse{
		Header: &api.ResponseHeader{
			NodeID: nodeID,
		},
		Value: val,
	}
	signature, err := nodeCrypto.Sign(response)
	if err != nil {
		return nil, err
	}
	return &api.GetStateResponseEnvelope{
		Payload:   response,
		Signature: signature,
	}, nil
}

func dbStatusToEnv(dbStatus *api.GetStatusResponse) (*api.GetStatusResponseEnvelope, error) {
	signature, err := nodeCrypto.Sign(dbStatus)
	if err != nil {
		return nil, err
	}
	return &api.GetStatusResponseEnvelope{
		Payload:   dbStatus,
		Signature: signature,
	}, nil
}
