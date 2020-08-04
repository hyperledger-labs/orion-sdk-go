package server

import (
	"context"
	"encoding/json"

	"github.ibm.com/blockchaindb/protos/types"
)

type mockdbserver struct {
	dbs    map[string]*mockdb
	height *height
}

type mockdb struct {
	values       map[string]*value
	defaultValue *types.Value
	server       *mockdbserver
}

func restartMockServer() *mockdbserver {
	mockserver := &mockdbserver{dbs: make(map[string]*mockdb, 0)}
	mockserver.dbs["_dbs"] = &mockdb{
		values: make(map[string]*value),
		server: mockserver,
	}
	mockserver.dbs["_users"] = &mockdb{
		values: make(map[string]*value),
		server: mockserver,
	}
	mockserver.dbs["testDb"] = &mockdb{
		server: mockserver,
	}

	testDbConfig := &types.DatabaseConfig{
		Name: "testDb",
		ReadAccessUsers: []string{
			"any",
		},
		WriteAccessUsers: []string{
			"any",
		},
	}
	testDbConfigBytes, _ := json.Marshal(testDbConfig)
	mockserver.dbs["_dbs"].values = map[string]*value{
		"testDb": {
			values: []*types.Value{
				{
					Value: testDbConfigBytes,
					Metadata: &types.Metadata{
						Version:          nil,
						ReadAccessUsers:  nil,
						WriteAccessUsers: nil,
					},
				},
			},
			index: 0,
		},
	}

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

	defaultResult := &types.Value{
		Value: []byte("Default1"),
		Metadata: &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
		},
	}

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

	mockserver.dbs["testDb"].values = results
	mockserver.dbs["testDb"].defaultValue = defaultResult
	mockserver.height = ledgerHeight

	return mockserver
}

func (dbs *mockdbserver) GetStatus(ctx context.Context, req *types.GetStatusQueryEnvelope) (*types.GetStatusResponseEnvelope, error) {
	_, ok := dbs.dbs[req.Payload.DBName]
	return dbStatusToEnv(&types.GetStatusResponse{
		Header: &types.ResponseHeader{
			NodeID: nodeID,
		},
		Exist: ok,
	})
}

func (db *mockdb) GetState(req *types.GetStateQueryEnvelope) *types.Value {
	val, ok := db.values[req.Payload.Key]
	if !ok {
		return nil
	}
	if val.index < len(val.values) {
		res := val.values[val.index]
		val.index += 1
		return res
	}
	return val.values[len(val.values)-1]
}

func (db *mockdb) PutState(req *types.KVWrite) error {
	_, ok := db.values[req.Key]
	if req.IsDelete {
		if !ok {
			db.values[req.Key] = &value{
				values: make([]*types.Value, 0),
			}
			return nil
		}
		db.values[req.Key].values = append(db.values[req.Key].values, nil)
		db.values[req.Key].index += 1
	}
	if !ok {
		db.values[req.Key] = &value{
			values: []*types.Value{
				{
					Value: req.Value,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: db.server.height.results[db.server.height.index].Height,
							TxNum:    0,
						},
						ReadAccessUsers:  []string{},
						WriteAccessUsers: []string{},
					},
				},
			},
			index: 0,
		}
	} else {
		db.values[req.Key].values = append(db.values[req.Key].values, &types.Value{
			Value: req.Value,
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: db.server.height.results[db.server.height.index].Height,
					TxNum:    0,
				},
				ReadAccessUsers:  []string{},
				WriteAccessUsers: []string{},
			},
		})
	}
	return nil
}
