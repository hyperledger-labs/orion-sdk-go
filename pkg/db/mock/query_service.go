package server

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.ibm.com/blockchaindb/server/api"
)

type queryServer struct {
	api.UnimplementedQueryServer
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
	results []*api.LedgerHeight
	index   int
}

type dbStatus struct {
	values []*api.DBStatus
	index  int
}

func (qs *queryServer) Get(ctx context.Context, req *api.DataQuery) (*api.Value, error) {
	val, ok := qs.values[req.Key]
	if !ok {
		val = qs.defaultValue
	}
	if val.index < len(val.values) {
		res := val.values[val.index]
		val.index += 1
		return res, nil
	}
	return val.values[len(val.values)-1], nil
}

func (qs *queryServer) GetDBStatus(ctx context.Context, req *api.DBName) (*api.DBStatus, error) {
	val, ok := qs.dbStatuses[req.DbName]
	if !ok {
		return &api.DBStatus{
			Exist: false,
		}, nil
	}
	if val.index < len(val.values) {
		res := val.values[val.index]
		val.index += 1
		return res, nil
	}
	return val.values[len(val.values)-1], nil
}

func (qs *queryServer) GetBlockHeight(ctx context.Context, req *empty.Empty) (*api.LedgerHeight, error) {
	if qs.ledgerHeight.index < len(qs.ledgerHeight.results) {
		res := qs.ledgerHeight.results[qs.ledgerHeight.index]
		qs.ledgerHeight.index += 1
		return res, nil
	}
	return qs.ledgerHeight.results[len(qs.ledgerHeight.results)-1], nil
}

func NewQueryServer() (*queryServer, error) {
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
		results: make([]*api.LedgerHeight, 0),
		index:   0,
	}
	ledgerHeight.results = append(ledgerHeight.results, &api.LedgerHeight{
		Height: 0,
	})
	ledgerHeight.results = append(ledgerHeight.results, &api.LedgerHeight{
		Height: 1,
	})

	results := make(map[string]*value)
	results["key1"] = key1result
	results["key2"] = key2result

	dbStatusResults := make(map[string]*dbStatus)
	testDBResult := &dbStatus{
		values: make([]*api.DBStatus, 0),
		index:  0,
	}

	testDBResult.values = append(testDBResult.values, &api.DBStatus{
		Exist: true,
	})
	dbStatusResults["testDb"] = testDBResult

	return &queryServer{
		values:       results,
		defaultValue: defaultResult,
		dbStatuses:   dbStatusResults,
		ledgerHeight: ledgerHeight,
	}, nil
}
