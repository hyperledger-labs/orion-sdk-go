package server

import (
	"context"
	"github.ibm.com/blockchaindb/server/api"
)

type queryServer struct {
	api.UnimplementedQueryServer
	getResults         map[string]*getResult
	defaultGetResult   *getResult
	getDBStatusResults map[string]*getDbStatusResult
}

type getResult struct {
	results []*api.Value
	index   int
}

type getDbStatusResult struct {
	results []*api.DBStatus
	index   int
}

func (qs *queryServer) Get(ctx context.Context, req *api.DataQuery) (*api.Value, error) {
	val, ok := qs.getResults[req.Key]
	if !ok {
		val = qs.defaultGetResult
	}
	if val.index < len(val.results) {
		res := val.results[val.index]
		val.index += 1
		return res, nil
	}
	return val.results[len(val.results)-1], nil
}

func (qs *queryServer) GetDBStatus(ctx context.Context, req *api.DBName) (*api.DBStatus, error) {
	val, ok := qs.getDBStatusResults[req.DbName]
	if !ok {
		return &api.DBStatus{
			Exist: false,
		}, nil
	}
	if val.index < len(val.results) {
		res := val.results[val.index]
		val.index += 1
		return res, nil
	}
	return val.results[len(val.results)-1], nil
}

func NewQueryServer() (*queryServer, error) {
	key1result := &getResult{
		results: make([]*api.Value, 0),
		index:   0,
	}
	key1result.results = append(key1result.results, &api.Value{
		Value: []byte("Testvalue11"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 0,
				TxNum:    0,
			},
		},
	})
	key1result.results = append(key1result.results, &api.Value{
		Value: []byte("Testvalue12"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 1,
				TxNum:    0,
			},
		},
	})

	key2result := &getResult{
		results: make([]*api.Value, 0),
		index:   0,
	}
	key2result.results = append(key2result.results, &api.Value{
		Value: []byte("Testvalue21"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 0,
				TxNum:    1,
			},
		},
	})
	defaultResult := &getResult{
		results: make([]*api.Value, 0),
		index:   0,
	}
	defaultResult.results = append(defaultResult.results, &api.Value{
		Value: []byte("Default1"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 1,
				TxNum:    1,
			},
		},
	})

	results := make(map[string]*getResult)
	results["key1"] = key1result
	results["key2"] = key2result

	dbStatusResults := make(map[string]*getDbStatusResult)
	testDBResult := &getDbStatusResult{
		results: make([]*api.DBStatus, 0),
		index:   0,
	}

	testDBResult.results = append(testDBResult.results, &api.DBStatus{
		Exist: true,
	})
	dbStatusResults["testDb"] = testDBResult

	return &queryServer{
		getResults:         results,
		defaultGetResult:   defaultResult,
		getDBStatusResults: dbStatusResults,
	}, nil
}
