// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"encoding/json"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

type QueryExecutor struct {
	*commonTxContext
}

func (q *QueryExecutor) ExecuteJSONQuery(dbName, query string) ([]*types.KVWithMetadata, error) {
	marshaledJSONQuery, err := json.Marshal(query)
	if err != nil {
		return nil, errors.WithMessage(err, "check whether the query string passed is in JSON format")
	}
	path := constants.URLForJSONQuery(dbName)
	resEnv := &types.DataQueryResponseEnvelope{}
	if err = q.handleRequestWithPost(
		path,
		marshaledJSONQuery,
		&types.DataJSONQuery{
			UserId: q.userID,
			DbName: dbName,
			Query:  query,
		},
		resEnv,
	); err != nil {
		q.logger.Errorf("failed to execute ledger block query %s, due to %s", path, err)
		return nil, err
	}

	return resEnv.GetResponse().KVs, nil
}

func (q *QueryExecutor) GetDataByRange(dbName, startKey, endKey string, limit uint64) (Iterator, error) {
	kvs, pendingResult, nextStartKey, err := q.getDataByRange(dbName, startKey, endKey, limit)
	if err != nil {
		return nil, err
	}

	return &RangeQueryIterator{
		kvs:           kvs,
		currentLoc:    0,
		pendingResult: pendingResult,
		dbName:        dbName,
		nextStartKey:  nextStartKey,
		endKey:        endKey,
		limit:         limit,
		limitReached:  false,
		q:             q,
	}, nil
}

func (q *QueryExecutor) getDataByRange(dbName, startKey, endKey string, limit uint64) ([]*types.KVWithMetadata, bool, string, error) {
	path := constants.URLForGetDataRange(dbName, startKey, endKey, limit)
	resEnv := &types.GetDataRangeResponseEnvelope{}
	if err := q.handleRequest(
		path,
		&types.GetDataRangeQuery{
			UserId:   q.userID,
			DbName:   dbName,
			StartKey: startKey,
			EndKey:   endKey,
			Limit:    limit,
		},
		resEnv,
	); err != nil {
		q.logger.Errorf("failed to execute range query %s, due to %s", path, err)
		return nil, false, "", err
	}

	return resEnv.GetResponse().KVs, resEnv.GetResponse().PendingResult, resEnv.GetResponse().NextStartKey, nil
}

type RangeQueryIterator struct {
	kvs           []*types.KVWithMetadata
	currentLoc    int
	pendingResult bool
	dbName        string
	nextStartKey  string
	endKey        string
	limit         uint64
	limitReached  bool
	q             *QueryExecutor
}

func (i *RangeQueryIterator) Next() (*types.KVWithMetadata, bool, error) {
	if i.currentLoc < len(i.kvs) {
		return i.fetchNextAndAdjustReaminingResultCount()
	}

	if !i.pendingResult || i.limitReached {
		return nil, false, nil
	}

	kvs, pending, next, err := i.q.getDataByRange(i.dbName, i.nextStartKey, i.endKey, i.limit)
	if err != nil {
		return nil, false, err
	}
	if len(kvs) == 0 {
		return nil, false, nil
	}

	i.kvs = kvs
	i.pendingResult = pending
	i.nextStartKey = next
	i.currentLoc = 0

	return i.fetchNextAndAdjustReaminingResultCount()
}

func (i *RangeQueryIterator) fetchNextAndAdjustReaminingResultCount() (*types.KVWithMetadata, bool, error) {
	kv := i.kvs[i.currentLoc]
	i.currentLoc++
	if i.limit > 0 {
		i.limit--
		if i.limit == 0 {
			i.limitReached = true
		}
	}

	return kv, true, nil
}
