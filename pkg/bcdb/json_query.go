// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"encoding/json"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

type JSONQueryExecutor struct {
	*commonTxContext
}

func (j *JSONQueryExecutor) Execute(dbName, query string) ([]*types.KVWithMetadata, error) {
	marshaledJSONQuery, err := json.Marshal(query)
	if err != nil {
		return nil, errors.WithMessage(err, "check whether the query string passed is in JSON format")
	}
	path := constants.URLForJSONQuery(dbName)
	resEnv := &types.DataQueryResponseEnvelope{}
	if err = j.handleRequestWithPost(
		path,
		marshaledJSONQuery,
		&types.DataJSONQuery{
			UserId: j.userID,
			DbName: dbName,
			Query:  query,
		},
		resEnv,
	); err != nil {
		j.logger.Errorf("failed to execute ledger block query %s, due to %s", path, err)
		return nil, err
	}

	return resEnv.GetResponse().KVs, nil
}
