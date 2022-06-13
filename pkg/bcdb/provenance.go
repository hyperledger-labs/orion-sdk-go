// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"errors"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type provenance struct {
	*commonTxContext
}

func (p *provenance) GetHistoricalData(dbName, key string) ([]*types.ValueWithMetadata, error) {
	path := constants.URLForGetHistoricalData(dbName, key)
	resEnv := &types.GetHistoricalDataResponseEnvelope{}
	err := p.handleRequest(
		path,
		&types.GetHistoricalDataQuery{
			UserId: p.userID,
			DbName: dbName,
			Key:    key,
		}, resEnv,
	)
	if err != nil {
		p.logger.Errorf("failed to execute historical data query %s, due to %s", path, err)
		return nil, err
	}
	return resEnv.GetResponse().GetValues(), nil
}

func (p *provenance) GetHistoricalDataAt(dbName, key string, version *types.Version) (*types.ValueWithMetadata, error) {
	path := constants.URLForGetHistoricalDataAt(dbName, key, version)
	resEnv := &types.GetHistoricalDataResponseEnvelope{}
	err := p.handleRequest(
		path,
		&types.GetHistoricalDataQuery{
			UserId:  p.userID,
			DbName:  dbName,
			Key:     key,
			Version: version,
		}, resEnv,
	)
	if err != nil {
		p.logger.Errorf("failed to parse execute data query %s, due to %s", path, err)
		return nil, err
	}

	values := resEnv.GetResponse().GetValues()
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > 1 {
		return nil, errors.New("error getting historical data fro specific version, more that one record returned")
	}
	return values[0], nil
}

func (p *provenance) GetPreviousHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error) {
	path := constants.URLForGetPreviousHistoricalData(dbName, key, version)
	resEnv := &types.GetHistoricalDataResponseEnvelope{}
	err := p.handleRequest(
		path,
		&types.GetHistoricalDataQuery{
			UserId:    p.userID,
			DbName:    dbName,
			Key:       key,
			Version:   version,
			Direction: "previous",
		}, resEnv,
	)
	if err != nil {
		p.logger.Errorf("failed to execute previous historical data query %s, due to %s", path, err)
		return nil, err
	}
	return resEnv.GetResponse().GetValues(), nil
}

func (p *provenance) GetNextHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error) {
	path := constants.URLForGetNextHistoricalData(dbName, key, version)
	resEnv := &types.GetHistoricalDataResponseEnvelope{}
	err := p.handleRequest(
		path,
		&types.GetHistoricalDataQuery{
			UserId:    p.userID,
			DbName:    dbName,
			Key:       key,
			Version:   version,
			Direction: "next",
		}, resEnv,
	)
	if err != nil {
		p.logger.Errorf("failed to execute next historical data query %s, due to %s", path, err)
		return nil, err
	}
	return resEnv.GetResponse().GetValues(), nil
}

func (p *provenance) GetDataReadByUser(userID string) (map[string]*types.KVsWithMetadata, error) {
	path := constants.URLForGetDataReadBy(userID)
	resEnv := &types.GetDataProvenanceResponseEnvelope{}
	err := p.handleRequest(
		path,
		&types.GetDataReadByQuery{
			UserId:       p.userID,
			TargetUserId: userID,
		}, resEnv,
	)
	if err != nil {
		p.logger.Errorf("failed to execute data read by user query %s, due to %s", path, err)
		return nil, err
	}
	return resEnv.GetResponse().GetDBKeyValues(), nil
}

func (p *provenance) GetDataWrittenByUser(userID string) (map[string]*types.KVsWithMetadata, error) {
	path := constants.URLForGetDataWrittenBy(userID)
	resEnv := &types.GetDataProvenanceResponseEnvelope{}
	err := p.handleRequest(
		path,
		&types.GetDataWrittenByQuery{
			UserId:       p.userID,
			TargetUserId: userID,
		}, resEnv,
	)
	if err != nil {
		p.logger.Errorf("failed to execute data written by user query %s, due to %s", path, err)
		return nil, err
	}

	return resEnv.GetResponse().GetDBKeyValues(), nil
}

func (p *provenance) GetReaders(dbName, key string) ([]string, error) {
	path := constants.URLForGetDataReaders(dbName, key)
	resEnv := &types.GetDataReadersResponseEnvelope{}
	err := p.handleRequest(
		path,
		&types.GetDataReadersQuery{
			UserId: p.userID,
			DbName: dbName,
			Key:    key,
		}, resEnv,
	)
	if err != nil {
		p.logger.Errorf("failed to execute data readers query %s, due to %s", path, err)
		return nil, err
	}

	res := resEnv.GetResponse()
	if res.GetReadBy() == nil {
		return nil, nil
	}
	readers := make([]string, 0)
	for k := range res.GetReadBy() {
		readers = append(readers, k)
	}
	return readers, nil
}

func (p *provenance) GetWriters(dbName, key string) ([]string, error) {
	path := constants.URLForGetDataWriters(dbName, key)
	resEnv := &types.GetDataWritersResponseEnvelope{}
	err := p.handleRequest(
		path,
		&types.GetDataWritersQuery{
			UserId: p.userID,
			DbName: dbName,
			Key:    key,
		}, resEnv,
	)
	if err != nil {
		p.logger.Errorf("failed to execute data writers query %s, due to %s", path, err)
		return nil, err
	}

	res := resEnv.GetResponse()
	if res.GetWrittenBy() == nil {
		return nil, nil
	}
	writers := make([]string, 0)
	for k := range res.GetWrittenBy() {
		writers = append(writers, k)
	}
	return writers, nil
}

func (p *provenance) GetTxIDsSubmittedByUser(userID string) ([]string, error) {
	path := constants.URLForGetTxIDsSubmittedBy(userID)
	resEnv := &types.GetTxIDsSubmittedByResponseEnvelope{}
	err := p.handleRequest(
		path,
		&types.GetTxIDsSubmittedByQuery{
			UserId:       p.userID,
			TargetUserId: userID,
		},
		resEnv,
	)
	if err != nil {
		p.logger.Errorf("failed to execute tx id by user query %s, due to %s", path, err)
		return nil, err
	}
	return resEnv.GetResponse().GetTxIDs(), nil
}
