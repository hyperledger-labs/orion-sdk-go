package bcdb

import (
	"errors"
	"fmt"

	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type provenance struct {
	commonTxContext
}

func (p *provenance) GetBlockHeader(blockNum uint64) (*types.BlockHeader, error) {
	res := &types.GetBlockResponseEnvelope{}
	err := handleRequest(p, constants.URLForLedgerBlock(blockNum), &types.GetBlockQuery{
		UserID:      p.userID,
		BlockNumber: blockNum,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute ledger block query %s, due to %s", constants.URLForLedgerBlock(blockNum), err)
		return nil, err
	}
	return res.GetPayload().GetBlockHeader(), nil
}

func (p *provenance) GetLedgerPath(startBlock, endBlock uint64) ([]*types.BlockHeader, error) {
	res := &types.GetLedgerPathResponseEnvelope{}
	err := handleRequest(p, constants.URLForLedgerPath(startBlock, endBlock), &types.GetLedgerPathQuery{
		UserID:           p.userID,
		StartBlockNumber: startBlock,
		EndBlockNumber:   endBlock,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute ledger path query path %s, due to %s", constants.URLForLedgerPath(startBlock, endBlock), err)
		return nil, err
	}
	return res.GetPayload().GetBlockHeaders(), nil
}

func (p *provenance) GetTransactionProof(blockNum uint64, txIndex int) ([][]byte, error) {
	res := &types.GetTxProofResponseEnvelope{}
	err := handleRequest(p, constants.URLTxProof(blockNum, txIndex), &types.GetTxProofQuery{
		UserID:      p.userID,
		BlockNumber: blockNum,
		TxIndex:     uint64(txIndex),
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute transaction proof query %s, due to %s", constants.URLTxProof(blockNum, txIndex), err)
		return nil, err
	}
	return res.GetPayload().GetHashes(), nil
}

func (p *provenance) GetTransactionReceipt(txId string) (*types.TxReceipt, error) {
	res := &types.GetTxReceiptResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetTransactionReceipt(txId), &types.GetTxReceiptQuery{
		UserID: p.userID,
		TxID:   txId,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute transaction receipt query %s, due to %s", constants.URLForGetTransactionReceipt(txId), err)
		return nil, err
	}

	return res.GetPayload().GetReceipt(), nil
}

func (p *provenance) GetHistoricalData(dbName, key string) ([]*types.ValueWithMetadata, error) {
	res := &types.GetHistoricalDataResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetHistoricalData(dbName, key), &types.GetHistoricalDataQuery{
		UserID: p.userID,
		DBName: dbName,
		Key:    key,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute historical data query %s, due to %s", constants.URLForGetHistoricalData(dbName, key), err)
		return nil, err
	}
	return res.GetPayload().GetValues(), nil
}

func (p *provenance) GetHistoricalDataAt(dbName, key string, version *types.Version) (*types.ValueWithMetadata, error) {
	res := &types.GetHistoricalDataResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetHistoricalDataAt(dbName, key, version), &types.GetHistoricalDataQuery{
		UserID:  p.userID,
		DBName:  dbName,
		Key:     key,
		Version: version,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to parse execute data query %s, due to %s", constants.URLForGetHistoricalDataAt(dbName, key, version), err)
		return nil, err
	}

	values := res.GetPayload().GetValues()
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > 1 {
		return nil, errors.New(fmt.Sprintf("error getting historical data fro specific version, more that one record returned"))
	}
	return values[0], nil
}

func (p *provenance) GetPreviousHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error) {
	res := &types.GetHistoricalDataResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetPreviousHistoricalData(dbName, key, version), &types.GetHistoricalDataQuery{
		UserID:    p.userID,
		DBName:    dbName,
		Key:       key,
		Version:   version,
		Direction: "previous",
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute previous historical data query %s, due to %s", constants.URLForGetPreviousHistoricalData(dbName, key, version), err)
		return nil, err
	}
	return res.GetPayload().GetValues(), nil
}

func (p *provenance) GetNextHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error) {
	res := &types.GetHistoricalDataResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetNextHistoricalData(dbName, key, version), &types.GetHistoricalDataQuery{
		UserID:    p.userID,
		DBName:    dbName,
		Key:       key,
		Version:   version,
		Direction: "next",
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute next historical data query %s, due to %s", constants.URLForGetNextHistoricalData(dbName, key, version), err)
		return nil, err
	}
	return res.GetPayload().GetValues(), nil
}

func (p *provenance) GetDataReadByUser(userID string) ([]*types.KVWithMetadata, error) {
	res := &types.GetDataReadByResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetDataReadBy(userID), &types.GetDataReadByQuery{
		UserID:       p.userID,
		TargetUserID: userID,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute data read by user query %s, due to %s", constants.URLForGetDataReadBy(userID), err)
		return nil, err
	}
	return res.GetPayload().GetKVs(), nil
}

func (p *provenance) GetDataWrittenByUser(userID string) ([]*types.KVWithMetadata, error) {
	res := &types.GetDataWrittenByResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetDataReadBy(userID), &types.GetDataWrittenByQuery{
		UserID:       p.userID,
		TargetUserID: userID,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute data written by user query %s, due to %s", constants.URLForGetDataWrittenBy(userID), err)
		return nil, err
	}
	return res.GetPayload().GetKVs(), nil
}

func (p *provenance) GetReaders(dbName, key string) ([]string, error) {
	res := &types.GetDataReadersResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetDataReaders(dbName, key), &types.GetDataReadersQuery{
		UserID: p.userID,
		DBName: dbName,
		Key:    key,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute data readers query %s, due to %s", constants.URLForGetDataReaders(dbName, key), err)
		return nil, err
	}

	if res.GetPayload().GetReadBy() == nil {
		return nil, nil
	}
	readers := make([]string, 0)
	for k, _ := range res.GetPayload().GetReadBy() {
		readers = append(readers, k)
	}
	return readers, nil
}

func (p *provenance) GetWriters(dbName, key string) ([]string, error) {
	res := &types.GetDataWritersResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetDataWriters(dbName, key), &types.GetDataWritersQuery{
		UserID: p.userID,
		DBName: dbName,
		Key:    key,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute data writers query %s, due to %s", constants.URLForGetDataWriters(dbName, key), err)
		return nil, err
	}

	if res.GetPayload().GetWrittenBy() == nil {
		return nil, nil
	}
	writers := make([]string, 0)
	for k, _ := range res.GetPayload().GetWrittenBy() {
		writers = append(writers, k)
	}
	return writers, nil
}

func (p *provenance) GetTxIDsSubmittedByUser(userID string) ([]string, error) {
	res := &types.GetTxIDsSubmittedByResponseEnvelope{}
	err := handleRequest(p, constants.URLForGetTxIDsSubmittedBy(userID), &types.GetTxIDsSubmittedByQuery{
		UserID:       p.userID,
		TargetUserID: userID,
	}, res)
	if err != nil {
		p.logger.Errorf("failed to execute tx id by user query %s, due to %s", constants.URLForGetTxIDsSubmittedBy(userID), err)
		return nil, err
	}
	return res.GetPayload().GetTxIDs(), nil
}

func (p *provenance) client() RestClient {
	return p.restClient
}

func (p *provenance) log() *logger.SugarLogger {
	return p.logger
}
