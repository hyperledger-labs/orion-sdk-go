// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"net/http"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type ledger struct {
	*commonTxContext
}

func (l *ledger) GetBlockHeader(blockNum uint64) (*types.BlockHeader, error) {
	path := constants.URLForLedgerBlock(blockNum, false)
	resEnv := &types.GetBlockResponseEnvelope{}
	err := l.handleRequest(
		path,
		&types.GetBlockQuery{
			UserId:      l.userID,
			BlockNumber: blockNum,
			Augmented:   false,
		},
		resEnv,
	)
	if err != nil {
		httpError, ok := err.(*httpError)
		if !ok || httpError.statusCode != http.StatusNotFound {
			l.logger.Errorf("failed to execute ledger block query %s, due to %s", path, err)
			return nil, err
		} else {
			return nil, &ErrorNotFound{err.Error()}
		}
	}

	return resEnv.GetResponse().GetBlockHeader(), nil
}

func (l *ledger) NewBlockHeaderDeliveryService(conf *BlockHeaderDeliveryConfig) BlockHeaderDelivererService {
	d := &blockHeaderDeliverer{
		blockHeaders: make(chan interface{}, conf.Capacity),
		stop:         make(chan struct{}),
		conf:         conf,
		txContext:    l.commonTxContext,
		logger:       l.logger,
	}

	go d.start()

	return d
}

func (l *ledger) GetLedgerPath(startBlock, endBlock uint64) ([]*types.BlockHeader, error) {
	path := constants.URLForLedgerPath(startBlock, endBlock)
	resEnv := &types.GetLedgerPathResponseEnvelope{}
	err := l.handleRequest(
		path,
		&types.GetLedgerPathQuery{
			UserId:           l.userID,
			StartBlockNumber: startBlock,
			EndBlockNumber:   endBlock,
		},
		resEnv,
	)
	if err != nil {
		l.logger.Errorf("failed to execute ledger path query path %s, due to %s", path, err)
		return nil, err
	}

	return resEnv.GetResponse().GetBlockHeaders(), nil
}

func (l *ledger) GetTransactionProof(blockNum uint64, txIndex int) (*TxProof, error) {
	path := constants.URLTxProof(blockNum, txIndex)
	resEnv := &types.GetTxProofResponseEnvelope{}
	err := l.handleRequest(
		path,
		&types.GetTxProofQuery{
			UserId:      l.userID,
			BlockNumber: blockNum,
			TxIndex:     uint64(txIndex),
		}, resEnv,
	)
	if err != nil {
		l.logger.Errorf("failed to execute transaction proof query %s, due to %s", path, err)
		return nil, err
	}

	return &TxProof{
		intermediateHashes: resEnv.GetResponse().GetHashes(),
	}, nil
}

func (l *ledger) GetTransactionReceipt(txId string) (*types.TxReceipt, error) {
	path := constants.URLForGetTransactionReceipt(txId)
	resEnv := &types.TxReceiptResponseEnvelope{}
	err := l.handleRequest(
		path,
		&types.GetTxReceiptQuery{
			UserId: l.userID,
			TxId:   txId,
		}, resEnv,
	)
	if err != nil {
		httpError, ok := err.(*httpError)
		if !ok || httpError.statusCode != http.StatusNotFound {
			l.logger.Errorf("failed to execute transaction receipt query %s, due to %s", path, err)
			return nil, err
		} else {
			return nil, &ErrorNotFound{err.Error()}
		}
	}

	return resEnv.GetResponse().GetReceipt(), nil
}

func (l *ledger) GetDataProof(blockNum uint64, dbName, key string, isDeleted bool) (*state.Proof, error) {
	path := constants.URLDataProof(blockNum, dbName, key, isDeleted)
	resEnv := &types.GetDataProofResponseEnvelope{}
	err := l.handleRequest(
		path,
		&types.GetDataProofQuery{
			UserId:      l.userID,
			BlockNumber: blockNum,
			DbName:      dbName,
			Key:         key,
			IsDeleted:   isDeleted,
		}, resEnv,
	)
	if err != nil {
		l.logger.Errorf("failed to execute state proof query %s, due to %s", path, err)
		return nil, err
	}

	return state.NewProof(resEnv.GetResponse().GetPath()), nil
}

// CalculateValueHash creates unique hash for specific value, by hashing concatenation
// of database name, key and value hashes
func CalculateValueHash(dbName, key string, value []byte) ([]byte, error) {
	stateTrieKey, err := state.ConstructCompositeKey(dbName, key)
	if err != nil {
		return nil, err
	}
	valueHash, err := state.CalculateKeyValueHash(stateTrieKey, value)
	if err != nil {
		return nil, err
	}
	return valueHash, nil
}
