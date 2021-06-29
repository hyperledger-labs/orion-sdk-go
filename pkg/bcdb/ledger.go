// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

type ledger struct {
	*commonTxContext
}

func (l *ledger) GetBlockHeader(blockNum uint64) (*types.BlockHeader, error) {
	path := constants.URLForLedgerBlock(blockNum)
	resEnv := &types.GetBlockResponseEnvelope{}
	err := l.handleRequest(
		path,
		&types.GetBlockQuery{
			UserId:      l.userID,
			BlockNumber: blockNum,
		},
		resEnv,
	)
	if err != nil {
		l.logger.Errorf("failed to execute ledger block query %s, due to %s", path, err)
		return nil, err
	}

	// TODO: signature verification

	return resEnv.GetResponse().GetBlockHeader(), nil
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

	// TODO: signature verification

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

	// TODO: signature verification

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
		l.logger.Errorf("failed to execute transaction receipt query %s, due to %s", path, err)
		return nil, err
	}

	// TODO: signature verification

	return resEnv.GetResponse().GetReceipt(), nil
}
