// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type ledger struct {
	*commonTxContext
}

func (l *ledger) GetBlockHeader(blockNum uint64) (*types.BlockHeader, error) {
	path := constants.URLForLedgerBlock(blockNum)
	res := &types.GetBlockResponse{}
	err := l.handleRequest(path, &types.GetBlockQuery{
		UserID:      l.userID,
		BlockNumber: blockNum,
	}, res)
	if err != nil {
		l.logger.Errorf("failed to execute ledger block query %s, due to %s", path, err)
		return nil, err
	}
	return res.GetBlockHeader(), nil
}

func (l *ledger) GetLedgerPath(startBlock, endBlock uint64) ([]*types.BlockHeader, error) {
	path := constants.URLForLedgerPath(startBlock, endBlock)
	res := &types.GetLedgerPathResponse{}
	err := l.handleRequest(path, &types.GetLedgerPathQuery{
		UserID:           l.userID,
		StartBlockNumber: startBlock,
		EndBlockNumber:   endBlock,
	}, res)
	if err != nil {
		l.logger.Errorf("failed to execute ledger path query path %s, due to %s", path, err)
		return nil, err
	}
	return res.GetBlockHeaders(), nil
}

func (l *ledger) GetTransactionProof(blockNum uint64, txIndex int) (*TxProof, error) {
	path := constants.URLTxProof(blockNum, txIndex)
	res := &types.GetTxProofResponse{}
	err := l.handleRequest(path, &types.GetTxProofQuery{
		UserID:      l.userID,
		BlockNumber: blockNum,
		TxIndex:     uint64(txIndex),
	}, res)
	if err != nil {
		l.logger.Errorf("failed to execute transaction proof query %s, due to %s", path, err)
		return nil, err
	}
	return &TxProof{intermediateHashes: res.GetHashes()}, nil
}

func (l *ledger) GetTransactionReceipt(txId string) (*types.TxReceipt, error) {
	path := constants.URLForGetTransactionReceipt(txId)
	res := &types.TxResponse{}
	err := l.handleRequest(path, &types.GetTxReceiptQuery{
		UserID: l.userID,
		TxID:   txId,
	}, res)
	if err != nil {
		l.logger.Errorf("failed to execute transaction receipt query %s, due to %s", path, err)
		return nil, err
	}

	return res.GetReceipt(), nil
}
