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
	res := &types.GetBlockResponseEnvelope{}
	err := l.handleRequest(path, &types.GetBlockQuery{
		UserID:      l.userID,
		BlockNumber: blockNum,
	}, res)
	if err != nil {
		l.logger.Errorf("failed to execute ledger block query %s, due to %s", path, err)
		return nil, err
	}
	return res.GetPayload().GetBlockHeader(), nil
}

func (l *ledger) GetLedgerPath(startBlock, endBlock uint64) ([]*types.BlockHeader, error) {
	path := constants.URLForLedgerPath(startBlock, endBlock)
	res := &types.GetLedgerPathResponseEnvelope{}
	err := l.handleRequest(path, &types.GetLedgerPathQuery{
		UserID:           l.userID,
		StartBlockNumber: startBlock,
		EndBlockNumber:   endBlock,
	}, res)
	if err != nil {
		l.logger.Errorf("failed to execute ledger path query path %s, due to %s", path, err)
		return nil, err
	}
	return res.GetPayload().GetBlockHeaders(), nil
}

func (l *ledger) GetTransactionProof(blockNum uint64, txIndex int) (*TxProof, error) {
	path := constants.URLTxProof(blockNum, txIndex)
	res := &types.GetTxProofResponseEnvelope{}
	err := l.handleRequest(path, &types.GetTxProofQuery{
		UserID:      l.userID,
		BlockNumber: blockNum,
		TxIndex:     uint64(txIndex),
	}, res)
	if err != nil {
		l.logger.Errorf("failed to execute transaction proof query %s, due to %s", path, err)
		return nil, err
	}
	return &TxProof{res.GetPayload().GetHashes()}, nil
}

func (l *ledger) GetTransactionReceipt(txId string) (*types.TxReceipt, error) {
	path := constants.URLForGetTransactionReceipt(txId)
	res := &types.GetTxReceiptResponseEnvelope{}
	err := l.handleRequest(path, &types.GetTxReceiptQuery{
		UserID: l.userID,
		TxID:   txId,
	}, res)
	if err != nil {
		l.logger.Errorf("failed to execute transaction receipt query %s, due to %s", path, err)
		return nil, err
	}

	return res.GetPayload().GetReceipt(), nil
}
