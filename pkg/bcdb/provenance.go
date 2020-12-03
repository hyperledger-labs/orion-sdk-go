package bcdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type provenance struct {
	commonTxContext
}

func (p *provenance) GetBlockHeader(blockNum uint64) (*types.BlockHeader, error) {
	getBlock := &url.URL{
		Path: constants.URLForLedgerBlock(blockNum),
	}
	replica := p.selectReplica()
	configREST := replica.ResolveReference(getBlock)

	ctx := context.TODO() // TODO: Replace with timeout
	response, err := p.restClient.Query(ctx, configREST.String(), &types.GetBlockQuery{
		UserID:      p.userID,
		BlockNumber: blockNum,
	})
	if err != nil {
		p.logger.Errorf("failed to send block query blockNum = %d, due to %s", blockNum, err)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		p.logger.Errorf("error getting block, server returned %s", response.Status)
		return nil, errors.New(fmt.Sprintf("error getting block, server returned %s", response.Status))
	}

	res := &types.GetBlockResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		p.logger.Errorf("failed to decode json response, due to", err)
		return nil, err
	}

	return res.GetPayload().GetBlockHeader(), nil
}

func (p *provenance) GetLedgerPath(startBlock, endBlock uint64) ([]*types.BlockHeader, error) {
	getPath, err := url.Parse(constants.URLForLedgerPath(startBlock, endBlock))
	if err != nil {
		p.logger.Errorf("failed to parse ledger path query path %s, due to %s", constants.URLForLedgerPath(startBlock, endBlock), err)
		return nil, err
	}

	replica := p.selectReplica()
	configREST := replica.ResolveReference(getPath)

	ctx := context.TODO() // TODO: Replace with timeout
	response, err := p.restClient.Query(ctx, configREST.String(), &types.GetLedgerPathQuery{
		UserID:           p.userID,
		StartBlockNumber: startBlock,
		EndBlockNumber:   endBlock,
	})
	if err != nil {
		p.logger.Errorf("failed to send ledger path query startBlockNum = %d, endBlock = %d, due to %s", startBlock, endBlock, err)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		p.logger.Errorf("error getting ledger path, server returned %s", response.Status)
		return nil, errors.New(fmt.Sprintf("error getting ledger path, server returned %s", response.Status))
	}

	res := &types.GetLedgerPathResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		p.logger.Errorf("failed to decode json response, due to", err)
		return nil, err
	}

	return res.GetPayload().GetBlockHeaders(), nil
}

func (p *provenance) GetTransactionProof(blockNum uint64, txIndex int) ([][]byte, error) {
	getProof, err := url.Parse(constants.URLTxProof(blockNum, txIndex))
	if err != nil {
		p.logger.Errorf("failed to parse transaction proof query %s, due to %s", constants.URLTxProof(blockNum, txIndex), err)
		return nil, err
	}
	replica := p.selectReplica()
	configREST := replica.ResolveReference(getProof)

	ctx := context.TODO() // TODO: Replace with timeout
	response, err := p.restClient.Query(ctx, configREST.String(), &types.GetTxProofQuery{
		UserID:      p.userID,
		BlockNumber: blockNum,
		TxIndex:     uint64(txIndex),
	})
	if err != nil {
		p.logger.Errorf("failed to send transaction proof query block = %d, txIndex = %d, due to %s", blockNum, txIndex, err)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		p.logger.Errorf("error getting transaction proof, server returned %s", response.Status)
		return nil, errors.New(fmt.Sprintf("error getting transaction proof, server returned %s", response.Status))
	}

	res := &types.GetTxProofResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		p.logger.Errorf("failed to decode json response, due to", err)
		return nil, err
	}

	return res.GetPayload().GetHashes(), nil
}

func (p *provenance) GetTransactionReceipt(txId string) (*types.TxReceipt, error) {
	getProof, err := url.Parse(constants.URLForGetTransactionReceipt(txId))
	if err != nil {
		p.logger.Errorf("failed to parse transaction receipt query %s, due to %s", constants.URLForGetTransactionReceipt(txId), err)
		return nil, err
	}
	replica := p.selectReplica()
	configREST := replica.ResolveReference(getProof)

	ctx := context.TODO() // TODO: Replace with timeout
	response, err := p.restClient.Query(ctx, configREST.String(), &types.GetTxReceiptQuery{
		UserID: p.userID,
		TxID:   txId,
	})
	if err != nil {
		p.logger.Errorf("failed to send transaction receipt query txID = %s, due to %s", txId, err)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		p.logger.Errorf("error getting transaction receipt, server returned %s", response.Status)
		return nil, errors.New(fmt.Sprintf("error getting transaction receipt, server returned %s", response.Status))
	}

	res := &types.GetTxReceiptResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		p.logger.Errorf("failed to decode json response, due to", err)
		return nil, err
	}

	return res.GetPayload().GetReceipt(), nil
}
