// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"fmt"
	"net/http"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"google.golang.org/protobuf/proto"
)

const GenesisBlockNumber = 1

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

func (l *ledger) GetLastBlockHeader() (*types.BlockHeader, error) {
	path := constants.URLForLastLedgerBlock()
	resEnv := &types.GetBlockResponseEnvelope{}
	err := l.handleRequest(
		path,
		&types.GetLastBlockQuery{
			UserId: l.userID,
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

func (l *ledger) GetLedgerPath(startBlock, endBlock uint64) (*LedgerPath, error) {
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

	return &LedgerPath{resEnv.GetResponse().GetBlockHeaders()}, nil
}

func (l *ledger) GetTransactionProof(blockNum uint64, txIndex int) (*TxProof, error) {
	path := constants.URLTxProof(blockNum, uint64(txIndex))
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
		IntermediateHashes: resEnv.GetResponse().GetHashes(),
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

func (l *ledger) GetTxContent(blockNum, txIndex uint64) (*types.GetTxResponse, error) {
	path := constants.URLTxContent(blockNum, txIndex)
	resEnv := &types.GetTxResponseEnvelope{}
	err := l.handleRequest(
		path,
		&types.GetTxContentQuery{
			UserId:      l.userID,
			BlockNumber: blockNum,
			TxIndex:     txIndex,
		},
		resEnv,
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

	return resEnv.GetResponse(), nil
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

func (l *ledger) GetFullTxProofAndVerify(txReceipt *types.TxReceipt, lastKnownBlockHeader *types.BlockHeader, tx proto.Message) (*TxProof, *LedgerPath, error) {
	txBlockHeader := txReceipt.GetHeader()
	if txBlockHeader.GetBaseHeader().GetNumber() <= GenesisBlockNumber ||
		txBlockHeader.GetBaseHeader().GetNumber() > lastKnownBlockHeader.GetBaseHeader().GetNumber() {
		return nil, nil, &ProofVerificationError{fmt.Sprintf("something wrong with blocks order: genesis: %d, tx block header %d, last know block header: %d",
			GenesisBlockNumber, txBlockHeader.GetBaseHeader().GetNumber(), lastKnownBlockHeader.GetBaseHeader().GetNumber())}
	}
	genesisHeader, err := l.GetBlockHeader(GenesisBlockNumber)
	if err != nil {
		return nil, nil, err
	}

	endBlockHeader, err := l.GetBlockHeader(lastKnownBlockHeader.GetBaseHeader().GetNumber())
	if err != nil {
		return nil, nil, err
	}
	if !proto.Equal(endBlockHeader, lastKnownBlockHeader) {
		return nil, nil, &ProofVerificationError{fmt.Sprintf("can't create proof, last known block (%d) is not same as in ledger", lastKnownBlockHeader.GetBaseHeader().GetNumber())}
	}

	pathPartOne := &LedgerPath{
		Path: []*types.BlockHeader{txBlockHeader},
	}
	if GenesisBlockNumber != txBlockHeader.GetBaseHeader().GetNumber() {
		pathPartOne, err = l.GetLedgerPath(GenesisBlockNumber, txBlockHeader.GetBaseHeader().GetNumber())
		if err != nil {
			return nil, nil, err
		}
	}

	pathPartTwo := &LedgerPath{
		Path: []*types.BlockHeader{txBlockHeader},
	}
	if txBlockHeader.GetBaseHeader().GetNumber() != lastKnownBlockHeader.GetBaseHeader().GetNumber() {
		pathPartTwo, err = l.GetLedgerPath(txBlockHeader.GetBaseHeader().GetNumber(), lastKnownBlockHeader.GetBaseHeader().GetNumber())
		if err != nil {
			return nil, nil, err
		}
	}

	txProof, err := l.GetTransactionProof(txBlockHeader.GetBaseHeader().GetNumber(), int(txReceipt.GetTxIndex()))
	if err != nil {
		return nil, nil, err
	}

	txValid, err := txProof.Verify(txReceipt, tx)
	if err != nil {
		return nil, nil, err
	}
	if !txValid {
		return nil, nil, &ProofVerificationError{"verification failed: tx merkle tree path"}
	}
	pathPartOneValid, err := pathPartOne.Verify(genesisHeader, txBlockHeader)
	if err != nil {
		return nil, nil, err
	}
	if !pathPartOneValid {
		return nil, nil, &ProofVerificationError{"verification failed: ledger path to genesis block"}
	}
	pathPartTwoValid, err := pathPartTwo.Verify(txBlockHeader, lastKnownBlockHeader)
	if err != nil {
		return nil, nil, err
	}
	if !pathPartTwoValid {
		return nil, nil, &ProofVerificationError{"verification failed: ledger path from last known block"}
	}
	return txProof, &LedgerPath{append(pathPartTwo.Path, pathPartOne.Path[1:]...)}, nil
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
