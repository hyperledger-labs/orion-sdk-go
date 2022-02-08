// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

// TxProof keeps Merkle tree proof for specific transaction
type TxProof struct {
	// IntermediateHashes are hashes between leaf (transaction) hash and tree root
	// for simplicity, both tx hash and tree root is part of IntermediateHashes
	IntermediateHashes [][]byte
}

// Verify the validity of the proof with respect to the Tx and TxReceipt.
// receipt stores the block header and the tx-index in that block. The block header contains the Merkle tree root and the tx validation info. The validation info is indexed by the tx-index.
// tx stores the transaction envelope content.
func (p *TxProof) Verify(receipt *types.TxReceipt, tx proto.Message) (bool, error) {
	txEnv, ok := tx.(*types.DataTxEnvelope)
	if !ok {
		return false, errors.Errorf("tx [%s] is not data transaction, only data transaction supported so far", tx.String())
	}
	valInfo := receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()]
	txBytes, err := json.Marshal(txEnv)
	if err != nil {
		return false, errors.Wrapf(err, "can't serialize tx [%s] to json", tx.String())
	}
	viBytes, err := json.Marshal(valInfo)
	if err != nil {
		return false, errors.Wrapf(err, "can't serialize validation info [%s] to json", valInfo.String())
	}
	txHash, err := crypto.ComputeSHA256Hash(append(txBytes, viBytes...))
	if err != nil {
		return false, errors.Wrap(err, "can't calculate concatenated hash of tx and its validation info")
	}
	var currHash []byte
	for i, pHash := range p.IntermediateHashes {
		if i == 0 {
			if !bytes.Equal(txHash, pHash) {
				return false, nil
			}
			currHash = txHash
			continue
		}
		currHash, err = crypto.ConcatenateHashes(currHash, pHash)
		if err != nil {
			return false, errors.Wrap(err, "can't calculate hash of two concatenated hashes")
		}
	}

	return bytes.Equal(receipt.GetHeader().GetTxMerkelTreeRootHash(), currHash), nil
}

// LedgerPath contains a skip list path in ledger, in form of block headers.
// It is used to make ledger path validation easier.
type LedgerPath struct {
	// Path keeps all block headers in ledger path.
	// Keep in mind that the skip list in the ledger is organized and validated backwards, from end of chain to genesis block,
	// so Path is sorted from higher block numbers to lower, for example the path from block 8 to block 1 is (8, 7, 5, 1).
	Path []*types.BlockHeader
}

// Verify ledger path correctness.
// begin is lower block number and end is higher, opposite to how path is actually sorted.
// This order makes it easier to the caller.
// Please, keep in mind that Path of one single block is correct by definition
func (lp *LedgerPath) Verify(begin, end *types.BlockHeader) (bool, error) {

	if len(lp.Path) < 1 {
		return false, &ProofVerificationError{"can't verify empty ledger path"}
	}
	if begin != nil {
		if !proto.Equal(begin, lp.Path[len(lp.Path)-1]) {
			return false, &ProofVerificationError{fmt.Sprintf("path begin not equal to provided begin block %+v %+v", lp.Path[len(lp.Path)-1], begin)}
		}
	}

	if end != nil {
		if !proto.Equal(end, lp.Path[0]) {
			return false, &ProofVerificationError{fmt.Sprintf("path end not equal to provided end block %+v %+v", lp.Path[0], end)}
		}
	}

	currentBlockHeader := lp.Path[0]
	for _, nextBlockHeader := range lp.Path[1:] {
		headerBytes, err := proto.Marshal(nextBlockHeader)
		if err != nil {
			return false, err
		}
		nextBlockHash, err := crypto.ComputeSHA256Hash(headerBytes)
		if err != nil {
			return false, err
		}

		hashFound := false
		for _, hash := range currentBlockHeader.GetSkipchainHashes() {
			if bytes.Equal(nextBlockHash, hash) {
				hashFound = true
				break
			}
		}

		if !hashFound {
			return false, &ProofVerificationError{fmt.Sprintf("hash of block %d not found in list of skip list hashes of block %d", nextBlockHeader.GetBaseHeader().GetNumber(), currentBlockHeader.GetBaseHeader().GetNumber())}
		}

		currentBlockHeader = nextBlockHeader
	}
	return true, nil
}

type ProofVerificationError struct {
	msg string
}

func (e *ProofVerificationError) Error() string {
	return e.msg
}
