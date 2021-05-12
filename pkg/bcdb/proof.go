// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"bytes"
	"encoding/json"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

type TxProof struct {
	intermediateHashes [][]byte
}

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
	for i, pHash := range p.intermediateHashes {
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
