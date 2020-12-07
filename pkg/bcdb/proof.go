package bcdb

import (
	"bytes"
	"encoding/json"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/types"
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
	currHash, err := crypto.ComputeSHA256Hash(append(txBytes, viBytes...))
	if err != nil {
		return false, errors.Wrap(err, "can't calculate concatenated hash of tx and its validation info")
	}
	for _, pHash := range p.intermediateHashes {
		currHash, err = crypto.ConcatenateHashes(currHash, pHash)
		if err != nil {
			return false, errors.Wrap(err, "can't calculate hash of two concatenated hashes")
		}
	}

	return bytes.Equal(receipt.GetHeader().GetTxMerkelTreeRootHash(), currHash), nil
}
