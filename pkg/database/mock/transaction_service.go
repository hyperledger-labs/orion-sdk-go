package server

import (
	"context"

	"github.ibm.com/blockchaindb/protos/types"
)

type transactionProcessor struct {
}

func NewTransactionServer() (*transactionProcessor, error) {
	return &transactionProcessor{}, nil
}

func (tp *transactionProcessor) SubmitTransaction(ctx context.Context, tx *types.TransactionEnvelope) error {
	return nil
}
