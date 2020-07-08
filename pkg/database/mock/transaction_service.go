package server

import (
	"context"

	"github.ibm.com/blockchaindb/server/api"
)

type transactionProcessor struct {
	api.UnimplementedTransactionSvcServer
}

func NewTransactionServer() (*transactionProcessor, error) {
	return &transactionProcessor{}, nil
}

func (tp *transactionProcessor) SubmitTransaction(ctx context.Context, tx *api.TransactionEnvelope) error {
	return nil
}
