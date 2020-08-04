package server

import (
	"context"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
)

type transactionProcessor struct {
	dbserver *mockdbserver
}

func NewTransactionServer(dbserver *mockdbserver) (*transactionProcessor, error) {
	return &transactionProcessor{
		dbserver: dbserver,
	}, nil
}

func (tp *transactionProcessor) SubmitTransaction(ctx context.Context, tx *types.TransactionEnvelope) error {
	db, ok := tp.dbserver.dbs[tx.Payload.DBName]
	if !ok {
		return errors.Errorf("database not exist %s", tx.Payload.DBName)
	}

	for _, kvWrite := range tx.Payload.Writes {
		db.PutState(kvWrite)
	}
	return nil
}
