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

func (tp *transactionProcessor) SubmitTransaction(ctx context.Context, tx *types.DataTxEnvelope) error {
	db, ok := tp.dbserver.dbs[tx.Payload.DBName]
	if !ok {
		return errors.Errorf("database not exist %s", tx.Payload.DBName)
	}

	for _, kvWrite := range tx.Payload.DataWrites {
		db.PutState(kvWrite)
	}

	for _, del := range tx.Payload.DataDeletes {
		db.DelState(del)
	}
	return nil
}
