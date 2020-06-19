package server

import (
	"github.com/golang/protobuf/proto"
	"github.ibm.com/blockchaindb/server/api"
	"io"
)

type transactionServer struct {
	api.UnimplementedTransactionSvcServer
}

func NewTransactionServer() (*transactionServer, error) {
	return &transactionServer{}, nil
}

func (ts *transactionServer) SubmitTransaction(srv api.TransactionSvc_SubmitTransactionServer) error {
	ctx := srv.Context()
	for {
		// exit if context is done
		// or continue
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		envelope, err := srv.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		payload := &api.Payload{}
		if err := proto.Unmarshal(envelope.Payload, payload); err != nil {
			return err
		}

		tx := &api.Transaction{}
		if err := proto.Unmarshal(payload.Data, tx); err != nil {
			return nil
		}

		res := &api.Response{
			Txid:      tx.TxId,
			Status:    false,
			Signature: nil,
		}
		if err := srv.Send(res); err != nil {
			return err
		}
	}
}
