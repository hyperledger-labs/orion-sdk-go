// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"google.golang.org/protobuf/proto"
)

type dbWrites struct {
}

// LoadedDataTxContext provides methods to realize multi-sign transaction.
// When a user receives a pre-compiled transaction envelope, the loaded
// transaction context can be used to load the pre-compiled envelope,
// inspect the operations performed by the transaction, and either
// co-sign the transaction to get the transaction envelope (maybe to
// pass it on to other users) or issue commit (which would internally
// co-sign the transaction).
type LoadedDataTxContext interface {
	// Embed general abstraction
	TxContext
	// MustSignUsers returns all the users in the MustSignUsers set of the
	// loaded multi-sign data tx. All those users must sign the transaction
	// for it to be valid. Note that, in addition, the signature of some
	// additional users may be needed, depending on the write ACLs of the
	// keys in the write-set."
	MustSignUsers() []string
	// SignedUsers returns all users who have signed the transaction envelope
	SignedUsers() []string
	// VerifySignatures verifies the existing signature on the loaded data transactions
	VerifySignatures() error
	// Reads return all read operations performed by the load data transaction on
	// different databases
	Reads() map[string][]*types.DataRead
	// Writes return all write operations performed by the load data transaction on
	// different databases
	Writes() map[string][]*types.DataWrite
	// Deletes return all delete operations performed by the load data transaction on
	// different databases
	Deletes() map[string][]*types.DataDelete
	// CoSignTxEnvelopeAndCloseTx adds the signature of the transaction's user to
	// the envelope, closes the transaction, and return the co-signed
	// transaction envelope
	CoSignTxEnvelopeAndCloseTx() (proto.Message, error)
}

type loadedDataTxContext struct {
	*commonTxContext
	txEnv *types.DataTxEnvelope
}

func (d *loadedDataTxContext) Commit(sync bool) (string, *types.TxReceiptResponseEnvelope, error) {
	return d.commit(d, constants.PostDataTx, sync)
}

func (d *loadedDataTxContext) Abort() error {
	return d.abort(d)
}

// CoSignTxEnvelopeAndCloseTx adds the signature of the transaction's user to
// the envelope, closes the transaction, and return the co-signed
// transaction envelope
func (d *loadedDataTxContext) CoSignTxEnvelopeAndCloseTx() (proto.Message, error) {
	d.logger.Debugf("compose transaction enveloped with txID = %s", d.txID)

	var err error
	d.txEnvelope, err = d.composeEnvelope(d.txID)
	if err != nil {
		d.logger.Errorf("failed to compose transaction envelope, due to %s", err)
		return nil, err
	}

	d.txSpent = true
	d.cleanCtx()
	return d.txEnvelope, nil
}

// MustSignUsers returns all users of the loaded multi-sign data tx
func (d *loadedDataTxContext) MustSignUsers() []string {
	return d.txEnv.Payload.MustSignUserIds
}

// SignedUsers returns all users who have signed the transaction envelope
func (d *loadedDataTxContext) SignedUsers() []string {
	var users []string

	for user := range d.txEnv.Signatures {
		users = append(users, user)
	}

	return users
}

// Reads return all read operations performed by the load data transaction on
// different databases
func (d *loadedDataTxContext) Reads() map[string][]*types.DataRead {
	reads := make(map[string][]*types.DataRead)
	for _, dbOps := range d.txEnv.Payload.DbOperations {
		var dr []*types.DataRead
		for _, r := range dbOps.DataReads {
			dr = append(dr, r)
		}
		reads[dbOps.DbName] = dr
	}

	return reads
}

// Writes return all write operations performed by the load data transaction on
// different databases
func (d *loadedDataTxContext) Writes() map[string][]*types.DataWrite {
	writes := make(map[string][]*types.DataWrite)
	for _, dbOps := range d.txEnv.Payload.DbOperations {
		var dw []*types.DataWrite
		for _, w := range dbOps.DataWrites {
			dw = append(dw, w)
		}
		writes[dbOps.DbName] = dw
	}

	return writes
}

// Deletes return all delete operations performed by the load data transaction on
// different databases
func (d *loadedDataTxContext) Deletes() map[string][]*types.DataDelete {
	deletes := make(map[string][]*types.DataDelete)
	for _, dbOps := range d.txEnv.Payload.DbOperations {
		var dd []*types.DataDelete
		for _, d := range dbOps.DataDeletes {
			dd = append(dd, d)
		}
		deletes[dbOps.DbName] = dd
	}

	return deletes
}

func (d *loadedDataTxContext) VerifySignatures() error {
	// TODO issue 174
	return nil
}

func (d *loadedDataTxContext) composeEnvelope(_ string) (proto.Message, error) {
	signature, err := cryptoservice.SignTx(d.signer, d.txEnv.Payload)
	if err != nil {
		return nil, err
	}

	d.txEnv.Signatures[d.userID] = signature

	return d.txEnv, nil
}

func (d *loadedDataTxContext) cleanCtx() {
	d.txEnv = nil
}
