package database

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash"
	"log"
	"sync"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/library/pkg/crypto_utils"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/sdk/pkg/rest"
)

type blockchainDB struct {
	dbConnector *dbConnector
	dbName      string
	userID      string
	openTx      map[string]*transactionContext
	isClosed    bool
	mu          sync.RWMutex
	verifiers   *crypto_utils.VerifiersRegistry
	*crypto.Signer
	*rest.Client
	*config.TxOptions
}

func (db *blockchainDB) Begin(options *config.TxOptions) (TxContext, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.isClosed {
		return nil, errors.New("db closed")
	}
	txID, err := computeTxID([]byte(db.userID), sha256.New())
	if err != nil {
		return nil, errors.Wrapf(err, "can't compute TxID")
	}
	ctx := &transactionContext{
		db:   db,
		txID: txID,
		rwset: &txRWSet{
			wset: make(map[string]*types.KVWrite),
			rset: make(map[string]*types.KVRead),
		},
		TxOptions: options,
	}
	db.openTx[hex.EncodeToString(ctx.txID)] = ctx
	return ctx, nil
}

func (db *blockchainDB) Close() error {
	db.mu.Lock()
	db.isClosed = true
	db.mu.Unlock()

	for _, tx := range db.openTx {
		tx.Abort()
	}
	db.openTx = nil

	return nil
}

func (db *blockchainDB) Get(key string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.isClosed {
		return nil, errors.New("db closed")
	}
	dq := &types.GetStateQuery{
		UserID: db.userID,
		DBName: db.dbName,
		Key:    key,
	}
	envelope := &types.GetStateQueryEnvelope{
		Payload: dq,
	}
	dqBytes, err := json.Marshal(dq)
	if err != nil {
		return nil, errors.Wrapf(err, "can'r serialize msg to json %v", dq)
	}
	envelope.Signature, err = db.Sign(dqBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "can't sign query message %v", dq)
	}
	val, _, err := getQueryValue(db, db.ReadOptions, envelope)
	if err != nil {
		return nil, errors.Wrapf(err, "can't get value")
	}
	return val, nil
}

func (db *blockchainDB) GetMerkleRoot() (*types.Digest, error) {
	panic("implement me")
}

func (db *blockchainDB) GetUsers() []*types.User {
	panic("implement me")
}

type transactionContext struct {
	db             *blockchainDB
	isClosed       bool
	isolationError error
	txID           []byte
	rwset          *txRWSet
	*config.TxOptions
}

type txRWSet struct {
	wset map[string]*types.KVWrite
	rset map[string]*types.KVRead
	mu   sync.Mutex
}

func (tx *transactionContext) Get(key string) ([]byte, error) {
	if tx.isClosed {
		return nil, errors.New("transaction context not longer valid")
	}
	dq := &types.GetStateQuery{
		UserID: tx.db.userID,
		DBName: tx.db.dbName,
		Key:    key,
	}
	envelope := &types.GetStateQueryEnvelope{
		Payload: dq,
	}
	dqBytes, err := json.Marshal(dq)
	if err != nil {
		return nil, errors.Wrapf(err, "can't serialize msg to json %v", dq)
	}
	envelope.Signature, err = tx.db.Sign(dqBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "can't sign query message %v", dq)
	}
	val, meta, err := getQueryValue(tx.db, tx.ReadOptions, envelope)
	if err != nil {
		return nil, errors.Wrapf(err, "can't get value")
	}
	rset := &types.KVRead{
		Key:     key,
		Version: meta.GetVersion(),
	}
	tx.rwset.mu.Lock()
	defer tx.rwset.mu.Unlock()
	if err = validateRSet(tx, rset); err != nil {
		tx.isolationError = err
		return nil, err
	}
	tx.rwset.rset[key] = rset
	return val, nil
}

func (tx *transactionContext) Put(key string, value []byte) error {
	if tx.isClosed {
		return errors.New("transaction context not longer valid")
	}
	tx.rwset.mu.Lock()
	defer tx.rwset.mu.Unlock()

	tx.rwset.wset[key] = &types.KVWrite{
		Key:      key,
		IsDelete: false,
		Value:    value,
	}
	return nil
}

func (tx *transactionContext) Delete(key string) error {
	if tx.isClosed {
		return errors.New("transaction context not longer valid")
	}
	tx.rwset.mu.Lock()
	defer tx.rwset.mu.Unlock()

	tx.rwset.wset[key] = &types.KVWrite{
		Key:      key,
		IsDelete: true,
		Value:    nil,
	}
	return nil
}

func (tx *transactionContext) GetUsers() []*types.User {
	panic("implement me")
}

func (tx *transactionContext) AddUser(user *types.User) error {
	panic("implement me")
}

func (tx *transactionContext) UpdateUser(user *types.User) error {
	panic("implement me")
}

func (tx *transactionContext) DeleteUser(user *types.User) error {
	panic("implement me")
}

func (tx *transactionContext) Commit() (*types.Digest, error) {
	if tx.isClosed {
		return nil, errors.New("transaction context not longer valid")
	}
	tx.isClosed = true

	tx.db.mu.Lock()
	delete(tx.db.openTx, hex.EncodeToString(tx.txID))
	tx.db.mu.Unlock()
	envelope := &types.TransactionEnvelope{}

	payload := &types.Transaction{
		UserID:    []byte(tx.db.userID),
		DBName:    tx.db.dbName,
		TxID:      tx.txID,
		DataModel: types.Transaction_KV,
		Reads:     make([]*types.KVRead, 0),
		Writes:    make([]*types.KVWrite, 0),
	}

	envelope.Payload = payload

	for _, v := range tx.rwset.wset {
		payload.Writes = append(payload.Writes, v)
	}

	for _, v := range tx.rwset.rset {
		payload.Reads = append(payload.Reads, v)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrapf(err, "can't serialize msg to json %v", payload)
	}
	if envelope.Signature, err = tx.db.Sign(payloadBytes); err != nil {
		return nil, errors.Wrapf(err, "can't sign transaction envelope payload %v", payload)
	}

	if _, err = tx.db.SubmitTransaction(context.Background(), envelope); err != nil {
		log.Printf("can't submit tx to service %s %v", tx.db.RawURL, err)
		return nil, err
	}

	// TODO: Wait for response
	return nil, nil
}

func (tx *transactionContext) Abort() error {
	if tx.isClosed {
		return errors.New("transaction context not longer valid")
	}
	tx.isClosed = true
	tx.db.mu.Lock()
	delete(tx.db.openTx, hex.EncodeToString(tx.txID))
	tx.db.mu.Unlock()
	return nil
}

const nonceSize = 24

func getRandomBytes(len int) ([]byte, error) {
	key := make([]byte, len)

	// TODO: rand could fill less bytes then len
	_, err := rand.Read(key)
	if err != nil {
		return nil, errors.Wrap(err, "error getting random bytes")
	}

	return key, nil
}

func computeTxID(creator []byte, h hash.Hash) ([]byte, error) {
	nonce, err := getRandomBytes(nonceSize)
	if err != nil {
		return nil, err
	}
	b := append(nonce, creator...)

	_, err = h.Write(b)
	if err != nil {
		return nil, err
	}
	digest := h.Sum(nil)
	return digest, nil
}

func validateRSet(tx *transactionContext, rset *types.KVRead) error {
	txIsolation := tx.db.TxIsolation
	if tx.TxIsolation != txIsolation {
		txIsolation = tx.TxIsolation
	}

	if storedRSet, ok := tx.rwset.rset[rset.Key]; ok {
		if rset.GetVersion() != storedRSet.GetVersion() {
			if rset.GetVersion() == nil || storedRSet.GetVersion() == nil {
				return errors.Errorf("tx isolation level not satisfied, Key value deleted, %v, %v", storedRSet, rset)
			}
			if rset.GetVersion().GetBlockNum() > storedRSet.GetVersion().GetBlockNum() {
				return errors.Errorf("tx isolation level not satisfied, Key value version changed during tx, %v, %v", storedRSet, rset)
			}
		}
	}

	if v, exist := tx.rwset.wset[rset.Key]; exist {
		return errors.Errorf("tx isolation not satisfied, Key value already changed inside this tx, %v, %v", rset, v)
	}
	return nil
}

// Trying to read value, while retrying to reconnect if case of failure
func getQueryValue(db *blockchainDB, ro *config.ReadOptions, dq *types.GetStateQueryEnvelope) ([]byte, *types.Metadata, error) {
	valueEnvelope, err := db.GetState(context.Background(), dq)
	if err != nil {
		log.Printf("Can't get value from service %s %v", db.RawURL, err)
		return nil, nil, err
	}
	return valueEnvelope.GetPayload().GetValue(), valueEnvelope.GetPayload().GetMetadata(), nil
}
