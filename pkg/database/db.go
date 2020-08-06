package database

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"log"
	mathrand "math/rand"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/library/pkg/crypto_utils"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/sdk/pkg/rest"
)

type blockchainDB struct {
	dbName      string
	userID      string
	connections []*rest.Client
	openTx      map[string]*transactionContext
	isClosed    bool
	mu          sync.RWMutex
	verifiers   *crypto_utils.VerifiersRegistry
	signer      *crypto.Signer
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
		rwset: &txRWSetAndStmt{
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
	envelope.Signature, err = db.signer.Sign(dqBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "can't sign query message %v", dq)
	}
	val, err := getMultipleQueryValue(db, db.ReadOptions, envelope)
	if err != nil {
		return nil, errors.Wrapf(err, "can't get value")
	}
	return val.Value, nil
}

func (db *blockchainDB) GetMerkleRoot() (*types.Digest, error) {
	panic("implement me")
}

func (db *blockchainDB) GetUsers() []*types.User {
	panic("implement me")
}

func (db *blockchainDB) GetUsersForRole(role string) []*types.User {
	panic("implement me")
}

type transactionContext struct {
	db             *blockchainDB
	isClosed       bool
	mu             sync.RWMutex
	isolationError error
	txID           []byte
	rwset          *txRWSetAndStmt
	*config.TxOptions
}

type txRWSetAndStmt struct {
	wset map[string]*types.KVWrite
	rset map[string]*types.KVRead
	mu   sync.Mutex
}

func (tx *transactionContext) Get(key string) ([]byte, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
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
	envelope.Signature, err = tx.db.signer.Sign(dqBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "can't sign query message %v", dq)
	}
	val, err := getMultipleQueryValue(tx.db, tx.ReadOptions, envelope)
	if err != nil {
		return nil, errors.Wrapf(err, "can't get value")
	}
	rset := &types.KVRead{
		Key:     key,
		Version: val.GetMetadata().GetVersion(),
	}
	tx.rwset.mu.Lock()
	defer tx.rwset.mu.Unlock()
	if err = validateRSet(tx, rset); err != nil {
		tx.isolationError = err
		return nil, err
	}
	tx.rwset.rset[key] = rset
	return val.GetValue(), nil
}

func (tx *transactionContext) Put(key string, value []byte) error {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
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
	tx.mu.RLock()
	defer tx.mu.RUnlock()
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
	tx.mu.Lock()
	defer tx.mu.Unlock()
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
	if envelope.Signature, err = tx.db.signer.Sign(payloadBytes); err != nil {
		return nil, errors.Wrapf(err, "can't sign transaction envelope payload %v", payload)
	}

	useServer := mathrand.Intn(len(tx.db.connections))
	if _, err = tx.db.connections[useServer].SubmitTransaction(context.Background(), envelope); err != nil {
		return nil, errors.Wrap(err, "can't access output stream")
	}

	// TODO: Wait for response
	return nil, nil
}

func (tx *transactionContext) Abort() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
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

// Trying to read exactly ReadOptions.serverNum copies of value from servers
func getMultipleQueryValue(db *blockchainDB, ro *config.ReadOptions, dq *types.GetStateQueryEnvelope) (*types.Value, error) {
	startServer := mathrand.Intn(len(db.connections))
	readValues := make([]*types.Value, 0)

	for i := startServer; (i - startServer) < len(db.connections); i++ {

		valueEnvelope, err := db.connections[i%len(db.connections)].GetState(context.Background(), dq)
		if err != nil {
			log.Println(fmt.Sprintf("Can't get value from service %v, moving to next Server", err))
			continue
		}
		nodeVerifier, err := db.verifiers.GetVerifier(string(valueEnvelope.Payload.Header.NodeID))
		if err != nil {
			log.Println(fmt.Sprintf("can't access crypto for verify server %s signature, %v, moving to next Server", string(valueEnvelope.Payload.Header.NodeID), err))
			continue

		}
		payloadBytes, err := json.Marshal(valueEnvelope.Payload)
		if err != nil {
			log.Println(fmt.Sprintf("can't serialize transaction to json %v, %v, moving to next Server", valueEnvelope.Payload, err))
			continue

		}
		if err := nodeVerifier.Verify(payloadBytes, valueEnvelope.Signature); err != nil {
			log.Println(fmt.Sprintf("inlavid value from service %v, moving to next Server", err))
			continue
		}
		val := valueEnvelope.Payload.Value
		sameValues := 1
		for _, cVal := range readValues {
			if proto.Equal(val, cVal) {
				sameValues += 1
			}
		}
		if sameValues >= ro.QuorumSize {
			return val, nil
		}
		readValues = append(readValues, val)
	}
	return nil, errors.Errorf("can't read %v copies of same value", ro.QuorumSize)
}
