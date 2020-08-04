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

var (
	// Internal database that stores all users' databases
	_dbs                              *blockchainDB
	internalDBManagementDatabaseMutex sync.RWMutex
)

// Open opens an existing database associated with the dbName
// Options may specify:
// 1. Required transaction isolation level
// 2. Read QuorumSize - number of servers used to read data
// 3. Commit QuorumSize - number of responses should be collected by Client SDK during Commit() call
//    to return success to Client
// 4. User crypto materials
func Open(dbName string, options *config.Options) (DB, error) {
	db := &blockchainDB{
		dbName:      dbName,
		connections: []*rest.Client{},
		openTx:      make(map[string]*transactionContext, 0),
		isClosed:    false,
		TxOptions:   options.TxOptions,
	}
	if options.User != nil {
		cryptoRegistry, err := crypto_utils.NewVerifiersRegistry(options.ServersVerify, certificateFetcher)
		if err != nil {
			return nil, err
		}
		db.verifiers = cryptoRegistry
		db.signer, err = crypto.NewSigner(options.User.Signer)
		if err != nil {
			return nil, err
		}
		db.userID = options.User.UserID

	}
	for _, serverOption := range options.ConnectionOptions {
		client, err := OpenConnection(serverOption)
		if err != nil {
			return nil, err
		}
		db.connections = append(db.connections, client)

		query := &types.GetStatusQuery{
			UserID: db.userID,
			DBName: dbName,
		}
		envelope := &types.GetStatusQueryEnvelope{
			Payload:   query,
			Signature: nil,
		}
		queryBytes, err := json.Marshal(query)
		if err != nil {
			return nil, err
		}
		envelope.Signature, err = db.signer.Sign(queryBytes)
		if err != nil {
			return nil, err
		}
		dbStatusEnvelope, err := client.GetStatus(context.Background(), envelope)
		if err != nil {
			return nil, err
		}
		if !dbStatusEnvelope.Payload.Exist {
			return nil, errors.Errorf("database %s doesn't exist", dbName)
		}
	}

	return db, nil
}

// Single threaded
func OpenConnection(options *config.ConnectionOption) (*rest.Client, error) {
	log.Println(fmt.Sprintf("Connecting to Server %s", options.URL))
	rc, err := rest.NewRESTClient(options.URL)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create REST client for %s", options.URL)
	}

	return rc, nil
}

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
			wset:       make(map[string]*types.KVWrite),
			rset:       make(map[string]*types.KVRead),
			statements: make([]*types.Statement, 0),
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
	wset       map[string]*types.KVWrite
	rset       map[string]*types.KVRead
	statements []*types.Statement
	mu         sync.Mutex
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
	stmt := &types.Statement{
		Operation: "GET",
		Arguments: make([][]byte, 0),
	}
	stmt.Arguments = append(stmt.Arguments, []byte(key))
	tx.rwset.statements = append(tx.rwset.statements, stmt)
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
	stmt := &types.Statement{
		Operation: "PUT",
		Arguments: make([][]byte, 0),
	}
	stmt.Arguments = append(stmt.Arguments, []byte(key), value)
	tx.rwset.statements = append(tx.rwset.statements, stmt)
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
	stmt := &types.Statement{
		Operation: "DELETE",
		Arguments: make([][]byte, 0),
	}
	stmt.Arguments = append(stmt.Arguments, []byte(key))
	tx.rwset.statements = append(tx.rwset.statements, stmt)
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
		UserID:     []byte(tx.db.userID),
		DBName:     tx.db.dbName,
		TxID:       tx.txID,
		DataModel:  types.Transaction_KV,
		Statements: tx.rwset.statements,
		Reads:      make([]*types.KVRead, 0),
		Writes:     make([]*types.KVWrite, 0),
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

func openInternalDBManagementDatabase(options *config.Options) (*blockchainDB, error) {
	internalDBManagementDatabaseMutex.Lock()
	defer internalDBManagementDatabaseMutex.Unlock()
	if _dbs != nil {
		return _dbs, nil
	}
	db, err := Open("_dbs", options)
	if err != nil {
		return nil, err
	}
	_dbs = db.(*blockchainDB)

	return _dbs, nil
}

func Create(dbName string, opt *config.Options, readACL, writeALC []string) error {
	if _, err := openInternalDBManagementDatabase(opt); err != nil {
		return err
	}

	tx, err := _dbs.Begin(opt.TxOptions)
	if err != nil {
		return err
	}
	defer tx.Abort()

	dbConfig := &types.DatabaseConfig{
		Name:             dbName,
		ReadAccessUsers:  readACL,
		WriteAccessUsers: writeALC,
	}

	orgDBconfig, err := tx.Get(dbName)
	if err != nil {
		return err
	}

	if orgDBconfig != nil {
		return errors.Errorf("can't create db %s, it already exist", dbName)
	}
	dbConfigBytes, err := json.Marshal(dbConfig)
	tx.Put(dbName, dbConfigBytes)
	if _, err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func Delete(dbName string, opt *config.Options) error {
	if _, err := openInternalDBManagementDatabase(opt); err != nil {
		return err
	}

	tx, err := _dbs.Begin(opt.TxOptions)
	if err != nil {
		return err
	}
	defer tx.Abort()

	orgDBconfig, err := tx.Get(dbName)
	if err != nil {
		return nil
	}

	if orgDBconfig == nil {
		return errors.Errorf("can't remove db %s, it does not exist", dbName)
	}
	tx.Delete(dbName)
	if _, err = tx.Commit(); err != nil {
		return err
	}
	return nil
}
