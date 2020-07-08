package database

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"log"
	mathrand "math/rand"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/sdk/pkg/cryptoprovider"
	"github.ibm.com/blockchaindb/sdk/pkg/rest"
	"github.ibm.com/blockchaindb/server/api"
)

// Store grpc connection data tp reuse
type ConnData struct {
	*rest.Client
	num int
}

// All opened grpc client connections. Protected by mutex, only single go routine can access it, even for read
var dbConnections map[string]*ConnData
var dbConnMutex sync.Mutex

func init() {
	dbConnections = make(map[string]*ConnData)
}

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
		connections: make([]*ConnData, 0),
		openTx:      make(map[string]*transactionContext, 0),
		isClosed:    false,
		TxOptions:   options.TxOptions,
	}
	if options.User != nil {
		userCrypto, err := options.User.LoadCrypto(nodeProvider)
		if err != nil {
			return nil, err
		}
		db.userCrypto = userCrypto
		db.userID = options.User.UserID

	}
	for _, serverOption := range options.ConnectionOptions {
		conn, err := OpenConnection(serverOption)
		if err != nil {
			return nil, err
		}
		db.connections = append(db.connections, conn)

		query := &api.GetStatusQuery{
			UserID: db.userID,
			DBName: dbName,
		}
		envelope := &api.GetStatusQueryEnvelope{
			Payload:   query,
			Signature: nil,
		}
		envelope.Signature, err = db.userCrypto.Sign(query)
		if err != nil {
			return nil, err
		}
		dbStatusEnvelope, err := conn.Client.GetStatus(context.Background(), envelope)
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
func OpenConnection(options *config.ConnectionOption) (*ConnData, error) {
	dbConnMutex.Lock()
	defer dbConnMutex.Unlock()
	if conn, ok := dbConnections[options.URL]; ok {
		log.Println(fmt.Sprintf("connection to Server %s already opened, reusing", options.URL))
		conn.num += 1
		return conn, nil
	}
	log.Println(fmt.Sprintf("Connecting to Server %s", options.URL))
	rc, err := rest.NewRESTClient(options.URL)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create REST client for %s", options.URL)
	}

	dbConnData := &ConnData{
		Client: rc,
		num:    1,
	}

	dbConnections[dbConnData.Client.RawURL] = dbConnData
	return dbConnData, nil
}

type blockchainDB struct {
	dbName      string
	userID      string
	connections []*ConnData
	openTx      map[string]*transactionContext
	isClosed    bool
	mu          sync.RWMutex
	userCrypto  *cryptoprovider.CryptoMaterials
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
			wset:       make(map[string]*api.KVWrite),
			rset:       make(map[string]*api.KVRead),
			statements: make([]*api.Statement, 0),
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
	dbConnMutex.Lock()
	defer dbConnMutex.Unlock()

	for _, tx := range db.openTx {
		tx.Abort()
	}
	db.openTx = nil

	for _, conn := range db.connections {
		conn.num -= 1
		if conn.num == 0 {
			delete(dbConnections, conn.Client.RawURL)
		}
	}
	return nil
}

func (db *blockchainDB) Get(key string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.isClosed {
		return nil, errors.New("db closed")
	}
	dq := &api.GetStateQuery{
		UserID: db.userID,
		DBName: db.dbName,
		Key:    key,
	}
	envelope := &api.GetStateQueryEnvelope{
		Payload: dq,
	}
	var err error
	envelope.Signature, err = db.userCrypto.Sign(dq)
	if err != nil {
		return nil, errors.Wrapf(err, "can't sign query message %v", dq)
	}
	val, err := getMultipleQueryValue(db, db.ReadOptions, envelope)
	if err != nil {
		return nil, errors.Wrapf(err, "can't get value")
	}
	return val.Value, nil
}

func (db *blockchainDB) GetMerkleRoot() (*api.Digest, error) {
	panic("implement me")
}

func (db *blockchainDB) GetUsers() []*api.User {
	panic("implement me")
}

func (db *blockchainDB) GetUsersForRole(role string) []*api.User {
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
	wset       map[string]*api.KVWrite
	rset       map[string]*api.KVRead
	statements []*api.Statement
	mu         sync.Mutex
}

func (tx *transactionContext) Get(key string) ([]byte, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	if tx.isClosed {
		return nil, errors.New("transaction context not longer valid")
	}
	dq := &api.GetStateQuery{
		UserID: tx.db.userID,
		DBName: tx.db.dbName,
		Key:    key,
	}
	envelope := &api.GetStateQueryEnvelope{
		Payload: dq,
	}
	var err error
	envelope.Signature, err = tx.db.userCrypto.Sign(dq)
	if err != nil {
		return nil, errors.Wrapf(err, "can't sign query message %v", dq)
	}
	val, err := getMultipleQueryValue(tx.db, tx.ReadOptions, envelope)
	if err != nil {
		return nil, errors.Wrapf(err, "can't get value")
	}
	rset := &api.KVRead{
		Key:     key,
		Version: val.Metadata.Version,
	}
	tx.rwset.mu.Lock()
	defer tx.rwset.mu.Unlock()
	if err = validateRSet(tx, rset); err != nil {
		tx.isolationError = err
		return nil, err
	}
	tx.rwset.rset[key] = rset
	stmt := &api.Statement{
		Operation: "GET",
		Arguments: make([][]byte, 0),
	}
	stmt.Arguments = append(stmt.Arguments, []byte(key))
	tx.rwset.statements = append(tx.rwset.statements, stmt)
	return val.Value, nil
}

func (tx *transactionContext) Put(key string, value []byte) error {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	if tx.isClosed {
		return errors.New("transaction context not longer valid")
	}
	tx.rwset.mu.Lock()
	defer tx.rwset.mu.Unlock()

	tx.rwset.wset[key] = &api.KVWrite{
		Key:      key,
		IsDelete: false,
		Value:    value,
	}
	stmt := &api.Statement{
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

	tx.rwset.wset[key] = &api.KVWrite{
		Key:      key,
		IsDelete: true,
		Value:    nil,
	}
	stmt := &api.Statement{
		Operation: "DELETE",
		Arguments: make([][]byte, 0),
	}
	stmt.Arguments = append(stmt.Arguments, []byte(key))
	tx.rwset.statements = append(tx.rwset.statements, stmt)
	return nil
}

func (tx *transactionContext) GetUsers() []*api.User {
	panic("implement me")
}

func (tx *transactionContext) GetUsersForRole(role string) []*api.User {
	panic("implement me")
}

func (tx *transactionContext) AddUser(user *api.User) error {
	panic("implement me")
}

func (tx *transactionContext) UpdateUser(user *api.User) error {
	panic("implement me")
}

func (tx *transactionContext) DeleteUser(user *api.User) error {
	panic("implement me")
}

func (tx *transactionContext) Commit() (*api.Digest, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.isClosed {
		return nil, errors.New("transaction context not longer valid")
	}
	tx.isClosed = true

	tx.db.mu.Lock()
	delete(tx.db.openTx, hex.EncodeToString(tx.txID))
	tx.db.mu.Unlock()
	envelope := &api.TransactionEnvelope{}

	payload := &api.Transaction{
		UserID:     []byte(tx.db.userID),
		DBName:     tx.db.dbName,
		TxID:       tx.txID,
		DataModel:  api.Transaction_KV,
		Statements: tx.rwset.statements,
		Reads:      make([]*api.KVRead, 0),
		Writes:     make([]*api.KVWrite, 0),
	}

	envelope.Payload = payload

	for _, v := range tx.rwset.wset {
		payload.Writes = append(payload.Writes, v)
	}

	for _, v := range tx.rwset.rset {
		payload.Reads = append(payload.Reads, v)
	}
	var err error

	if envelope.Signature, err = tx.db.userCrypto.Sign(payload); err != nil {
		return nil, errors.Wrapf(err, "can't sign transaction envelope payload %v", payload)
	}

	useServer := mathrand.Intn(len(tx.db.connections))
	if _, err = tx.db.connections[useServer].Client.SubmitTransaction(context.Background(), envelope); err != nil {
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

func validateRSet(tx *transactionContext, rset *api.KVRead) error {
	txIsolation := tx.db.TxIsolation
	if tx.TxIsolation != txIsolation {
		txIsolation = tx.TxIsolation
	}

	if rs, ok := tx.rwset.rset[rset.Key]; ok {
		if rset.Version.BlockNum > rs.Version.BlockNum {
			return errors.Errorf("tx isolation level not satisfied, KeyFilePath value version changed during tx, %v, %v", rs, rset)
		}
	}

	if v, exist := tx.rwset.wset[rset.Key]; exist {
		return errors.Errorf("tx isolation not satisfied, KeyFilePath value already changed inside this tx, %v, %v", rset, v)
	}
	return nil
}

// Trying to read exactly ReadOptions.serverNum copies of value from servers
func getMultipleQueryValue(db *blockchainDB, ro *config.ReadOptions, dq *api.GetStateQueryEnvelope) (*api.Value, error) {
	startServer := mathrand.Intn(len(db.connections))
	readValues := make([]*api.Value, 0)

	for i := startServer; (i - startServer) < len(db.connections); i++ {

		valueEnvelope, err := db.connections[i%len(db.connections)].Client.GetState(context.Background(), dq)
		if err != nil {
			log.Println(fmt.Sprintf("Can't get value from service %v, moving to next Server", err))
			continue
		}
		if err := db.userCrypto.Validate(valueEnvelope.Payload.Header.NodeID, valueEnvelope.Payload, valueEnvelope.Signature); err != nil {
			log.Println(fmt.Sprintf("inlavid value from service %v, moving to next Server", err))
			continue
		}
		val := valueEnvelope.Payload.Value
		if val != nil {
			sameValues := 1
			for _, cVal := range readValues {
				if isNilValue(val) || isNilValue(cVal) {
					if isNilValue(val) && isNilValue(cVal) {
						sameValues += 1
					}
				} else if proto.Equal(cVal, val) {
					sameValues += 1
				}
			}
			if sameValues >= ro.QuorumSize {
				return val, nil
			}
			readValues = append(readValues, val)
		}
	}
	return nil, errors.Errorf("can't read %v copies of same value", ro.QuorumSize)
}

func isNilValue(val *api.Value) bool {
	if val.Value == nil || len(val.Value) == 0 {
		return true
	}
	return false
}
