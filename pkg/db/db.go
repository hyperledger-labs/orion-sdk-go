package db

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/api"
	"google.golang.org/grpc"
	"hash"
	"log"
	mathrand "math/rand"
	"sync"
	"time"
)

// Store grpc connection data tp reuse
type ConnData struct {
	conn        *grpc.ClientConn
	queryServer api.QueryClient
	txServer    api.TransactionSvcClient
	num         int
	addr        string
}

// All opened grpc client connections. Protected by mutex, only single go routine can access it, even for read
var dbConnections map[string]*ConnData
var dbConnMutex sync.Mutex

func init() {
	dbConnections = make(map[string]*ConnData)
}

// Open opens an existing db associated with the dbName
// Options may specify:
// 1. Required transaction isolation level
// 2. Read QuorumSize - number of servers used to read data
// 3. Commit QuorumSize - number of responses should be collected by Client SDK during Commit() call
//    to return success to Client
// 4. User crypto materials
func Open(dbName string, options *Options) (DB, error) {
	db := &blockchainDB{
		dbName:      dbName,
		connections: make([]*ConnData, 0),
		openTx:      make(map[string]*transactionContext, 0),
		isClosed:    false,
		TxOptions:   options.TxOptions,
	}
	if options.user != nil {
		userCrypto, err := options.user.LoadCrypto()
		if err != nil {
			return nil, err
		}
		db.userCrypto = userCrypto
		db.userId = options.user.UserID

	}
	for _, serverOption := range options.connectionOptions {
		conn, err := OpenConnection(serverOption)
		if err != nil {
			return nil, err
		}
		db.connections = append(db.connections, conn)
		dbStatus, err := conn.queryServer.GetStatus(context.Background(), &api.DB{
			Name: dbName,
		})
		if err != nil {
			return nil, err
		}
		if !dbStatus.Exist {
			return nil, errors.Errorf("database %s doesn't exist", dbName)
		}
	}

	return db, nil
}

// Single threaded
func OpenConnection(options *ConnectionOption) (*ConnData, error) {
	addr := fmt.Sprintf("%s:%d", options.server, options.port)
	dbConnMutex.Lock()
	defer dbConnMutex.Unlock()
	if conn, ok := dbConnections[addr]; ok {
		log.Println(fmt.Sprintf("connection to server %s already opened, reusing", addr))
		conn.num += 1
		return conn, nil
	}
	log.Println(fmt.Sprintf("Connecting to server %s", addr))
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrapf(err, "could not dial %s", addr)
	}
	log.Println(fmt.Sprintf("Connected to server %s", addr))
	dbConnData := &ConnData{
		conn:        conn,
		queryServer: api.NewQueryClient(conn),
		txServer:    api.NewTransactionSvcClient(conn),
		num:         1,
		addr:        addr,
	}

	dbConnections[addr] = dbConnData
	return dbConnData, nil
}

type blockchainDB struct {
	dbName      string
	userId      []byte
	connections []*ConnData
	openTx      map[string]*transactionContext
	isClosed    bool
	mu          sync.RWMutex
	userCrypto  *cryptoMaterials
	*TxOptions
}

func (db *blockchainDB) Begin(options *TxOptions) (TxContext, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.isClosed {
		return nil, errors.New("db closed")
	}
	txId, err := computeTxID(db.userId, sha256.New())
	if err != nil {
		return nil, errors.Wrapf(err, "can't compute TxID")
	}
	ctx := &transactionContext{
		db:   db,
		txId: txId,
		rwset: &txRWSetAndStmt{
			wset:       make(map[string]*api.KVWrite),
			rset:       make(map[string]*api.KVRead),
			statements: make([]*api.Statement, 0),
		},
		TxOptions: options,
	}
	db.openTx[hex.EncodeToString(ctx.txId)] = ctx
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
			conn.conn.Close()
			delete(dbConnections, conn.addr)
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
	dq := &api.DataQuery{
		Header: &api.QueryHeader{
			User: &api.User{
				UserID:          db.userId,
				UserCertificate: nil,
				Roles:           nil,
			},
			Signature: nil,
			DBName:    db.dbName,
		},
		Key: key,
	}
	queryBytes, err := proto.Marshal(dq)
	if err != nil {
		return nil, errors.Wrapf(err, "can't marshal query message %v", dq)
	}
	dq.Header.Signature, err = Sign(db.userCrypto, queryBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "can't sign query message %v", dq)
	}
	val, err := getMultipleQueryValue(db, db.ro, dq)
	if err != nil {
		return nil, errors.Wrapf(err, "can't get value")
	}
	return val.Value, nil
}

func (db *blockchainDB) GetValueAtTime(key string, date time.Time) (*api.HistoricalData, error) {
	panic("implement me")
}

func (db *blockchainDB) GetHistoryIterator(key string, opt QueryOption) (HistoryIterator, error) {
	panic("implement me")
}

func (db *blockchainDB) GetTxProof(txId string) (*api.Proof, error) {
	panic("implement me")
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
	txId           []byte
	rwset          *txRWSetAndStmt
	*TxOptions
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
	dq := &api.DataQuery{
		Header: &api.QueryHeader{
			User: &api.User{
				UserID:          tx.db.userId,
				UserCertificate: nil,
				Roles:           nil,
			},
			Signature: nil,
			DBName:    tx.db.dbName,
		},
		Key: key,
	}
	queryBytes, err := proto.Marshal(dq)
	if err != nil {
		return nil, errors.Wrapf(err, "can't marshal query message %v", dq)
	}
	dq.Header.Signature, err = Sign(tx.db.userCrypto, queryBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "can't sign query message %v", dq)
	}
	val, err := getMultipleQueryValue(tx.db, tx.ro, dq)
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
	delete(tx.db.openTx, hex.EncodeToString(tx.txId))
	tx.db.mu.Unlock()
	env := &api.Envelope{}

	payload := &api.Payload{}

	nonce, err := getRandomBytes(nonceSize)
	if err != nil {
		return nil, errors.Wrap(err, "can't calculate nonce")
	}

	x509Cert, err := x509.ParseCertificate(tx.db.userCrypto.tlsPair.Certificate[0])
	if err != nil {
		return nil, errors.Wrap(err, "not valid x509 certificate")
	}

	payload.Header = &api.Header{
		Creator: x509Cert.Raw,
		Nonce:   nonce,
	}

	pbtx := &api.Transaction{
		TxId:       tx.txId,
		Datamodel:  api.Transaction_KV,
		Statements: tx.rwset.statements,
	}

	rwset := &api.KVRWSet{
		Rset: make([]*api.KVRead, 0),
		Wset: make([]*api.KVWrite, 0),
	}

	for _, v := range tx.rwset.wset {
		rwset.Wset = append(rwset.Wset, v)
	}

	for _, v := range tx.rwset.rset {
		rwset.Rset = append(rwset.Rset, v)
	}

	pbtx.Rwset, err = proto.Marshal(rwset)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal rwset")
	}

	payload.Data, err = proto.Marshal(pbtx)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal tx")
	}

	env.Payload, err = proto.Marshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal payload")
	}

	env.Signature, err = Sign(tx.db.userCrypto, env.Payload)
	if err != nil {
		return nil, err
	}

	useServer := mathrand.Intn(len(tx.db.connections))
	stream, err := tx.db.connections[useServer].txServer.SubmitTransaction(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "can't access output stream")
	}

	err = stream.Send(env)
	if err != nil {
		return nil, errors.Wrap(err, "can't send transaction envelope")
	}
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
	delete(tx.db.openTx, hex.EncodeToString(tx.txId))
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

func Sign(userCrypto *cryptoMaterials, msgBytes []byte) ([]byte, error) {
	digest := sha256.New()
	digest.Write(msgBytes)
	singer, ok := userCrypto.tlsPair.PrivateKey.(crypto.Signer)
	if !ok {
		return nil, errors.New("can't sign using private key, not implement signer interface")
	}
	return singer.Sign(rand.Reader, digest.Sum(nil), crypto.SHA256)
}

func validateRSet(tx *transactionContext, rset *api.KVRead) error {
	txIsolation := tx.db.txIsolation
	if tx.txIsolation != txIsolation {
		txIsolation = tx.txIsolation
	}

	if rs, ok := tx.rwset.rset[rset.Key]; ok {
		if rset.Version.BlockNum > rs.Version.BlockNum {
			return errors.Errorf("tx isolation level not satisfied, key value version changed during tx, %v, %v", rs, rset)
		}
	}

	if v, exist := tx.rwset.wset[rset.Key]; exist {
		return errors.Errorf("tx isolation not satisfied, key value already changed inside this tx, %v, %v", rset, v)
	}
	return nil
}

// Trying to read exactly ReadOptions.serverNum copies of value from servers
func getMultipleQueryValue(db *blockchainDB, ro *ReadOptions, dq *api.DataQuery) (*api.Value, error) {
	startServer := mathrand.Intn(len(db.connections))
	readValues := make([]*api.Value, 0)

	for i := startServer; (i - startServer) < len(db.connections); i++ {

		val, err := db.connections[i%len(db.connections)].queryServer.GetState(context.Background(), dq)
		if err != nil {
			log.Println(fmt.Sprintf("Can't get value from service %v, moving to next server", err))
		}
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
