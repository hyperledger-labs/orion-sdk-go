package db

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/api"
	"google.golang.org/grpc"
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
		openTx:      make(map[string]*txContext, 0),
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
		dbStatus, err := conn.queryServer.GetDBStatus(context.Background(), &api.DBName{
			DbName: dbName,
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
	userId       []byte
	connections []*ConnData
	mu          sync.RWMutex
	openTx      map[string]*txContext
	isClosed    bool
	userCrypto   *cryptoMaterials
	*TxOptions
}

func (db *blockchainDB) Begin(options *TxOptions) (TxContext, error) {
	panic("implement me")
}

func (db *blockchainDB) Close() error {
	db.mu.Lock()
	db.isClosed = true
	db.mu.Unlock()
	dbConnMutex.Lock()
	defer dbConnMutex.Unlock()
	db.openTx = nil
	for _, conn := range db.connections {
		conn.num -= 1
		if conn.num == 0 {
			conn.conn.Close()
			delete (dbConnections, conn.addr)
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
			DbName:    db.dbName,
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

type txContext struct {
	db         *blockchainDB
	isClosed   bool
	wset       map[string]*api.KVWrite
	rset       map[string]*api.KVRead
	txId       []byte
	statements []*api.Statement
	*TxOptions
}

func (ctx *txContext) Get(key string) ([]byte, error) {
	panic("implement me")
}

func (ctx *txContext) Put(key string, value []byte) error {
	panic("implement me")
}

func (ctx *txContext) Delete(key string) error {
	panic("implement me")
}

func (ctx *txContext) GetUsers() []*api.User {
	panic("implement me")
}

func (ctx *txContext) GetUsersForRole(role string) []*api.User {
	panic("implement me")
}

func (ctx *txContext) AddUser(user *api.User) error {
	panic("implement me")
}

func (ctx *txContext) UpdateUser(user *api.User) error {
	panic("implement me")
}

func (ctx *txContext) DeleteUser(user *api.User) error {
	panic("implement me")
}

func (ctx *txContext) Commit() (*api.Digest, error) {
	panic("implement me")
}

func (ctx *txContext) Abort() error {
	panic("implement me")
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

// Trying to read exactly ReadOptions.serverNum copies of value from servers
func getMultipleQueryValue(db *blockchainDB, ro *ReadOptions, dq *api.DataQuery) (*api.Value, error) {
	startServer := mathrand.Intn(len(db.connections))
	readValues := make([]*api.Value, 0)

	for i := startServer; (i - startServer) < len(db.connections); i++ {

		val, err := db.connections[i%len(db.connections)].queryServer.Get(context.Background(), dq)
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

// Each
func isNilValue(val *api.Value) bool {
	if (val.Value == nil || len(val.Value) == 0) {
		return true
	}
	return false
}