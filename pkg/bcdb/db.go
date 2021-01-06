package bcdb

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/sdk/pkg/config"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// BCDB Blockchain Database interface, defines set of APIs
// required to operate with BCDB instance
type BCDB interface {
	// Session instantiates session to the database
	Session(config *config.SessionConfig) (DBSession, error)
}

// DBSession captures user's session
type DBSession interface {
	UsersTx() (UsersTxContext, error)
	DataTx(database string) (DataTxContext, error)
	DBsTx() (DBsTxContext, error)
	ConfigTx() (ConfigTxContext, error)
	Provenance() (Provenance, error)
	Ledger() (Ledger, error)
}

var ErrTxSpent = errors.New("transaction committed or aborted")

// TxContet an abstract API to capture general purpose
// functionality for all types of transactions context
type TxContext interface {
	// Commit submits transaction to the server, returns
	// txID of submitted transaction
	Commit() (string, error)
	// Abort cancel submission and abandon all changes
	// within given transaction context
	Abort() error
	// TxEnvelope returns transaction envelope, can be called only after Commit(), otherwise will return nil
	TxEnvelope() (proto.Message, error)
}

type Ledger interface {
	// GetBlockHeader returns block header from ledger
	GetBlockHeader(blockNum uint64) (*types.BlockHeader, error)
	// GetLedgerPath returns cryptographically verifiable path between any block pairs in ledger skip list
	GetLedgerPath(startBlock, endBlock uint64) ([]*types.BlockHeader, error)
	// GetTransactionProof returns intermediate hashes from hash(tx, validating info) to root of
	// tx merkle tree stored in block header
	GetTransactionProof(blockNum uint64, txIndex int) (*TxProof, error)
	// GetTransactionReceipt return block header where tx is stored and tx index inside block
	GetTransactionReceipt(txId string) (*types.TxReceipt, error)
}

type Provenance interface {
	// GetHistoricalData return all historical values for specific dn and key
	// Value returned with its associated metadata, including block number, tx index, etc
	GetHistoricalData(dbName, key string) ([]*types.ValueWithMetadata, error)
	// GetHistoricalDataAt returns value for specific version, if exist
	GetHistoricalDataAt(dbName, key string, version *types.Version) (*types.ValueWithMetadata, error)
	// GetPreviousHistoricalData returns value precedes given version, including its metadata, i.e version
	GetPreviousHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error)
	// GetNextHistoricalData returns value succeeds given version, including its metadata
	GetNextHistoricalData(dbName, key string, version *types.Version) ([]*types.ValueWithMetadata, error)
	// GetDataReadByUser returns all user reads
	GetDataReadByUser(userID string) ([]*types.KVWithMetadata, error)
	// GetDataWrittenByUser returns all user writes
	GetDataWrittenByUser(userID string) ([]*types.KVWithMetadata, error)
	// GetReaders returns all users who read value associated with the key
	GetReaders(dbName, key string) ([]string, error)
	// GetWriters returns all users who wrote value associated with the key
	GetWriters(dbName, key string) ([]string, error)
	// GetTxIDsSubmittedByUser IDs of all tx submitted by user
	GetTxIDsSubmittedByUser(userID string) ([]string, error)
}

//go:generate mockery --dir . --name Signer --case underscore --output mocks/

type Signer interface {
	crypto.Signer
}

// Create prepares connection context to work with BCDB instance
// loads root CA certificates
func Create(config *config.ConnectionConfig) (BCDB, error) {
	dbLogger := config.Logger
	if dbLogger == nil {
		c := &logger.Config{
			Level:         "info",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		}
		var err error
		dbLogger, err = logger.New(c)
		if err != nil {
			return nil, err
		}
	}

	// Load root CA certificates
	certsPool := x509.NewCertPool()
	for _, rootCAPath := range config.RootCAs {
		rootCABytes, err := ioutil.ReadFile(rootCAPath)
		if err != nil {
			dbLogger.Errorf("failed to read root CA certificate, due to", err)
			return nil, errors.Wrap(err, "failed to read root CA certificate")
		}
		// TODO there are might be multiple PEM encoded blocks need to make
		// sure we read correct one
		pemBlock, _ := pem.Decode(rootCABytes)
		if pemBlock == nil {
			dbLogger.Error("failed decoding root CA certificate")
			return nil, errors.New("failed decoding root CA certificate")
		}
		rootCACert, err := x509.ParseCertificate(pemBlock.Bytes)
		if err != nil {
			dbLogger.Errorf("failed to parse X509 root CA certificate, due to", err)
			return nil, errors.Wrap(err, "failed to parse X509 root CA certificate")
		}
		certsPool.AddCert(rootCACert)
	}
	// Validate replica set URIs
	urls := map[string]*url.URL{}
	for _, uri := range config.ReplicaSet {
		replicaURL, err := url.Parse(uri.Endpoint)
		if err != nil {
			dbLogger.Errorf("error parsing replica URI, %s", uri.Endpoint)
			return nil, errors.Wrapf(err, "error parsing replica URI, %s", uri.Endpoint)
		}
		urls[uri.ID] = replicaURL
	}

	return &bDB{
		replicaSet: urls,
		rootCAs:    certsPool,
		logger:     dbLogger,
	}, nil
}

type bDB struct {
	replicaSet map[string]*url.URL
	rootCAs    *x509.CertPool
	logger     *logger.SugarLogger
}

// Session parses sessions configuration and opens session to BCDB, takes
// care to read user
func (b *bDB) Session(cfg *config.SessionConfig) (DBSession, error) {
	signer, err := crypto.NewSigner(&crypto.SignerOptions{
		KeyFilePath: cfg.UserConfig.PrivateKeyPath,
	})
	if err != nil {
		b.logger.Errorf("cannot create signer with user's private key, from %s, due to",
			cfg.UserConfig.PrivateKeyPath, err)
		return nil, errors.Wrap(err, "cannot create signer with user's private key")
	}

	certBytes, err := ioutil.ReadFile(cfg.UserConfig.CertPath)
	if err != nil {
		b.logger.Errorf("cannot read user's certificate with user's private key, from %s, due to",
			cfg.UserConfig.CertPath, err)
		return nil, errors.Wrap(err, "cannot read user's certificate with user's private key")
	}

	return &dbSession{
		userID:     cfg.UserConfig.UserID,
		signer:     signer,
		userCert:   certBytes,
		replicaSet: b.replicaSet,
		rootCAs:    b.rootCAs,
		logger:     b.logger,
	}, nil
}

type dbSession struct {
	userID     string
	signer     Signer
	userCert   []byte
	replicaSet map[string]*url.URL
	rootCAs    *x509.CertPool
	logger     *logger.SugarLogger
}

func (d *dbSession) getNodesCerts(replica *url.URL, httpClient *http.Client) (map[string]*x509.Certificate, error) {
	nodesCerts := map[string]*x509.Certificate{}
	getConfig := &url.URL{
		Path: constants.URLForGetConfig(),
	}
	configREST := replica.ResolveReference(getConfig)
	ctx := context.TODO()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configREST.String(), nil)
	if err != nil {
		return nil, err
	}

	signature, err := cryptoservice.SignQuery(d.signer, &types.GetConfigQuery{
		UserID: d.userID,
	})
	if err != nil {
		d.logger.Errorf("failed signed transaction, %s", err)
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set(constants.UserHeader, d.userID)
	req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(signature))
	response, err := httpClient.Do(req)

	if err != nil {
		d.logger.Errorf("failed to send transaction to server %s, due to %s", getConfig.String(), err)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		d.logger.Errorf("error response from the server, %s", response.Status)
		return nil, errors.New(fmt.Sprintf("error response from the server, %s", response.Status))
	}

	res := &types.GetConfigResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		return nil, err
	}

	for _, node := range res.GetPayload().GetConfig().GetNodes() {
		cert, err := x509.ParseCertificate(node.Certificate)
		if err != nil {
			return nil, err
		}

		_, err = cert.Verify(x509.VerifyOptions{
			Roots: d.rootCAs,
		})
		if err != nil {
			return nil, err
		}

		nodesCerts[node.ID] = cert
	}

	return nodesCerts, nil
}

// UsersTx returns user's transaction context
func (d *dbSession) UsersTx() (UsersTxContext, error) {
	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}
	userTx := &userTxContext{
		commonTxContext: commonCtx,
	}
	return userTx, nil
}

// DBsTx returns database management transaction context
func (d *dbSession) DBsTx() (DBsTxContext, error) {
	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}
	dbsTx := &dbsTxContext{
		commonTxContext: commonCtx,
		createdDBs:      map[string]bool{},
		deletedDBs:      map[string]bool{},
	}
	return dbsTx, nil
}

// DataTx returns data's transaction context
func (d *dbSession) DataTx(database string) (DataTxContext, error) {
	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}
	dataTx := &dataTxContext{
		commonTxContext: commonCtx,
		database:        database,
		dataWrites:      make(map[string]*types.DataWrite),
		dataDeletes:     make(map[string]*types.DataDelete),
	}
	return dataTx, nil
}

// ConfigTx returns config transaction context
func (d *dbSession) ConfigTx() (ConfigTxContext, error) {
	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}
	configTx := &configTxContext{
		commonTxContext:      commonCtx,
		oldConfig:            nil,
		readOldConfigVersion: nil,
		newConfig:            nil,
	}

	if err = configTx.queryClusterConfig(); err != nil {
		return nil, err
	}

	return configTx, nil
}

// Provenance returns handler to access provenance
func (d *dbSession) Provenance() (Provenance, error) {
	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}
	return &provenance{
		commonCtx,
	}, nil
}

// Ledger returns handler to access bcdb ledger data
func (d *dbSession) Ledger() (Ledger, error) {
	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}
	return &ledger{
		commonCtx,
	}, nil
}

func (d *dbSession) newCommonTxContext() (*commonTxContext, error) {
	httpClient := d.newHTTPClient()

	nodesCerts, err := d.getServerCertificates(httpClient)
	if err != nil {
		return nil, err
	}
	commonTxContext := &commonTxContext{
		userID:     d.userID,
		signer:     d.signer,
		userCert:   d.userCert,
		replicaSet: d.replicaSet,
		nodesCerts: nodesCerts,
		restClient: NewRestClient(d.userID, httpClient, d.signer),
		logger:     d.logger,
	}
	return commonTxContext, nil
}

func (d *dbSession) getServerCertificates(httpClient *http.Client) (map[string]*x509.Certificate, error) {
	var nodesCerts map[string]*x509.Certificate
	var err error
	for _, replica := range d.replicaSet {
		nodesCerts, err = d.getNodesCerts(replica, httpClient)
		if err != nil {
			d.logger.Errorf("failed to obtain server's certificate, replica: %s", replica)
			continue
		}
	}

	if len(nodesCerts) == 0 {
		d.logger.Errorf("failed to obtain server's certificate, replicaSet: %s", d.replicaSet)
		return nil, errors.New("failed to obtain server's certificate")
	}
	return nodesCerts, nil
}

func (d *dbSession) newHTTPClient() *http.Client {
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	return httpClient
}

func ComputeTxID(userCert []byte) (string, error) {
	nonce := make([]byte, 24)
	_, err := rand.Read(nonce)
	if err != nil {
		return "", err
	}

	b := append(nonce, userCert...)

	sha256Hash, err := crypto.ComputeSHA256Hash(b)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(sha256Hash), err
}

func UsersMap(users ...string) map[string]bool {
	m := make(map[string]bool)
	for _, u := range users {
		m[u] = true
	}
	return m
}
