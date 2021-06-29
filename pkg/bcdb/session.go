package bcdb

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/pkg/certificateauthority"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
)

//TODO refresh replicaSet and signature verifier when cluster config changes.
type dbSession struct {
	userID       string
	signer       Signer
	verifier     SignatureVerifier
	userCert     []byte
	replicaSet   map[string]*url.URL
	rootCAs      *certificateauthority.CACertCollection
	txTimeout    time.Duration
	queryTimeout time.Duration
	logger       *logger.SugarLogger
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
func (d *dbSession) DataTx() (DataTxContext, error) {
	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}
	dataTx := &dataTxContext{
		commonTxContext: commonCtx,
		operations:      make(map[string]*dbOperations),
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
	httpClient := newHTTPClient()

	commonTxContext := &commonTxContext{
		userID:        d.userID,
		signer:        d.signer,
		userCert:      d.userCert,
		replicaSet:    d.replicaSet,
		verifier:      d.verifier,
		restClient:    NewRestClient(d.userID, httpClient, d.signer),
		commitTimeout: d.txTimeout,
		queryTimeout:  d.queryTimeout,
		logger:        d.logger,
	}
	return commonTxContext, nil
}

func (d *dbSession) sigVerifier(httpClient *http.Client) (SignatureVerifier, error) {
	var verifier SignatureVerifier
	var err error
	for _, replica := range d.replicaSet {
		//TODO choose the cert-set from the best replica - the one  with the highest config version. See:
		// https://github.com/IBM-Blockchain/bcdb-sdk/issues/27
		verifier, err = d.getNodesCerts(replica, httpClient)
		if err == nil {
			break
		}
		d.logger.Errorf("failed to obtain the servers' certificates from replica: %s, error: %s", replica, err)
	}

	if verifier == nil {
		d.logger.Errorf("failed to obtain the servers' certificates, replicaSet: %s", d.replicaSet)
		return nil, errors.New("failed to obtain the servers' certificates")
	}
	return verifier, nil
}

func (d *dbSession) getNodesCerts(replica *url.URL, httpClient *http.Client) (SignatureVerifier, error) {
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
		UserId: d.userID,
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

	resEnv := &types.GetConfigResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(resEnv)
	if err != nil {
		return nil, err
	}

	for _, node := range resEnv.GetResponse().GetConfig().GetNodes() {
		err := d.rootCAs.VerifyLeafCert(node.Certificate)
		if err != nil {
			return nil, err
		}
		cert, err := x509.ParseCertificate(node.Certificate)
		if err != nil {
			return nil, err
		}
		nodesCerts[node.Id] = cert
	}

	verifier, err := NewVerifier(nodesCerts, d.logger)
	if err != nil {
		return nil, err
	}

	respBytes, err := json.Marshal(resEnv.GetResponse())
	if err != nil {
		return nil, err
	}

	if err = verifier.Verify(
		resEnv.GetResponse().GetHeader().GetNodeId(),
		respBytes,
		resEnv.GetSignature()); err != nil {
		d.logger.Errorf("failed to verify configuration response, error = %s", err)
		return nil, errors.Errorf("failed to verify configuration response, error = %s", err)
	}

	return verifier, err
}

//TODO expose HTTP parameters, make client configurable, with good defaults. See:
// https://github.com/IBM-Blockchain/bcdb-sdk/issues/28
func newHTTPClient() *http.Client {
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

func computeTxID(userCert []byte) (string, error) {
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
