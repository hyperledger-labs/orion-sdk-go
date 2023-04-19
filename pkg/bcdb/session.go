// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/hyperledger-labs/orion-sdk-go/internal"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
)

// TODO refresh replicaSet and signature verifier when cluster config changes.
type dbSession struct {
	userID               string
	signer               Signer
	verifier             SignatureVerifier
	userCert             []byte
	replicaSet           internal.ReplicaSet
	replicaSetVersion    *types.Version
	rootCAs              *certificateauthority.CACertCollection
	tlsEnabled           bool
	tlsRootCAs           *certificateauthority.CACertCollection
	clientAuthRequired   bool
	clientTlsConfig      *tls.Config
	txTimeout            time.Duration
	queryTimeout         time.Duration
	logger               *logger.SugarLogger
	restClient           RestClient
	updateReplicaSetFlag bool
}

// TxContextOption is a function that operates on a commonTxContext and applies a configuration option.
type TxContextOption func(svc *commonTxContext) error

// WithTxID provides an external transaction ID (txID) to the transaction context.
// The given txID must be unique in the database cluster to which it is submitted.
// The given txID must be safe to use as a URL segment (see segment-nz at: https://www.ietf.org/rfc/rfc3986.txt).
//
// Note that the database may reveal the transaction IDs it executed to other clients. Therefore, it is recommended
// not to include any sensitive information in it. It is recommended to generate the txID by hashing some identifier
// that includes enough entropy such that it reveals no information, e.g. `Hash(ID + Nonce)`, and convert the bytes to
// a URL-safe string representation.
func WithTxID(txID string) TxContextOption {
	return func(txCtx *commonTxContext) error {
		if len(txID) == 0 {
			return errors.New("WithTxID: empty txID")
		}
		if err := constants.SafeURLSegmentNZ(txID); err != nil {
			return errors.WithMessage(err, "WithTxID")
		}

		txCtx.txID = txID
		return nil
	}
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
		createdDBs:      map[string]*types.DBIndex{},
		deletedDBs:      map[string]bool{},
	}
	return dbsTx, nil
}

// DataTx returns data's transaction context
func (d *dbSession) DataTx(options ...TxContextOption) (DataTxContext, error) {
	commonCtx, err := d.newCommonTxContext(options...)
	if err != nil {
		return nil, err
	}

	dataTx := &dataTxContext{
		commonTxContext: commonCtx,
		operations:      make(map[string]*dbOperations),
		txUsers: map[string]bool{
			commonCtx.userID: true,
		},
	}
	return dataTx, nil
}

// LoadDataTx loads a given data transaction envelope for inspection, co-signing, and commit
func (d *dbSession) LoadDataTx(txEnv *types.DataTxEnvelope) (LoadedDataTxContext, error) {
	switch {
	case txEnv == nil:
		return nil, errors.New("transaction envelope is nil")
	case txEnv.GetPayload() == nil:
		return nil, errors.New("payload in the transaction envelope is nil")
	case txEnv.GetSignatures() == nil || len(txEnv.GetSignatures()) == 0:
		return nil, errors.New("transaction envelope does not have a signature")
	case txEnv.GetPayload().GetTxId() == "":
		return nil, errors.New("transaction ID in the transaction envelope is empty")
	case len(txEnv.GetPayload().GetMustSignUserIds()) == 0:
		return nil, errors.New("no user ID in the transaction envelope")
	}

	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}

	commonCtx.txID = txEnv.Payload.TxId
	dataTx := &loadedDataTxContext{
		commonTxContext: commonCtx,
		txEnv:           txEnv,
	}
	return dataTx, nil
}

// ConfigTx returns config transaction context.
// This methods first reads the current ClusterConfig from the cluster.
// This is only available to sessions of an admin user.
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

// Query returns handler to access bcdb data through JSON query
func (d *dbSession) Query() (Query, error) {
	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}
	return &QueryExecutor{
		commonCtx,
	}, nil
}

func (d *dbSession) ReplicaSet(refresh bool) ([]*config.Replica, error) {
	if refresh {
		httpClient := newHTTPClient(d.tlsEnabled, d.clientTlsConfig, nil)
		if err := d.updateReplicaSetAndVerifier(httpClient, d.tlsEnabled); err != nil {
			d.logger.Errorf("cannot update the replica set and signature verifier, error: %s", err)
			return nil, errors.Wrap(err, "cannot update the replica set and signature verifier")
		}
	}

	return d.replicaSet.ToConfigReplicaSet(), nil
}

func (d *dbSession) newCommonTxContext(options ...TxContextOption) (*commonTxContext, error) {
	// checkRedirectPolicyFunc specifies a policy of how redirects should be handled when newHTTPClient is created to send a transaction
	// HTTP client will follow 10 redirects, and then it will return an error. We need to remember that redirect occurred to update replica set for future txs.
	checkRedirectPolicyFunc := func(req *http.Request, via []*http.Request) error {
		if len(via) > 10 {
			return errors.Errorf("Too many redirects: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		}
		d.updateReplicaSetFlag = true
		return nil
	}

	// updateReplicaSetFlag becomes true when redirection occurred.
	// In this case we want to update replica set for future requests when they are created
	if d.updateReplicaSetFlag {
		_, errRefreshRes := d.ReplicaSet(true)
		if errRefreshRes != nil {
			return nil, errRefreshRes
		}
		d.updateReplicaSetFlag = false
	}

	if d.restClient == nil {
		d.restClient = NewRestClient(d.userID, newHTTPClient(d.tlsEnabled, d.clientTlsConfig, checkRedirectPolicyFunc), d.signer)
	}

	commonTxCtx := &commonTxContext{
		userID:        d.userID,
		signer:        d.signer,
		userCert:      d.userCert,
		replicaSet:    d.replicaSet,
		verifier:      d.verifier,
		restClient:    d.restClient,
		commitTimeout: d.txTimeout,
		queryTimeout:  d.queryTimeout,
		logger:        d.logger,
		dbSession:     d,
	}

	for _, opt := range options {
		if err := opt(commonTxCtx); err != nil {
			return nil, errors.WithMessage(err, "error while applying option")
		}
	}

	if len(commonTxCtx.txID) == 0 {
		var err error
		commonTxCtx.txID, err = computeTxID(d.userCert)
		if err != nil {
			return nil, err
		}
	}

	return commonTxCtx, nil
}

// updateReplicaSetAndVerifier connects to the cluster, pulls the most recent cluster status, builds a signature
// verifier from it, and updates the replica-set.
func (d *dbSession) updateReplicaSetAndVerifier(httpClient *http.Client, tlsEnabled bool) error {
	// get the latest status from replica set
	clusterStatusEnv, err := d.getLatestClusterStatus(httpClient)
	if err != nil {
		return errors.Wrap(err, "failed to obtain the latest cluster status")
	}

	if d.replicaSetVersion != nil && compareVersion(clusterStatusEnv.GetResponse().GetVersion(), d.replicaSetVersion) < 0 {
		d.logger.Debugf("Cluster config version from server: [%v] is smaller than the latest replica set version: [%v], skipping update.", clusterStatusEnv.GetResponse().GetVersion(), d.replicaSetVersion)
		return nil
	}

	// create the verifier and verify the cluster status response envelope
	verifier, err := d.createSignatureVerifier(clusterStatusEnv)
	if err != nil {
		return errors.Wrap(err, "failed to create signature verifier form servers' certificates")
	}

	replicaSet, err := internal.ClusterStatusToReplicaSet(clusterStatusEnv.GetResponse(), tlsEnabled)
	if err != nil {
		return errors.Wrap(err, "failed to create replica set with role from cluster status")
	}

	d.verifier = verifier
	d.replicaSet = replicaSet
	d.replicaSetVersion = clusterStatusEnv.GetResponse().GetVersion()
	d.updateReplicaSetFlag = false
	d.logger.Debugf("updated replica set, version: %+v, set: %v", d.replicaSetVersion, d.replicaSet)

	return nil
}

func (d *dbSession) getClusterStatusFrom(replica *url.URL, httpClient *http.Client) (*types.GetClusterStatusResponseEnvelope, error) {
	getStatus := &url.URL{
		Path: constants.GetClusterStatus,
	}
	statusREST := replica.ResolveReference(getStatus)
	ctx := context.TODO()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusREST.String(), nil)
	if err != nil {
		return nil, err
	}

	signature, err := cryptoservice.SignQuery(d.signer, &types.GetClusterStatusQuery{
		UserId:         d.userID,
		NoCertificates: false,
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
		d.logger.Errorf("failed to send transaction to server %s, due to %s", getStatus.String(), err)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		d.logger.Errorf("error response from the server, %s", response.Status)
		return nil, errors.New(fmt.Sprintf("error response from the server, %s", response.Status))
	}

	resEnv := &types.GetClusterStatusResponseEnvelope{}

	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	err = protojson.Unmarshal(responseBytes, resEnv)

	statusResp := resEnv.GetResponse()

	d.logger.Debugf("Cluster status (from: %s) version: %+v", replica.String(), statusResp.GetVersion())

	return resEnv, nil
}

// getLatestClusterStatus get the most updated cluster status out of all servers in the replica set.
// If the replica set is empty, use the bootstrap replica set.
func (d *dbSession) getLatestClusterStatus(httpClient *http.Client) (*types.GetClusterStatusResponseEnvelope, error) {
	latestStatusEnv := &types.GetClusterStatusResponseEnvelope{}
	latestFrom := ""
	var lastErr error

	for _, replica := range d.replicaSet {
		statusRespEnv, err := d.getClusterStatusFrom(replica.URL, httpClient)
		if err != nil {
			d.logger.Debugf("Failed to get cluster status from server: %s; because: %s", replica.String(), err)
			lastErr = err
			continue
		}

		latestVersion := latestStatusEnv.GetResponse().GetVersion()
		fetchedVersion := statusRespEnv.GetResponse().GetVersion()
		if compareVersion(fetchedVersion, latestVersion) > 0 {
			latestStatusEnv = statusRespEnv
			latestFrom = fmt.Sprintf("%s", replica.String())
			d.logger.Debugf("Got latest cluster status from server: %s", latestFrom)
		}
	}

	if latestStatusEnv.GetResponse() == nil {
		return nil, errors.New(fmt.Sprintf("failed to get cluster status from replica set: %+v; version: %+v, last error: %s", d.replicaSet, d.replicaSetVersion, lastErr))
	}

	d.logger.Debugf("Latest cluster status (from: %s) is: %+v", latestFrom, latestStatusEnv.GetResponse())

	return latestStatusEnv, nil
}

// createSignatureVerifier creates a SignatureVerifier and verifies the cluster status response envelope with it
func (d *dbSession) createSignatureVerifier(clusterStatusEnv *types.GetClusterStatusResponseEnvelope) (SignatureVerifier, error) {
	clusterStatus := clusterStatusEnv.GetResponse()
	nodes := clusterStatus.GetNodes()
	numNodes := len(nodes)
	nodesCerts := map[string]*x509.Certificate{}
	for i, node := range nodes {
		d.logger.Debugf("Cluster Nodes: [%d/%d]: %s", i+1, numNodes, nodeConfigToString(node))
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

	respBytes, err := marshal.DefaultMarshaller().Marshal(clusterStatus)
	if err != nil {
		return nil, err
	}

	if err = verifier.Verify(
		clusterStatus.GetHeader().GetNodeId(),
		respBytes,
		clusterStatusEnv.GetSignature()); err != nil {
		d.logger.Errorf("failed to verify configuration response, error = %s", err)
		return nil, errors.Errorf("failed to verify configuration response, error = %s", err)
	}

	d.logger.Debugf("Cluster Status: Leader: %s, Active: %v, Version: %v",
		clusterStatus.GetLeader(), clusterStatus.GetActive(), clusterStatus.GetVersion())

	return verifier, err
}

// TODO expose HTTP parameters, make client configurable, with good defaults. See:
// https://github.com/hyperledger-labs/orion-sdk-go/issues/28
func newHTTPClient(tlsEnabled bool, tlsConfig *tls.Config, checkRedirectPolicyFunc func(req *http.Request, via []*http.Request) error) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		CheckRedirect: checkRedirectPolicyFunc,
	}
	if tlsEnabled {
		httpClient.Transport.(*http.Transport).TLSClientConfig = tlsConfig
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
	return base64.URLEncoding.EncodeToString(sha256Hash), nil
}

func nodeConfigToString(n *types.NodeConfig) string {
	return fmt.Sprintf("Id: %s, Address: %s, Port: %d, Cert-hash: %x", n.Id, n.Address, n.Port, crc32.ChecksumIEEE(n.Certificate))
}

func compareVersion(x, y *types.Version) int {
	if x.GetBlockNum() > y.GetBlockNum() {
		return 1
	}

	if x.GetBlockNum() < y.GetBlockNum() {
		return -1
	}

	if x.GetTxNum() > y.GetTxNum() {
		return 1
	}

	if x.GetTxNum() < y.GetTxNum() {
		return -1
	}

	return 0
}
