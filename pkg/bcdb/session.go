// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
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

// JSONQuery returns handler to access bcdb data through JSON query
func (d *dbSession) JSONQuery() (JSONQuery, error) {
	commonCtx, err := d.newCommonTxContext()
	if err != nil {
		return nil, err
	}
	return &JSONQueryExecutor{
		commonCtx,
	}, nil
}

func (d *dbSession) newCommonTxContext(options ...TxContextOption) (*commonTxContext, error) {
	httpClient := newHTTPClient()

	commonTxCtx := &commonTxContext{
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

func (d *dbSession) sigVerifier(httpClient *http.Client) (SignatureVerifier, error) {
	var verifier SignatureVerifier
	var err error
	for _, replica := range d.replicaSet {
		//TODO choose the cert-set from the best replica - the one  with the highest config version. See:
		// https://github.com/hyperledger-labs/orion-sdk-go/issues/27
		verifier, err = d.getNodesCerts(replica, httpClient)
		if err == nil {
			break
		}
		d.logger.Errorf("failed to obtain the servers' certificates from replica: %s, error: %s", replica, err)
	}

	if verifier == nil {
		d.logger.Errorf("failed to obtain the servers' certificates, bootstrapReplicaSet: %s", d.replicaSet)
		return nil, errors.New("failed to obtain the servers' certificates")
	}
	return verifier, nil
}

func (d *dbSession) getNodesCerts(replica *url.URL, httpClient *http.Client) (SignatureVerifier, error) {
	nodesCerts := map[string]*x509.Certificate{}
	getStatus := &url.URL{
		Path: constants.GetClusterStatus,
	}
	statusREST := replica.ResolveReference(getStatus)
	ctx := context.TODO()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusREST.String(), nil)
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
		d.logger.Errorf("failed to send transaction to server %s, due to %s", getStatus.String(), err)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		var errMsg string
		if response.Body != nil {
			errRes := &types.HttpResponseErr{}
			if err := json.NewDecoder(response.Body).Decode(errRes); err != nil {
				d.logger.Errorf("failed to parse the server's error message, due to %s", err)
				errMsg = "(failed to parse the server's error message)"
			} else {
				errMsg = errRes.Error()
			}
		}
		err := &httpError{
			status:     response.Status,
			statusCode: response.StatusCode,
			errMsg:     errMsg,
		}
		d.logger.Errorf("error response from the server, %s", err)
		return nil, err
	}

	resEnv := &types.GetClusterStatusResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(resEnv)
	if err != nil {
		return nil, err
	}

	statusResp := resEnv.GetResponse()
	nodes := statusResp.GetNodes()
	numNodes := len(nodes)
	for i, node := range nodes {
		d.logger.Debugf("Cluster Nodes (from %s): [%d/%d]: %s", replica.String(), i+1, numNodes, nodeConfigToString(node))
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

	respBytes, err := json.Marshal(statusResp)
	if err != nil {
		return nil, err
	}

	if err = verifier.Verify(
		statusResp.GetHeader().GetNodeId(),
		respBytes,
		resEnv.GetSignature()); err != nil {
		d.logger.Errorf("failed to verify configuration response, error = %s", err)
		return nil, errors.Errorf("failed to verify configuration response, error = %s", err)
	}

	d.logger.Debugf("Cluster Status (from %s): Leader: %s, Active: %v, Version: %v",
		replica.String(), statusResp.GetLeader(), statusResp.GetActive(), statusResp.GetVersion())

	return verifier, err
}

//TODO expose HTTP parameters, make client configurable, with good defaults. See:
// https://github.com/hyperledger-labs/orion-sdk-go/issues/28
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
	return base64.URLEncoding.EncodeToString(sha256Hash), nil
}

func nodeConfigToString(n *types.NodeConfig) string {
	return fmt.Sprintf("Id: %s, Address: %s, Port: %d, Cert-hash: %x", n.Id, n.Address, n.Port, crc32.ChecksumIEEE(n.Certificate))
}
