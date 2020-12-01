package bcdb

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/golang/protobuf/proto"
	"github.ibm.com/blockchaindb/server/pkg/logger"
)

type commonTxContext struct {
	userID     string
	signer     Signer
	userCert   []byte
	replicaSet map[string]*url.URL
	nodesCerts map[string]*x509.Certificate
	restClient RestClient
	logger     *logger.SugarLogger
}

type txContext interface {
	composeEnvelope(txID string) (proto.Message, error)
	cleanCtx()
}

func (t *commonTxContext) commit(tx txContext, postEndpoint string) (string, error) {
	replica := t.selectReplica()
	postEndpointResolved := replica.ResolveReference(&url.URL{Path: postEndpoint})

	txID, err := ComputeTxID(t.userCert)
	if err != nil {
		return "", err
	}

	t.logger.Debugf("compose transaction enveloped with txID = %s", txID)
	envelope, err := tx.composeEnvelope(txID)
	if err != nil {
		t.logger.Errorf("failed to compose transaction envelope, due to", err)
		return txID, err
	}

	ctx := context.TODO() // TODO: Replace with timeout
	response, err := t.restClient.Submit(ctx, postEndpointResolved.String(), envelope)
	if err != nil {
		t.logger.Errorf("failed to submit transaction txID = %s, due to", txID, err)
		return txID, err
	}

	if response.StatusCode != http.StatusOK {
		t.logger.Errorf("error status from server, %s", response.Status)
		return txID, errors.New(fmt.Sprintf("error status from server, %s", response.Status))
	}

	tx.cleanCtx()
	return txID, nil
}

func (t *commonTxContext) abort(tx txContext) error {
	tx.cleanCtx()
	return nil
}

func (t *commonTxContext) selectReplica() *url.URL {
	// Pick first replica to send request to
	for _, replica := range t.replicaSet {
		return replica
	}
	return nil
}
