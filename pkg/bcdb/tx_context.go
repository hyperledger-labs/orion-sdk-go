package bcdb

import (
	"context"
	"crypto/x509"
	"encoding/json"
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
	txEnvelope proto.Message
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
	t.txEnvelope, err = tx.composeEnvelope(txID)
	if err != nil {
		t.logger.Errorf("failed to compose transaction envelope, due to", err)
		return txID, err
	}
	ctx := context.TODO() // TODO: Replace with timeout
	response, err := t.restClient.Submit(ctx, postEndpointResolved.String(), t.txEnvelope)
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

func (t *commonTxContext) handleRequest(rawurl string, query proto.Message, res proto.Message) error {
	parsedURL, err := url.Parse(rawurl)
	if err != nil {
		return err
	}
	restURL := t.selectReplica().ResolveReference(parsedURL).String()
	response, err := t.restClient.Query(context.TODO(), restURL, query)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("error handling request, server returned %s", response.Status))
	}
	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		t.logger.Errorf("failed to decode json response, due to", err)
		return err
	}
	return nil
}

func (t *commonTxContext) TxEnvelope() (proto.Message, error) {
	if t.txEnvelope == nil {
		return nil, ErrTxNotFinalized
	}
	return t.txEnvelope, nil
}

var ErrTxNotFinalized = errors.New("can't access tx envelope, transaction not finalized")
