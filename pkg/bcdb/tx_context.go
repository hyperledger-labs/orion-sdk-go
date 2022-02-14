// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

const (
	contextTimeoutMargin = time.Second
)

type commonTxContext struct {
	userID        string
	txID          string
	signer        Signer
	userCert      []byte
	replicaSet    map[string]*url.URL
	verifier      SignatureVerifier
	restClient    RestClient
	txEnvelope    proto.Message
	commitTimeout time.Duration
	queryTimeout  time.Duration
	txSpent       bool
	logger        *logger.SugarLogger
}

type txContext interface {
	composeEnvelope(txID string) (proto.Message, error)
	// structs which embed the commonTXContext must implement this to clean the parts of the state that are not common.
	cleanCtx()
}

func (t *commonTxContext) commit(tx txContext, postEndpoint string, sync bool) (string, *types.TxReceiptResponseEnvelope, error) {
	if t.txSpent {
		return "", nil, ErrTxSpent
	}

	replica := t.selectReplica()
	postEndpointResolved := replica.ResolveReference(&url.URL{Path: postEndpoint})

	t.logger.Debugf("compose transaction enveloped with txID = %s", t.txID)
	var err error
	t.txEnvelope, err = tx.composeEnvelope(t.txID)
	if err != nil {
		t.logger.Errorf("failed to compose transaction envelope, due to %s", err)
		return t.txID, nil, err
	}
	ctx := context.Background()
	serverTimeout := time.Duration(0)
	if sync {
		serverTimeout = t.commitTimeout
		contextTimeout := t.commitTimeout + contextTimeoutMargin
		var cancelFnc context.CancelFunc
		ctx, cancelFnc = context.WithTimeout(context.Background(), contextTimeout)
		defer cancelFnc()
	}
	defer tx.cleanCtx()

	response, err := t.restClient.Submit(ctx, postEndpointResolved.String(), t.txEnvelope, serverTimeout)
	if err != nil {
		t.logger.Errorf("failed to submit transaction txID = %s, due to %s", t.txID, err)
		return t.txID, nil, err
	}

	if response.StatusCode != http.StatusOK {
		var errMsg string
		if response.StatusCode == http.StatusAccepted {
			return t.txID, nil, &ServerTimeout{TxID: t.txID}
		}
		if response.Body != nil {
			errRes := &types.HttpResponseErr{}
			if err := json.NewDecoder(response.Body).Decode(errRes); err != nil {
				t.logger.Errorf("failed to parse the server's error message, due to %s", err)
				errMsg = "(failed to parse the server's error message)"
			} else {
				errMsg = errRes.Error()
			}
		}

		return t.txID, nil, errors.Errorf("failed to submit transaction, server returned: status: %s, message: %s", response.Status, errMsg)
	}

	txResponseEnvelope := &types.TxReceiptResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(txResponseEnvelope)
	if err != nil {
		t.logger.Errorf("failed to decode json response, due to %s", err)
		return t.txID, nil, err
	}

	nodeID := txResponseEnvelope.GetResponse().GetHeader().GetNodeId()
	respBytes, err := json.Marshal(txResponseEnvelope.GetResponse())
	if err != nil {
		t.logger.Errorf("failed to marshal the response")
		return t.txID, nil, err
	}

	err = t.verifier.Verify(nodeID, respBytes, txResponseEnvelope.GetSignature())
	if err != nil {
		t.logger.Errorf("signature verification failed nodeID %s, due to %s", nodeID, err)
		return "", nil, errors.Errorf("signature verification failed nodeID %s, due to %s", nodeID, err)
	}

	t.txSpent = true
	tx.cleanCtx()

	receipt := txResponseEnvelope.GetResponse().GetReceipt()

	if sync {
		validationInfo := receipt.GetHeader().GetValidationInfo()
		if validationInfo == nil {
			return t.txID, nil, errors.Errorf("server error: validation info is nil")
		} else {
			validFlag := validationInfo[receipt.TxIndex].GetFlag()
			if validFlag != types.Flag_VALID {
				return t.txID, txResponseEnvelope, &ErrorTxValidation{TxID: t.txID, Flag: validFlag.String(), Reason: validationInfo[receipt.TxIndex].ReasonIfInvalid}
			}
		}
	}

	return t.txID, txResponseEnvelope, nil
}

func (t *commonTxContext) abort(tx txContext) error {
	if t.txSpent {
		return ErrTxSpent
	}

	t.txSpent = true
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

func (t *commonTxContext) handleRequest(rawurl string, msgToSign, res proto.Message) error {
	return t.handleGetPostRequest(rawurl, http.MethodGet, nil, msgToSign, res)
}

func (t *commonTxContext) handleRequestWithPost(rawurl string, postData []byte, msgToSign, res proto.Message) error {
	return t.handleGetPostRequest(rawurl, http.MethodPost, postData, msgToSign, res)
}

type httpError struct {
	status     string
	statusCode int
	errMsg     string
}

func (e *httpError) Error() string {
	return "error handling request, server returned:" +
		" status: " + e.status +
		", status code: " + strconv.Itoa(e.statusCode) +
		", message: " + e.errMsg
}

func (t *commonTxContext) handleGetPostRequest(rawurl, httpMethod string, postData []byte, msgToSign, res proto.Message) error {
	parsedURL, err := url.Parse(rawurl)
	if err != nil {
		return err
	}
	restURL := t.selectReplica().ResolveReference(parsedURL).String()
	ctx := context.Background()
	if t.queryTimeout > 0 {
		contextTimeout := t.queryTimeout
		var cancelFnc context.CancelFunc
		ctx, cancelFnc = context.WithTimeout(context.Background(), contextTimeout)
		defer cancelFnc()
	}

	signature, err := cryptoservice.SignQuery(t.signer, msgToSign)
	if err != nil {
		return err
	}

	response, err := t.restClient.Query(ctx, restURL, httpMethod, postData, signature)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		var errMsg string
		if response.Body != nil {
			errRes := &types.HttpResponseErr{}
			if err := json.NewDecoder(response.Body).Decode(errRes); err != nil {
				t.logger.Errorf("failed to parse the server's error message, due to %s", err)
				errMsg = "(failed to parse the server's error message)"
			} else {
				errMsg = errRes.Error()
			}
		}
		return &httpError{
			status:     response.Status,
			statusCode: response.StatusCode,
			errMsg:     errMsg,
		}
	}

	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		t.logger.Errorf("failed to decode json response, due to %s", err)
		return err
	}

	if _, ok := res.(ResponseEnvelop); ok {
		responsePayload, err := ResponseSelector(res.(ResponseEnvelop))
		if err != nil {
			t.logger.Errorf("failed to recognize resopnse type: %s", err)
			return err
		}
		respBytes, err := json.Marshal(responsePayload)
		if err != nil {
			t.logger.Errorf("failed to marshal the response")
			return err
		}
		nodeID := responsePayload.GetHeader().GetNodeId()
		err = t.verifier.Verify(nodeID, respBytes, res.(ResponseEnvelop).GetSignature())
		if err != nil {
			t.logger.Errorf("signature verification failed nodeID %s, due to %s", nodeID, err)
			return errors.Errorf("signature verification failed nodeID %s, due to %s", nodeID, err)
		}
	} else {
		t.logger.Errorf("can't identify response type, unable to validate signature")
		return errors.New("can't identify response type, unable to validate signature")
	}

	return nil
}

type ResponseEnvelop interface {
	GetSignature() []byte
}

type ResponseWithHeader interface {
	GetHeader() *types.ResponseHeader
}

func ResponseSelector(envelop ResponseEnvelop) (ResponseWithHeader, error) {
	switch t := envelop.(type) {
	case *types.GetDBStatusResponseEnvelope:
		return envelop.(*types.GetDBStatusResponseEnvelope).GetResponse(), nil
	case *types.GetDataResponseEnvelope:
		return envelop.(*types.GetDataResponseEnvelope).GetResponse(), nil
	case *types.GetUserResponseEnvelope:
		return envelop.(*types.GetUserResponseEnvelope).GetResponse(), nil
	case *types.GetConfigResponseEnvelope:
		return envelop.(*types.GetConfigResponseEnvelope).GetResponse(), nil
	case *types.GetNodeConfigResponseEnvelope:
		return envelop.(*types.GetNodeConfigResponseEnvelope).GetResponse(), nil
	case *types.GetBlockResponseEnvelope:
		return envelop.(*types.GetBlockResponseEnvelope).GetResponse(), nil
	case *types.GetAugmentedBlockHeaderResponseEnvelope:
		return envelop.(*types.GetAugmentedBlockHeaderResponseEnvelope).GetResponse(), nil
	case *types.GetLedgerPathResponseEnvelope:
		return envelop.(*types.GetLedgerPathResponseEnvelope).GetResponse(), nil
	case *types.GetTxProofResponseEnvelope:
		return envelop.(*types.GetTxProofResponseEnvelope).GetResponse(), nil
	case *types.GetDataProofResponseEnvelope:
		return envelop.(*types.GetDataProofResponseEnvelope).GetResponse(), nil
	case *types.GetHistoricalDataResponseEnvelope:
		return envelop.(*types.GetHistoricalDataResponseEnvelope).GetResponse(), nil
	case *types.GetDataReadersResponseEnvelope:
		return envelop.(*types.GetDataReadersResponseEnvelope).GetResponse(), nil
	case *types.GetDataWritersResponseEnvelope:
		return envelop.(*types.GetDataWritersResponseEnvelope).GetResponse(), nil
	case *types.GetDataProvenanceResponseEnvelope:
		return envelop.(*types.GetDataProvenanceResponseEnvelope).GetResponse(), nil
	case *types.GetTxIDsSubmittedByResponseEnvelope:
		return envelop.(*types.GetTxIDsSubmittedByResponseEnvelope).GetResponse(), nil
	case *types.TxReceiptResponseEnvelope:
		return envelop.(*types.TxReceiptResponseEnvelope).GetResponse(), nil
	case *types.DataQueryResponseEnvelope:
		return envelop.(*types.DataQueryResponseEnvelope).GetResponse(), nil
	case *types.GetDBIndexResponseEnvelope:
		return envelop.(*types.GetDBIndexResponseEnvelope).GetResponse(), nil

	default:
		return nil, errors.Errorf("unknown response type %T", t)
	}
}

func (t *commonTxContext) CommittedTxEnvelope() (proto.Message, error) {
	if t.txEnvelope == nil {
		return nil, ErrTxNotFinalized
	}
	return t.txEnvelope, nil
}

var ErrTxNotFinalized = errors.New("can't access tx envelope, transaction not finalized")

type ServerTimeout struct {
	TxID string
}

func (e *ServerTimeout) Error() string {
	return "timeout occurred on server side while submitting transaction, converted to asynchronous completion, TxID: " + e.TxID
}

type ErrorTxValidation struct {
	TxID   string
	Flag   string
	Reason string
}

func (e *ErrorTxValidation) Error() string {
	return "transaction txID = " + e.TxID + " is not valid, flag: " + e.Flag + ", reason: " + e.Reason
}

type ErrorNotFound struct {
	Message string
}

func (e *ErrorNotFound) Error() string {
	return e.Message
}
