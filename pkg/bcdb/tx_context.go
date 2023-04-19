// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger-labs/orion-sdk-go/internal"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	contextTimeoutMargin = time.Second
)

var retriesTimoeoutConfig = 5 * time.Second

type commonTxContext struct {
	userID        string
	txID          string
	signer        Signer
	userCert      []byte
	replicaSet    internal.ReplicaSet
	verifier      SignatureVerifier
	restClient    RestClient
	txEnvelope    proto.Message
	commitTimeout time.Duration
	queryTimeout  time.Duration
	txSpent       bool
	logger        *logger.SugarLogger
	dbSession     *dbSession
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

	var err error
	var response *http.Response

	// we do 7 attempts at increasing intervals or up to retriesTimeout
	retriesTimeout := time.After(retriesTimoeoutConfig)
	retryInterval := retriesTimoeoutConfig / 64
	countRetries := 0

	ctx := context.Background()
	var cancelFnc context.CancelFunc

	for {
		replica, err := t.selectReplica()
		if err != nil {
			t.logger.Errorf("failed to select replica, due to %s", err)
			return t.txID, nil, errors.WithMessage(err, "failed to select replica")
		}
		postEndpointResolved := replica.ResolveReference(&url.URL{Path: postEndpoint})

		t.logger.Debugf("compose transaction enveloped with txID = %s", t.txID)

		t.txEnvelope, err = tx.composeEnvelope(t.txID)
		if err != nil {
			t.logger.Errorf("failed to compose transaction envelope, due to %s", err)
			return t.txID, nil, err
		}

		serverTimeout := time.Duration(0)
		if sync {
			serverTimeout = t.commitTimeout
			contextTimeout := t.commitTimeout + contextTimeoutMargin
			ctx, cancelFnc = context.WithTimeout(context.Background(), contextTimeout)
			defer cancelFnc()
		}
		defer tx.cleanCtx()

		response, err = t.restClient.Submit(ctx, postEndpointResolved.String(), t.txEnvelope, serverTimeout)

		if err != nil {
			// if error is not nil we need to check if its due to a connection refused, in such case we want to retry, otherwise we return with error
			if !strings.Contains(err.Error(), "connection refused") {
				t.logger.Errorf("failed to submit transaction txID = %s, due to %s", t.txID, err)
				return t.txID, nil, err
			} else {
				t.logger.Warnf("failed to submit transaction txID = %s, due to %s, will try again in %s ", t.txID, err, retryInterval)
			}
		} else {
			// if error is nil we want to check the response, if the response is 503 service unavailable we want to retry
			if response != nil {
				if response.StatusCode != http.StatusOK {
					var errMsg string
					if response.StatusCode == http.StatusServiceUnavailable {
						t.logger.Warnf("failed to submit transaction txID = %s, due to cluster leader unavailability, server returned: status: %s, will try again in %s ", t.txID, response.Status, retryInterval)
					} else {
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
				} else {
					break
				}
			} else {
				return t.txID, nil, errors.Errorf("failed to submit transaction, server's response is empty")
			}
		}

		countRetries++
		select {
		case <-time.After(retryInterval):
			retryInterval = 2 * retryInterval
			_, errReplicaSet := t.dbSession.ReplicaSet(true)
			if errReplicaSet != nil {
				return t.txID, nil, errors.Errorf("failed to submit transaction, %s", errReplicaSet.Error())
			}
			t.replicaSet = t.dbSession.replicaSet
			t.verifier = t.dbSession.verifier
			continue
		case <-retriesTimeout:
			if err != nil {
				t.logger.Errorf("failed to submit transaction after %d retries, a timeout occured after %s, last error was: %s", countRetries, retriesTimoeoutConfig, err)
				return t.txID, nil, errors.Wrapf(err, "failed to submit transaction after %d retries, a timeout occured after %s, last error was: %s", countRetries, retriesTimoeoutConfig, err)
			} else {
				if response != nil {
					if response.StatusCode == http.StatusServiceUnavailable {
						t.logger.Errorf("failed to submit transaction after %d retries, a timeout occured after %s, service is unavailable, server returned: status: %s", countRetries, retriesTimoeoutConfig, response.Status)
						return t.txID, nil, errors.Errorf("failed to submit transaction after %d retries, a timeout occured after %s, service is unavailable, server returned: status: %s", countRetries, retriesTimoeoutConfig, response.Status)
					} else {
						t.logger.Errorf("failed to submit transaction after %d retries, a timeout occured after %s, last error was: %s", countRetries, retriesTimoeoutConfig, response.Status)
						return t.txID, nil, errors.Wrapf(err, "failed to submit transaction after %d retries, a timeout occured after %s, last error was: %s", countRetries, retriesTimoeoutConfig, response.Status)
					}
				} else {
					t.logger.Errorf("failed to submit transaction after %d retries, a timeout occured after %s, server's response is empty", countRetries, retriesTimoeoutConfig)
					return t.txID, nil, errors.Wrapf(err, "failed to submit transaction after %d retries, a timeout occured after %s, server's response is empty", countRetries, retriesTimoeoutConfig)
				}
			}
		}
	}

	t.logger.Debugf("succeed to submit transaction after %d ret", countRetries)

	txResponseEnvelope := &types.TxReceiptResponseEnvelope{}
	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return t.txID, nil, err
	}

	err = protojson.Unmarshal(responseBytes, txResponseEnvelope)
	if err != nil {
		t.logger.Errorf("failed to decode json response, due to %s", err)
		return t.txID, nil, err
	}

	nodeID := txResponseEnvelope.GetResponse().GetHeader().GetNodeId()
	respBytes, err := marshal.DefaultMarshaller().Marshal(txResponseEnvelope.GetResponse())
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

func (t *commonTxContext) TxID() string {
	return t.txID
}

func (t *commonTxContext) selectReplica() (url *url.URL, err error) {
	// Pick first replica to send request to, as that is the leader.
	// TODO a cyclic retry mechanism for when the last choice failed to connect.
	for _, replica := range t.replicaSet {
		return replica.URL, nil
	}

	return nil, errors.New("empty replica set")
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

	replicaURL, err := t.selectReplica()
	if err != nil {
		return errors.WithMessage(err, "failed to select replica")
	}

	restURL := replicaURL.ResolveReference(parsedURL).String()

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

	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	if err := protojson.Unmarshal(responseBytes, res); err != nil {
		t.logger.Errorf("failed to decode json response, due to %s", err)
		return err
	}

	if _, ok := res.(ResponseEnvelop); ok {
		responsePayload, err := ResponseSelector(res.(ResponseEnvelop))
		if err != nil {
			t.logger.Errorf("failed to recognize resopnse type: %s", err)
			return err
		}
		respBytes, err := marshal.DefaultMarshaller().Marshal(responsePayload.(protoreflect.ProtoMessage))
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
	case *types.GetDataRangeResponseEnvelope:
		return envelop.(*types.GetDataRangeResponseEnvelope).GetResponse(), nil
	case *types.GetTxResponseEnvelope:
		return envelop.(*types.GetTxResponseEnvelope).GetResponse(), nil

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
