package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/protos/types"
)

type DBServer struct {
	router     *mux.Router
	qs         *queryProcessor
	ts         *transactionProcessor
	mockserver *mockdbserver
}

type ErrorResponse struct {
	Error string `json:"error,omitempty"`
}

func NewDBServer() (*DBServer, error) {
	rs := &DBServer{}
	var err error

	rs.mockserver = restartMockServer()

	if rs.qs, err = NewQueryServer(rs.mockserver); err != nil {
		return nil, errors.Wrap(err, "failed to initiate query service")
	}
	if rs.ts, err = NewTransactionServer(rs.mockserver); err != nil {
		return nil, errors.Wrap(err, "failed to initiate transaction service")
	}

	rs.router = mux.NewRouter()
	rs.router.HandleFunc("/db/{dbname}/state/{key}", rs.handleDataQuery).Methods(http.MethodGet)
	rs.router.HandleFunc("/db/{dbname}", rs.handleStatusQuery).Methods(http.MethodGet)
	rs.router.HandleFunc("/tx", rs.handleTransactionSubmit).Methods(http.MethodPost)
	return rs, nil
}

func (rs *DBServer) handleStatusQuery(w http.ResponseWriter, r *http.Request) {
	user, signature, err := validateAndParseQueryHeader(r)
	if err != nil {
		composeJSONResponse(w, http.StatusBadRequest, &ErrorResponse{Error: err.Error()})
		return
	}

	params := mux.Vars(r)
	dbname, ok := params["dbname"]
	if !ok {
		composeJSONResponse(w, http.StatusBadRequest, &ErrorResponse{Error: "query error - no dbname provided"})
		return
	}
	dbQueryEnvelope := &types.GetStatusQueryEnvelope{
		Payload: &types.GetStatusQuery{
			UserID: user,
			DBName: dbname,
		},
		Signature: signature,
	}

	statusEnvelope, err := rs.qs.GetStatus(context.Background(), dbQueryEnvelope)
	if err != nil {
		composeJSONResponse(w, http.StatusInternalServerError, &ErrorResponse{Error: fmt.Sprintf("error while processing %v, %v", dbQueryEnvelope, err)})
		return
	}
	composeJSONResponse(w, http.StatusOK, statusEnvelope)
}

func (rs *DBServer) handleDataQuery(w http.ResponseWriter, r *http.Request) {
	user, signature, err := validateAndParseQueryHeader(r)
	if err != nil {
		composeJSONResponse(w, http.StatusBadRequest, &ErrorResponse{Error: err.Error()})
		return
	}

	params := mux.Vars(r)
	dbname, ok := params["dbname"]
	if !ok {
		composeJSONResponse(w, http.StatusBadRequest, &ErrorResponse{Error: "query error - no dbname provided"})
		return
	}
	key, ok := params["key"]
	if !ok {
		composeJSONResponse(w, http.StatusBadRequest, &ErrorResponse{Error: "query error - no key provided"})
		return
	}
	dataQueryEnvelope := &types.GetStateQueryEnvelope{
		Payload: &types.GetStateQuery{
			UserID: user,
			DBName: dbname,
			Key:    key,
		},
		Signature: signature,
	}
	valueEnvelope, err := rs.qs.GetState(context.Background(), dataQueryEnvelope)

	if err != nil {
		composeJSONResponse(w, http.StatusInternalServerError, &ErrorResponse{Error: fmt.Sprintf("error while processing %v, %v", dataQueryEnvelope, err)})
		return
	}
	composeJSONResponse(w, http.StatusOK, valueEnvelope)
}

func (rs *DBServer) handleTransactionSubmit(w http.ResponseWriter, r *http.Request) {
	tx := new(types.DataTxEnvelope)
	err := json.NewDecoder(r.Body).Decode(tx)
	if err != nil {
		composeJSONResponse(w, http.StatusBadRequest, &ErrorResponse{Error: err.Error()})
		return
	}
	err = rs.ts.SubmitTransaction(context.Background(), tx)
	if err != nil {
		composeJSONResponse(w, http.StatusInternalServerError, &ErrorResponse{Error: err.Error()})
		return
	}
	composeJSONResponse(w, http.StatusOK, new(empty.Empty))
}

func composeJSONResponse(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func validateAndParseQueryHeader(r *http.Request) (string, []byte, error) {
	user := r.Header.Get(constants.UserHeader)
	if user == "" {
		return "", nil, errors.New("empty user")
	}
	signature := r.Header.Get(constants.SignatureHeader)
	if signature == "" {
		return "", nil, errors.New("empty signature")
	}
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return "", nil, errors.New("wrongly encoded signature")
	}
	return user, signatureBytes, nil
}
