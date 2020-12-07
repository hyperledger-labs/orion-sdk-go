package commands

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// MintRequest a dealer issues a mint-request for a car.
func MintRequest(demoDir, dealerID, carRegistration string, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("dealer-ID: %s, Car: %s", dealerID, carRegistration)

	serverUrl, err := loadServerUrl(demoDir)
	if err != nil {
		return "", errors.Wrap(err, "error loading server URL")
	}

	db, err := createDBInstance(demoDir, serverUrl)
	if err != nil {
		return "", errors.Wrap(err, "error creating database instance")
	}

	session, err := createUserSession(demoDir, db, dealerID)
	if err != nil {
		return "", errors.Wrap(err, "error creating database session")
	}

	record := &MintRequestRecord{
		Dealer:          dealerID,
		CarRegistration: carRegistration,
	}
	key := record.Key()

	dataTx, err := session.DataTx(CarDBName)
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	recordBytes, err := dataTx.Get(key)
	if err != nil {
		return "", errors.Wrapf(err, "error getting MintRequest: %s", key)
	}
	if recordBytes != nil {
		return "", errors.Errorf("MintRequest already exists: %s", key)
	}

	recordBytes, err = json.Marshal(record)

	err = dataTx.Put(key, recordBytes,
		&types.AccessControl{
			ReadUsers:      bcdb.UsersMap("dmv", dealerID),
			ReadWriteUsers: bcdb.UsersMap(dealerID),
		},
	)
	if err != nil {
		return "", errors.Wrap(err, "error during data transaction")
	}

	txID, err := dataTx.Commit()
	if err != nil {
		return "", errors.Wrap(err, "error during transaction commit")
	}

	txEnv := dataTx.TxEnvelope()
	if txEnv == nil {
		return "", errors.New("error getting transaction envelope")
	}

	txReceipt, err := waitForTxCommit(session, txID)
	if err != nil {
		return "", err
	}
	lg.Infof("MintRequest committed successfully: %s", txID)

	err = saveTxEvidence(demoDir, txID, txEnv, txReceipt, lg)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("MintRequest: committed, txID: %s, Key: %s", txID, key), nil
}

// MintApprove the dmv reviews and approves the mint-request.
// creates a car record in the database with the dealer as owner.
func MintApprove(demoDir, dmvID, mintReqRecordKey string, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("dmv-ID: %s, Record-key: %s", dmvID, mintReqRecordKey)

	serverUrl, err := loadServerUrl(demoDir)
	if err != nil {
		return "", errors.Wrap(err, "error loading server URL")
	}

	db, err := createDBInstance(demoDir, serverUrl)
	if err != nil {
		return "", errors.Wrap(err, "error creating database instance")
	}

	session, err := createUserSession(demoDir, db, dmvID)
	if err != nil {
		return "", errors.Wrap(err, "error creating database session")
	}

	mintReqRec := &MintRequestRecord{}

	dataTx, err := session.DataTx(CarDBName)
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	recordBytes, err := dataTx.Get(mintReqRecordKey)
	if err != nil {
		return "", errors.Wrapf(err, "error getting MintRequest: %s", mintReqRecordKey)
	}
	if recordBytes == nil {
		return "", errors.Errorf("MintRequest not found: %s", mintReqRecordKey)
	}

	if err = json.Unmarshal(recordBytes, mintReqRec); err != nil {
		return "", errors.Wrapf(err, "error unmarshaling data transaction value, key: %s", mintReqRecordKey)
	}

	lg.Infof("Inspecting MintRequest: %s", mintReqRec)
	reqID := mintReqRecordKey[len(MintRequestRecordKeyPrefix):]
	if reqID != mintReqRec.RequestID() {
		return "", errors.Errorf("MintRequest content compromised: expected: %s != actual: %s", reqID, mintReqRec.RequestID())
	}

	//TODO Do provenance query on dealer and respective TX
	// check dealer
	// check car

	carRecord := &CarRecord{
		Owner:           mintReqRec.Dealer,
		CarRegistration: mintReqRec.CarRegistration,
	}
	carKey := carRecord.Key()
	carRecordBytes, err := json.Marshal(carRecord)
	if err != nil {
		return "", errors.Wrapf(err, "error marshaling car record: %s", carRecord)
	}

	err = dataTx.Put(carKey, carRecordBytes,
		&types.AccessControl{
			ReadUsers:      bcdb.UsersMap(mintReqRec.Dealer),
			ReadWriteUsers: bcdb.UsersMap(dmvID),
		},
	)
	if err != nil {
		return "", errors.Wrap(err, "error during data transaction")
	}

	txID, err := dataTx.Commit()
	if err != nil {
		return "", errors.Wrap(err, "error during transaction commit")
	}

	txEnv := dataTx.TxEnvelope()
	if txEnv == nil {
		return "", errors.New("error getting transaction envelope")
	}

	txReceipt, err := waitForTxCommit(session, txID)
	if err != nil {
		return "", err
	}
	lg.Infof("MintApprove committed successfully: %s", txID)

	err = saveTxEvidence(demoDir, txID, txEnv, txReceipt, lg)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("MintApprove: committed, txID: %s, Key: %s", txID, carKey), nil
}
