package commands

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type MintRequestRecord struct {
	Dealer          string
	CarRegistration string
}

const MintRequestRecordKeyPrefix = "mint-request~"

func (r *MintRequestRecord) Key() string {
	return MintRequestRecordKeyPrefix + r.RequestID()
}

func (r *MintRequestRecord) RequestID() string {
	str := r.Dealer + "_" + r.CarRegistration
	sha256Hash, _ := crypto.ComputeSHA256Hash([]byte(str))
	return base64.URLEncoding.EncodeToString(sha256Hash)
}

type CarRecord struct {
	Owner           string
	CarRegistration string
}

const CarRecordKeyPrefix = "car~"

func (r *CarRecord) Key() string {
	return CarRecordKeyPrefix + r.CarRegistration
}

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

	if err = waitForTxCommit(session, txID); err != nil {
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

	if err = waitForTxCommit(session, txID); err != nil {
		return "", err
	}

	return fmt.Sprintf("MintApprove: committed, txID: %s, Key: %s", txID, carKey), nil
}

func waitForTxCommit(session bcdb.DBSession, txID string) error {
	p, err := session.Provenance()
	if err != nil {
		return errors.Wrap(err, "error accessing provenance data")
	}
	for {
		select {
		case <-time.After(1 * time.Second):
			return errors.Errorf("timeout while waiting for transaction %s to commit to BCDB", txID)

		case <-time.After(50 * time.Millisecond):
			receipt, err := p.GetTransactionReceipt(txID)
			if err == nil {
				validationInfo := receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()]
				if validationInfo.GetFlag() == types.Flag_VALID {
					return nil
				}
				return errors.Errorf("transaction [%s] is invalid, reason %s ", txID, validationInfo.GetReasonIfInvalid())
			}
		}
	}
}
