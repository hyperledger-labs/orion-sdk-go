package commands

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TransferTo(demoDir, ownerID, buyerID, carRegistration string, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("owner-ID: %s, buyer-ID: %s, Car-Reg", ownerID, buyerID, carRegistration)

	serverUrl, err := loadServerUrl(demoDir)
	if err != nil {
		return "", errors.Wrap(err, "error loading server URL")
	}

	db, err := createDBInstance(demoDir, serverUrl)
	if err != nil {
		return "", errors.Wrap(err, "error creating database instance")
	}

	session, err := createUserSession(demoDir, db, ownerID)
	if err != nil {
		return "", errors.Wrap(err, "error creating database session")
	}

	dataTx, err := session.DataTx(CarDBName)
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	carKey := CarRecordKeyPrefix + carRegistration
	carRecBytes, err := dataTx.Get(carKey)
	if err != nil {
		return "", errors.Wrapf(err, "error getting car record, key: %s", carKey)
	}
	if len(carRecBytes) == 0 {
		return "", errors.Wrapf(err, "car record does not exist, key: %s", carKey)
	}

	carRec := &CarRecord{}
	if err = json.Unmarshal(carRecBytes, carRec); err != nil {
		return "", errors.Wrapf(err, "error unmarshaling data transaction value, key: %s", carKey)
	}

	if carRec.Owner != ownerID {
		return "", errors.Errorf("car has different owner")
	}

	ttRecord := &TransferToRecord{
		Owner:           ownerID,
		Buyer:           buyerID,
		CarRegistration: carRegistration,
	}
	ttRecBytes, err := json.Marshal(ttRecord)
	ttRecKey := ttRecord.Key()
	err = dataTx.Put(ttRecKey, ttRecBytes,
		&types.AccessControl{
			ReadUsers:      bcdb.UsersMap("dmv", buyerID),
			ReadWriteUsers: bcdb.UsersMap(ownerID),
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
	lg.Infof("TransferTo committed successfully: %s", txID)

	err = saveTxEvidence(demoDir, txID, txEnv, txReceipt, lg)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("TransferTo: committed, txID: %s, Key: %s", txID, ttRecKey), nil
}

func TransferReceive(demoDir, buyerID, carRegistration, transferToRecordKey string, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("buyer-ID: %s, Car-Reg: %s, Rec-Key", buyerID, carRegistration, transferToRecordKey)

	serverUrl, err := loadServerUrl(demoDir)
	if err != nil {
		return "", errors.Wrap(err, "error loading server URL")
	}

	db, err := createDBInstance(demoDir, serverUrl)
	if err != nil {
		return "", errors.Wrap(err, "error creating database instance")
	}

	session, err := createUserSession(demoDir, db, buyerID)
	if err != nil {
		return "", errors.Wrap(err, "error creating database session")
	}

	dataTx, err := session.DataTx(CarDBName)
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	ttRec := &TransferToRecord{}
	recordBytes, err := dataTx.Get(transferToRecordKey)
	if err != nil {
		return "", errors.Wrapf(err, "error getting TransferTo : %s", transferToRecordKey)
	}
	if recordBytes == nil {
		return "", errors.Errorf("TransferTo not found: %s", transferToRecordKey)
	}

	if err = json.Unmarshal(recordBytes, ttRec); err != nil {
		return "", errors.Wrapf(err, "error unmarshaling data transaction value, key: %s", transferToRecordKey)
	}

	lg.Infof("Inspecting TransferTo: %s", ttRec)
	reqID := transferToRecordKey[len(TransferToRecordKeyPrefix):]
	if reqID != ttRec.RequestID() {
		return "", errors.Errorf("TransferTo content compromised: expected: %s != actual: %s", reqID, ttRec.RequestID())
	}
	if buyerID != ttRec.Buyer {
		return "", errors.New("TransferTo has different buyer")
	}
	if carRegistration != ttRec.CarRegistration {
		return "", errors.New("TransferTo has different car")
	}

	// TODO do provenance of owner and respective Tx

	trRec := &TransferReceiveRecord{
		Buyer:               buyerID,
		CarRegistration:     carRegistration,
		TransferToRecordKey: transferToRecordKey,
	}
	trRecBytes, err := json.Marshal(trRec)
	if err != nil {
		return "", errors.Wrapf(err, "error marshaling transfer-receive record: %s", trRec)
	}
	trRecKey := trRec.Key()

	err = dataTx.Put(trRecKey, trRecBytes, &types.AccessControl{
		ReadUsers:      bcdb.UsersMap("dmv", ttRec.Owner),
		ReadWriteUsers: bcdb.UsersMap(buyerID),
	})
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
	lg.Infof("TransferReceive committed successfully: %s", txID)

	err = saveTxEvidence(demoDir, txID, txEnv, txReceipt, lg)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("TransferReceive: committed, txID: %s, Key: %s", txID, trRecKey), nil
}

func Transfer(demoDir, dmvID, transferToRecordKey, transferRcvRecordKey string, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("dmv-ID: %s, TrnsTo-Key: %s, TrnsRcv-Key: %s", dmvID, transferToRecordKey, transferRcvRecordKey)
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

	dataTx, err := session.DataTx(CarDBName)
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	ttRec := &TransferToRecord{}
	recordBytes, err := dataTx.Get(transferToRecordKey)
	if err != nil {
		return "", errors.Wrapf(err, "error getting TransferTo : %s", transferToRecordKey)
	}
	if recordBytes == nil {
		return "", errors.Errorf("TransferTo not found: %s", transferToRecordKey)
	}
	if err = json.Unmarshal(recordBytes, ttRec); err != nil {
		return "", errors.Wrapf(err, "error unmarshaling data transaction value, key: %s", transferToRecordKey)
	}

	trRec := &TransferReceiveRecord{}
	recordBytes, err = dataTx.Get(transferRcvRecordKey)
	if err != nil {
		return "", errors.Wrapf(err, "error getting TransferTo : %s", transferToRecordKey)
	}
	if recordBytes == nil {
		return "", errors.Errorf("TransferReceive not found: %s", transferToRecordKey)
	}
	if err = json.Unmarshal(recordBytes, trRec); err != nil {
		return "", errors.Wrapf(err, "error unmarshaling data transaction value, key: %s", transferRcvRecordKey)
	}

	carRec := &CarRecord{}
	carKey := CarRecordKeyPrefix + ttRec.CarRegistration
	recordBytes, err = dataTx.Get(carKey)
	if err != nil {
		return "", errors.Wrapf(err, "error getting TransferTo : %s", carKey)
	}
	if recordBytes == nil {
		return "", errors.Errorf("Car not found: %s", carKey)
	}
	if err = json.Unmarshal(recordBytes, carRec); err != nil {
		return "", errors.Wrapf(err, "error unmarshaling data transaction value, key: %s", carKey)
	}

	if err = validateTransfer(carRec, ttRec, trRec); err != nil {
		return "", errors.WithMessage(err, "transfer validation failed")
	}

	carRec.Owner = ttRec.Buyer
	recordBytes, err = json.Marshal(carRec)

	err = dataTx.Put(carKey, recordBytes,
		&types.AccessControl{
			ReadUsers:      bcdb.UsersMap(ttRec.Buyer),
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
	lg.Infof("Transfer committed successfully: %s", txID)

	err = saveTxEvidence(demoDir, txID, txEnv, txReceipt, lg)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Transfer: committed, txID: %s, Key: %s", txID, carKey), nil
}

// Any validation, including provenance
func validateTransfer(carRec *CarRecord, ttRec *TransferToRecord, trRec *TransferReceiveRecord) error {
	if ttRec.Buyer != trRec.Buyer {
		return errors.New("Records have different buyers")
	}
	if ttRec.CarRegistration != trRec.CarRegistration {
		return errors.New("Records have different cars")
	}
	if carRec.Owner != ttRec.Owner {
		return errors.New("Car has different owner")
	}

	return nil
}
