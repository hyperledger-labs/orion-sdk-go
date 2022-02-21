// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package commands

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

func Transfer(demoDir, dmvID, seller, buyer, carReg string, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("dmv-ID: %s, seller-ID: %s, buyer-ID: %s, Car-Registration: %s, TrnsRcv-Key: %s", dmvID, seller, buyer, carReg)
	serverUrl, err := loadServerUrl(demoDir)
	if err != nil {
		return "", errors.Wrap(err, "error loading server URL")
	}

	db, err := createDBInstance(demoDir, serverUrl)
	if err != nil {
		return "", errors.Wrap(err, "error creating database instance")
	}

	// === Seller prepares the transaction an co-signs ===
	sessionSeller, err := createUserSession(demoDir, db, seller)
	if err != nil {
		return "", errors.Wrapf(err, "error creating database session with seller: %s", seller)
	}

	dataTx, err := sessionSeller.DataTx()
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	dataTx.AddMustSignUser(buyer)

	carRec := &CarRecord{}
	carKey := CarRecordKeyPrefix + carReg
	recordBytes, _, err := dataTx.Get(CarDBName, carKey)
	if err != nil {
		return "", errors.Wrapf(err, "error getting Car : %s", carKey)
	}
	if recordBytes == nil {
		return "", errors.Errorf("Car not found: %s", carKey)
	}
	if err = json.Unmarshal(recordBytes, carRec); err != nil {
		return "", errors.Wrapf(err, "error unmarshaling data transaction value, key: %s", carKey)
	}

	carRec.Owner = buyer
	recordBytes, err = json.Marshal(carRec)

	err = dataTx.Put(CarDBName, carKey, recordBytes,
		&types.AccessControl{
			ReadWriteUsers:     usersMap(dmvID, buyer),
			SignPolicyForWrite: types.AccessControl_ALL,
		},
	)
	if err != nil {
		return "", errors.Wrap(err, "error during data transaction")
	}

	txEnv, err := dataTx.SignConstructedTxEnvelopeAndCloseTx()
	if err != nil {
		return "", errors.Wrapf(err, "error during transaction envelope signing by seller: %s", seller)
	}

	// === buyer inspects transaction and co-signs ===
	sessionBuyer, err := createUserSession(demoDir, db, buyer)
	if err != nil {
		return "", errors.Wrapf(err, "error creating database session with buyer: %s", buyer)
	}

	buyerTx, err := sessionBuyer.LoadDataTx(txEnv.(*types.DataTxEnvelope))
	if err != nil {
		return "", errors.Wrapf(err, "error loading tx envelope by buyer: %s", buyer)
	}

	if err = buyerValidateTransfer(buyer, seller, dmvID, carReg, buyerTx); err != nil {
		return "", errors.WithMessage(err, "buyer transfer validation failed")
	}

	txEnv, err = buyerTx.CoSignTxEnvelopeAndCloseTx()
	if err != nil {
		return "", errors.Wrapf(err, "error during transaction envelope signing by buyer: %s", buyer)
	}

	// dmv inspects transaction, co-signs, and commits
	sessionDMV, err := createUserSession(demoDir, db, dmvID)
	if err != nil {
		return "", errors.Wrapf(err, "error creating database session with DMV-ID: %s", dmvID)
	}

	dmvTx, err := sessionDMV.LoadDataTx(txEnv.(*types.DataTxEnvelope))
	if err != nil {
		return "", errors.Wrapf(err, "error loading tx envelope by dmv: %s", dmvID)
	}

	if err = dmvValidateTransfer(dmvID, sessionDMV, dmvTx); err != nil {
		return "", errors.WithMessage(err, "dmv transfer validation failed")
	}

	txID, receiptEnv, err := dmvTx.Commit(true)
	if err != nil {
		return "", errors.Wrap(err, "error committing transaction")
	}

	txEnv, err = dmvTx.CommittedTxEnvelope()
	if err != nil {
		return "", errors.New("error getting transaction envelope")
	}

	err = saveTxEvidence(demoDir, txID, txEnv, receiptEnv, lg)
	if err != nil {
		return "", err
	}

	lg.Infof("Transfer committed successfully: %s", txID)

	return fmt.Sprintf("Transfer: committed, txID: %s, Key: %s", txID, carKey), nil
}

func buyerValidateTransfer(buyerID, sellerID, dmvID, carReg string, buyerTx bcdb.LoadedDataTxContext) error {
	reads := buyerTx.Reads()[CarDBName]
	for _, dr := range reads {
		switch {
		case strings.HasPrefix(dr.GetKey(), CarRecordKeyPrefix):
			if CarRecordKeyPrefix+carReg != dr.GetKey() {
				return errors.New("not the car I wanted!")
			}
		default:
			return errors.Errorf("unexpected read key: %s", dr.GetKey())
		}
	}

	newCarRec := &CarRecord{}
	var newCarACL *types.AccessControl
	writes := buyerTx.Writes()[CarDBName]
	for _, dw := range writes {
		switch {
		case strings.HasPrefix(dw.GetKey(), CarRecordKeyPrefix):
			if err := json.Unmarshal(dw.GetValue(), newCarRec); err != nil {
				return err
			}
			newCarACL = dw.Acl
		default:
			return errors.Errorf("unexpected write key: %s", dw.GetKey())
		}
	}

	mustSignUsers := buyerTx.MustSignUsers()
	signedUsers := buyerTx.SignedUsers()

	hasSeller := false
	hasBuyer := false
	for _, u := range mustSignUsers {
		if u == sellerID {
			hasSeller = true
		}
		if u == newCarRec.Owner {
			hasBuyer = true
		}
	}
	if !hasBuyer {
		return errors.New("Car buyer is not in must-sign-users")
	}
	if !hasSeller {
		return errors.New("Car seller is not in must-sign-users")
	}

	hasSeller = false
	for _, u := range signedUsers {
		if u == sellerID {
			hasSeller = true
		}
	}
	if !hasSeller {
		return errors.New("Car seller is not in signed-users")
	}

	//validate the writes
	if newCarRec.Owner != buyerID {
		return errors.New("Car new owner is not the buyer")
	}
	if !newCarACL.ReadWriteUsers[newCarRec.Owner] || !newCarACL.ReadWriteUsers[dmvID] ||
		len(newCarACL.ReadWriteUsers) != 2 || len(newCarACL.ReadUsers) != 0 ||
		newCarACL.SignPolicyForWrite != types.AccessControl_ALL {
		return errors.New("Car new ACL is wrong")
	}

	return nil
}

// Any validation, including provenance
func dmvValidateTransfer(dmvID string, sessionDMV bcdb.DBSession, dmvTx bcdb.LoadedDataTxContext) error {
	carRec := &CarRecord{}

	tx, err := sessionDMV.DataTx()
	if err != nil {
		return err
	}

	reads := dmvTx.Reads()[CarDBName]
	for _, dr := range reads {
		recordBytes, _, err := tx.Get(CarDBName, dr.GetKey())
		if err != nil {
			return err
		}

		switch {
		case strings.HasPrefix(dr.GetKey(), CarRecordKeyPrefix):
			if err = json.Unmarshal(recordBytes, carRec); err != nil {
				return err
			}
		default:
			return errors.Errorf("unexpected read key: %s", dr.GetKey())
		}
	}

	newCarRec := &CarRecord{}
	var newCarACL *types.AccessControl
	writes := dmvTx.Writes()[CarDBName]
	for _, dw := range writes {
		switch {
		case strings.HasPrefix(dw.GetKey(), CarRecordKeyPrefix):
			if err = json.Unmarshal(dw.GetValue(), newCarRec); err != nil {
				return err
			}
			newCarACL = dw.Acl
		default:
			return errors.Errorf("unexpected write key: %s", dw.GetKey())
		}
	}

	mustSignUsers := dmvTx.MustSignUsers()
	signedUsers := dmvTx.SignedUsers()

	hasSeller := false
	hasBuyer := false
	for _, u := range mustSignUsers {
		if u == carRec.Owner {
			hasSeller = true
		}
		if u == newCarRec.Owner {
			hasBuyer = true
		}
	}
	if !hasBuyer {
		return errors.New("Car buyer is not in must-sign-users")
	}
	if !hasSeller {
		return errors.New("Car seller is not in must-sign-users")
	}

	hasSeller = false
	hasBuyer = false
	for _, u := range signedUsers {
		if u == carRec.Owner {
			hasSeller = true
		}
		if u == newCarRec.Owner {
			hasBuyer = true
		}
	}
	if !hasBuyer {
		return errors.New("Car buyer is not in signed-users")
	}
	if !hasSeller {
		return errors.New("Car seller is not in signed-users")
	}

	//validate the writes
	if newCarRec.CarRegistration != carRec.CarRegistration {
		return errors.New("Car registration changed")
	}

	if !newCarACL.ReadWriteUsers[newCarRec.Owner] || !newCarACL.ReadWriteUsers[dmvID] ||
		len(newCarACL.ReadWriteUsers) != 2 || len(newCarACL.ReadUsers) != 0 ||
		newCarACL.SignPolicyForWrite != types.AccessControl_ALL {
		return errors.New("Car new ACL is wrong")
	}

	// validate the seller, buyer, car, are not on a black list, etc.
	return nil
}
