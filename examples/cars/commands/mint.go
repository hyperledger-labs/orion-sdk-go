package commands

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/sdk/pkg/bcdb"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/types"
	"time"
)

type MintRequestRecord struct {
	Dealer string
	Car    string
}

func (r *MintRequestRecord) Key() string {
	return "mint-request:" + r.RequestID()
}

func (r *MintRequestRecord) RequestID() string {
	str := r.Dealer + "_" + r.Car
	sha256Hash, _ := crypto.ComputeSHA256Hash([]byte(str))
	return base64.StdEncoding.EncodeToString(sha256Hash)
}

type CarRecord struct {
	Owner string
	Car   string
}

func (r *CarRecord) Key() string {
	return "car:" + r.Car
}

// MintRequest a dealer issues a mint-request for a car.
func MintRequest(demoDir, dealerID, carRegistration string) (out string, err error) {
	fmt.Printf("MintRequest: dealer-ID: %s, Car: %s \n", dealerID, carRegistration)

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
		Dealer: dealerID,
		Car:    carRegistration,
	}
	key := record.Key()

	dataTx, err := session.DataTx(CarDBName)
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	recordBytes, err := dataTx.Get(key)
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

	if err = waitForTxCommit(session, key, txID); err != nil {
		return "", err
	}

	return fmt.Sprintf("MintRequest committed: txID: %s, Key: %s", txID, key), nil
}

// MintApprove the dmv reviews and approves the mint-request.
// creates a car record in the database with the dealer as owner.
func MintApprove(demoDir, dmvID, mintReqID string) (out string, err error) {
	fmt.Printf("MintApprove: dmv-ID: %s, Request-ID: %s \n", dmvID, mintReqID)
	//TODO
	return "", errors.New("not implemented yet")
}

func waitForTxCommit(session bcdb.DBSession, key, txID string) error {
wait_for_commit:
	for {
		select {
		case <-time.After(1 * time.Second):
			return errors.Errorf("timeout while waiting for transaction %s to commit to BCDB", txID)

		case <-time.After(50 * time.Millisecond):
			pollTx, err := session.DataTx(CarDBName)
			if err != nil {
				return errors.Wrap(err, "error creating data transaction")
			}

			recordBytes, err := pollTx.Get(key)
			if recordBytes != nil && err == nil {
				break wait_for_commit
			}
		}
	}

	return nil
}
