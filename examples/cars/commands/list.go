package commands

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/pkg/logger"
)

func ListCar(demoDir, userID, carRegistration string, provenance bool, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("user-ID: %s, Car-Reg %s, provenance: %v", userID, carRegistration, provenance)

	serverUrl, err := loadServerUrl(demoDir)
	if err != nil {
		return "", errors.Wrap(err, "error loading server URL")
	}

	db, err := createDBInstance(demoDir, serverUrl)
	if err != nil {
		return "", errors.Wrap(err, "error creating database instance")
	}

	session, err := createUserSession(demoDir, db, userID)
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
		return fmt.Sprintf("ListCar: executed, Car key: '%s',  Car record: %s\n", carKey, "not found"), nil
	}

	carRec := &CarRecord{}
	if err = json.Unmarshal(carRecBytes, carRec); err != nil {
		return "", errors.Wrapf(err, "error unmarshaling data transaction value, key: %s", carKey)
	}

	if provenance {
		// TODO do a provenance query
	}

	return fmt.Sprintf("ListCar: executed, Car key: '%s',  Car record: %s\n", carKey, carRec), nil
}
