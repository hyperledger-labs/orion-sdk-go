// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package commands

import (
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/jsonpb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
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

	dataTx, err := session.DataTx()
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	carKey := CarRecordKeyPrefix + carRegistration
	carRecBytes, _, err := dataTx.Get(CarDBName, carKey)
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
		adminSession, err := createUserSession(demoDir, db, "admin")
		if err != nil {
			return "", errors.Wrap(err, "error creating admin session with the database")
		}
		provQ, err := adminSession.Provenance()
		if err != nil {
			return "", errors.Wrap(err, "error creating provenance query")
		}
		histData, err := provQ.GetHistoricalData(CarDBName, carKey)
		if err != nil {
			return "", errors.Wrap(err, "error getting historical data")
		}

		var provReport = "\nHistorical data:\n"
		for i, histItem := range histData {
			value := histItem.GetValue()
			carRecHist := &CarRecord{}
			if err = json.Unmarshal(value, carRecHist); err != nil {
				return "", errors.Wrapf(err, "error unmarshaling historical data, key: %s, index: %d", carKey, i)
			}

			provReport = fmt.Sprintf("%s\nRecord number: %d\n", provReport, i)
			provReport = fmt.Sprintf("%sCar: %s\n", provReport, carRecHist)
			m := &jsonpb.Marshaler{EmitDefaults: true}
			meta, err := m.MarshalToString(histItem.Metadata)
			if err != nil {
				return "", errors.Wrapf(err, "error unmarshaling historical metadata data, key: %s, index: %d", carKey, i)
			}
			provReport = fmt.Sprintf("%sMetadata: %s\n", provReport, meta)
		}

		return fmt.Sprintf("ListCar: executed, Car key: '%s',  Car provenance: %s\n", carKey, provReport), nil
	}

	return fmt.Sprintf("ListCar: executed, Car key: '%s',  Car record: %s\n", carKey, carRec), nil
}
