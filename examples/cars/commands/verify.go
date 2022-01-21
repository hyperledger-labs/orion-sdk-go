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

func VerifyEvidence(demoDir, userID, txID string, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("user-ID: %s, txID: %s", userID, txID)

	txEnv, txRcpt, err := loadTxEvidence(demoDir, txID, lg)
	if err != nil {
		return "", errors.Wrap(err, "error loading transaction evidence")
	}

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

	ledger, err := session.Ledger()
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	// Verify the transaction existence proof
	txProof, err := ledger.GetTransactionProof(txRcpt.Header.BaseHeader.Number, int(txRcpt.TxIndex))
	if err != nil {
		return "", errors.Wrap(err, "error getting transaction proof")
	}

	okTx, err := txProof.Verify(txRcpt, txEnv)
	if err != nil {
		return "", errors.Wrapf(err, "error verifying transaction evidence against the proof")
	}
	if !okTx {
		return "", errors.New("failed to verify Tx-existence")
	}

	// Verify the transaction data existence proof
	var carDataWrite *types.DataWrite
	newCarRec := &CarRecord{}
	writes := txEnv.GetPayload().GetDbOperations()[0].GetDataWrites()
	for _, dw := range writes {
		switch {
		case strings.HasPrefix(dw.GetKey(), CarRecordKeyPrefix):
			carDataWrite = dw
			if err := json.Unmarshal(dw.GetValue(), newCarRec); err != nil {
				return "", err
			}
			break
		default:
			return "", errors.Errorf("unexpected write key: %s", dw.GetKey())
		}
	}

	dataHash, err := bcdb.CalculateValueHash(CarDBName, carDataWrite.GetKey(), carDataWrite.GetValue())
	if err != nil {
		return "", errors.Wrap(err, "error calculating data hash")
	}
	stateProof, err := ledger.GetDataProof(txRcpt.Header.BaseHeader.Number, CarDBName, carDataWrite.GetKey(), false)
	if err != nil {
		return "", errors.Wrap(err, "error getting data proof")
	}
	okData, err := stateProof.Verify(dataHash, txRcpt.Header.StateMerkelTreeRootHash, false)
	if err != nil {
		return "", errors.Wrap(err, "error verifying data proof")
	}
	if !okData {
		return "", errors.New("failed to verify Data-existence")
	}

	lg.Infof("Verified evidence for txID: %s, Tx-existence result: %t, Data-existence result: %t", txID, okTx, okData)
	return fmt.Sprintf("VerifyEvidence: txID: %s, Tx-existence result: %t, Data-existence result: %t", txID, okTx, okData), nil
}
