package commands

import (
	"fmt"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/pkg/logger"
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

	txProof, err := ledger.GetTransactionProof(txRcpt.Header.BaseHeader.Number, int(txRcpt.TxIndex))
	if err != nil {
		return "", errors.Wrap(err, "error getting transaction proof")
	}

	lg.Infof("Transaction proof from BCDB server: %+v", txProof)

	ok, err := txProof.Verify(txRcpt, txEnv)
	if err != nil {
		return "", errors.Wrap(err, "error verifying transaction evidence against the proof")
	}

	lg.Infof("Verified evidence for txID: %s, result: %t", txID, ok)

	return fmt.Sprintf("VerifyEvidence: txID: %s, result: %t", txID, ok), nil
}
