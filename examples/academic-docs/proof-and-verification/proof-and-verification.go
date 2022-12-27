// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/hyperledger-labs/orion-sdk-go/examples/academic-docs/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

func main() {
	db, err := util.CreateConnection()
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}

	session, err := util.OpenSession(db, "bob")
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}

	lgr, err := session.Ledger()
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}

	jsonReceipt, err := ioutil.ReadFile("../prepare-data/person0.txt")
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}

	txReceipt := &types.TxReceipt{}
	if err = json.Unmarshal(jsonReceipt, txReceipt); err != nil {
		fmt.Errorf(err.Error())
		return
	}

	ms := `{"roll_number":0,"physics":100,"chemistry":100,"mathematics":100,"pass":true,"year":2010}`
	markSheetHash, err := bcdb.CalculateValueHash("db1", "person0", []byte(ms))
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}

	proof, err := lgr.GetDataProof(txReceipt.Header.BaseHeader.Number, "db1", "person0", false)
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}

	fmt.Println(ms)
	isSuccess, err := proof.Verify(markSheetHash, txReceipt.Header.StateMerkleTreeRootHash, false)
	if isSuccess {
		fmt.Println("This marksheet is valid")
	} else {
		fmt.Println("This marksheet is invalid")
	}

	ms = `{"roll_number":0,"physics":90,"chemistry":100,"mathematics":100,"pass":true,"year":2010}`
	markSheetHash, err = bcdb.CalculateValueHash("db1", "person0", []byte(ms))
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}

	fmt.Println(ms)
	isSuccess, err = proof.Verify(markSheetHash, txReceipt.Header.StateMerkleTreeRootHash, false)
	if isSuccess {
		fmt.Println("marksheet is valid")
	} else {
		fmt.Println("marksheet is invalid")
	}
}
