// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/hyperledger-labs/orion-sdk-go/examples/academic-docs/util"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type MarkSheet struct {
	RollNumber  int  `json:"roll_number"`
	Physics     int  `json:"physics"`
	Chemistry   int  `json:"chemistry"`
	Mathematics int  `json:"mathematics"`
	Pass        bool `json:"pass"`
	Year        int  `json:"year"`
}

func main() {
	db, err := util.CreateConnection()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	session, err := util.OpenSession(db, "alice")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	acl := &types.AccessControl{
		ReadWriteUsers: map[string]bool{
			"alice": true,
		},
	}

	phy := []int{100, 40, 25}
	chem := []int{100, 40, 25}
	math := []int{100, 40, 25}

	for i := 0; i < len(phy); i++ {
		tx, err := session.DataTx()
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		m, err := createMarkSheet(i, phy[i], chem[i], math[i], true, 2010)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		fmt.Println("storing " + string(m) + " for persion" + strconv.Itoa(i))
		if err = tx.Put("db1", "person"+strconv.Itoa(i), m, acl); err != nil {
			fmt.Println(err.Error())
			return
		}
		_, receiptEnv, err := tx.Commit(true)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		jsonTxReceipt, err := json.Marshal(receiptEnv.GetResponse().GetReceipt())
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		if err = ioutil.WriteFile("person"+strconv.Itoa(i)+".txt", jsonTxReceipt, 0644); err != nil {
			fmt.Println(err.Error())
			return
		}
	}
}

func createMarkSheet(r int, p, c, m int, pass bool, year int) ([]byte, error) {
	ms := &MarkSheet{
		RollNumber:  r,
		Physics:     p,
		Chemistry:   c,
		Mathematics: m,
		Pass:        pass,
		Year:        year,
	}

	return json.Marshal(ms)
}
