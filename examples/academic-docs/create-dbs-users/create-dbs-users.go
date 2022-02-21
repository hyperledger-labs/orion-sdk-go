// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/hyperledger-labs/orion-sdk-go/examples/academic-docs/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

func main() {
	fmt.Println("\n===Creating a Connection to Orion===")
	fmt.Println()
	db, err := util.CreateConnection()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("\n===Creating a Session to Orion===")
	fmt.Println()
	session, err := util.OpenSession(db, "admin")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("\n===Creating a Database db1===")
	fmt.Println()
	if err = createDBs(session); err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("\n===Creating Users alice and bob===")
	fmt.Println()
	if err = createUsers(session); err != nil {
		fmt.Println(err.Error())
		return
	}
}

func createDBs(s bcdb.DBSession) error {
	index := map[string]types.IndexAttributeType{
		"pass":        types.IndexAttributeType_BOOLEAN,
		"roll_number": types.IndexAttributeType_NUMBER,
		"physics":     types.IndexAttributeType_NUMBER,
		"chemistry":   types.IndexAttributeType_NUMBER,
		"mathematics": types.IndexAttributeType_NUMBER,
	}

	dbtx, err := s.DBsTx()
	if err != nil {
		return err
	}

	exist, err := dbtx.Exists("db1")
	if err != nil {
		return err
	}
	if exist {
		return errors.New("database db1 already exist")
	}

	if err := dbtx.CreateDB("db1", index); err != nil {
		return err
	}

	txID, receiptEnv, err := dbtx.Commit(true)
	if err != nil {
		return err
	}

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receiptEnv.GetResponse().GetReceipt().GetHeader().GetBaseHeader().GetNumber())))
	return nil
}

func createUsers(s bcdb.DBSession) error {
	alicePemUserCert, err := ioutil.ReadFile("../crypto/alice/alice.pem")
	if err != nil {
		return err
	}

	aliceCertBlock, _ := pem.Decode(alicePemUserCert)

	alice := &types.User{
		Id:          "alice",
		Certificate: aliceCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				"db1": types.Privilege_ReadWrite,
			},
		},
	}

	bobPemUserCert, err := ioutil.ReadFile("../crypto/bob/bob.pem")
	if err != nil {
		return err
	}

	bobCertBlock, _ := pem.Decode(bobPemUserCert)

	bob := &types.User{
		Id:          "bob",
		Certificate: bobCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				"db1": types.Privilege_Read,
			},
		},
	}

	tx, err := s.UsersTx()
	if err != nil {
		return err
	}

	if err = tx.PutUser(alice, nil); err != nil {
		return err
	}

	if err = tx.PutUser(bob, nil); err != nil {
		return err
	}

	txID, receiptEnv, err := tx.Commit(true)
	if err != nil {
		return err
	}

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receiptEnv.GetResponse().GetReceipt().GetHeader().GetBaseHeader().GetNumber())))
	return nil
}
