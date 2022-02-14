package main

import (
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/examples/manufacture/util"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"strconv"
)

func main() {
	err := createMultiSignTx("tx1.json")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("===== Tx passes from Operator3 to Operator2 =====")

	err = coSignTx("tx1.json", "tx2.json")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("===== Tx passes from Operator2 to Controller =====")

	err = coSignTxAndCommit("tx2.json")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func createMultiSignTx(fileName string) error {
	session, err := util.OpenDBSession("../util/op3_config.yml")
	if err != nil {
		return err
	}

	fmt.Println("===== Operator3 starts transaction =====")
	dataTx, err := session.DataTx()
	if err != nil {
		return err
	}

	machineJsonConfig, meta, err := dataTx.Get("machines", "machine2")
	if err != nil {
		return err
	}

	fmt.Println("===== Operator3 updates machine ACL =====")

	delete(meta.GetAccessControl().ReadWriteUsers, "operator3")
	meta.GetAccessControl().ReadWriteUsers["operator2"] = true
	err = dataTx.Put("machines", "machine2", machineJsonConfig, meta.GetAccessControl())
	if err != nil {
		return err
	}

	fmt.Println("===== Operator3 adds Operator2 and Controller as co-signer =====")
	dataTx.AddMustSignUser("operator2")
	dataTx.AddMustSignUser("controller")

	fmt.Println("===== Operator3 sign and save tx =====")
	envelope, err := dataTx.SignConstructedTxEnvelopeAndCloseTx()
	if err != nil {
		return err
	}

	util.StoreProtoAsJsonOrPanic(envelope, fileName)
	return nil
}

func coSignTx(inFileName string, outFileName string) error {
	session, err := util.OpenDBSession("../util/op2_config.yml")
	if err != nil {
		return err
	}

	fmt.Println("===== Operator2 gets transaction to sign =====")
	storedDataTxEnv := &types.DataTxEnvelope{}
	util.LoadProtoFromFileOrPanic(storedDataTxEnv, inFileName)

	fmt.Println("===== Operator2 loads transaction =====")
	dataTx, err := session.LoadDataTx(storedDataTxEnv)

	fmt.Println("===== Operator2 inspects the transaction by: =====")
	fmt.Printf("=====   reading the Must sign users: %v\n", dataTx.MustSignUsers())
	fmt.Printf("=====   reading who already signed the TX: %v\n", dataTx.SignedUsers())
	fmt.Printf("=====   reading the data operation:\n=====    read operations: %v\n=====    write operations: %v\n=====    "+
		"delete operations: %v\n", dataTx.Reads(), dataTx.Writes(), dataTx.Deletes())

	fmt.Println("===== Operator2 sign and save tx =====")
	envelope, err := dataTx.CoSignTxEnvelopeAndCloseTx()
	if err != nil {
		return err
	}

	util.StoreProtoAsJsonOrPanic(envelope, outFileName)
	return nil
}


func coSignTxAndCommit(fileName string) error {
	session, err := util.OpenDBSession("../util/controller_config.yml")
	if err != nil {
		return err
	}

	fmt.Println("===== Controller gets transaction to sign =====")
	storedDataTxEnv := &types.DataTxEnvelope{}
	util.LoadProtoFromFileOrPanic(storedDataTxEnv, fileName)

	fmt.Println("===== Controller loads transaction =====")
	dataTx, err  := session.LoadDataTx(storedDataTxEnv)

	fmt.Println("===== Controller inspects the transaction by: =====")
	fmt.Printf("=====   reading the Must sign users: %v\n", dataTx.MustSignUsers())
	fmt.Printf("=====   reading who already signed the TX: %v\n", dataTx.SignedUsers())
	fmt.Printf("=====   reading the data operation:\n=====    read operations: %v\n=====    write operations: %v\n=====    "+
		"delete operations: %v\n", dataTx.Reads(), dataTx.Writes(), dataTx.Deletes())

	fmt.Println("===== Controller signs ans commits tx =====")
	txID, receipt, err := dataTx.Commit(true)

	txEnvelope, err := dataTx.CommittedTxEnvelope()
	if err != nil {
		return err
	}
	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetResponse().GetReceipt().GetHeader().GetBaseHeader().GetNumber())))

	util.StoreProtoAsJsonOrPanic(txEnvelope, "finalTx.json")
	util.StoreProtoAsJsonOrPanic(receipt, "receipt.json")

	return util.PrintMachineConfig(session, "machine2")
}

