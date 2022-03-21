package main

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/examples/manufacture/util"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"strconv"
)

func main() {
	err := createMultiSignTx("tx.json")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("===== Tx passes from Operator1 to Controller =====")

	err = coSignTxAndCommit("tx.json")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func createMultiSignTx(fileName string) error {
	session, err := util.OpenDBSession("../util/op1_config.yml")
	if err != nil {
		return err
	}

	fmt.Println("===== Operator1 starts transaction =====")
	dataTx, err := session.DataTx()
	if err != nil {
		return err
	}

	machineJsonConfig, meta, err := dataTx.Get("machines", "machine1")
	if err != nil {
		return err
	}

	machineConfig := &util.MachineConfig{}
	err = json.Unmarshal(machineJsonConfig, machineConfig)
	if err != nil {
		return err
	}

	fmt.Println("===== Operator1 updates machine config =====")
	machineConfig.Param1 = machineConfig.Param1 + "Updated"
	machineConfig.Param3 = machineConfig.Param3 + "Updated"
	machineJsonConfig, err = json.Marshal(machineConfig)
	if err != nil {
		return err
	}

	err = dataTx.Put("machines", "machine1", machineJsonConfig, meta.GetAccessControl())
	if err != nil {
		return err
	}

	fmt.Println("===== Operator1 adds Controller as co-signer =====")
	dataTx.AddMustSignUser("controller")

	fmt.Println("===== Operator1 sign and save tx =====")
	envelope, err := dataTx.SignConstructedTxEnvelopeAndCloseTx()
	if err != nil {
		return err
	}

	util.StoreProtoAsJsonOrPanic(envelope, fileName)
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

	fmt.Println("===== Controller signs and commits tx =====")
	txID, receipt, err := dataTx.Commit(true)

	txEnvelope, err := dataTx.CommittedTxEnvelope()
	if err != nil {
		return err
	}
	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetResponse().GetReceipt().GetHeader().GetBaseHeader().GetNumber())))

	util.StoreProtoAsJsonOrPanic(txEnvelope, "finalTx.json")
	util.StoreProtoAsJsonOrPanic(receipt, "receipt.json")

	return util.PrintMachineConfig(session, "machine1")
}

