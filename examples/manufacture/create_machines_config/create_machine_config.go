package main

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/examples/manufacture/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"strconv"
)

func main() {
	session, err := util.OpenDBSession("../util/controller_config.yml")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	machines := []string{"machine1", "machine2"}
	operators := []string{"operator1", "operator3"}

	fmt.Println("\n===== Creating a initial machines configurations =====")
	fmt.Println()

	err = createMachinesConfig(session, machines, operators)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, machine := range machines {
		util.PrintMachineConfig(session, machine)
	}
}


func createMachinesConfig(session bcdb.DBSession, machines []string, operators []string) error {
	dataTx, err := session.DataTx()
	if err != nil {
		return err
	}

	for i, machine := range machines {
		machineConfig := &util.MachineConfig {
			Id: machine,
			Param1: machine + "Param1Value",
			Param2: machine + "Param2Value",
			Param3: machine + "Param3Value",
			Param4: machine + "Param4Value",
			Param5: machine + "Param5Value",
		}

		acl := &types.AccessControl{
			ReadWriteUsers: map[string]bool{
				operators[i]: true,
			},
			ReadUsers: map[string]bool{
				"controller":true,
				"auditor":true,
			},
		}

		machineConfigJson, err := json.Marshal(machineConfig)
		if err != nil {
			return err
		}

		err = dataTx.Put("machines", machine, machineConfigJson, acl)
		if err != nil {
			return err
		}
	}
	txID, receiptEnv, err := dataTx.Commit(true)
	if err != nil {
		return err
	}

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receiptEnv.GetResponse().GetReceipt().GetHeader().GetBaseHeader().GetNumber())))

	txEnvelope, err := dataTx.CommittedTxEnvelope()
	if err != nil {
		return err
	}
	util.StoreProtoAsJsonOrPanic(txEnvelope, "tx.json")
	util.StoreProtoAsJsonOrPanic(receiptEnv, "receipt.json")

	return nil
}

