package util

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

type MachineConfig struct {
	Id string `json:"id"`
	Param1 string `json:"param1"`
	Param2 string `json:"param2"`
	Param3 string `json:"param3"`
	Param4 string `json:"param4"`
	Param5 string `json:"param5"`
}

func PrintMachineConfig(session bcdb.DBSession, machine string) error {
	fmt.Println("===== Printing machine configuration for " + machine + " =====")

	dataTx, err := session.DataTx()
	if err != nil {
		return err
	}

	machineConfigJson, meta, err := dataTx.Get("machines", machine)
	if err != nil {
		return err
	}
	machineConfig := &MachineConfig{}
	err = json.Unmarshal(machineConfigJson, machineConfig)
	if err != nil {
		return err
	}

	fmt.Printf("=====    Machine configuration is %s\n=====    ACL is %+v\n", machineConfigJson, meta.AccessControl)

	err = dataTx.Abort()
	if err != nil {
		return err
	}
	return nil
}

