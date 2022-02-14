package main

import (
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/examples/manufacture/util"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

func main() {
	session, err := util.OpenDBSession("../util/auditor_config.yml")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("===== Auditor gets tx envelop stored by controller ======")
	txEnvelop := &types.DataTxEnvelope{}
	util.LoadProtoFromFileOrPanic(txEnvelop, "../change_machine_config/finalTx.json")
	fmt.Println("===== Auditor gets tx receipt stored by controller ======")
	receiptEnvelop := &types.TxReceiptResponseEnvelope{}
	util.LoadProtoFromFileOrPanic(receiptEnvelop, "../change_machine_config/receipt.json")

	fmt.Println("===== Auditor validates both data items ======")


	ledger, err := session.Ledger()

	fmt.Println("===== Auditor gets last known block from ledger ======")
	lastBlock, err := ledger.GetLastBlockHeader()
	fmt.Println("===== Auditor asks Orion for tx proof and verifies it ======")
	txProof, ledgerPath, err := ledger.GetFullTxProofAndVerify(receiptEnvelop.GetResponse().GetReceipt(), lastBlock, txEnvelop)
	if err != nil {
		fmt.Println("!!!!!!!!!!! TX VERIFICATION FAILED: " + err.Error() + " !!!!!!!!!!!!!")
		return
	}

	util.StoreProtoAsJsonOrPanic(txProof, "txProof.json")
	util.StoreProtoAsJsonOrPanic(ledgerPath, "ledgerPath.json")
}
