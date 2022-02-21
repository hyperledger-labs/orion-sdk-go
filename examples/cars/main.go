// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/hyperledger-labs/orion-sdk-go/examples/cars/commands"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"gopkg.in/alecthomas/kingpin.v2"
)

const demoDirEnvar = "CARS_DEMO_DIR"

func main() {
	kingpin.Version("0.0.1")

	c := &logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "cars-demo",
	}
	lg, err := logger.New(c)

	output, exit, err := executeForArgs(os.Args[1:], lg)
	if err != nil {
		kingpin.Fatalf("parsing arguments: %s. Try --help", err)
	}
	fmt.Printf("\n%s\n", output)
	os.Exit(exit)
}

func executeForArgs(args []string, lg *logger.SugarLogger) (output string, exit int, err error) {
	//
	// command line flags
	//
	app := kingpin.New("cars", "Car registry demo")
	demoDir := app.Flag("demo-dir",
		fmt.Sprintf("Path to the folder that will contain all the material for the demo. If missing, taken from envar: %s", demoDirEnvar)).
		Short('d').
		Envar(demoDirEnvar).
		Required().
		String()

	generate := app.Command("generate", "Generate crypto material for all roles: admin, dmv, dealer, alice, bob; and the BCDB server.")

	init := app.Command("init", "Initialize the server, load it with users, create databases.")

	mintRequest := app.Command("mint-request", "Issue a request to mint a car by a dealer.")
	mrUserID := mintRequest.Flag("user", "dealer user ID").Short('u').Required().String()
	mrCarRegistry := mintRequest.Flag("car", "car registration plate").Short('c').Required().String()

	mintApprove := app.Command("mint-approve", "Approve a request to mint a car, create car record.")
	maUserID := mintApprove.Flag("user", "DMV user ID").Short('u').Required().String()
	maRequestRecKey := mintApprove.Flag("mint-request-key", "mint-request record key").Short('k').Required().String()

	transferOwnership := app.Command("transfer", "Transfer car ownership between a seller and a buyer")
	toUserID := transferOwnership.Flag("user", "dmv user ID").Short('u').Required().String()
	toSellerID := transferOwnership.Flag("seller", "seller user ID").Short('s').Required().String()
	toBuyerID := transferOwnership.Flag("buyer", "buyer user ID").Short('b').Required().String()
	toCarRegistry := transferOwnership.Flag("car", "car registration plate").Short('c').Required().String()

	listCar := app.Command("list-car", "List a car record")
	lsCarUserID := listCar.Flag("user", "user ID").Short('u').Required().String()
	lsCarCar := listCar.Flag("car", "car registration plate").Short('c').Required().String()
	lsCarProv := listCar.Flag("provenance", "complete provenance info on a car.").Short('p').Bool()

	verifyTx := app.Command("verify-tx", "Verify Tx evidence (Tx Receipt & Envelope) against a BCDB Proof")
	verUserID := verifyTx.Flag("user", "user ID").Short('u').Required().String()
	verTxID := verifyTx.Flag("txid", "user ID").Short('t').Required().String()

	command := kingpin.MustParse(app.Parse(args))

	//
	// call the underlying implementations
	//
	var resp *http.Response

	switch command {
	case generate.FullCommand():
		err := commands.Generate(*demoDir)
		if err != nil {
			fmt.Println(command)
			return errorOutput(err), 1, nil
		}

		return "Generated demo materials to: " + *demoDir, 0, nil

	case init.FullCommand():
		err := commands.Init(*demoDir, lg)
		if err != nil {
			fmt.Println(command)
			return errorOutput(err), 1, nil
		}
		return "Initialized server from: " + *demoDir, 0, nil

	case mintRequest.FullCommand():
		out, err := commands.MintRequest(*demoDir, *mrUserID, *mrCarRegistry, lg)
		if err != nil {
			fmt.Println(command)
			return errorOutput(err), 1, nil
		}

		return fmt.Sprintf("Issued mint request:\n%s\n", out), 0, nil

	case mintApprove.FullCommand():
		out, err := commands.MintApprove(*demoDir, *maUserID, *maRequestRecKey, lg)
		if err != nil {
			fmt.Println(command)
			return errorOutput(err), 1, nil

		}

		return fmt.Sprintf("Approved mint request:\n%s\n", out), 0, nil

	case transferOwnership.FullCommand():
		out, err := commands.Transfer(*demoDir, *toUserID, *toSellerID, *toBuyerID, *toCarRegistry, lg)
		if err != nil {
			fmt.Println(command)
			return errorOutput(err), 1, nil
		}

		return fmt.Sprintf("Issued transfer:\n%s\n", out), 0, nil

	case listCar.FullCommand():
		out, err := commands.ListCar(*demoDir, *lsCarUserID, *lsCarCar, *lsCarProv, lg)
		if err != nil {
			fmt.Println(command)
			return errorOutput(err), 1, nil
		}

		return fmt.Sprintf("%s\n", out), 0, nil

	case verifyTx.FullCommand():
		out, err := commands.VerifyEvidence(*demoDir, *verUserID, *verTxID, lg)
		if err != nil {
			fmt.Println(command)
			return errorOutput(err), 1, nil
		}

		return fmt.Sprintf("%s\n", out), 0, nil
	}

	if err != nil {
		fmt.Println(command)
		return errorOutput(err), 1, nil
	}

	bodyBytes, err := readBodyBytes(resp.Body)
	if err != nil {
		return errorOutput(err), 1, nil
	}

	return responseOutput(resp.StatusCode, bodyBytes), 0, nil
}

func responseOutput(statusCode int, responseBody []byte) string {
	status := fmt.Sprintf("Status: %d", statusCode)

	var buffer bytes.Buffer
	json.Indent(&buffer, responseBody, "", "\t")
	response := fmt.Sprintf("%s", buffer.Bytes())

	output := fmt.Sprintf("%s\n%s", status, response)

	return output
}

func readBodyBytes(body io.ReadCloser) ([]byte, error) {
	bodyBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading http response body: %s", err)
	}
	body.Close()

	return bodyBytes, nil
}

func errorOutput(err error) string {
	return fmt.Sprintf("Error: %s\n", err)
}
