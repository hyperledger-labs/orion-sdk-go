package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.ibm.com/blockchaindb/sdk/examples/cars/commands"
	"gopkg.in/alecthomas/kingpin.v2"
	"io"
	"io/ioutil"
	"net/http"
	"os"
)

func main() {
	kingpin.Version("0.0.1")

	output, exit, err := executeForArgs(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("parsing arguments: %s. Try --help", err)
	}
	fmt.Println(output)
	os.Exit(exit)
}

func executeForArgs(args []string) (output string, exit int, err error) {
	//
	// command line flags
	//
	app := kingpin.New("cars", "Car registry demo")
	demoDir := app.Flag("demo-dir", "Path to the folder that will contain all the material for the demo").Short('d').Required().String()

	generate := app.Command("generate", "Generate crypto material for all roles: admin, dmv, dealer, alice, bob; and the BCDB server.")

	start := app.Command("start", "Start the server, load it with users, create databases.")

	command := kingpin.MustParse(app.Parse(args))

	//
	// flag validation
	//

	//
	// call the underlying implementations
	//
	var resp *http.Response

	switch command {
	case generate.FullCommand():
		err := commands.Generate(*demoDir)
		if err != nil {
			return "", 1, err
		}
		return "Generated demo materials to: " + *demoDir, 0, nil

	case start.FullCommand():
		err := commands.Start(*demoDir)
		if err != nil {
			return "", 1, err
		}
		return "Generated crypto material to: " + *demoDir, 0, nil

		//case join.FullCommand():
		//	resp, err = osnadmin.Join(osnURL, marshaledConfigBlock, caCertPool, tlsClientCert)
		//case list.FullCommand():
		//	if *listChannelID != "" {
		//		resp, err = osnadmin.ListSingleChannel(osnURL, *listChannelID, caCertPool, tlsClientCert)
		//		break
		//	}
		//	resp, err = osnadmin.ListAllChannels(osnURL, caCertPool, tlsClientCert)
		//case remove.FullCommand():
		//	resp, err = osnadmin.Remove(osnURL, *removeChannelID, caCertPool, tlsClientCert)
	}

	if err != nil {
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
