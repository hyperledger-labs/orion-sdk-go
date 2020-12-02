package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.ibm.com/blockchaindb/sdk/examples/cars/commands"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	kingpin.Version("0.0.1")

	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "bcdb-client",
	}
	logger, err := logger.New(c)

	output, exit, err := executeForArgs(os.Args[1:], logger)
	if err != nil {
		kingpin.Fatalf("parsing arguments: %s. Try --help", err)
	}
	fmt.Println(output)
	os.Exit(exit)
}

func executeForArgs(args []string, logger *logger.SugarLogger) (output string, exit int, err error) {
	//
	// command line flags
	//
	app := kingpin.New("cars", "Car registry demo")
	demoDir := app.Flag("demo-dir", "Path to the folder that will contain all the material for the demo").Short('d').Required().String()

	generate := app.Command("generate", "Generate crypto material for all roles: admin, dmv, dealer, alice, bob; and the BCDB server.")

	init := app.Command("init", "Initialize the server, load it with users, create databases.")
	replica := init.Flag("server", "URI of blockchain DB replica, http://host:port, to connect to").Short('s').URL()

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

	case init.FullCommand():
		err := commands.Init(*demoDir, *replica, logger)
		if err != nil {
			return "", 1, err
		}
		return "Generated crypto material to: " + *demoDir, 0, nil
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
