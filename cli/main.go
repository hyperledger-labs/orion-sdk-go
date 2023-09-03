// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/hyperledger-labs/orion-sdk-go/cli/commands"
	"os"
)

func main() {
	cmd := commands.InitializeOrionCli()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
