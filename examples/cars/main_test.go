// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/stretchr/testify/require"
)

func TestCars_Generate(t *testing.T) {
	tempDir, err := ioutil.TempDir("/tmp", "cars-demo-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	args := []string{
		"-d", path.Join(tempDir), "generate",
	}
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "cars-demo",
	}
	logger, err := logger.New(c)

	out, exitCode, err := executeForArgs(args, logger)
	require.Equal(t, out, "Generated demo materials to: "+path.Join(tempDir))
	require.Equal(t, 0, exitCode)
	require.NoError(t, err)

	for _, name := range []string{"admin", "dmv", "dealer", "alice", "bob", "server"} {
		_, _ = testutils.LoadTestCrypto(t, path.Join(tempDir, "crypto", name), name)
	}
}
