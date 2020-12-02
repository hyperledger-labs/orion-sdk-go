package main

import (
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestCars_Generate(t *testing.T) {
	tempDir, err := ioutil.TempDir("/tmp", "cars-demo-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	args := []string{
		"-d", path.Join(tempDir), "generate",
	}
	out, exitCode, err := executeForArgs(args)
	require.Equal(t, out, "Generated demo materials to: "+path.Join(tempDir))
	require.Equal(t, 0, exitCode)
	require.NoError(t, err)

	for _, name := range []string{"admin", "dmv", "dealer", "alice", "bob", "server"} {
		_, _ = testutils.LoadTestClientCrypto(t, path.Join(tempDir, "crypto", name), name)
	}
}
