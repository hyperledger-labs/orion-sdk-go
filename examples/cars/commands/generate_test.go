// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package commands

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	demoDir, err := ioutil.TempDir("/tmp", "cars-demo-test")
	require.NoError(t, err)
	defer os.RemoveAll(demoDir)

	err = Generate(demoDir)
	require.NoError(t, err)

	serverUrl, err := loadServerUrl(demoDir)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(serverUrl.String(), "http://127.0.0.1:32"))
}
