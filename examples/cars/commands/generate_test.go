package commands

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestGenerate(t *testing.T) {
	demoDir, err := ioutil.TempDir("/tmp", "cars-demo-test")
	require.NoError(t, err)
	defer os.RemoveAll(demoDir)

	err = Generate(demoDir)
	require.NoError(t, err)

	serverUrl, err := loadServerUrl(demoDir)
	require.NoError(t, err)
	require.Equal(t,"http://127.0.0.1:8080", serverUrl.String())
}