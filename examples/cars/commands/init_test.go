// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package commands

import (
	"github.ibm.com/blockchaindb/sdk/internal/test"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server"
)

func TestInit(t *testing.T) {
	demoDir, err := ioutil.TempDir("/tmp", "cars-demo-test")
	require.NoError(t, err)
	defer os.RemoveAll(demoDir)

	err = Generate(demoDir)
	require.NoError(t, err)

	testServer, _, err := setupTestServer(t, demoDir)
	require.NoError(t, err)
	defer func() {
		if testServer != nil {
			err = testServer.Stop()
			require.NoError(t, err)
		}
	}()
	require.NoError(t, err)
	err = testServer.Start()
	require.NoError(t, err)

	serverPort, err := testServer.Port()
	require.NoError(t, err)

	serverUrl, err := url.Parse("http://127.0.0.1:" + serverPort)
	require.NoError(t, err)

	err = saveServerUrl(demoDir, serverUrl)
	require.NoError(t, err)

	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "cars-demo",
	}
	logger, err := logger.New(c)

	err = Init(demoDir, logger)
	require.NoError(t, err)
}

func TestInit_saveServerUrl(t *testing.T) {
	demoDir, err := ioutil.TempDir("/tmp", "cars-demo-test")
	require.NoError(t, err)
	defer os.RemoveAll(demoDir)

	serverUrl, err := url.Parse("http://127.0.0.1:8080")
	require.NoError(t, err)
	err = saveServerUrl(demoDir, serverUrl)
	require.NoError(t, err)
	loadedUrl, err := loadServerUrl(demoDir)
	require.NoError(t, err)
	require.Equal(t, serverUrl.String(), loadedUrl.String())
}

func setupTestServer(t *testing.T, demoDir string) (*server.BCDBHTTPServer, string, error) {
	tempDataDir, err := ioutil.TempDir("/tmp", "car-demo-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDataDir)
	})

	cryptoDir := path.Join(demoDir, "crypto")

	nodePort, peerPort := test.GetPorts()

	server, err := server.New(&config.Configurations{
		LocalConfig: &config.LocalConfiguration{
			Server: config.ServerConf{
				Identity: config.IdentityConf{ID: "demo",
					CertificatePath: path.Join(cryptoDir, "server", "server.pem"),
					KeyPath:         path.Join(cryptoDir, "server", "server.key"),
				},
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    nodePort,
				},
				Database: config.DatabaseConf{
					Name:            "leveldb",
					LedgerDirectory: path.Join(tempDataDir, "ledger"),
				},
				Replication: config.ReplicationConf{},
				QueueLength: config.QueueLengthConf{
					Block:                     10,
					Transaction:               10,
					ReorderedTransactionBatch: 10,
				},
				LogLevel: "info",
			},
			BlockCreation: config.BlockCreationConf{
				MaxBlockSize:                1000000,
				MaxTransactionCountPerBlock: 1,
				BlockTimeout:                500 * time.Millisecond,
			},
			Replication: config.ReplicationConf{},
			Bootstrap:   config.BootstrapConf{},
		},

		SharedConfig: &config.SharedConfiguration{
			Nodes: []config.NodeConf{
				{
					NodeID:          "demo",
					Host:            "127.0.0.1",
					Port:            nodePort,
					CertificatePath: path.Join(cryptoDir, "server", "server.pem"),
				},
			},
			Consensus: &config.ConsensusConf{
				Algorithm: "raft",
				Members: []*config.PeerConf{
					{
						NodeId:   "demo",
						RaftId:   1,
						PeerHost: "127.0.0.1",
						PeerPort: peerPort,
					},
				},
				RaftConfig: &config.RaftConf{
					TickInterval:   "100ms",
					ElectionTicks:  100,
					HeartbeatTicks: 10,
				},
			},
			CAConfig: config.CAConfiguration{
				RootCACertsPath: []string{path.Join(cryptoDir, "CA", "CA.pem")},
			},
			Admin: config.AdminConf{
				ID:              "admin",
				CertificatePath: path.Join(cryptoDir, "admin", "admin.pem"),
			},
		},
	})

	return server, tempDataDir, err
}
