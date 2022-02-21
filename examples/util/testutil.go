// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-sdk-go/internal/test"
	sdkConfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/server"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func SetupTestEnv(t *testing.T, tempDir string, nodePort uint32) (*server.BCDBHTTPServer, uint32, uint32, error) {
	s, e := SetupTestEnvWithParams(t, tempDir, nodePort, nodePort+1000, 500*time.Millisecond, 1)
	return s, nodePort, nodePort + 1000, e
}

func StartTestServer(t *testing.T, s *server.BCDBHTTPServer) {
	err := s.Start()
	require.NoError(t, err)
	require.Eventually(t, func() bool { return s.IsLeader() == nil }, 30*time.Second, 100*time.Millisecond)
}

func SetupTestEnvWithParams(t *testing.T, tempDir string, nodePort uint32, raftPort uint32, blockTime time.Duration, txPerBlock uint32) (*server.BCDBHTTPServer, error) {
	config, err := CreateTestEnvFilesAndConfigs(t, tempDir, nodePort, raftPort, blockTime, txPerBlock)
	if err != nil {
		return nil, err
	}

	server, err := server.New(config)
	return server, err
}

func CreateTestEnvFilesAndConfigs(t *testing.T, tempDir string, nodePort uint32, raftPort uint32, blockTime time.Duration, txPerBlock uint32) (*config.Configurations, error) {
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	if nodePort == 0 && raftPort == 0 {
		nodePort, raftPort = test.GetPorts()
	}
	cryptoDir := path.Join(tempDir, "crypto")
	err := GenerateTestDirectoryStruct(tempDir, cryptoDir, nodePort, []string{"admin", "alice", "bob", "charlie", "node"})
	if err != nil {
		return nil, err
	}

	config := GenerateSampleServerConfig(tempDir, cryptoDir, nodePort, raftPort, blockTime, txPerBlock)

	return config, nil
}

func GenerateTestDirectoryStruct(tempDir, cryptoDir string, serverPort uint32, users []string) error {
	if len(users) == 0 {
		return errors.New("no users defined")
	}
	clientConfig := &Config{
		ConnectionConfig: sdkConfig.ConnectionConfig{
			ReplicaSet: []*sdkConfig.Replica{
				{
					ID:       "server1",
					Endpoint: fmt.Sprintf("http://127.0.0.1:%d", serverPort),
				},
			},
			RootCAs: []string{path.Join(cryptoDir, "CA", "CA.pem")},
			Logger:  nil,
		},
		SessionConfig: sdkConfig.SessionConfig{
			UserConfig: &sdkConfig.UserConfig{
				UserID:         "admin",
				CertPath:       path.Join(cryptoDir, users[0], users[0]+".pem"),
				PrivateKeyPath: path.Join(cryptoDir, users[0], users[0]+".key"),
			},
			TxTimeout:    10 * time.Second,
			QueryTimeout: 20 * time.Second,
		},
	}
	err := GenerateCrypto(cryptoDir, users)
	if err != nil {
		return err
	}
	return WriteConfigToYaml(clientConfig, path.Join(tempDir, "config.yml"))
}

func GenerateCrypto(cryptoDir string, users []string) error {
	err := os.MkdirAll(cryptoDir, 0755)
	if err != nil {
		return err
	}
	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("BCDB RootCA", "127.0.0.1")
	if err != nil {
		return err
	}

	caDir := path.Join(cryptoDir, "CA")
	err = os.MkdirAll(caDir, 0755)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path.Join(cryptoDir, "CA", "CA.pem"), rootCAPemCert, 0644)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path.Join(cryptoDir, "CA", "CA.key"), caPrivKey, 0644)
	if err != nil {
		return err
	}

	for _, name := range users {
		keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
		if err != nil {
			return err
		}

		pemCert, privKey, err := testutils.IssueCertificate("BCDB Client "+name, "127.0.0.1", keyPair)
		if err != nil {
			return err
		}

		certDir := path.Join(cryptoDir, name)
		err = os.MkdirAll(certDir, 0755)
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(path.Join(cryptoDir, name, name+".pem"), pemCert, 0644)
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(path.Join(cryptoDir, name, name+".key"), privKey, 0644)
		if err != nil {
			return err
		}
	}
	return nil

}

func GenerateSampleServerConfig(tempDir, cryptoDir string, nodePort uint32, raftPort uint32, blockTime time.Duration, txPerBlock uint32) *config.Configurations {
	return &config.Configurations{
		LocalConfig: &config.LocalConfiguration{
			Server: config.ServerConf{
				Identity: config.IdentityConf{ID: "server1",
					CertificatePath: path.Join(cryptoDir, "node", "node.pem"),
					KeyPath:         path.Join(cryptoDir, "node", "node.key"),
				},
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    nodePort,
				},
				Database: config.DatabaseConf{
					Name:            "leveldb",
					LedgerDirectory: path.Join(tempDir, "ledger"),
				},
				QueueLength: config.QueueLengthConf{
					Block:                     10,
					Transaction:               10,
					ReorderedTransactionBatch: 10,
				},
				LogLevel: "info",
			},
			BlockCreation: config.BlockCreationConf{
				MaxBlockSize:                1000000,
				MaxTransactionCountPerBlock: txPerBlock,
				BlockTimeout:                blockTime,
			},
			Replication: config.ReplicationConf{
				WALDir:  path.Join(tempDir, "raft", "wal"),
				SnapDir: path.Join(tempDir, "raft", "snap"),
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    raftPort},
				TLS: config.TLSConf{Enabled: false},
			},
			Bootstrap: config.BootstrapConf{},
		},
		SharedConfig: &config.SharedConfiguration{
			Nodes: []*config.NodeConf{
				{
					NodeID:          "server1",
					Host:            "127.0.0.1",
					Port:            nodePort,
					CertificatePath: path.Join(cryptoDir, "node", "node.pem"),
				},
			},
			Consensus: &config.ConsensusConf{
				Algorithm: "raft",
				Members: []*config.PeerConf{
					{
						NodeId:   "server1",
						RaftId:   1,
						PeerHost: "127.0.0.1",
						PeerPort: raftPort,
					},
				},
				RaftConfig: &config.RaftConf{
					TickInterval:         "10ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: math.MaxInt64,
				},
			},
			CAConfig: config.CAConfiguration{RootCACertsPath: []string{path.Join(cryptoDir, "CA", "CA.pem")}},
			Admin: config.AdminConf{
				ID:              "admin",
				CertificatePath: path.Join(cryptoDir, "admin", "admin.pem"),
			},
		},
	}
}

func WriteConfigToYaml(configObject interface{}, configFile string) error {
	data, err := yaml.Marshal(configObject)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(configFile, data, 0644)
	if err != nil {
		return err
	}
	return nil
}
