// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package commands

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/hyperledger-labs/orion-sdk-go/internal/test"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"gopkg.in/yaml.v2"
)

// Generate demo materials:
// crypto under demoDir/crypto
// server config under demoDir/config
// server db dir under demoDir/database
// server replication dir under demoDir/replication
// transactions dir under demoDir/txs
func Generate(demoDir string) error {
	if !strings.HasSuffix(demoDir, "/") {
		demoDir = demoDir + "/"
	}

	if err := os.MkdirAll(path.Dir(demoDir), 0755); err != nil {
		return err
	}

	for _, subdir := range []string{"crypto", "database", "replication", "config", "txs"} {
		if err := os.MkdirAll(path.Join(demoDir, subdir), 0755); err != nil {
			return err
		}
	}

	err := writeConfigFile(demoDir)
	if err != nil {
		return err
	}

	for _, role := range []string{"admin", "dmv", "dealer", "alice", "bob", "server", "CA"} {
		if err := os.MkdirAll(path.Join(demoDir, "crypto", role), 0755); err != nil {
			return err
		}
	}

	cryptoDir := path.Join(demoDir, "crypto")

	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("Car registry RootCA", "127.0.0.1")
	if err != nil {
		return err
	}

	// CA
	rootCACertFile, err := os.Create(path.Join(cryptoDir, "CA", "CA.pem"))
	_, err = rootCACertFile.Write(rootCAPemCert)
	err = rootCACertFile.Close()
	rootCAKeyFile, err := os.Create(path.Join(cryptoDir, "CA", "CA.key"))
	_, err = rootCAKeyFile.Write(rootCAPemCert)
	err = rootCAKeyFile.Close()

	// Roles
	for _, name := range []string{"admin", "dmv", "dealer", "alice", "bob", "server"} {
		keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
		if err != nil {
			return err
		}

		pemCert, privKey, err := testutils.IssueCertificate("Car registry Client "+name, "127.0.0.1", keyPair)
		if err != nil {
			return err
		}

		pemCertFile, err := os.Create(path.Join(cryptoDir, name, name+".pem"))
		if err != nil {
			return err
		}
		_, err = pemCertFile.Write(pemCert)
		if err != nil {
			return err
		}
		err = pemCertFile.Close()
		if err != nil {
			return err
		}

		pemPrivKeyFile, err := os.Create(path.Join(cryptoDir, name, name+".key"))
		if err != nil {
			return err
		}
		_, err = pemPrivKeyFile.Write(privKey)
		if err != nil {
			return err
		}
		err = pemPrivKeyFile.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func writeConfigFile(demoDir string) error {
	nodePort, peerPort := test.GetPorts()
	localConfig := &config.LocalConfiguration{
		Server: config.ServerConf{
			Identity: config.IdentityConf{
				ID:              "demo",
				CertificatePath: path.Join(demoDir, "crypto", "server", "server.pem"),
				KeyPath:         path.Join(demoDir, "crypto", "server", "server.key"),
			},
			Network: config.NetworkConf{
				Address: "127.0.0.1",
				Port:    nodePort,
			},
			Database: config.DatabaseConf{
				Name:            "leveldb",
				LedgerDirectory: path.Join(demoDir, "database"),
			},
			QueueLength: config.QueueLengthConf{
				Transaction:               10,
				ReorderedTransactionBatch: 10,
				Block:                     10,
			},
			LogLevel: "info",
		},
		BlockCreation: config.BlockCreationConf{
			MaxBlockSize:                1000000,
			MaxTransactionCountPerBlock: 1,
			BlockTimeout:                500 * time.Millisecond,
		},
		Replication: config.ReplicationConf{
			WALDir:  path.Join(demoDir, "replication", "wal"),
			SnapDir: path.Join(demoDir, "replication", "snap"),
			AuxDir:  path.Join(demoDir, "replication", "aux"),
			Network: config.NetworkConf{
				Address: "127.0.0.1",
				Port:    peerPort,
			},
			TLS: config.TLSConf{Enabled: false},
		},
		Bootstrap: config.BootstrapConf{
			Method: "genesis",
			File:   path.Join(demoDir, "config", "bootstrap-shared-config.yaml"),
		},
	}
	bootstrap := &config.SharedConfiguration{
		Nodes: []*config.NodeConf{
			{
				NodeID:          "demo",
				Host:            "127.0.0.1",
				Port:            nodePort,
				CertificatePath: path.Join(demoDir, "crypto", "server", "server.pem"),
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
				TickInterval:         "100ms",
				ElectionTicks:        100,
				HeartbeatTicks:       10,
				MaxInflightBlocks:    50,
				SnapshotIntervalSize: math.MaxInt64,
			},
		},
		CAConfig: config.CAConfiguration{
			RootCACertsPath: []string{path.Join(demoDir, "crypto", "CA", "CA.pem")},
		},
		Admin: config.AdminConf{
			ID:              "admin",
			CertificatePath: path.Join(demoDir, "crypto", "admin", "admin.pem"),
		},
	}

	c, err := yaml.Marshal(localConfig)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path.Join(demoDir, "config", "config.yml"), c, 0644)
	if err != nil {
		return err
	}

	b, err := yaml.Marshal(bootstrap)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path.Join(demoDir, "config", "bootstrap-shared-config.yaml"), b, 0644)
	if err != nil {
		return err
	}

	serverUrl, err := url.Parse(fmt.Sprintf("http://%s:%d", localConfig.Server.Network.Address, localConfig.Server.Network.Port))
	if err != nil {
		return err
	}

	if err = saveServerUrl(demoDir, serverUrl); err != nil {
		return err
	}

	return err
}
