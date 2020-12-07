package commands

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"gopkg.in/yaml.v2"
)

// Generate demo materials:
// crypto under demoDir/crypto
// server config under demoDir/config
// server db dir under demoDir/database
func Generate(demoDir string) error {
	if !strings.HasSuffix(demoDir, "/") {
		demoDir = demoDir + "/"
	}

	if err := os.MkdirAll(path.Dir(demoDir), 0755); err != nil {
		return err
	}

	for _, subdir := range []string{"crypto", "database", "config", "txs"} {
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
	config := &config.Configurations{
		Node: config.NodeConf{
			Identity: config.IdentityConf{
				ID:              "demo",
				CertificatePath: path.Join(demoDir, "crypto", "server", "server.pem"),
				KeyPath:         path.Join(demoDir, "crypto", "server", "server.key"),
			},
			Network: config.NetworkConf{
				Address: "127.0.0.1",
				Port:    8080,
			},
			LogLevel: "debug",
			Database: config.DatabaseConf{
				Name:            "leveldb",
				LedgerDirectory: path.Join(demoDir, "database"),
			},
			QueueLength: config.QueueLengthConf{
				ReorderedTransactionBatch: 1,
				Transaction:               1,
				Block:                     1,
			},
		},
		Admin: config.AdminConf{
			ID:              "admin",
			CertificatePath: path.Join(demoDir, "crypto", "admin", "admin.pem"),
		},
		RootCA: config.RootCAConf{
			CertificatePath: path.Join(demoDir, "crypto", "CA", "CA.pem"),
		},
		Consensus: config.ConsensusConf{
			MaxTransactionCountPerBlock: 1,
			MaxBlockSize:                1,
			BlockTimeout:                100 * time.Millisecond,
			Algorithm:                   "solo",
		},
	}

	c, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path.Join(demoDir, "config", "config.yaml"), c, 0644)
	if err != nil {
		return err
	}

	serverUrl, err := url.Parse(fmt.Sprintf("http://%s:%d",config.Node.Network.Address, config.Node.Network.Port))
	if err != nil {
		return err
	}

	if err = saveServerUrl(demoDir, serverUrl); err != nil {
		return err
	}

	return err
}
