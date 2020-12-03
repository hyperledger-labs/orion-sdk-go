package commands

import (
	"crypto/tls"
	"os"
	"path"
	"strings"

	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
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

	for _, subdir := range []string{"crypto", "database", "config"} {
		if err := os.MkdirAll(path.Join(demoDir, subdir), 0755); err != nil {
			return err
		}
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
