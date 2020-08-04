package database

import (
	"encoding/pem"
	"io/ioutil"
	"log"
)

// TODO: Replace with NodeCryptoProvider that access node info
var (
	certificateFetcher    *hardcodedCertificateFetcher
	certPossibleLocations = []string{"../database/testdata/", "pkg/database/testdata/", "../pkg/database/testdata/", "../../library/pkg/crypto/testdata/"}
)

func init() {
	certificateFetcher = &hardcodedCertificateFetcher{}

	for _, loc := range certPossibleLocations {
		// TODO: After replacing for access to node info over network, no certificates loading here
		rawCert, err := loadRawCertificate(createNodeCertificatePath(loc))
		if err != nil {
			continue
		}
		certificateFetcher.rawCertificate = rawCert
		return
	}
	log.Panicln("can't load hardcoded node configuration")
}

func createNodeCertificatePath(location string) string {
	return location + "service.pem"
}

type hardcodedCertificateFetcher struct {
	rawCertificate []byte
}

func (p *hardcodedCertificateFetcher) GetRawCertificate(nodeID string) ([]byte, error) {
	return p.rawCertificate, nil
}

func loadRawCertificate(pemFile string) ([]byte, error) {
	b, err := ioutil.ReadFile(pemFile)
	if err != nil {
		return nil, err
	}
	bl, _ := pem.Decode(b)
	if err != nil {
		return nil, err
	}
	return bl.Bytes, nil
}
