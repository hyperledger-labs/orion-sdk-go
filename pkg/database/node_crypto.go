package database

import (
	"encoding/pem"
	"github.com/pkg/errors"
	"io/ioutil"
)

// TODO: Replace with NodeCryptoProvider that access node info
var (
	certPossibleLocations = []string{"../database/testdata/", "pkg/database/testdata/", "../pkg/database/testdata/", "../../library/pkg/crypto/testdata/"}
)

func CreateHardcodedFetcher() (*hardcodedCertificateFetcher, error) {
	certificateFetcher := &hardcodedCertificateFetcher{}

	for _, loc := range certPossibleLocations {
		// TODO: After replacing for access to node info over network, no certificates loading here
		rawCert, err := loadRawCertificate(createNodeCertificatePath(loc))
		if err != nil {
			continue
		}
		certificateFetcher.rawCertificate = rawCert
		return certificateFetcher, nil
	}
	return nil, errors.Errorf("Can't create load hadrcode certificate fetcher, possible certificate locations are %v", certPossibleLocations)
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
