// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"crypto/x509"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
)

//go:generate mockery --dir . --name SignatureVerifier --case underscore --output mocks/

// SignatureVerifier verifies servers' signatures provided within response
type SignatureVerifier interface {
	// Verify signature created by entityID for given payload
	Verify(entityID string, payload, signature []byte) error
}

type sigVerifier struct {
	nodesCerts map[string]*x509.Certificate
	logger     *logger.SugarLogger
}

// NewVerifier creates instance of the SignatureVerifier
func NewVerifier(certs map[string]*x509.Certificate, logger *logger.SugarLogger) (SignatureVerifier, error) {
	if len(certs) == 0 {
		logger.Error("no servers' certificates provided")
		return nil, errors.New("no servers' certificates provided")
	}

	return &sigVerifier{
		nodesCerts: certs,
		logger:     logger,
	}, nil
}

// Verify signature created by entityID for given payload
func (s *sigVerifier) Verify(entityID string, payload, signature []byte) error {
	cert, exists := s.nodesCerts[entityID]
	if !exists {
		return errors.Errorf("there is no certificate for entityID = %s", entityID)
	}

	err := cert.CheckSignature(cert.SignatureAlgorithm, payload, signature)
	if err != nil {
		s.logger.Errorf("signature verification failed entityID %s, due to %s", entityID, err)
		return errors.Errorf("signature verification failed entityID %s, due to %s", entityID, err)
	}
	return nil
}
