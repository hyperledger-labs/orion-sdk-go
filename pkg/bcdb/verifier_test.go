// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	crypto2 "crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/stretchr/testify/require"
)

func TestSigVerifier_Verify(t *testing.T) {
	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("Clients RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, rootCAPemCert)
	require.NotNil(t, caPrivKey)

	keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	require.NoError(t, err)
	require.NotNil(t, keyPair)

	pemCert, privKey, err := testutils.IssueCertificate("BCDB Client Test", "127.0.0.1", keyPair)
	require.NoError(t, err)

	payload := []byte{0}
	message, err := crypto.ComputeSHA256Hash(payload)
	require.NoError(t, err)

	keyLoader := &crypto.KeyLoader{}
	signerKey, err := keyLoader.Load(privKey)
	require.NoError(t, err)

	sig, err := signerKey.(*ecdsa.PrivateKey).Sign(rand.Reader, message, crypto2.SHA256)
	require.NoError(t, err)

	asn1Cert, _ := pem.Decode(pemCert)
	cert, err := x509.ParseCertificate(asn1Cert.Bytes)
	require.NoError(t, err)

	tests := []struct {
		name            string
		certs           map[string]*x509.Certificate
		payload         []byte
		signature       []byte
		nodeName        string
		isErrorExpected bool
		errorMessage    string
	}{
		{
			name:     "valid signature",
			nodeName: "node1",
			certs: map[string]*x509.Certificate{
				"node1": cert,
			},
			payload:   payload,
			signature: sig,
		},
		{
			name:     "wrong signature",
			nodeName: "node1",
			certs: map[string]*x509.Certificate{
				"node1": cert,
			},
			payload:         payload,
			signature:       []byte{0},
			isErrorExpected: true,
			errorMessage:    "signature verification failed",
		},
		{
			name:     "wrong payload",
			nodeName: "node1",
			certs: map[string]*x509.Certificate{
				"node1": cert,
			},
			payload:         []byte{1},
			signature:       sig,
			isErrorExpected: true,
			errorMessage:    "signature verification failed",
		},
		{
			name:     "missing cert",
			nodeName: "node1",
			certs: map[string]*x509.Certificate{
				"node2": cert,
			},
			payload:         payload,
			signature:       sig,
			isErrorExpected: true,
			errorMessage:    "there is no certificate for entityID",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logger := createTestLogger(t)

			verifier, err := NewVerifier(test.certs, logger)
			require.NoError(t, err)
			err = verifier.Verify(test.nodeName, test.payload, test.signature)
			if !test.isErrorExpected {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errorMessage)
			}
		})
	}
}

func TestFailToCreateVerifier(t *testing.T) {
	t.Parallel()
	logger := createTestLogger(t)

	_, err := NewVerifier(map[string]*x509.Certificate{}, logger)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no servers' certificates provided")

}
