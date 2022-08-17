package bcdb

import (
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestDataTXWithServerTLS(t *testing.T) {
	certTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithTLS(t, certTempDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstanceWithTLS(t, certTempDir, serverPort)
	// New session with admin user context
	session := openUserSession(t, bcdb, "admin", certTempDir)

	putKeySync(t, "bdb", "key1", "value1", "admin", session)

	// Validate
	tx, err := session.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx)
	val, meta, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("value1"), val)
	require.NotNil(t, meta)
}

func TestDBTXWithServerTLS(t *testing.T) {
	certTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "server"})
	testServer, _, _, err := SetupTestServerWithTLS(t, certTempDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstanceWithTLS(t, certTempDir, serverPort)
	// New session with admin user context
	session := openUserSession(t, bcdb, "admin", certTempDir)

	tx, err := session.DBsTx()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		exist, err := tx.Exists("bdb")
		return err == nil && exist
	}, time.Minute, 200*time.Millisecond)
}

func TestLedgerTLSServerAndClient(t *testing.T) {
	clientCertTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "server"})
	testServer, _, _, err := SetupTestServerWithParamsAndTLS(t, clientCertTempDir, 20*time.Millisecond, 1, true, true, generateCorrectTLSCrypto)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstanceWithTLSConfig(t, clientCertTempDir, serverPort, true, true, updateClientTLSConfigCorrect)
	// New session with admin user context
	session := openUserSessionWithQueryTimeout(t, bcdb, "admin", clientCertTempDir, 0, true)

	txEnvelopesPerBlock := make([]proto.Message, 0)
	txReceiptsPerBlock := make([]*types.TxReceipt, 0)

	// 20 blocks, each 1 tx
	for i := 0; i < 20; i++ {
		receipt, _, env := putKeySync(t, "bdb", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "admin", session)
		txEnvelopesPerBlock = append(txEnvelopesPerBlock, env)
		txReceiptsPerBlock = append(txReceiptsPerBlock, receipt)
	}

	p, err := session.Ledger()
	require.NoError(t, err)

	genesis, err := p.GetBlockHeader(GenesisBlockNumber)
	require.NoError(t, err)

	blockHeader, err := p.GetBlockHeader(10)
	require.NoError(t, err)
	txProof, path, err := p.GetFullTxProofAndVerify(txReceiptsPerBlock[5], blockHeader, txEnvelopesPerBlock[5])
	require.NoError(t, err)
	res, err := txProof.Verify(txReceiptsPerBlock[5], txEnvelopesPerBlock[5])
	require.NoError(t, err)
	require.True(t, res)
	res, err = path.Verify(genesis, blockHeader)
	require.NoError(t, err)
	require.True(t, res)
}

func TestSessionWithoutServerTLSAndWithClientTLS(t *testing.T) {
	certTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, certTempDir, 500*time.Millisecond, 1, false, true)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstanceWithTLS(t, certTempDir, serverPort)
	// New session with admin user context
	_, err = openUserSessionWithQueryTimeoutAndTLS(bcdb, "admin", certTempDir, 0, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot update the replica set and signature verifier: failed to obtain the latest cluster status: failed to get cluster status from replica set: [Id: testNode1, Role: UNKNOWN, URL:")
	require.Contains(t, err.Error(), "http: server gave HTTP response to HTTPS client")
}

func TestSessionWithServerTLSAndNotConfiguredClient(t *testing.T) {
	certTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithTLS(t, certTempDir)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstance(t, certTempDir, serverPort)
	// New session with admin user context
	_, err = openUserSessionWithQueryTimeoutAndTLS(bcdb, "admin", certTempDir, 0, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot update the replica set and signature verifier: failed to obtain the latest cluster status: failed to get cluster status from replica set: [Id: testNode1, Role: UNKNOWN, URL:")
	require.Contains(t, err.Error(), "error response from the server, 400 Bad Request")
}

func TestSessionServerTLSAndClientTLSIncorrectClientCA(t *testing.T) {
	certTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParamsAndTLS(t, certTempDir, 500*time.Millisecond, 1, true, true, generateIncorrectTLSCryptoWrongClientCA)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstanceWithTLS(t, certTempDir, serverPort)
	// New session with admin user context
	_, err = openUserSessionWithQueryTimeoutAndTLS(bcdb, "admin", certTempDir, 0, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot update the replica set and signature verifier: failed to obtain the latest cluster status: failed to get cluster status from replica set: [Id: testNode1, Role: UNKNOWN, URL:")
}

func TestSessionServerTLSNoClientTLSIncorrectCAOnClientSide(t *testing.T) {
	certTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, certTempDir, 500*time.Millisecond, 1, true, false)
	defer testServer.Stop()
	require.NoError(t, err)
	StartTestServer(t, testServer)

	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstanceWithTLSConfig(t, certTempDir, serverPort, true, false, updateClientTLSConfigIncorrectCA)
	// New session with admin user context
	_, err = openUserSessionWithQueryTimeoutAndTLS(bcdb, "admin", certTempDir, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot update the replica set and signature verifier: failed to obtain the latest cluster status: failed to get cluster status from replica set: [Id: testNode1, Role: UNKNOWN, URL:")
	require.Contains(t, err.Error(), "x509: certificate signed by unknown authority")
}

// Generates correct TLS CA certificate for server, correct TLS server certificates, but client side TLS certificates signed by incorrect CA
func generateIncorrectTLSCryptoWrongClientCA(t *testing.T, serverTlsEnabled, clientTLSEnabled bool, cryptoTempDir string) {
	tlsRootCAPemCert, tlsCaPrivKey, err := testutils.GenerateRootCA("Orion TLS RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, tlsRootCAPemCert)
	require.NotNil(t, tlsCaPrivKey)

	tlsCAKeyPair, err := tls.X509KeyPair(tlsRootCAPemCert, tlsCaPrivKey)
	require.NoError(t, err)
	require.NotNil(t, tlsCAKeyPair)

	block, _ := pem.Decode(tlsRootCAPemCert)
	tlsCertsCollection, err := certificateauthority.NewCACertCollection([][]byte{block.Bytes}, nil)
	require.NoError(t, err)

	err = tlsCertsCollection.VerifyCollection()
	require.NoError(t, err)

	err = os.WriteFile(path.Join(cryptoTempDir, "tlsServerRootCACert.pem"), tlsRootCAPemCert, 0666)
	require.NoError(t, err)

	tlsServerPemCert, tlsServerPrivKey, err := testutils.IssueCertificate("Orion TLS Instance", "127.0.0.1", tlsCAKeyPair)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(cryptoTempDir, "tlsServer.pem"), tlsServerPemCert, 0666)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(cryptoTempDir, "tlsServer.key"), tlsServerPrivKey, 0666)
	require.NoError(t, err)

	anotherTLSRootCAPemCert, anotherTLSCaPrivKey, err := testutils.GenerateRootCA("Another Orion TLS RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, anotherTLSRootCAPemCert)
	require.NotNil(t, anotherTLSCaPrivKey)

	anotherTLSCAKeyPair, err := tls.X509KeyPair(anotherTLSRootCAPemCert, anotherTLSCaPrivKey)
	require.NoError(t, err)
	require.NotNil(t, tlsCAKeyPair)

	tlsClientBlock, _ := pem.Decode(anotherTLSRootCAPemCert)
	anotherTLSCertsCollection, err := certificateauthority.NewCACertCollection([][]byte{tlsClientBlock.Bytes}, nil)
	require.NoError(t, err)

	err = anotherTLSCertsCollection.VerifyCollection()
	require.NoError(t, err)

	err = os.WriteFile(path.Join(cryptoTempDir, "anotherTLSServerRootCACert.pem"), anotherTLSRootCAPemCert, 0666)
	require.NoError(t, err)

	tlsClientPemCert, tlsClientPrivKey, err := testutils.IssueCertificate("Orion Client TLS Instance", "127.0.0.1", anotherTLSCAKeyPair)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(cryptoTempDir, "tlsClient.pem"), tlsClientPemCert, 0666)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(cryptoTempDir, "tlsClient.key"), tlsClientPrivKey, 0666)
	require.NoError(t, err)
}

// Generates another TLS CA certificate (not know to server) and configure it inside client as server TLS CA certificate
func updateClientTLSConfigIncorrectCA(t *testing.T, cryptoDir, serverPort string, conf *sdkconfig.ConnectionConfig, tlsEnabled, clientTLSRequired bool) {
	anotherTLSRootCAPemCert, anotherTLSCaPrivKey, err := testutils.GenerateRootCA("Another Orion TLS RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, anotherTLSRootCAPemCert)
	require.NotNil(t, anotherTLSCaPrivKey)
	err = os.WriteFile(path.Join(cryptoDir, "anotherTLSServerRootCACert.pem"), anotherTLSRootCAPemCert, 0666)
	require.NoError(t, err)

	if tlsEnabled {
		conf.ReplicaSet[0].Endpoint = fmt.Sprintf("https://127.0.0.1:%s", serverPort)
		conf.TLSConfig.Enabled = true
		conf.TLSConfig.ClientAuthRequired = clientTLSRequired
		conf.TLSConfig.CaConfig.RootCACertsPath = []string{path.Join(cryptoDir, "anotherTLSServerRootCACert.pem")}
		conf.TLSConfig.CaConfig.IntermediateCACertsPath = nil
	}
}
