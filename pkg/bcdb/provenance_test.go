package bcdb

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func Test_provenance_GetBlockHeader(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, tempDir, clientCertTemDir, "alice")

	for i := 1; i < 10; i++ {
		putKeyAndValidate(t, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "alice", aliceSession)
	}

	p, err := aliceSession.Provenance()
	require.NoError(t, err)
	for i := 1; i < 10; i++ {
		header, err := p.GetBlockHeader(uint64(i))
		require.NoError(t, err)
		require.NotNil(t, header)
		require.Equal(t, uint64(i), header.GetBaseHeader().GetNumber())
	}
}

func Test_provenance_GetLedgerPath(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, tempDir, clientCertTemDir, "alice")

	for i := 1; i < 10; i++ {
		putKeyAndValidate(t, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "alice", aliceSession)
	}

	p, err := aliceSession.Provenance()
	require.NoError(t, err)

	existBlocks := make([]*types.BlockHeader, 0)
	for i := 1; i < 11; i++ {
		header, err := p.GetBlockHeader(uint64(i))
		require.NoError(t, err)
		require.NotNil(t, header)
		existBlocks = append(existBlocks, header)
	}

	tests := []struct {
		name    string
		start   uint64
		end     uint64
		path    []*types.BlockHeader
		wantErr bool
	}{
		{
			name:    "from 3 to 2",
			start:   2,
			end:     3,
			path:    []*types.BlockHeader{existBlocks[2], existBlocks[1]},
			wantErr: false,
		},
		{
			name:    "from 4 to 2",
			start:   2,
			end:     4,
			path:    []*types.BlockHeader{existBlocks[3], existBlocks[2], existBlocks[1]},
			wantErr: false,
		},
		{
			name:    "from 6 to 1",
			start:   1,
			end:     6,
			path:    []*types.BlockHeader{existBlocks[5], existBlocks[4], existBlocks[0]},
			wantErr: false,
		},
		{
			name:    "from 1 to 6",
			start:   6,
			end:     1,
			path:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := p.GetLedgerPath(tt.start, tt.end)
			if !tt.wantErr {
				require.NoError(t, err)
				for i, b := range tt.path {
					require.True(t, proto.Equal(b, path[i]), fmt.Sprintf("Expected block number %d, actual block number %d", b.GetBaseHeader().GetNumber(), path[i].GetBaseHeader().GetNumber()))
				}
			} else {
				require.Error(t, err)
			}
		})

	}
}

func Test_provenance_GetTransactionProof(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServerWithParams(t, clientCertTemDir, time.Second, 10)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, tempDir, clientCertTemDir, "alice")

	txEnvelopesPerBlock := make([][]proto.Message, 0)

	// Ten blocks, each 10 tx
	for i := 0; i < 10; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 10; j++ {
			keys = append(keys, fmt.Sprintf("key%d_%d", i, j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		txEnvelopesPerBlock = append(txEnvelopesPerBlock, putMultipleKeysAndValidate(t, keys, values, "alice", aliceSession))
	}

	tests := []struct {
		name    string
		block   uint64
		txIdx   int
		wantErr bool
	}{
		{
			name:    "block 3, tx 5",
			block:   3,
			txIdx:   5,
			wantErr: false,
		},
		{
			name:    "block 5, tx 8",
			block:   5,
			txIdx:   8,
			wantErr: false,
		},
		{
			name:    "block 15, tx 0, block not exist",
			block:   15,
			txIdx:   0,
			wantErr: true,
		},
		{
			name:    "block 10, tx 30, tx not exist in block",
			block:   10,
			txIdx:   30,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Provenance()
			require.NoError(t, err)
			proof, err := p.GetTransactionProof(tt.block, tt.txIdx)
			if !tt.wantErr {
				require.NoError(t, err)
				txEnv := txEnvelopesPerBlock[tt.block-3][tt.txIdx]
				blockHeader, err := p.GetBlockHeader(tt.block)
				require.NoError(t, err)
				valInfo := blockHeader.GetValidationInfo()[tt.txIdx]
				txBytes, err := json.Marshal(txEnv)
				require.NoError(t, err)
				viBytes, err := json.Marshal(valInfo)
				require.NoError(t, err)
				currHash, err := crypto.ComputeSHA256Hash(append(txBytes, viBytes...))
				require.NoError(t, err)
				for _, pHash := range proof {
					currHash, err = crypto.ConcatenateHashes(currHash, pHash)
					require.NoError(t, err)
				}
				require.EqualValues(t, blockHeader.GetTxMerkelTreeRootHash(), currHash)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func Test_provenance_GetTransactionReceipt(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServerWithParams(t, clientCertTemDir, time.Second, 10)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, tempDir, clientCertTemDir, "alice")

	txEnvelopesPerBlock := make([][]proto.Message, 0)

	// Ten blocks, each 10 tx
	for i := 0; i < 10; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 10; j++ {
			keys = append(keys, fmt.Sprintf("key%d_%d", i, j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		txEnvelopesPerBlock = append(txEnvelopesPerBlock, putMultipleKeysAndValidate(t, keys, values, "alice", aliceSession))
	}

	tests := []struct {
		name    string
		block   uint64
		txIdx   uint64
		txID    string
		wantErr bool
	}{
		{
			name:    "block 3, tx 5",
			block:   3,
			txIdx:   5,
			txID:    txEnvelopesPerBlock[0][5].(*types.DataTxEnvelope).GetPayload().GetTxID(),
			wantErr: false,
		},
		{
			name:    "block 5, tx 8",
			block:   5,
			txIdx:   8,
			txID:    txEnvelopesPerBlock[2][8].(*types.DataTxEnvelope).GetPayload().GetTxID(),
			wantErr: false,
		},
		{
			name:    "block 11, tx 2",
			block:   11,
			txIdx:   2,
			txID:    txEnvelopesPerBlock[8][2].(*types.DataTxEnvelope).GetPayload().GetTxID(),
			wantErr: false,
		},
		{
			name:    "tx not exist",
			block:   0,
			txIdx:   0,
			txID:    "not_exist",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Provenance()
			require.NoError(t, err)
			receipt, err := p.GetTransactionReceipt(tt.txID)
			if !tt.wantErr {
				require.NoError(t, err)
				require.NotNil(t, receipt)
				require.Equal(t, tt.block, receipt.GetHeader().GetBaseHeader().GetNumber())
				require.Equal(t, tt.txIdx, receipt.GetTxIndex())
			} else {
				require.Error(t, err)
				require.Nil(t, receipt)
			}
		})
	}
}

func Test_provenance_GetHistoricalData(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice"})
	testServer, _, tempDir, err := setupTestServerWithParams(t, clientCertTemDir, time.Second, 1)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, tempDir, clientCertTemDir, "alice")

	txEnvelopesPerBlock := make([][]proto.Message, 0)

	// 25 blocks, each 1 tx
	for i := 0; i < 5; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 5; j++ {
			keys = append(keys, fmt.Sprintf("key%d", j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		txEnvelopesPerBlock = append(txEnvelopesPerBlock, putMultipleKeysAndValidate(t, keys, values, "alice", aliceSession))
	}

	tests := []struct {
		name    string
		key     string
		values  [][]byte
		wantErr bool
	}{
		{
			name:    "key0 - 5 values",
			key:     "key0",
			values:  [][]byte{[]byte("value0_0"), []byte("value1_0"), []byte("value2_0"), []byte("value3_0"), []byte("value4_0")},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Provenance()
			require.NoError(t, err)
			hData, err := p.GetHistoricalData("bdb", tt.key)
			if !tt.wantErr {
				require.NoError(t, err)
				require.NotNil(t, hData)
				resValues := make([][]byte, 0)
				for _, d := range hData {
					resValues = append(resValues, d.Value)
				}
				require.ElementsMatch(t, tt.values, resValues)
			} else {
				require.Error(t, err)
				require.Nil(t, hData)
			}
		})
	}
}
