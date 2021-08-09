// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestGetBlockHeader(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	for i := 1; i < 10; i++ {
		putKeySync(t, "bdb", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "alice", aliceSession)
	}

	l, err := aliceSession.Ledger()
	require.NoError(t, err)
	for i := 1; i < 10; i++ {
		header, err := l.GetBlockHeader(uint64(i))
		require.NoError(t, err)
		require.NotNil(t, header)
		require.Equal(t, uint64(i), header.GetBaseHeader().GetNumber())
	}

	header, err := l.GetBlockHeader(100)
	require.EqualError(t, err, "error handling request, server returned: status: 404 Not Found, message: error while processing 'GET /ledger/block/100' because block not found: 100")
	require.Nil(t, header)
}

func TestGetLedgerPath(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	for i := 1; i < 10; i++ {
		putKeySync(t, "bdb", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "alice", aliceSession)
	}

	p, err := aliceSession.Ledger()
	require.NoError(t, err)

	existBlocks := make([]*types.BlockHeader, 0)
	for i := 1; i < 11; i++ {
		header, err := p.GetBlockHeader(uint64(i))
		require.NoError(t, err)
		require.NotNil(t, header)
		existBlocks = append(existBlocks, header)
	}

	tests := []struct {
		name       string
		start      uint64
		end        uint64
		path       []*types.BlockHeader
		errMessage string
	}{
		{
			name:  "from 3 to 2",
			start: 2,
			end:   3,
			path:  []*types.BlockHeader{existBlocks[2], existBlocks[1]},
		},
		{
			name:  "from 4 to 2",
			start: 2,
			end:   4,
			path:  []*types.BlockHeader{existBlocks[3], existBlocks[2], existBlocks[1]},
		},
		{
			name:  "from 6 to 1",
			start: 1,
			end:   6,
			path:  []*types.BlockHeader{existBlocks[5], existBlocks[4], existBlocks[0]},
		},
		{
			name:       "from 1 to 6 - error reverse range",
			start:      6,
			end:        1,
			path:       nil,
			errMessage: "error handling request, server returned: status: 400 Bad Request, message: query error: startId=6 > endId=1",
		},
		{
			name:       "from 100 to 1 - error not found",
			start:      1,
			end:        100,
			path:       nil,
			errMessage: "error handling request, server returned: status: 404 Not Found, message: error while processing 'GET /ledger/path?start=1&end=100' because can't find path in blocks skip list between 100 1: block not found: 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := p.GetLedgerPath(tt.start, tt.end)
			if tt.errMessage == "" {
				require.NoError(t, err)
				for i, b := range tt.path {
					require.True(t, proto.Equal(b, path[i]), fmt.Sprintf("Expected block number %d, actual block number %d", b.GetBaseHeader().GetNumber(), path[i].GetBaseHeader().GetNumber()))
				}
			} else {
				require.EqualError(t, err, tt.errMessage)
			}
		})
	}
}

func TestGetTransactionProof(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTemDir, 5*time.Second, 10)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	txEnvelopesPerBlock := make([][]proto.Message, 0)

	// Ten blocks, each 10 tx
	for i := 0; i < 10; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 10; j++ {
			keys = append(keys, fmt.Sprintf("key%d_%d", i, j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		txEnvelopesPerBlock = append(txEnvelopesPerBlock, putMultipleKeysAndValues(t, keys, values, "alice", aliceSession))
	}

	tests := []struct {
		name       string
		block      uint64
		txIdx      int
		errMessage string
	}{
		{
			name:  "block 3, tx 5",
			block: 3,
			txIdx: 5,
		},
		{
			name:  "block 5, tx 8",
			block: 5,
			txIdx: 8,
		},
		{
			name:       "block 15, tx 0, block not exist",
			block:      15,
			txIdx:      0,
			errMessage: "error handling request, server returned: status: 404 Not Found, message: error while processing 'GET /ledger/proof/tx/15?idx=0' because requested block number [15] cannot be greater than the last committed block number",
		},
		{
			name:       "block 10, tx 30, tx not exist in block",
			block:      10,
			txIdx:      30,
			errMessage: "error handling request, server returned: status: 404 Not Found, message: error while processing 'GET /ledger/proof/tx/10?idx=30' because node with index 30 is not part of merkle tree (0, 9)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Ledger()
			require.NoError(t, err)
			proof, err := p.GetTransactionProof(tt.block, tt.txIdx)
			if tt.errMessage == "" {
				require.NoError(t, err)
				txEnv := txEnvelopesPerBlock[tt.block-3][tt.txIdx]
				blockHeader, err := p.GetBlockHeader(tt.block)
				require.NoError(t, err)
				receipt := &types.TxReceipt{
					Header:  blockHeader,
					TxIndex: uint64(tt.txIdx),
				}

				res, err := proof.Verify(receipt, txEnv)
				require.NoError(t, err)
				require.True(t, res)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMessage)
			}
		})
	}
}

func TestGetTransactionReceipt(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTemDir, 5*time.Second, 10)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	txEnvelopesPerBlock := make([][]proto.Message, 0)

	// Ten blocks, each 10 tx
	for i := 0; i < 10; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 10; j++ {
			keys = append(keys, fmt.Sprintf("key%d_%d", i, j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		txEnvelopesPerBlock = append(txEnvelopesPerBlock, putMultipleKeysAndValues(t, keys, values, "alice", aliceSession))
	}

	tests := []struct {
		name       string
		block      uint64
		txIdx      uint64
		txID       string
		wantErr    bool
		errMessage string
	}{
		{
			name:    "block 3, tx 5",
			block:   3,
			txIdx:   5,
			txID:    txEnvelopesPerBlock[0][5].(*types.DataTxEnvelope).GetPayload().GetTxId(),
			wantErr: false,
		},
		{
			name:    "block 5, tx 8",
			block:   5,
			txIdx:   8,
			txID:    txEnvelopesPerBlock[2][8].(*types.DataTxEnvelope).GetPayload().GetTxId(),
			wantErr: false,
		},
		{
			name:    "block 11, tx 2",
			block:   11,
			txIdx:   2,
			txID:    txEnvelopesPerBlock[8][2].(*types.DataTxEnvelope).GetPayload().GetTxId(),
			wantErr: false,
		},
		{
			name:       "tx not exist",
			block:      0,
			txIdx:      0,
			txID:       "not_exist",
			wantErr:    true,
			errMessage: "error handling request, server returned: status: 404 Not Found, message: error while processing 'GET /ledger/tx/receipt/not_exist' because TxID not found: not_exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Ledger()
			require.NoError(t, err)
			receipt, err := p.GetTransactionReceipt(tt.txID)
			if tt.errMessage == "" {
				require.NoError(t, err)
				require.NotNil(t, receipt)
				require.Equal(t, tt.block, receipt.GetHeader().GetBaseHeader().GetNumber())
				require.Equal(t, tt.txIdx, receipt.GetTxIndex())
			} else {
				require.EqualError(t, err, tt.errMessage)
				require.Nil(t, receipt)
			}
		})
	}
}
