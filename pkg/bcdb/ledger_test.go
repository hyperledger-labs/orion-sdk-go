// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestGetBlockHeader(t *testing.T) {
	clientCertTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTempDir)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTempDir, "alice")

	txReceipts := make([]*types.TxReceipt, 0)
	firstDataBlockIndex := 0
	for i := 1; i < 10; i++ {
		txReceipt, _, _ := putKeySync(t, "bdb", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "alice", aliceSession)
		txReceipts = append(txReceipts, txReceipt)
		if firstDataBlockIndex == 0 {
			firstDataBlockIndex = int(txReceipt.Header.BaseHeader.Number)
		}
	}

	l, err := aliceSession.Ledger()
	require.NoError(t, err)
	for i := 1; i < 10; i++ {
		header, err := l.GetBlockHeader(uint64(i))
		require.NoError(t, err)
		require.NotNil(t, header)
		require.Equal(t, uint64(i), header.GetBaseHeader().GetNumber())
		if i >= firstDataBlockIndex {
			require.True(t, proto.Equal(txReceipts[i-firstDataBlockIndex].Header, header))
		}
	}

	header, err := l.GetBlockHeader(100)
	require.EqualError(t, err, "error handling request, server returned: status: 404 Not Found, status code: 404, message: error while processing 'GET /ledger/block/100' because block not found: 100")
	require.Nil(t, header)
}

func TestGetLastBlockHeader(t *testing.T) {
	clientCertTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTempDir)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTempDir, "alice")

	l, err := aliceSession.Ledger()

	for i := 1; i < 10; i++ {
		txReceipt, _, _ := putKeySync(t, "bdb", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "alice", aliceSession)
		header, err := l.GetLastBlockHeader()
		require.NoError(t, err)
		require.NotNil(t, header)
		require.True(t, proto.Equal(txReceipt.Header, header))
	}
}

func TestGetLedgerPath(t *testing.T) {
	clientCertTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTempDir)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTempDir, "alice")

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
			errMessage: "error handling request, server returned: status: 400 Bad Request, status code: 400, message: query error: startId=6 > endId=1",
		},
		{
			name:       "from 100 to 1 - error not found",
			start:      1,
			end:        100,
			path:       nil,
			errMessage: "error handling request, server returned: status: 404 Not Found, status code: 404, message: error while processing 'GET /ledger/path?start=1&end=100' because can't find path in blocks skip list between 100 1: block not found: 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := p.GetLedgerPath(tt.start, tt.end)
			if tt.errMessage == "" {
				require.NoError(t, err)
				for i, b := range tt.path {
					require.True(t, proto.Equal(b, path.Path[i]), fmt.Sprintf("Expected block number %d, actual block number %d", b.GetBaseHeader().GetNumber(), path.Path[i].GetBaseHeader().GetNumber()))
				}
				begin, err := p.GetBlockHeader(tt.start)
				require.NoError(t, err)
				end, err := p.GetBlockHeader(tt.end)
				require.NoError(t, err)
				verified, err := path.Verify(begin, end)
				require.NoError(t, err)
				require.True(t, verified)
				verified, err = path.Verify(nil, nil)
				require.NoError(t, err)
				require.True(t, verified)
				verified, err = path.Verify(end, begin)
				require.Error(t, err)
				require.False(t, verified)
			} else {
				require.EqualError(t, err, tt.errMessage)
			}
		})
	}
}

func TestGetTransactionProof(t *testing.T) {
	clientCertTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTempDir, 5*time.Second, 10, false, false)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTempDir, "alice")

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
			errMessage: "error handling request, server returned: status: 404 Not Found, status code: 404, message: error while processing 'GET /ledger/proof/tx/15?idx=0' because requested block number [15] cannot be greater than the last committed block number [12]",
		},
		{
			name:       "block 10, tx 30, tx not exist in block",
			block:      10,
			txIdx:      30,
			errMessage: "error handling request, server returned: status: 404 Not Found, status code: 404, message: error while processing 'GET /ledger/proof/tx/10?idx=30' because node with index 30 is not part of merkle tree (0, 9)",
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
	clientCertTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTempDir, 5*time.Second, 10, false, false)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTempDir, "alice")

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
			errMessage: "error handling request, server returned: status: 404 Not Found, status code: 404, message: error while processing 'GET /ledger/tx/receipt/not_exist' because TxID not found: not_exist",
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

func TestGetStateProof(t *testing.T) {
	clientCertTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTempDir, 20*time.Second, 10, false, false)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTempDir, "alice")

	txEnvelopesPerBlock := make([][]proto.Message, 0)

	// Ten blocks, each 10 tx
	for i := 0; i < 10; i++ {
		keys := make([]string, 0)
		values := make([]string, 0)
		for j := 0; j < 10; j++ {
			keys = append(keys, fmt.Sprintf("key%d_%d", i, j))
			values = append(values, fmt.Sprintf("value%d_%d", i, j))
		}
		blockTx := putMultipleKeysAndValues(t, keys, values, "alice", aliceSession)
		txEnvelopesPerBlock = append(txEnvelopesPerBlock, blockTx)
	}

	tests := []struct {
		name       string
		block      uint64
		dbName     string
		key        string
		value      []byte
		isDeleted  bool
		incorrect  bool
		errMessage string
	}{
		{
			name:      "block 3, key0_0",
			block:     3,
			dbName:    "bdb",
			key:       "key0_0",
			value:     []byte("value0_0"),
			isDeleted: false,
		},
		{
			name:      "block 3, key0_0, incorrect value",
			block:     3,
			dbName:    "bdb",
			key:       "key0_0",
			value:     []byte("value0_2"),
			isDeleted: false,
			incorrect: true,
		},
		{
			name:       "block 3, key6_1, not created yet",
			block:      3,
			dbName:     "bdb",
			key:        "key6_1",
			value:      []byte("value6_1"),
			isDeleted:  false,
			errMessage: "because no proof for block 3, db bdb, key key6_1, isDeleted false found",
		},
		{
			name:      "block 9, key6_1",
			block:     9,
			dbName:    "bdb",
			key:       "key6_1",
			value:     []byte("value6_1"),
			isDeleted: false,
		},
		{
			name:       "block 19, key6_1, block not exist",
			block:      19,
			dbName:     "bdb",
			key:        "key6_1",
			value:      []byte("value6_1"),
			isDeleted:  false,
			errMessage: "404 Not Found, status code: 404, message: error while processing 'GET /ledger/proof/data/bdb/key6_1?block=19' because block not found: 19",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := aliceSession.Ledger()
			require.NoError(t, err)
			proof, err := p.GetDataProof(tt.block, tt.dbName, tt.key, tt.isDeleted)
			if tt.errMessage == "" {
				require.NoError(t, err)
				kvHash, err := CalculateValueHash(tt.dbName, tt.key, tt.value)
				require.NoError(t, err)
				blockHeader, err := p.GetBlockHeader(tt.block)
				require.NoError(t, err)
				res, err := proof.Verify(kvHash, blockHeader.GetStateMerkelTreeRootHash(), tt.isDeleted)
				require.NoError(t, err)
				if tt.incorrect {
					require.False(t, res)
				} else {
					require.True(t, res)
				}
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMessage)
			}
		})
	}
}

func TestGetFullTxProofAndVerify(t *testing.T) {
	clientCertTempDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServerWithParams(t, clientCertTempDir, 20*time.Millisecond, 1, false, false)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTempDir, "alice")

	txEnvelopesPerBlock := make([]proto.Message, 0)
	txReceiptsPerBlock := make([]*types.TxReceipt, 0)

	// 20 blocks, each 1 tx
	for i := 0; i < 20; i++ {
		receipt, _, env := putKeySync(t, "bdb", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "alice", aliceSession)
		txEnvelopesPerBlock = append(txEnvelopesPerBlock, env)
		txReceiptsPerBlock = append(txReceiptsPerBlock, receipt)
	}

	p, err := aliceSession.Ledger()
	require.NoError(t, err)

	genesis, err := p.GetBlockHeader(GenesisBlockNumber)
	require.NoError(t, err)

	t.Run("TestVerifyFullTxProof_Valid", func(t *testing.T) {

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

		blockHeader, err = p.GetBlockHeader(18)
		require.NoError(t, err)
		txProof, path, err = p.GetFullTxProofAndVerify(txReceiptsPerBlock[10], blockHeader, txEnvelopesPerBlock[10])
		require.NoError(t, err)
		res, err = txProof.Verify(txReceiptsPerBlock[10], txEnvelopesPerBlock[10])
		require.NoError(t, err)
		require.True(t, res)
		res, err = path.Verify(genesis, blockHeader)
		require.NoError(t, err)
		require.True(t, res)
	})

	t.Run("TestVerifyFullTxProof tx in latest block", func(t *testing.T) {
		lastBlockReceipt := txReceiptsPerBlock[len(txReceiptsPerBlock)-1]
		lastBlockTx := txEnvelopesPerBlock[len(txReceiptsPerBlock)-1]
		blockHeader, err := p.GetBlockHeader(lastBlockReceipt.GetHeader().GetBaseHeader().GetNumber())
		require.NoError(t, err)
		txProof, path, err := p.GetFullTxProofAndVerify(lastBlockReceipt, blockHeader, lastBlockTx)
		require.NoError(t, err)
		res, err := txProof.Verify(lastBlockReceipt, lastBlockTx)
		require.NoError(t, err)
		require.True(t, res)
		res, err = path.Verify(genesis, blockHeader)
		require.NoError(t, err)
		require.True(t, res)
	})

	t.Run("TestVerifyFullTxProof tx in block after latest", func(t *testing.T) {
		lastBlockReceipt := txReceiptsPerBlock[len(txReceiptsPerBlock)-1]
		lastBlockTx := txEnvelopesPerBlock[len(txReceiptsPerBlock)-1]
		txReceiptsPerBlock[len(txReceiptsPerBlock)-1].GetHeader().GetBaseHeader().GetNumber()
		blockHeader, err := p.GetBlockHeader(lastBlockReceipt.GetHeader().GetBaseHeader().GetNumber() - 1)
		require.NoError(t, err)
		_, _, err = p.GetFullTxProofAndVerify(lastBlockReceipt, blockHeader, lastBlockTx)
		require.Error(t, err)
		require.Equal(t, "something wrong with blocks order: genesis: 1, tx block header 22, last know block header: 21", err.Error())
	})

	t.Run("TestVerifyFullTxProof_TamperedEnvelop", func(t *testing.T) {
		blockHeader, err := p.GetBlockHeader(10)
		require.NoError(t, err)
		txProof, path, err := p.GetFullTxProofAndVerify(txReceiptsPerBlock[4], blockHeader, txEnvelopesPerBlock[6])
		require.Error(t, err)
		require.Nil(t, txProof)
		require.Nil(t, path)
	})

	t.Run("TestVerifyFullTxProof_TamperedReceipt", func(t *testing.T) {
		blockHeader, err := p.GetBlockHeader(10)
		require.NoError(t, err)
		receipt := txReceiptsPerBlock[3]
		orgReceipt := receipt
		receipt = proto.Clone(orgReceipt).(*types.TxReceipt)
		receipt.Header.SkipchainHashes[0][0] = receipt.Header.SkipchainHashes[0][0] + 1
		txProof, path, err := p.GetFullTxProofAndVerify(receipt, blockHeader, txEnvelopesPerBlock[3])
		require.Error(t, err)
		require.Nil(t, txProof)
		require.Nil(t, path)
	})
}
