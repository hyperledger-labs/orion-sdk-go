// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

func TestBlockHeaderDeliveryService(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

	txReceipts := []*types.TxReceipt{}
	txIDs := []string{}
	totalDataBlock := 10
	firstdataBlockIndex := 0
	for i := 1; i <= 10; i++ {
		txReceipt, txID, _ := putKeySync(t, "bdb", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), "alice", aliceSession)
		txReceipts = append(txReceipts, txReceipt)
		txIDs = append(txIDs, txID)
		if firstdataBlockIndex == 0 {
			firstdataBlockIndex = int(txReceipt.Header.BaseHeader.Number)
		}
	}

	t.Run("successful run with augmented block header", func(t *testing.T) {
		l, err := aliceSession.Ledger()
		require.NoError(t, err)

		deliveryService := l.NewBlockHeaderDeliveryService(
			&BlockHeaderDeliveryConfig{
				StartBlockNumber: 1,
				RetryInterval:    1 * time.Second,
				Capacity:         2,
				IncludeTxIDs:     true,
			},
		)
		defer deliveryService.Stop()

		currentBlockNum := 1
		totalDataBlockProcessed := 0
		for {
			require.NoError(t, deliveryService.Error())
			b := deliveryService.Receive().(*types.AugmentedBlockHeader)
			require.Equal(t, uint64(currentBlockNum), b.GetHeader().GetBaseHeader().GetNumber())
			if currentBlockNum >= firstdataBlockIndex {
				index := currentBlockNum - firstdataBlockIndex
				require.True(t, proto.Equal(txReceipts[index].Header, b.GetHeader()))
				require.Equal(t, txIDs[index:index+1], b.GetTxIds())
				totalDataBlockProcessed++
			}

			if totalDataBlockProcessed == totalDataBlock {
				require.NoError(t, deliveryService.Error())
				deliveryService.Stop()
				break
			}

			currentBlockNum++
		}
	})

	t.Run("successful run with normal block header", func(t *testing.T) {
		l, err := aliceSession.Ledger()
		require.NoError(t, err)

		deliveryService := l.NewBlockHeaderDeliveryService(
			&BlockHeaderDeliveryConfig{
				StartBlockNumber: 1,
				RetryInterval:    1 * time.Second,
				Capacity:         2,
				IncludeTxIDs:     false,
			},
		)
		defer deliveryService.Stop()

		currentBlockNum := 1
		totalDataBlockProcessed := 0
		for {
			require.NoError(t, deliveryService.Error())
			b := deliveryService.Receive().(*types.BlockHeader)
			require.Equal(t, uint64(currentBlockNum), b.GetBaseHeader().GetNumber())
			if currentBlockNum >= firstdataBlockIndex {
				index := currentBlockNum - firstdataBlockIndex
				require.True(t, proto.Equal(txReceipts[index].Header, b))
				totalDataBlockProcessed++
			}

			if totalDataBlockProcessed == totalDataBlock {
				time.Sleep(2 * time.Second)
				require.NoError(t, deliveryService.Error())
				deliveryService.Stop()
				break
			}

			currentBlockNum++
		}
	})

	t.Run("stop the service while waiting on receive", func(t *testing.T) {
		l, err := aliceSession.Ledger()
		require.NoError(t, err)

		deliveryService := l.NewBlockHeaderDeliveryService(
			&BlockHeaderDeliveryConfig{
				StartBlockNumber: 1000, // future block
				RetryInterval:    1 * time.Second,
				Capacity:         2,
				IncludeTxIDs:     false,
			},
		)
		defer deliveryService.Stop()

		go func() {
			deliveryService.Stop()
		}()

		require.NoError(t, deliveryService.Error())
		require.Nil(t, deliveryService.Receive())
	})

	t.Run("test retry", func(t *testing.T) {
		l, err := aliceSession.Ledger()
		require.NoError(t, err)

		var retryCount int
		var wgRetry1 sync.WaitGroup
		wgRetry1.Add(1)
		var wgRetry2 sync.WaitGroup
		wgRetry2.Add(1)

		lg, err := logger.New(
			&logger.Config{
				Level:         "debug",
				OutputPath:    []string{"stdout"},
				ErrOutputPath: []string{"stderr"},
				Encoding:      "console",
			},
			zap.Hooks(func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, "Retrying") {
					t.Logf("Retry is working! %s | %s", entry.Caller, entry.Message)
					if retryCount == 0 {
						wgRetry1.Done()
					}
					if retryCount == 1 {
						wgRetry2.Done()
					}
					retryCount++
				}
				return nil
			}),
		)
		require.NoError(t, err)
		l.(*ledger).logger = lg

		deliveryService := l.NewBlockHeaderDeliveryService(
			&BlockHeaderDeliveryConfig{
				StartBlockNumber: 1000, // future block
				RetryInterval:    1 * time.Second,
				Capacity:         2,
				IncludeTxIDs:     false,
			},
		)
		defer deliveryService.Stop()

		wgRetry1.Wait()
		wgRetry2.Wait()
		require.NoError(t, deliveryService.Error())
	})

	t.Run("failed run", func(t *testing.T) {
		clientCertTemDir := testutils.GenerateTestCrypto(t, []string{"admin", "alice", "server"})
		testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
		require.NoError(t, err)
		_, _, aliceSession := startServerConnectOpenAdminCreateUserAndUserSession(t, testServer, clientCertTemDir, "alice")

		testServer.Stop()

		l, err := aliceSession.Ledger()
		require.NoError(t, err)

		deliveryService := l.NewBlockHeaderDeliveryService(
			&BlockHeaderDeliveryConfig{
				StartBlockNumber: 1,
				RetryInterval:    1 * time.Second,
				Capacity:         2,
				IncludeTxIDs:     false,
			},
		)

		// delivery service must result in an error and hence, the
		// receive should eventually return a nil
		require.Nil(t, deliveryService.Receive())
		require.Contains(t, deliveryService.Error().Error(), "connection refused")
		// we can call stop on delivery service multiple times
		deliveryService.Stop()
		deliveryService.Stop()
		// we can call receive on delivery service multiple times after
		// the service is closed. It would return only nil
		require.Nil(t, deliveryService.Receive())
		require.Nil(t, deliveryService.Receive())
	})
}
