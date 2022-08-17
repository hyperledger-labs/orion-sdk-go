// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"net/http"
	"sync"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"google.golang.org/protobuf/proto"
)

// BlockHeaderDeliveryConfig holds the configuration of the
// delivery service
type BlockHeaderDeliveryConfig struct {
	// StartBlockNumber informs the service to start deliverying
	// block from this given block number
	StartBlockNumber uint64
	// RetryInterval denotes how long to wait before the retrying
	// the lastt failed retrieval of the block headerr
	RetryInterval time.Duration
	// Capacity denotes the maximum numberr of block headers to be
	// kept in the buffer
	Capacity int
	// IncludeTxIDs denotes whether the block header should include
	// transactions' ID or not
	IncludeTxIDs bool
}

// BlockHeaderDelivererService deliverys block header to the caller
type BlockHeaderDelivererService interface {
	// Receive returns
	//    - *types.BlockHeader if IncludeTxIDs is set to false in the delivery config
	//    - *types.AugmentedBlockHeader if IncludeTxIDs is set to true in the delivery config
	//    - nil if service has been stopped either by the caller or due to an error
	Receive() interface{}
	// Stop stops the delivery service
	Stop()
	// Error returns any accumulated error
	Error() error
}

// blockHeaderDeliverer delivers block header from a given starting
// block number to all the future block till the service is stopped
type blockHeaderDeliverer struct {
	// blockHeaders holds the retrieved header of each block till the
	// caller receives it.
	blockHeaders chan interface{}
	// conf holds the configuration of the delivery service
	conf      *BlockHeaderDeliveryConfig
	txContext *commonTxContext
	logger    *logger.SugarLogger

	// stop channel would be closed during error or
	// when the caller stop the delivery service
	stop chan struct{}
	// error is set when any occurs during the whole
	// lifecycle of delivery service
	err error
	// mu mutex is used to synchronize close on stop channel
	// and to read/write error
	mu sync.Mutex
}

func (d *blockHeaderDeliverer) start() {
	var resEnv proto.Message

	augmented := d.conf.IncludeTxIDs
	blockNum := d.conf.StartBlockNumber
	for {
		select {
		case <-d.stop:
			d.logger.Debug("stopping the delivery service")
			close(d.blockHeaders)
			return

		default:
			path := constants.URLForLedgerBlock(blockNum, augmented)

			if augmented {
				resEnv = &types.GetAugmentedBlockHeaderResponseEnvelope{}
			} else {
				resEnv = &types.GetBlockResponseEnvelope{}
			}

			err := d.txContext.handleRequest(
				path,
				&types.GetBlockQuery{
					UserId:      d.txContext.userID,
					BlockNumber: blockNum,
					Augmented:   augmented,
				},
				resEnv,
			)
			if err != nil {
				httpError, ok := err.(*httpError)
				if !ok || httpError.statusCode != http.StatusNotFound {
					d.logger.Errorf("failed to execute ledger block query %s, due to %s", path, err)
					d.setError(err)
					close(d.blockHeaders)
					return
				}

				d.logger.Debugf("requested block number %d not yet found. Retrying after "+d.conf.RetryInterval.String(), blockNum)

				select {
				case <-time.After(d.conf.RetryInterval):
					continue
				case <-d.stop:
					close(d.blockHeaders)
					return
				}
			}

			if augmented {
				d.blockHeaders <- resEnv.(*types.GetAugmentedBlockHeaderResponseEnvelope).GetResponse().GetBlockHeader()
			} else {
				d.blockHeaders <- resEnv.(*types.GetBlockResponseEnvelope).GetResponse().GetBlockHeader()
			}
			blockNum++
		}
	}
}

func (d *blockHeaderDeliverer) Receive() interface{} {
	return <-d.blockHeaders
}

func (d *blockHeaderDeliverer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()

	select {
	case <-d.stop:
		// already stopped
		return
	default:
		close(d.stop)
	}
}

func (d *blockHeaderDeliverer) Error() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.err
}

func (d *blockHeaderDeliverer) setError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.err = err
}
