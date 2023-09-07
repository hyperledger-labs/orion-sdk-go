// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"io/ioutil"
	"path"
	"testing"
	"time"

	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

// Start a 3-node cluster.
// Add, Update and Delete a 4th node on a 3-node cluster.
// This tests the SDK APIs, not the cluster itself, so we never actually start the 4th node.
func TestClusterConfig_ClusterNode(t *testing.T) {

	dir, err := ioutil.TempDir("", "cluster-test")
	require.NoError(t, err)

	nPort := uint32(6581)
	pPort := uint32(6681)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())

	require.Eventually(t, func() bool { return c.AgreedLeader(t, 0, 1, 2) >= 0 }, 30*time.Second, time.Second)

	connConfig := &sdkconfig.ConnectionConfig{
		RootCAs: []string{path.Join(setupConfig.TestDirAbsolutePath, "ca", testutils.RootCAFileName+".pem")},
		ReplicaSet: []*sdkconfig.Replica{
			{
				ID:       "node-1",
				Endpoint: c.Servers[0].URL(),
			},
			{
				ID:       "node-2",
				Endpoint: c.Servers[1].URL(),
			},
			{
				ID:       "node-3",
				Endpoint: c.Servers[2].URL(),
			},
		},
	}

	bcdb, err := Create(connConfig)
	require.NoError(t, err)
	require.NotNil(t, bcdb)

	t.Logf("Adding a 4th node")

	session := openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))

	tx, err := session.ConfigTx()
	require.NoError(t, err)
	require.NotNil(t, tx)

	clusterConfig, version, err := tx.GetClusterConfig()
	require.NoError(t, err)
	require.NotNil(t, clusterConfig)
	require.NotNil(t, version)

	require.Len(t, clusterConfig.GetNodes(), 3)
	require.Len(t, clusterConfig.GetConsensusConfig().GetMembers(), 3)

	require.NoError(t, err)
	node4 := &types.NodeConfig{
		Id:          "node-4",
		Address:     "127.0.0.1",
		Port:        nPort + 3,
		Certificate: clusterConfig.Nodes[0].Certificate,
	}
	peer4 := &types.PeerConfig{
		NodeId:   "node-4",
		RaftId:   4,
		PeerHost: "127.0.0.1",
		PeerPort: pPort + 3,
	}
	err = tx.AddClusterNode(node4, peer4)
	require.NoError(t, err)

	txID, receiptEnv, err := tx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, txID)
	require.NotNil(t, receiptEnv)
	require.Equal(t, types.Flag_VALID, receiptEnv.Response.Receipt.Header.ValidationInfo[receiptEnv.Response.Receipt.GetTxIndex()].Flag)

	// TODO wait for replication to finish, or use read concern leader
	c.AgreedHeight(t, 2, 0, 1, 2)
	require.Eventually(t,
		func() bool {
			tx, err = session.ConfigTx()
			require.NoError(t, err)
			clusterConfig, version, err = tx.GetClusterConfig()
			require.NoError(t, err)
			require.NotNil(t, version)

			if len(clusterConfig.GetNodes()) == 4 {
				return true
			}

			err = tx.Abort()
			require.NoError(t, err)
			return false
		},
		20*time.Second, time.Second,
	)

	found, index := NodeExists("node-4", clusterConfig.Nodes)
	require.True(t, found)
	require.Equal(t, clusterConfig.Nodes[index].Port, node4.Port)

	t.Logf("Updating the 4th node")

	node4.Address = "10.0.0.10"
	peer4.PeerHost = "10.0.0.10"
	err = tx.UpdateClusterNode(node4, peer4)
	require.NoError(t, err)

	txID, receiptEnv, err = tx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, txID)
	require.NotNil(t, receiptEnv)
	require.Equal(t, types.Flag_VALID, receiptEnv.Response.Receipt.Header.ValidationInfo[receiptEnv.Response.Receipt.GetTxIndex()].Flag)

	// TODO wait for replication to finish, or use read concern leader
	c.AgreedHeight(t, 3, 0, 1, 2)
	require.Eventually(t,
		func() bool {
			tx, err = session.ConfigTx()
			require.NoError(t, err)
			clusterConfig, version, err = tx.GetClusterConfig()
			require.NoError(t, err)
			require.NotNil(t, version)

			found, index = NodeExists("node-4", clusterConfig.Nodes)
			if found && clusterConfig.Nodes[index].Address == "10.0.0.10" {
				return true
			}

			err = tx.Abort()
			require.NoError(t, err)
			return false
		},
		20*time.Second, time.Second,
	)

	found, index = PeerExists("node-4", clusterConfig.ConsensusConfig.Members)
	require.Equal(t, clusterConfig.ConsensusConfig.Members[index].PeerHost, "10.0.0.10")

	t.Logf("Delete the 4th node")

	err = tx.DeleteClusterNode("node-4")
	require.NoError(t, err)

	txID, receiptEnv, err = tx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, txID)
	require.NotNil(t, receiptEnv)
	require.Equal(t, types.Flag_VALID, receiptEnv.Response.Receipt.Header.ValidationInfo[receiptEnv.Response.Receipt.GetTxIndex()].Flag)

	// TODO wait for replication to finish, or read concern leader
	c.AgreedHeight(t, 4, 0, 1, 2)
	require.Eventually(t,
		func() bool {
			tx, err = session.ConfigTx()
			require.NoError(t, err)
			clusterConfig, version, err = tx.GetClusterConfig()
			require.NoError(t, err)
			require.NotNil(t, version)

			if len(clusterConfig.GetNodes()) == 3 {
				return true
			}

			err = tx.Abort()
			require.NoError(t, err)
			return false
		},
		20*time.Second, time.Second,
	)

	found, _ = NodeExists("node-4", clusterConfig.Nodes)
	require.False(t, found)
	found, _ = PeerExists("node-4", clusterConfig.ConsensusConfig.Members)
	require.False(t, found)
}
