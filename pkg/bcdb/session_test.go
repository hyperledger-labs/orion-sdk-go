// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/internal/test"
	"io/ioutil"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-sdk-go/internal"
	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

// Scenario: replica set gets updated when a new session is created.
// - start a 3 node cluster
// - db instance with a partial bootstrap
// - create a session
// - db instance with a full bootstrap
// - creat a session
// - shutdown leader
// - create a session
func TestDbSession_UpdateReplicaSet(t *testing.T) {
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
	leader := -1
	require.Eventually(t, func() bool {
		leader = c.AgreedLeader(t, 0, 1, 2)
		return leader >= 0
	}, 30*time.Second, time.Second)

	// With a partial bootstrap replica set
	connConfig := &sdkconfig.ConnectionConfig{
		RootCAs: []string{path.Join(setupConfig.TestDirAbsolutePath, "ca", testutils.RootCAFileName+".pem")},
		ReplicaSet: []*sdkconfig.Replica{
			{
				ID:       c.Servers[0].ID(),
				Endpoint: c.Servers[0].URL(),
			},
		},
	}

	bcdb, err := Create(connConfig)
	require.NoError(t, err)
	require.NotNil(t, bcdb)

	session := openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))
	sessionImpl := session.(*dbSession)
	require.Len(t, sessionImpl.replicaSet, 3)
	require.Equal(t, internal.ReplicaRole_LEADER, sessionImpl.replicaSet[0].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[1].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[2].Role)

	// With a full bootstrap replica set
	connConfig = &sdkconfig.ConnectionConfig{
		RootCAs: []string{path.Join(setupConfig.TestDirAbsolutePath, "ca", testutils.RootCAFileName+".pem")},
		ReplicaSet: []*sdkconfig.Replica{
			{
				ID:       c.Servers[0].ID(),
				Endpoint: c.Servers[0].URL(),
			},
			{
				ID:       c.Servers[1].ID(),
				Endpoint: c.Servers[1].URL(),
			},
			{
				ID:       c.Servers[2].ID(),
				Endpoint: c.Servers[2].URL(),
			},
		},
	}

	bcdb, err = Create(connConfig)
	require.NoError(t, err)
	require.NotNil(t, bcdb)

	session = openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))
	sessionImpl = session.(*dbSession)
	require.Len(t, sessionImpl.replicaSet, 3)
	require.Equal(t, internal.ReplicaRole_LEADER, sessionImpl.replicaSet[0].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[1].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[2].Role)

	// Replica set updates on a new session with status of cluster - change of leader
	require.NoError(t, c.ShutdownServer(c.Servers[leader]))
	newLeader := -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, (leader+1)%3, (leader+2)%3)
		return (newLeader >= 0) && (newLeader != leader)
	}, 30*time.Second, time.Second)

	session = openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))
	sessionImpl = session.(*dbSession)
	require.Len(t, sessionImpl.replicaSet, 3)
	require.Equal(t, internal.ReplicaRole_LEADER, sessionImpl.replicaSet[0].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[1].Role)
	require.Equal(t, internal.ReplicaRole_UNKNOWN, sessionImpl.replicaSet[2].Role)
}

func TestDbSession_compareVersion(t *testing.T) {
	type testCase struct {
		x        *types.Version
		y        *types.Version
		expected int
	}

	for i, tc := range []testCase{
		{
			x:        nil,
			y:        nil,
			expected: 0,
		},
		{
			x:        nil,
			y:        &types.Version{},
			expected: 0,
		},
		{
			x:        &types.Version{},
			y:        &types.Version{},
			expected: 0,
		},
		{
			x:        &types.Version{BlockNum: 8, TxNum: 5},
			y:        &types.Version{BlockNum: 8, TxNum: 5},
			expected: 0,
		},
		{
			x:        &types.Version{BlockNum: 9, TxNum: 4},
			y:        &types.Version{BlockNum: 8, TxNum: 5},
			expected: 1,
		},
		{
			x:        &types.Version{BlockNum: 9, TxNum: 5},
			y:        &types.Version{BlockNum: 8, TxNum: 5},
			expected: 1,
		},
		{
			x:        &types.Version{BlockNum: 9, TxNum: 6},
			y:        &types.Version{BlockNum: 8, TxNum: 5},
			expected: 1,
		},
		{
			x:        &types.Version{BlockNum: 8, TxNum: 6},
			y:        &types.Version{BlockNum: 8, TxNum: 5},
			expected: 1,
		},
	} {
		t.Run(fmt.Sprintf("test case %d", i), func(t *testing.T) {
			require.Equal(t, tc.expected, compareVersion(tc.x, tc.y))
			require.Equal(t, -1*tc.expected, compareVersion(tc.y, tc.x))
		})
	}
}

// Scenario:
// - Start a 3 node cluster
// - Shutdown the leader and wait for a new leader to be chosen
// - Start original leader again
// - Commit DataTx
// - Commit another DataTx and check that replicaSet was updated
func TestRedirectionSucceed(t *testing.T) {
	// start a 3-node cluster
	dir, err := ioutil.TempDir("", "cluster-test")
	require.NoError(t, err)

	nPort, pPort := test.GetPorts()
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
	// wait for leader
	originalLeader := -1
	require.Eventually(t, func() bool {
		originalLeader = c.AgreedLeader(t, 0, 1, 2)
		return originalLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

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

	// create blockchainDB instance and open admin session for future txs to be sent
	bcdb, err := Create(connConfig)
	require.NoError(t, err)
	require.NotNil(t, bcdb)

	session := openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))
	sessionImpl := session.(*dbSession)
	require.Len(t, sessionImpl.replicaSet, 3)
	require.Equal(t, internal.ReplicaRole_LEADER, sessionImpl.replicaSet[0].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[1].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[2].Role)
	require.False(t, sessionImpl.updateReplicaSetFlag)

	// shut down the leader and wait for a new leader
	require.NoError(t, c.ShutdownServer(c.Servers[originalLeader]))
	newLeader := -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, (originalLeader+1)%3, (originalLeader+2)%3)
		return (newLeader >= 0) && (newLeader != originalLeader)
	}, 30*time.Second, 100*time.Millisecond)

	// check that replica set includes a leader, a follower and an unknown node as the last one is down
	session2 := openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))
	sessionImpl2 := session2.(*dbSession)
	require.Len(t, sessionImpl2.replicaSet, 3)
	require.Equal(t, internal.ReplicaRole_LEADER, sessionImpl2.replicaSet[0].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl2.replicaSet[1].Role)
	require.Equal(t, internal.ReplicaRole_UNKNOWN, sessionImpl2.replicaSet[2].Role)
	require.False(t, sessionImpl2.updateReplicaSetFlag)

	// start original leader again, which is now a follower
	require.NoError(t, c.StartServer(c.Servers[originalLeader]))

	// check that replica set is updated explicitly
	require.Eventually(t, func() bool {
		session2.ReplicaSet(true)
		return len(sessionImpl2.replicaSet) == 3 &&
			internal.ReplicaRole_LEADER == sessionImpl2.replicaSet[0].Role &&
			internal.ReplicaRole_FOLLOWER == sessionImpl2.replicaSet[1].Role &&
			internal.ReplicaRole_FOLLOWER == sessionImpl2.replicaSet[2].Role &&
			sessionImpl2.updateReplicaSetFlag == false
	}, 30*time.Second, 100*time.Millisecond)

	// prepare a data tx
	tx, err := session.DataTx()
	require.NoError(t, err)
	err = tx.Put("bdb", "key1", []byte("val1"), &types.AccessControl{
		ReadUsers: map[string]bool{
			"admin": true,
		},
		ReadWriteUsers: map[string]bool{
			"admin": true,
		},
		SignPolicyForWrite: 0,
	})
	require.NoError(t, err)
	require.False(t, sessionImpl.updateReplicaSetFlag)

	// commit tx
	// commit should cause a redirection response, means commit succeeds and updateReplicaSetFlag becomes true
	_, _, err = tx.Commit(true)
	require.NoError(t, err)
	require.True(t, sessionImpl.updateReplicaSetFlag)

	// check commit succeeded and data record exists in db
	tx, err = session.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx)
	value, _, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("val1"), value)

	// commit another Tx - lead to update of replicaSet and updateReplicaSetFlag become false again
	tx, err = session.DataTx()
	require.NoError(t, err)
	err = tx.Put("bdb", "key2", []byte("val2"), nil)
	require.NoError(t, err)

	require.False(t, sessionImpl.updateReplicaSetFlag)
	newLeaderID := c.Servers[newLeader].ID()
	require.Equal(t, session.(*dbSession).replicaSet[0].Id, newLeaderID)

	_, _, err = tx.Commit(false)
	require.NoError(t, err)
}

func TestRetryMechanismAfterLeaderShutDown(t *testing.T) {
	// start a 3-node cluster
	dir, err := ioutil.TempDir("", "cluster-test")
	require.NoError(t, err)

	nPort, pPort := test.GetPorts()
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
	// wait for leader
	originalLeader := -1
	require.Eventually(t, func() bool {
		originalLeader = c.AgreedLeader(t, 0, 1, 2)
		return originalLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

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

	// create blockchainDB instance and open admin session for future txs to be sent
	bcdb, err := Create(connConfig)
	require.NoError(t, err)
	require.NotNil(t, bcdb)

	session := openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))
	sessionImpl := session.(*dbSession)
	require.Len(t, sessionImpl.replicaSet, 3)
	require.Equal(t, internal.ReplicaRole_LEADER, sessionImpl.replicaSet[0].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[1].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[2].Role)

	// shut down the leader
	require.NoError(t, c.ShutdownServer(c.Servers[originalLeader]))

	// wait for a new leader
	newLeader := -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, (originalLeader+1)%3, (originalLeader+2)%3)
		return (newLeader >= 0) && (newLeader != originalLeader)
	}, 30*time.Second, time.Second)

	// prepare a data tx
	tx, err := session.DataTx()
	require.NoError(t, err)
	err = tx.Put("bdb", "key2", []byte("val2"), &types.AccessControl{
		ReadUsers: map[string]bool{
			"admin": true,
		},
		ReadWriteUsers: map[string]bool{
			"admin": true,
		},
		SignPolicyForWrite: 0,
	})

	// commit tx, commit should succeed after a new leader was elected, after one retry as the first try is according to old replica set
	retriesTimoeoutConfig = 30 * time.Second
	_, _, err = tx.Commit(true)
	require.NoError(t, err)
}

func TestRetryMechanismShutDownSuchThatNoLeader(t *testing.T) {
	// start a 3-node cluster
	dir, err := ioutil.TempDir("", "cluster-test")
	require.NoError(t, err)

	nPort, pPort := test.GetPorts()
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
	// wait for leader
	originalLeader := -1
	require.Eventually(t, func() bool {
		originalLeader = c.AgreedLeader(t, 0, 1, 2)
		return originalLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

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

	// create blockchainDB instance and open admin session for future txs to be sent
	bcdb, err := Create(connConfig)
	require.NoError(t, err)
	require.NotNil(t, bcdb)

	session := openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))
	sessionImpl := session.(*dbSession)
	require.Len(t, sessionImpl.replicaSet, 3)
	require.Equal(t, internal.ReplicaRole_LEADER, sessionImpl.replicaSet[0].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[1].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[2].Role)

	// shut down the leader and one of the followers, so no leader
	require.NoError(t, c.ShutdownServer(c.Servers[originalLeader]))
	require.NoError(t, c.ShutdownServer(c.Servers[(originalLeader+1)%3]))

	// check that replica set includes a follower, and two unknown nodes
	session2 := openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))
	sessionImpl2 := session2.(*dbSession)
	session2.ReplicaSet(true)
	require.Len(t, sessionImpl2.replicaSet, 3)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl2.replicaSet[0].Role)
	require.Equal(t, internal.ReplicaRole_UNKNOWN, sessionImpl2.replicaSet[1].Role)
	require.Equal(t, internal.ReplicaRole_UNKNOWN, sessionImpl2.replicaSet[2].Role)

	// prepare a data tx
	tx, err := session.DataTx()
	require.NoError(t, err)
	err = tx.Put("bdb", "key2", []byte("val2"), &types.AccessControl{
		ReadUsers: map[string]bool{
			"admin": true,
		},
		ReadWriteUsers: map[string]bool{
			"admin": true,
		},
		SignPolicyForWrite: 0,
	})

	// commit tx, commit should fail as no leader, after some retries timeout will occur with server status: 503 Service Unavailable
	retriesTimoeoutConfig = 10 * time.Second
	_, _, err = tx.Commit(true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to submit transaction")
	require.Contains(t, err.Error(), "a timeout occured after "+retriesTimoeoutConfig.String())
	require.Contains(t, err.Error(), "Service Unavailable")
}

func TestRetryMechanismShutDownAndRecover(t *testing.T) {
	// start a 3-node cluster
	dir, err := ioutil.TempDir("", "cluster-test")
	require.NoError(t, err)
	nPort, pPort := test.GetPorts()
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

	// wait for leader
	originalLeader := -1
	require.Eventually(t, func() bool {
		originalLeader = c.AgreedLeader(t, 0, 1, 2)
		return originalLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)
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

	// create blockchainDB instance and open admin session for future txs to be sent
	bcdb, err := Create(connConfig)
	require.NoError(t, err)
	require.NotNil(t, bcdb)
	session := openUserSession(t, bcdb, "admin", path.Join(setupConfig.TestDirAbsolutePath, "users"))
	sessionImpl := session.(*dbSession)
	require.Len(t, sessionImpl.replicaSet, 3)
	require.Equal(t, internal.ReplicaRole_LEADER, sessionImpl.replicaSet[0].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[1].Role)
	require.Equal(t, internal.ReplicaRole_FOLLOWER, sessionImpl.replicaSet[2].Role)

	// shut down the leader and one of the followers, so no leader
	require.NoError(t, c.ShutdownServer(c.Servers[originalLeader]))
	require.NoError(t, c.ShutdownServer(c.Servers[(originalLeader+1)%3]))

	// prepare a data tx
	tx, err := session.DataTx()
	require.NoError(t, err)
	err = tx.Put("bdb", "key2", []byte("val2"), &types.AccessControl{
		ReadUsers: map[string]bool{
			"admin": true,
		},
		ReadWriteUsers: map[string]bool{
			"admin": true,
		},
		SignPolicyForWrite: 0,
	})

	retriesTimoeoutConfig = 30 * time.Second

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		// start one of the servers again, at some time a new leader will be chosen
		require.NoError(t, c.StartServer(c.Servers[originalLeader]))
	}()

	// commit transaction
	_, _, err = tx.Commit(true)

	wg.Wait()

	require.NoError(t, err)

	// validate tx submission
	tx, err = session.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx)
	val, meta, err := tx.Get("bdb", "key2")
	require.NoError(t, err)
	require.EqualValues(t, []byte("val2"), val)
	require.NotNil(t, meta)
}
