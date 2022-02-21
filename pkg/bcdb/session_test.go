// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"fmt"
	"io/ioutil"
	"path"
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
