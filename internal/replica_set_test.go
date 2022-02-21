// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestClusterStatusToReplicaSet(t *testing.T) {
	nodes := testNodes(5)

	t.Run("nil", func(t *testing.T) {
		r, err := ClusterStatusToReplicaSet(nil, false)
		require.EqualError(t, err, "ClusterStatus is nil")
		require.Len(t, r, 0)
		r.SortByRole() //does not throw
	})

	t.Run("empty", func(t *testing.T) {
		r, err := ClusterStatusToReplicaSet(&types.GetClusterStatusResponse{}, false)
		require.NoError(t, err)
		require.Len(t, r, 0)
		r.SortByRole() //does not throw
	})

	t.Run("no leader", func(t *testing.T) {
		clusterStatus := &types.GetClusterStatusResponse{
			Header:  nil,
			Nodes:   nodes,
			Version: &types.Version{BlockNum: 1, TxNum: 0},
			Leader:  "",
			Active:  []string{"node-3", "node-5"},
		}
		r, err := ClusterStatusToReplicaSet(clusterStatus, false)
		require.NoError(t, err)
		require.Len(t, r, 5)

		for i := 0; i < 5; i++ {
			switch i {
			case 0, 1:
				require.Equal(t, ReplicaRole_FOLLOWER, r[i].Role)
			default:
				require.Equal(t, ReplicaRole_UNKNOWN, r[i].Role)
			}
		}
	})

	t.Run("with leader", func(t *testing.T) {
		clusterStatus := &types.GetClusterStatusResponse{
			Header:  nil,
			Nodes:   nodes,
			Version: &types.Version{BlockNum: 1, TxNum: 0},
			Leader:  "node-1",
			Active:  []string{"node-1", "node-2", "node-3"},
		}
		r, err := ClusterStatusToReplicaSet(clusterStatus, false)
		require.NoError(t, err)
		require.Len(t, r, 5)

		for i := 0; i < 5; i++ {
			switch i {
			case 0:
				require.Equal(t, ReplicaRole_LEADER, r[i].Role)
			case 1, 2:
				require.Equal(t, ReplicaRole_FOLLOWER, r[i].Role)
			default:
				require.Equal(t, ReplicaRole_UNKNOWN, r[i].Role)
			}
		}
	})

	t.Run("with leader unordered", func(t *testing.T) {
		clusterStatus := &types.GetClusterStatusResponse{
			Header:  nil,
			Nodes:   nodes,
			Version: &types.Version{BlockNum: 1, TxNum: 0},
			Leader:  "node-5",
			Active:  []string{"node-1", "node-3", "node-5"},
		}
		r, err := ClusterStatusToReplicaSet(clusterStatus, false)
		require.NoError(t, err)
		require.Len(t, r, 5)

		for i := 0; i < 5; i++ {
			switch i {
			case 0:
				require.Equal(t, ReplicaRole_LEADER, r[i].Role)
			case 1, 2:
				require.Equal(t, ReplicaRole_FOLLOWER, r[i].Role)
			default:
				require.Equal(t, ReplicaRole_UNKNOWN, r[i].Role)
			}
		}
	})

	t.Run("bad url", func(t *testing.T) {
		clusterStatus := &types.GetClusterStatusResponse{
			Header:  nil,
			Nodes:   nodes,
			Version: &types.Version{BlockNum: 1, TxNum: 0},
			Leader:  "node-1",
			Active:  []string{"node-1", "node-2", "node-3"},
		}
		clusterStatus.Nodes[0].Address = "127.%JK.1" //bad
		r, err := ClusterStatusToReplicaSet(clusterStatus, false)
		require.EqualError(t, err, "parse \"http://127.%JK.1:6001\": invalid URL escape \"%JK\"")
		require.Nil(t, r)
	})
}

func TestReplicaSet_SortByRole(t *testing.T) {
	r := make(ReplicaSet, 5)
	r[0] = &ReplicaWithRole{Role: ReplicaRole_UNKNOWN}
	r[1] = &ReplicaWithRole{Role: ReplicaRole_FOLLOWER}
	r[2] = &ReplicaWithRole{Role: ReplicaRole_FOLLOWER}
	r[3] = &ReplicaWithRole{Role: ReplicaRole_FOLLOWER}
	r[4] = &ReplicaWithRole{Role: ReplicaRole_LEADER}

	r.SortByRole()

	for i := 0; i < 5; i++ {
		switch i {
		case 0:
			require.Equal(t, ReplicaRole_LEADER, r[i].Role)
		case 1, 2, 3:
			require.Equal(t, ReplicaRole_FOLLOWER, r[i].Role)
		default:
			require.Equal(t, ReplicaRole_UNKNOWN, r[i].Role)
		}
	}
}

func TestReplicaWithRole_String(t *testing.T) {
	url, _ := url.Parse("http://10.10.10.10:9999")
	r := &ReplicaWithRole{
		Id:   "node",
		URL:  url,
		Role: ReplicaRole_UNKNOWN,
	}
	require.Equal(t, "Id: node, Role: UNKNOWN, URL: http://10.10.10.10:9999", r.String())
	r.Role = ReplicaRole_FOLLOWER
	require.Equal(t, "Id: node, Role: FOLLOWER, URL: http://10.10.10.10:9999", r.String())
	r.Role = ReplicaRole_LEADER
	require.Equal(t, "Id: node, Role: LEADER, URL: http://10.10.10.10:9999", r.String())

}

func testNodes(num uint32) []*types.NodeConfig {
	var nodes []*types.NodeConfig
	for i := uint32(1); i <= num; i++ {
		nodes = append(nodes,
			&types.NodeConfig{
				Id:          fmt.Sprintf("node-%d", i),
				Address:     "10.10.10.10",
				Port:        6000 + i,
				Certificate: []byte("bogus"),
			},
		)
	}

	return nodes
}
