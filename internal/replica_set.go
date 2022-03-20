// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"fmt"
	"net/url"
	"sort"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

type ReplicaRole int32

const (
	ReplicaRole_LEADER   ReplicaRole = 0
	ReplicaRole_FOLLOWER ReplicaRole = 1
	ReplicaRole_UNKNOWN  ReplicaRole = 2
)

var ReplicaRoleName = map[int32]string{
	0: "LEADER",
	1: "FOLLOWER",
	2: "UNKNOWN",
}

var ReplicaRoleValue = map[string]int32{
	"LEADER":   0,
	"FOLLOWER": 1,
	"UNKNOWN":  2,
}

type ReplicaWithRole struct {
	Id   string
	URL  *url.URL
	Role ReplicaRole
}

func (r *ReplicaWithRole) String() string {
	stateName, _ := ReplicaRoleName[int32(r.Role)]
	return fmt.Sprintf("Id: %s, Role: %s, URL: %s", r.Id, stateName, r.URL.String())
}

type ReplicaSet []*ReplicaWithRole

// SortByRole sort the replicas such that the leader is first, then followers, then unknown.
func (r ReplicaSet) SortByRole() {
	if r == nil {
		return
	}
	sort.Slice(r, func(i, j int) bool { return r[i].Role < r[j].Role })
}

// ToConfigReplicaSet returns an array of config.Replica objects that corresponds the ReplicaSet.
func (r ReplicaSet) ToConfigReplicaSet() []*config.Replica {
	if r == nil {
		return nil
	}

	var configReplicaSet []*config.Replica
	for _, v := range r {
		configReplicaSet = append(configReplicaSet, &config.Replica{
			ID:       v.Id,
			Endpoint: v.URL.String(),
		})
	}

	return configReplicaSet
}

// ToReplicaMap returns map of ID->URL that corresponds the ReplicaSet.
func (r ReplicaSet) ToReplicaMap() map[string]*url.URL {
	replicaMap := make(map[string]*url.URL)

	if r == nil {
		return nil
	}

	for _, v := range r {
		replicaMap[v.Id] = v.URL
	}

	return replicaMap
}

// ClusterStatusToReplicaSet creates a sorted array of ReplicaWithRole objects, leader first.
func ClusterStatusToReplicaSet(clusterStatus *types.GetClusterStatusResponse, tlsEnabled bool) (ReplicaSet, error) {
	if clusterStatus == nil {
		return nil, errors.New("ClusterStatus is nil")
	}

	var replicas ReplicaSet

	urlPattern := "http://%s:%d"
	if tlsEnabled {
		urlPattern = "https://%s:%d"
	}
	for _, node := range clusterStatus.Nodes {
		parsedURL, err := url.ParseRequestURI(fmt.Sprintf(urlPattern, node.Address, node.Port))
		if err != nil {
			return nil, err
		}
		r := &ReplicaWithRole{
			Id:   node.Id,
			URL:  parsedURL,
			Role: ReplicaRole_UNKNOWN,
		}

		if node.Id == clusterStatus.GetLeader() {
			r.Role = ReplicaRole_LEADER
		} else {
			for _, active := range clusterStatus.GetActive() {
				if node.Id == active {
					r.Role = ReplicaRole_FOLLOWER
					break
				}
			}
		}

		replicas = append(replicas, r)
	}

	replicas.SortByRole()

	return replicas, nil
}
