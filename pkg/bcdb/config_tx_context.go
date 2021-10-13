// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// ConfigTxContext transaction context to operate with
// configuration management related transactions.
// Add, delete and update an admin record; Add, delete and update a cluster node config.
type ConfigTxContext interface {
	// Embed general abstraction.
	TxContext

	// AddAdmin add admin record.
	AddAdmin(admin *types.Admin) error

	// DeleteAdmin delete admin record.
	DeleteAdmin(adminID string) error

	// UpdateAdmin update admin record.
	UpdateAdmin(admin *types.Admin) error

	// AddClusterNode add cluster node record.
	AddClusterNode(node *types.NodeConfig, peer *types.PeerConfig) error

	// DeleteClusterNode delete cluster node record.
	DeleteClusterNode(nodeID string) error

	// UpdateClusterNode Update cluster node record.
	UpdateClusterNode(node *types.NodeConfig, peer *types.PeerConfig) error

	// GetClusterConfig returns the current cluster config.
	// A ConfigTxContext only gets the current config once, subsequent calls return a cached value.
	// The value returned is a deep clone of the cached value and can be manipulated.
	GetClusterConfig() (*types.ClusterConfig, error)
}

type configTxContext struct {
	*commonTxContext
	oldConfig            *types.ClusterConfig
	readOldConfigVersion *types.Version
	newConfig            *types.ClusterConfig
}

func (c *configTxContext) Commit(sync bool) (string, *types.TxReceipt, error) {
	return c.commit(c, constants.PostConfigTx, sync)
}

func (c *configTxContext) Abort() error {
	return c.abort(c)
}

func (c *configTxContext) AddAdmin(admin *types.Admin) (err error) {
	if c.txSpent {
		return ErrTxSpent
	}

	if exist, _ := AdminExists(admin.Id, c.oldConfig.Admins); exist {
		return errors.Errorf("admin already exists in current config: %s", admin.Id)
	}

	if c.newConfig == nil {
		c.newConfig = proto.Clone(c.oldConfig).(*types.ClusterConfig)
	} else if exist, _ := AdminExists(admin.Id, c.newConfig.Admins); exist {
		return errors.Errorf("admin already exists in pending config: %s", admin.Id)
	}

	c.newConfig.Admins = append(c.newConfig.Admins, admin)

	c.logger.Debugf("Added admin: %v", admin)

	return nil
}

func (c *configTxContext) DeleteAdmin(adminID string) (err error) {
	if c.txSpent {
		return ErrTxSpent
	}

	if exist, _ := AdminExists(adminID, c.oldConfig.Admins); !exist {
		return errors.Errorf("admin does not exist in current config: %s", adminID)
	}

	if c.newConfig == nil {
		c.newConfig = proto.Clone(c.oldConfig).(*types.ClusterConfig)
	}

	var newAdmins []*types.Admin
	for _, existingAdmin := range c.newConfig.Admins {
		if existingAdmin.Id != adminID {
			newAdmins = append(newAdmins, existingAdmin)
		}
	}

	if len(c.newConfig.Admins) == len(newAdmins) {
		return errors.Errorf("admin does not exist in pending config: %s", adminID)
	}
	c.newConfig.Admins = newAdmins

	c.logger.Debugf("Removed admin: %s", adminID)

	return nil
}

func (c *configTxContext) UpdateAdmin(admin *types.Admin) (err error) {
	if c.txSpent {
		return ErrTxSpent
	}

	if exist, _ := AdminExists(admin.Id, c.oldConfig.Admins); !exist {
		return errors.Errorf("admin does not exist in current config: %s", admin.Id)
	}

	if c.newConfig == nil {
		c.newConfig = proto.Clone(c.oldConfig).(*types.ClusterConfig)
	}

	found, index := AdminExists(admin.Id, c.newConfig.Admins)
	if !found {
		return errors.Errorf("admin does not exist in pending config: %s", admin.Id)
	}
	c.newConfig.Admins[index] = admin

	c.logger.Debugf("Updated admin: %v", admin)

	return nil
}

func (c *configTxContext) AddClusterNode(node *types.NodeConfig, peer *types.PeerConfig) (err error) {
	if c.txSpent {
		return ErrTxSpent
	}

	if node.Id != peer.NodeId {
		return errors.Errorf("node.Id [%s] does not match peer.NodeId [%s]", node.Id, peer.NodeId)
	}

	if exist, _ := NodeExists(node.Id, c.oldConfig.Nodes); exist {
		return errors.Errorf("node already exists in current config: %s", node.Id)
	}

	if exist, _ := PeerExists(peer.NodeId, c.oldConfig.ConsensusConfig.Members); exist {
		return errors.Errorf("peer already exists in current config: %s", peer.NodeId)
	}

	if c.newConfig == nil {
		c.newConfig = proto.Clone(c.oldConfig).(*types.ClusterConfig)
	} else {
		if exist, _ := NodeExists(node.Id, c.newConfig.Nodes); exist {
			return errors.Errorf("node already added: %s", node.Id)
		}
		if exist, _ := PeerExists(node.Id, c.newConfig.ConsensusConfig.Members); exist {
			return errors.Errorf("peer already added: %s", node.Id)
		}
	}

	c.newConfig.Nodes = append(c.newConfig.Nodes, node)
	c.newConfig.ConsensusConfig.Members = append(c.newConfig.ConsensusConfig.Members, peer)

	c.logger.Debugf("Added node: %v", node)

	return nil
}

func (c *configTxContext) DeleteClusterNode(nodeID string) (err error) {
	if c.txSpent {
		return ErrTxSpent
	}

	if exist, _ := NodeExists(nodeID, c.oldConfig.Nodes); !exist {
		return errors.Errorf("node does not exist in current config: %s", nodeID)
	}

	if exist, _ := PeerExists(nodeID, c.oldConfig.ConsensusConfig.Members); !exist {
		return errors.Errorf("peer does not exist in current config: %s", nodeID)
	}

	if len(c.oldConfig.Nodes) == 1 {
		return errors.Errorf("cannot remove the last node in the cluster: %s", nodeID)
	}

	if c.newConfig == nil {
		c.newConfig = proto.Clone(c.oldConfig).(*types.ClusterConfig)
	}

	var newNodes []*types.NodeConfig
	for _, existingNode := range c.newConfig.Nodes {
		if existingNode.Id != nodeID {
			newNodes = append(newNodes, existingNode)
		}
	}

	var newPeers []*types.PeerConfig
	for _, existingPeer := range c.newConfig.ConsensusConfig.Members {
		if existingPeer.NodeId != nodeID {
			newPeers = append(newPeers, existingPeer)
		}
	}

	if len(c.newConfig.Nodes) == len(newNodes) {
		return errors.Errorf("node already removed: %s", nodeID)
	}
	c.newConfig.Nodes = newNodes
	c.newConfig.ConsensusConfig.Members = newPeers

	c.logger.Debugf("Removed node: %v", nodeID)

	return nil
}

func (c *configTxContext) UpdateClusterNode(node *types.NodeConfig, peer *types.PeerConfig) (err error) {
	if c.txSpent {
		return ErrTxSpent
	}

	if node.Id != peer.NodeId {
		return errors.Errorf("node.ID [%s] does not match peer.NodeId [%s]", node.Id, peer.NodeId)
	}

	if exist, _ := NodeExists(node.Id, c.oldConfig.Nodes); !exist {
		return errors.Errorf("node does not exist in current config: %s", node.Id)
	}

	if exist, _ := PeerExists(node.Id, c.oldConfig.ConsensusConfig.Members); !exist {
		return errors.Errorf("peer does not exist in current config: %s", node.Id)
	}

	if c.newConfig == nil {
		c.newConfig = proto.Clone(c.oldConfig).(*types.ClusterConfig)
	}

	found, nIndex := NodeExists(node.Id, c.newConfig.Nodes)
	if !found {
		return errors.Errorf("node does not exist in pending config: %s", node.Id)
	}
	c.newConfig.Nodes[nIndex] = node
	found, pIndex := PeerExists(node.Id, c.newConfig.ConsensusConfig.Members)
	if !found {
		return errors.Errorf("peer does not exist in pending config: %s", node.Id)
	}
	c.newConfig.ConsensusConfig.Members[pIndex] = peer

	c.logger.Debugf("Updated: node: %+v, peer: %+v", node, peer)

	return nil
}

func (c *configTxContext) GetClusterConfig() (*types.ClusterConfig, error) {
	if c.txSpent {
		return nil, ErrTxSpent
	}
	// deep clone
	return proto.Clone(c.oldConfig).(*types.ClusterConfig), nil
}

func (c *configTxContext) queryClusterConfig() error {
	if c.oldConfig != nil {
		return nil
	}

	configResponseEnv := &types.GetConfigResponseEnvelope{}
	path := constants.URLForGetConfig()
	err := c.handleRequest(
		path,
		&types.GetConfigQuery{
			UserId: c.userID,
		},
		configResponseEnv,
	)
	if err != nil {
		c.logger.Errorf("failed to execute cluster config query path %s, due to %s", path, err)
		return err
	}

	// TODO need to validate signature in the response envelope
	// configResponseEnv.Signature - the signature over response
	// configResponseEnv.GetResponse().GetHeader().NodeID - the id of the node signed response

	confResp := configResponseEnv.GetResponse()
	c.oldConfig = confResp.GetConfig()
	c.readOldConfigVersion = confResp.GetMetadata().GetVersion()

	return nil
}

func (c *configTxContext) composeEnvelope(txID string) (proto.Message, error) {
	payload := &types.ConfigTx{
		UserId:               c.userID,
		TxId:                 txID,
		ReadOldConfigVersion: c.readOldConfigVersion,
		NewConfig:            c.newConfig,
	}

	signature, err := cryptoservice.SignTx(c.signer, payload)
	if err != nil {
		return nil, err
	}

	return &types.ConfigTxEnvelope{
		Payload:   payload,
		Signature: signature,
	}, nil
}

func (c *configTxContext) cleanCtx() {
	c.oldConfig = nil
	c.readOldConfigVersion = nil
	c.newConfig = nil
}

func NodeExists(nodeID string, nodeSet []*types.NodeConfig) (bool, int) {
	for index, existingNode := range nodeSet {
		if existingNode.Id == nodeID {
			return true, index
		}
	}
	return false, -1
}

func PeerExists(nodeID string, peerSet []*types.PeerConfig) (bool, int) {
	for index, existingPeer := range peerSet {
		if existingPeer.NodeId == nodeID {
			return true, index
		}
	}
	return false, -1
}

func AdminExists(adminID string, adminSet []*types.Admin) (bool, int) {
	for index, existingAdmin := range adminSet {
		if existingAdmin.Id == adminID {
			return true, index
		}
	}
	return false, -1
}
