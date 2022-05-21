package paxos

import (
	"github.com/AllenShaw19/paxos/log"
	"sync"
)

type Communicate struct {
	config   *Config
	netWork  NetWork
	myNodeID NodeID
}

func NewCommunicate(config *Config, netWork NetWork, myNodeID NodeID) *Communicate {
	return &Communicate{
		config:   config,
		netWork:  netWork,
		myNodeID: myNodeID,
	}
}

func (c *Communicate) SendMessage(groupIdx int, sendToNodeID NodeID, message string) error {
	return c.send(groupIdx, NewNodeInfo(sendToNodeID), message)
}

func (c *Communicate) BroadcastMessage(groupIdx int, message string) error {
	nodeInfos := c.config.GetSystemVSM().GetMembershipMap()

	var wg sync.WaitGroup

	for nodeID := range nodeInfos {
		if nodeID != c.myNodeID {
			wg.Add(1)
			go func(nodeID NodeID) {
				defer wg.Done()
				c.send(groupIdx, NewNodeInfo(nodeID), message)
			}(nodeID)
		}
	}

	wg.Wait()
	return nil
}

func (c *Communicate) BroadcastMessageFollower(groupIdx int, message string) error {
	followerNodeInfos := c.config.GetMyFollowerMap()

	var wg sync.WaitGroup

	for nodeID := range followerNodeInfos {
		if nodeID != c.myNodeID {
			wg.Add(1)
			go func(nodeID NodeID) {
				defer wg.Done()
				c.send(groupIdx, NewNodeInfo(nodeID), message)
			}(nodeID)
		}
	}

	wg.Wait()
	log.Info("node count", log.Int("node_count", len(followerNodeInfos)))
	return nil
}

func (c *Communicate) BroadcastMessageTempNode(groupIdx int, message string) error {
	tmpNodes := c.config.GetTmpNodeMap()
	var wg sync.WaitGroup

	for nodeID := range tmpNodes {
		if nodeID != c.myNodeID {
			wg.Add(1)
			go func(nodeID NodeID) {
				defer wg.Done()
				c.send(groupIdx, NewNodeInfo(nodeID), message)
			}(nodeID)
		}
	}
	wg.Wait()
	log.Info("node count", log.Int("node_count", len(tmpNodes)))
	return nil
}

func (c *Communicate) send(groupIdx int, nodeInfo *NodeInfo, message string) error {
	if len(message) > MaxValueSize() {
		log.Error("message size too large, skip message", log.Int("msg_size", len(message)),
			log.Int("max_size", MaxValueSize()))
		return ErrMsgTooLarge
	}

	return c.netWork.SendMessage(groupIdx, nodeInfo.ip, nodeInfo.port, message)
}
