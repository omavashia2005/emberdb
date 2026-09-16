package clusters

import (
	"fmt"
	"strings"
	"time"
)

const CLUSTERMSG_GOSSIP_SIZE = 40 + 4 + 4 + 2 + 2 + 2

type clusterMsgDataGossip struct {
	nodeName     string
	cport        uint16
	port         uint16
	flags        uint16
	pingSent     uint32
	pongReceived uint32
}

func (g *clusterMsgDataGossip) GetNodeName() string             { return g.nodeName }
func (g *clusterMsgDataGossip) SetNodeName(name string)         { g.nodeName = name }
func (g *clusterMsgDataGossip) GetClusterBusPort() uint16       { return g.cport }
func (g *clusterMsgDataGossip) SetClusterBusPort(port uint16)   { g.cport = port }
func (g *clusterMsgDataGossip) GetClientPort() uint16           { return g.port }
func (g *clusterMsgDataGossip) SetClientPort(port uint16)       { g.port = port }
func (g *clusterMsgDataGossip) GetFlags() uint16                { return g.flags }
func (g *clusterMsgDataGossip) SetFlags(flags uint16)           { g.flags = flags }
func (g *clusterMsgDataGossip) GetPingSent() uint32             { return g.pingSent }
func (g *clusterMsgDataGossip) SetPingSent(sent uint32)         { g.pingSent = sent }
func (g *clusterMsgDataGossip) GetPongReceived() uint32         { return g.pongReceived }
func (g *clusterMsgDataGossip) SetPongReceived(received uint32) { g.pongReceived = received }

func clusterSetGossipEntry(hdr *clusterMsg, node NodeSnapshot) {
	gossip := &clusterMsgDataGossip{}
	gossip.SetNodeName(node.Name)

	if sent := node.PingSent; !sent.IsZero() {
		gossip.SetPingSent(uint32(sent.Unix()))
	}
	if received := node.PongReceived; !received.IsZero() {
		gossip.SetPongReceived(uint32(received.Unix()))
	}

	gossip.SetClientPort(uint16(node.TCPPort))
	gossip.SetClusterBusPort(uint16(node.ClusterBusPort))
	gossip.SetFlags(uint16(node.Flags))
	hdr.AddGossip(gossip)
}

func clusterProcessGossip(msg *clusterMsg) {
	for _, gossip := range msg.GetGossip() {
		// Don't learn ourselves through gossip.
		if gossip.GetNodeName() == serverState.GetSelf().GetName() {
			continue
		}

		node, exists := serverState.GetNode(gossip.GetNodeName())

		if !exists {
			node = NewNode(int(gossip.GetClientPort()), "", int(gossip.GetFlags()), false)
			node.UpdateGossip(
				gossip.GetNodeName(),
				int(gossip.GetClientPort()),
				int(gossip.GetClusterBusPort()),
				int(gossip.GetFlags()),
			)

			serverState.SetNode(node)
			continue
		}

		// Refresh our existing view of this node.
		node.UpdateGossip(
			gossip.GetNodeName(),
			int(gossip.GetClientPort()),
			int(gossip.GetClusterBusPort()),
			int(gossip.GetFlags()),
		)
	}
}

// using the gossip, update serverState
// send PONG via clusterSendPing()
func clusterProcessMsg(link *clusterLink, msg *clusterMsg) {
	node := link.GetNode()
	slots := msg.GetSlots()
	if node != nil {
		if strings.HasPrefix(node.GetName(), "temp_") {
			serverState.DeleteNode(node.GetName())
			node.UpdateMessage(
				msg.GetSender(),
				int(msg.GetClientPort()),
				int(msg.GetClusterBusPort()),
				int(msg.GetFlags()),
				slots,
			)
		} else {
			node.SetOwnedSlots(slots)
		}
	} else {
		node = NewNode(int(msg.GetClientPort()), "", int(msg.GetFlags()), false)
		node.UpdateMessage(
			msg.GetSender(),
			int(msg.GetClientPort()),
			int(msg.GetClusterBusPort()),
			int(msg.GetFlags()),
			slots,
		)
		link.SetNode(node)
	}

	serverState.SetNode(node)

	clusterProcessGossip(msg)

	switch msg.GetType() {
	case CLUSTERMSG_TYPE_PING:
		clusterSendPing(link, CLUSTERMSG_TYPE_PONG)
	case CLUSTERMSG_TYPE_PONG:
		if node != nil {
			node.MarkPongReceived(time.Now())
		}
	}

	for name, node := range serverState.GetNodes() {
		fmt.Printf(
			"  %s port=%d cport=%d\n",
			name,
			node.GetClientPort(),
			node.GetClusterBusPort(),
		)
	}

}
